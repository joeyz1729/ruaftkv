package shardkv

import (
	"bytes"
	"github.com/joeyz1729/ruaftkv/shardctrler"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joeyz1729/ruaftkv/labgob"
	"github.com/joeyz1729/ruaftkv/labrpc"
	"github.com/joeyz1729/ruaftkv/raft"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead           int32
	lastApplied    int
	shards         map[int]*MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]*LastOperationInfo
	currentConfig  shardctrler.Config
	prevConfig     shardctrler.Config // 用于shard migration
	mck            *shardctrler.Clerk
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 判断请求 key 是否属于当前 Group
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOperation,
		Data:    Op{Key: args.Key, OpType: OpGet},
	})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// matchGroup
func (kv *ShardKV) matchGroup(key string) bool {
	shard := key2shard(key)
	shardStatus := kv.shards[shard].Status
	return kv.currentConfig.Shards[shard] == kv.gid && (shardStatus == Normal || shardStatus == GC)
}

// requestDuplicated
func (kv *ShardKV) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	// 判断请求 key 是否所属当前 Group
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 判断请求是否重复
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		// 如果是重复请求，直接返回结果
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 调用 raft，将请求存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOperation,
		Data: Op{
			Key:      args.Key,
			Value:    args.Value,
			OpType:   getOperationType(args.Op),
			ClientId: args.ClientId,
			SeqId:    args.SeqId,
		},
	})

	// 如果不是 Leader 的话，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(RaftCommand{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationArgs{})
	labgob.Register(ShardOperationReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplied = 0
	kv.shards = make(map[int]*MemoryKVStateMachine)
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]*LastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.prevConfig = shardctrler.DefaultConfig()

	// 从 snapshot 中恢复状态
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	go kv.fetchConfigTask()
	go kv.shardMigrationTask()
	go kv.shardGCTask()

	return kv
}

func (kv *ShardKV) applyToStateMachine(op Op, shardId int) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.shards[shardId].Get(op.Key)
	case OpPut:
		err = kv.shards[shardId].Put(op.Key, op.Value)
	case OpAppend:
		err = kv.shards[shardId].Append(op.Key, op.Value)
	}
	return &OpReply{Value: value, Err: err}
}

func (kv *ShardKV) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *ShardKV) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(kv.shards)
	_ = enc.Encode(kv.duplicateTable)
	_ = enc.Encode(kv.currentConfig)
	_ = enc.Encode(kv.prevConfig)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = NewMemoryKVStateMachine()
			}
		}
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var (
		shards        map[int]*MemoryKVStateMachine
		dupTable      map[int64]*LastOperationInfo
		currentConfig shardctrler.Config
		prevConfig    shardctrler.Config
	)

	if dec.Decode(&shards) != nil ||
		dec.Decode(&dupTable) != nil ||
		dec.Decode(&currentConfig) != nil ||
		dec.Decode(&prevConfig) != nil {
		panic("failed to restore state from snapshpt")
	}

	kv.shards = shards
	kv.duplicateTable = dupTable
	kv.currentConfig = currentConfig
	kv.prevConfig = prevConfig
}
