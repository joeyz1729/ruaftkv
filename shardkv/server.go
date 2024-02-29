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
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead           int32
	lastApplied    int
	stateMachine   *MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]*LastOperationInfo
	currentConfig  shardctrler.Config
	mck            *shardctrler.Clerk
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 判断Shard Group
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		Key:    args.Key,
		OpType: OpGet,
	})

	// follower节点不处理请求
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
	return
}

func (kv *ShardKV) matchGroup(key string) bool {
	shard := key2shard(key)
	return kv.currentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) requestDuplicated(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.requestDuplicated(args.ClientId, args.SeqId) {
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   getOperationType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	})

	// follower节点不处理请求
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
	return
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

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]*LastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()

	// Use something like this to talk to the shardmaster:
	kv.mck = shardctrler.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.fetchConfigTask()

	return kv
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
	_ = enc.Encode(kv.stateMachine)
	_ = enc.Encode(kv.duplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var (
		stateMachine *MemoryKVStateMachine
		dupTable     map[int64]*LastOperationInfo
	)
	if dec.Decode(&stateMachine) != nil || dec.Decode(&dupTable) != nil {
		panic("failed to restore state from snapshot, decode err")
	}
	kv.stateMachine = stateMachine
	kv.duplicateTable = dupTable

}
