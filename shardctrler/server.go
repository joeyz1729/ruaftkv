package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/joeyz1729/ruaftkv/labgob"
	"github.com/joeyz1729/ruaftkv/labrpc"
	"github.com/joeyz1729/ruaftkv/raft"
)

const (
	ClientRequestTimeout = 500 * time.Millisecond
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead           int32 // set by Kill()
	lastApplied    int
	stateMachine   *MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]*LastOperationInfo

	configs []Config // indexed by config num
}

// Join 添加
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var opReply = &OpReply{}
	sc.command(&Op{
		OpType:   OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Servers:  args.Servers,
	}, opReply)

	reply.Err = opReply.Err

}

// Leave 删除
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var opReply = &OpReply{}
	sc.command(&Op{
		OpType:   OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		GIDs:     args.GIDs,
	}, opReply)

	reply.Err = opReply.Err

}

// Move 移动
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var opReply = &OpReply{}
	sc.command(&Op{
		OpType:   OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Shard:    args.Shard,
		GID:      args.GID,
	}, opReply)

	reply.Err = opReply.Err

}

// Query 请求
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var opReply = &OpReply{}
	sc.command(&Op{
		OpType: OpQuery,
		Num:    args.Num,
	}, opReply)

	reply.Config = opReply.ControllerConfig
	reply.Err = opReply.Err

}

func (sc *ShardCtrler) command(args *Op, reply *OpReply) {
	sc.mu.Lock()
	if args.OpType != OpQuery && sc.requestDuplicated(args.ClientId, args.SeqId) {
		opReply := sc.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	notifyCh := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.ControllerConfig = result.ControllerConfig
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeNotifyChannel(index)
		sc.mu.Unlock()
	}()

}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.lastApplied = 0
	sc.stateMachine = NewMemoryKVStateMachine()
	sc.notifyChans = make(map[int]chan *OpReply)
	sc.duplicateTable = make(map[int64]*LastOperationInfo)

	go sc.applyTask()
	return sc
}

// applyTask handle apply task
func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex
				op := message.Command.(Op)
				var reply = new(OpReply)

				// 判断操作是否重复
				if op.OpType != OpQuery && sc.requestDuplicated(op.ClientId, op.SeqId) {
					reply = sc.duplicateTable[op.ClientId].Reply
				} else {
					reply = sc.applyToStateMachine(op)
					if op.OpType != OpQuery {
						// 保存去重信息
						sc.duplicateTable[op.ClientId] = &LastOperationInfo{
							SeqId: op.SeqId,
							Reply: reply,
						}
					}
				}
				if _, isLeader := sc.rf.GetState(); isLeader {
					notifyCh := sc.getNotifyChannel(message.CommandIndex)
					notifyCh <- reply
				}

				sc.mu.Unlock()
			} else if message.SnapshotValid {
				sc.mu.Lock()
				sc.lastApplied = message.SnapshotIndex
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyToStateMachine(op Op) *OpReply {
	//var (
	//	value string
	//	err   Err
	//)
	//switch op.OpType {
	//case OpGet:
	//	value, err = sc.stateMachine.Get(op.Key)
	//case OpPut:
	//	err = sc.stateMachine.Put(op.Key, op.Value)
	//case OpAppend:
	//	err = sc.stateMachine.Append(op.Key, op.Value)
	//default:
	//}
	//return &OpReply{
	//	Value: value,
	//	Err:   err,
	//}
	return nil
}

func (sc *ShardCtrler) getNotifyChannel(index int) chan *OpReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeNotifyChannel(index int) {
	delete(sc.notifyChans, index)
}

func (sc *ShardCtrler) requestDuplicated(clientId, seqId int64) bool {
	info, ok := sc.duplicateTable[clientId]
	return ok && seqId <= info.SeqId

}
