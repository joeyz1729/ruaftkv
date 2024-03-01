package shardkv

import (
	"sync"
	"time"
)

// 处理 apply 任务
func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				var reply = new(OpReply)
				kv.lastApplied = message.CommandIndex
				raftCommand := message.Command.(RaftCommand)

				if raftCommand.CmdType == ClientOperation {
					op := raftCommand.Data.(Op)
					reply = kv.applyClientOperation(op)
				} else {
					reply = kv.handleConfigChangeMessage(raftCommand)
				}

				// 判断操作是否重复
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- reply
				}

				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// fetchConfigTask 获取当前配置
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			needFetch := true
			kv.mu.Lock()
			// 如果有 shard 的状态是非 Normal 的，则说明前一个配置变更的任务正在进行中
			for _, shard := range kv.shards {
				if shard.Status != Normal {
					needFetch = false
					break
				}
			}
			currentNum := kv.currentConfig.Num
			kv.mu.Unlock()
			// 配置同步
			if needFetch {
				newConfig := kv.mck.Query(currentNum + 1)
				if newConfig.Num == currentNum+1 {
					kv.ConfigCommand(RaftCommand{CmdType: ConfigChange, Data: newConfig}, &OpReply{})
				}
			}
		}

		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 找到需要迁移进来的 shard
			gidToShards := kv.getShardByStatus(MoveIn)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					// 遍历该 Group 中每一个节点，然后从 Leader 中读取到对应的 shard 数据
					getShardArgs := ShardOperationArgs{configNum, shardIds}
					for _, server := range servers {
						var getShardReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardsData", &getShardArgs, &getShardReply)
						// 获取到shard数据并进行迁移
						if ok && getShardReply.Err == OK {
							kv.ConfigCommand(RaftCommand{ShardMigration, getShardReply}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}

			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardMigrationInterval)
	}
}

func (kv *ShardKV) shardGCTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()

			gidToShards := kv.getShardByStatus(GC)
			var wg sync.WaitGroup

			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()

					shardGCArgs := ShardOperationArgs{configNum, shardIds}
					for _, server := range servers {
						// rpc 通知follower进行GC
						var shardGCReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.DeleteShardsData", &shardGCArgs, &shardGCReply)
						if ok && shardGCReply.Err == OK {
							// 当前leader节点进行GC
							kv.ConfigCommand(RaftCommand{ShardGC, shardGCArgs}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardGCInterval)
	}
}

// getShardByStatus 根据状态查找 shard
func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			gid := kv.prevConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}
	return gidToShards
}

// GetShardsData 获取 shards 数据
func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].copyData()
	}

	reply.DuplicateTable = make(map[int64]*LastOperationInfo)
	for clientId, op := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = op.copyData()
	}

	reply.ConfigNum = args.ConfigNum
	reply.Err = OK
}

func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 只需要从 Leader 获取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OpReply
	kv.ConfigCommand(RaftCommand{ShardGC, *args}, &opReply)
	reply.Err = opReply.Err
}

func (kv *ShardKV) applyClientOperation(op Op) *OpReply {
	if kv.matchGroup(op.Key) {
		var reply = &OpReply{}
		if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
			reply = kv.duplicateTable[op.ClientId].Reply
		} else {
			// 将操作应用状态机中
			shardId := key2shard(op.Key)
			reply = kv.applyToStateMachine(op, shardId)
			if op.OpType != OpGet {
				kv.duplicateTable[op.ClientId] = &LastOperationInfo{
					SeqId: op.SeqId,
					Reply: reply,
				}
			}
		}
		return reply
	}
	return &OpReply{Err: ErrWrongGroup}
}
