package shardkv

import (
	"time"
)

// applyTask handle apply task
func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				var reply = new(OpReply)
				kv.lastApplied = message.CommandIndex
				raftCommand := message.Command.(RaftCommand)

				if raftCommand.CmdType == ClientOperation {
					op := raftCommand.Data.(Op)
					if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
						// 重复更新操作
						reply = kv.duplicateTable[op.ClientId].Reply
					} else {
						// 应用到状态机
						shardId := key2shard(op.Key)
						reply = kv.applyToStateMachine(op, shardId)
						if op.OpType != OpGet {
							// 保存去重信息
							kv.duplicateTable[op.ClientId] = &LastOperationInfo{
								SeqId: op.SeqId,
								Reply: reply,
							}
						}
					}
				} else { // ConfigChange
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

// fetchConfigTask
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		kv.mu.Lock()
		newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
		kv.mu.Unlock()
		// 配置同步
		kv.ConfigCommand(RaftCommand{CmdType: ConfigChange, Data: newConfig}, &OpReply{})
		time.Sleep(FetchConfigInterval)
	}
}
