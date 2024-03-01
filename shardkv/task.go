package shardkv

import (
	"fmt"
	"github.com/joeyz1729/ruaftkv/shardctrler"
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
						reply = kv.duplicateTable[op.ClientId].Reply
					} else {
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
				} else {
					// ConfigChange
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
	kv.mu.Lock()
	for !kv.killed() {
		newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
		kv.ConfigCommand(RaftCommand{
			CmdType: ConfigChange,
			Data:    newConfig,
		}, &OpReply{})
		kv.currentConfig = newConfig
		kv.mu.Unlock()
		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		newConfig := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	default:
		panic(fmt.Sprintf("invalid command type: %d", command.CmdType))
	}

}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 == newConfig.Num {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				// 迁移进入
			}

			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				// 迁移退出
			}
		}
	}
	kv.currentConfig = newConfig
	return &OpReply{}
}
