package shardkv

import (
	"fmt"
	"github.com/joeyz1729/ruaftkv/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	// raft 日志同步
	index, _, isLeader := kv.rf.Start(command)

	// 仅leader处理请求
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

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		newConfig := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	case ShardMigration:
		shardData := command.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
	case ShardGC:
		shardsInfo := command.Data.(ShardOperationArgs)
		return kv.applyShardGC(&shardsInfo)
	default:
		panic(fmt.Sprintf("invalid command type: %d", command.CmdType))
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 != newConfig.Num {
		return &OpReply{Err: ErrWrongConfig}
	}

	for i := 0; i < shardctrler.NShards; i++ {
		if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
			// 迁移进入
			gid := kv.currentConfig.Shards[i]
			if gid != 0 {
				kv.shards[i].Status = MoveIn
			}
		}
		if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
			// shard 需要迁移出去
			gid := newConfig.Shards[i]
			if gid != 0 {
				kv.shards[i].Status = MoveOut
			}
		}
	}
	kv.prevConfig = kv.currentConfig
	kv.currentConfig = newConfig
	return &OpReply{Err: OK}
}

func (kv *ShardKV) applyShardMigration(shardDataReply *ShardOperationReply) *OpReply {
	if shardDataReply.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardDataReply.ShardData {
			shard := kv.shards[shardId]
			// 将数据存储到当前 Group 对应的 shard 中
			if shard.Status == MoveIn {
				for k, v := range shardData {
					shard.KV[k] = v
				}
				// 状态置为 GC，等待清理
				shard.Status = GC
			} else {
				break
			}
		}

		// 拷贝去重表数据
		for clientId, dupTable := range shardDataReply.DuplicateTable {
			table, ok := kv.duplicateTable[clientId]
			if !ok || table.SeqId < dupTable.SeqId {
				kv.duplicateTable[clientId] = dupTable
			}
		}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardGC(shardsInfo *ShardOperationArgs) *OpReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardsInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == GC {
				shard.Status = Normal
			} else if shard.Status == MoveOut {
				kv.shards[shardId] = NewMemoryKVStateMachine()
			} else {
				break
			}
		}
	}
	return &OpReply{Err: OK}
}
