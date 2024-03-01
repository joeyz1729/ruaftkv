package shardkv

import (
	"fmt"
	"github.com/joeyz1729/ruaftkv/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(command)

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
}

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		newConfig := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	case ShardMigration:
		shardData := command.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
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
			// 迁移退出
			gid := kv.currentConfig.Shards[i]
			if gid != 0 {
				kv.shards[i].Status = MoveOut
			}
		}
	}
	kv.prevConfig = kv.currentConfig
	kv.currentConfig = newConfig
	return &OpReply{}
}

// applyShardMigration
func (kv *ShardKV) applyShardMigration(reply *ShardOperationReply) *OpReply {
	if reply.ConfigNum != kv.currentConfig.Num {
		return &OpReply{Err: ErrWrongConfig}
	}

	for shardId, shardData := range reply.ShardData {
		shard := kv.shards[shardId]
		if shard.Status == MoveIn {
			for k, v := range shardData {
				shard.KV[k] = v
			}
			shard.Status = GC
		} else {
			break
		}
	}
	for clientId, dupTable := range reply.DuplicateTable {
		table, ok := kv.duplicateTable[clientId]
		if !ok || table.SeqId < dupTable.SeqId {
			kv.duplicateTable[clientId] = dupTable
		}
	}

	return &OpReply{Err: OK}
}
