package shardkv

import "time"

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
				kv.lastApplied = message.CommandIndex
				op := message.Command.(Op)
				var reply = new(OpReply)

				// 判断操作是否重复
				if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
					reply = kv.duplicateTable[op.ClientId].Reply
				} else {
					reply = kv.applyToStateMachine(op)
					if op.OpType != OpGet {
						// 保存去重信息
						kv.duplicateTable[op.ClientId] = &LastOperationInfo{
							SeqId: op.SeqId,
							Reply: reply,
						}
					}
				}
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

func (kv *ShardKV) fetchConfigTask() {
	kv.mu.Lock()
	for !kv.killed() {
		newConfig := kv.mck.Query(-1)
		kv.currentConfig = newConfig
		kv.mu.Unlock()
		time.Sleep(FetchConfigInterval)
	}
}
