package raft

// applicationTicker 定期执行
func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		// 通过该信号量等待，如果leader提交了新的日志，会通过信号量通知。
		rf.applyCond.Wait()
		entries := make([]*LogEntry, 0)
		snapPendingApply := rf.snapPending
		if !snapPendingApply {
			if rf.lastApplied < rf.log.snapLastIndex {
				rf.lastApplied = rf.log.snapLastIndex
			}

			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()
		// 将需要执行的日志全部发送到执行channel中。
		if !snapPendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIndex,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		rf.mu.Lock()
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", 0, rf.log.snapLastIndex)
			rf.lastApplied = rf.log.snapLastIndex
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}
