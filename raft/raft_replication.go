package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool
	Command      interface{}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry

	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

// AppendEntries 心跳以及与leader日志同步
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	// 对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, rf.log.String())
		}
	}()

	// 日志同步
	if args.PrevLogIndex >= rf.log.size() {
		// follower节点的日志缺少太多，同步位置应该更早
		reply.ConflictTerm = InvalidTerm
		reply.ConflictIndex = rf.log.size()
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	if args.PrevLogIndex < rf.log.snapLastIndex {
		// follower的快照日志够多，同步位置应该更晚
		reply.ConflictTerm = rf.log.snapLastTerm
		reply.ConflictIndex = rf.log.snapLastIndex
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIndex)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		// 同步日志的前一条任期不对，需要重新同步
		reply.ConflictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConflictIndex = rf.log.firstFor(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log, prev log's term not match, [%d]: T%d != T%d", args.LeaderId, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// 此时follower和leader的前部分日志已经对齐，可以继续同步日志
	rf.log.appendFrom(args.PrevLogIndex, args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 如果leader节点日志已经提交，follower需要同步提交日志。
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// getMajorityIndexLocked 找到大部分节点已经同步的位置。
func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndices := make([]int, len(rf.peers))
	copy(tmpIndices, rf.matchIndex)
	sort.Ints(tmpIndices)
	majorityIdx := (len(rf.peers) - 1) / 2
	return tmpIndices[majorityIdx]
}

// startReplication leader向follower发起日志同步，并处理返回结果
func (rf *Raft) startReplication(term int) bool {

	// 并发发送并处理返回值
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// 收到的任期更大
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if !rf.contextCheckLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Context Lost, T%d:Leader->T%d:%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 日志同步失败
		if !reply.Success {
			prevNextIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				// follower 日志过短，需要同步更早的日志
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				// follower 日志term不匹配，找到冲突任期的第一条日志
				firstTermIndex := rf.log.firstFor(reply.ConflictTerm)
				if firstTermIndex != InvalidIndex {
					// 如果存在，则重新同步该任期的日志
					rf.nextIndex[peer] = firstTermIndex
				} else {
					// 如果不存在，重新同步follower index位置的日志
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}

			// 注意单调性，不能增加跳过
			if rf.nextIndex[peer] > prevNextIndex {
				// 防止leader记录的peer nextIndex增大
				rf.nextIndex[peer] = prevNextIndex
			}

			// 更新前一条日志的任期
			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIndex {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}

			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.log.String())
			return
		}
		// 更新各节点log index
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 如果一个日志（index+term）被大部分节点都同步了，那么就可以提交。
		// 更新leader节点保存的索引，并通过信号量进行通知。
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.contextCheckLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	// 并发发送请求
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}
		prevIndex := rf.nextIndex[peer] - 1
		// 如果日志太小了，前面的已经保存快照了。
		// 就将快照文件发送给follower，先安装快照。
		if prevIndex < rf.log.snapLastIndex {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIndex,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", peer, args.String())
			go rf.installToPeer(peer, term, args)
			continue
		}
		// 否则就进行正常的日志同步流程，把后面的全部发送。
		prevTerm := rf.log.at(prevIndex).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIndex + 1),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		go replicateToPeer(peer, args)
	}
	return true
}

// replicationTicker 心跳和日志同步逻辑，生命周期为term任期内
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicationInterval)
	}
}
