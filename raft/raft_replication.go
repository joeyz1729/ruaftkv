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
	Entries      []LogEntry

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
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log, higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	defer func() {
		rf.resetElectionTimeoutLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, rf.logString())
		}
	}()

	// 日志同步
	if args.PrevLogIndex >= len(rf.log) {
		// 日志过短
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log, follower log too short, len: %d <= Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 日志与leader任期不匹配
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.firstLogFor(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log, prev log's term not match, [%d]: T%d != T%d", args.LeaderId, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 检查 log commit index
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Update commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndices := make([]int, len(rf.peers))
	copy(tmpIndices, rf.matchIndex)
	sort.Ints(tmpIndices)
	majorityIdx := len(rf.peers) / 2
	return tmpIndices[majorityIdx]
}

// startReplication 发起日志同步
func (rf *Raft) startReplication(term int) bool {
	replicationToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, &reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
			peer, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1])
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.logString())

		if !rf.contextCheckLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Context Lost, T%d:Leader->T%d:%d", peer, term, rf.currentTerm, rf.role)
			return
		}

		if !reply.Success {
			prevNextIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				// follower 日志过短
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				// follower 日志term不匹配
				firstTermIndex := rf.firstLogFor(reply.ConflictTerm)
				if firstTermIndex != InvalidIndex {
					// 以leader为准，跳过conflictTerm的所有日志
					rf.nextIndex[peer] = firstTermIndex
				} else {
					// leader没有该term的日志，以follower为准
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}

			if rf.nextIndex[peer] > prevNextIndex {
				// 防止leader记录的peer nextIndex增大
				rf.nextIndex[peer] = prevNextIndex
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not match at S%d, try next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}
		// 更新各节点log index
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 更新leader commit log index
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log[majorityMatched].Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader Update commit index %d->%d", rf.commitIndex, majorityMatched)
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

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}
		prevIndex := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIndex].Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      append([]LogEntry(nil), rf.log[prevIndex+1:]...), // 防止竞争
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		go replicationToPeer(peer, args)
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
