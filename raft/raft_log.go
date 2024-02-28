package raft

import (
	"fmt"
	"github.com/joeyz1729/ruaftkv/labgob"
)

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int

	snapshot []byte      // [1, snapLastIndex]
	tailLog  []*LogEntry // [snapLastIndex + 1, ...)
}

func NewRaftLog(snapLastIndex, snapLastTerm int, snapshot []byte, entries []*LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}
	rl.tailLog = append(rl.tailLog, &LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var (
		snapLastIndex, snapLastTerm int
		//snapshot []byte
		entries []*LogEntry
	)
	if err := d.Decode(&snapLastIndex); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIndex = snapLastIndex

	if err := d.Decode(&snapLastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = snapLastTerm

	//if err := d.Decode(&snapshot); err != nil {
	//	return fmt.Errorf("decode last include snapshot failed")
	//}
	//rl.snapshot = snapshot

	if err := d.Decode(&entries); err != nil {
		return fmt.Errorf("decode last include log entries failed")
	}
	rl.tailLog = entries

	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIndex)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return rl.snapLastIndex + len(rl.tailLog)
}

func (rl *RaftLog) idx(globalIndex int) int {
	if globalIndex < rl.snapLastIndex || globalIndex >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", globalIndex, rl.snapLastIndex, rl.size()-1))
	}
	return globalIndex - rl.snapLastIndex
}

func (rl *RaftLog) at(globalIdx int) *LogEntry {
	return rl.tailLog[rl.idx(globalIdx)]
}

func (rl *RaftLog) last() (int, int) {
	return rl.size() - 1, rl.tailLog[len(rl.tailLog)-1].Term
}

func (rl *RaftLog) firstFor(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIndex
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) tail(startIndex int) []*LogEntry {
	if startIndex >= rl.size() {
		return nil
	}
	return append([]*LogEntry(nil), rl.tailLog[rl.idx(startIndex):]...)
}

func (rl *RaftLog) append(e ...*LogEntry) {
	rl.tailLog = append(rl.tailLog, e...)
}

func (rl *RaftLog) appendFrom(globalPrevIndex int, e ...*LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(globalPrevIndex)+1], e...)
}

func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIndex
	for i, entry := range rl.tailLog {
		if entry.Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, i-1+rl.snapLastIndex, prevTerm)
			prevTerm = entry.Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rl.size()-1, prevTerm)
	return terms
}

func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)
	rl.snapLastIndex = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot
	newEntries := make([]*LogEntry, 0, rl.size()-1-rl.snapLastIndex)
	newEntries = append(newEntries, &LogEntry{Term: rl.snapLastTerm}) // dummy log entry
	newEntries = append(newEntries, rl.tailLog[idx+1:]...)
}
