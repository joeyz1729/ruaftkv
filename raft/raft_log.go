package raft

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int

	snapshot []byte      // [1, snapLastIndex]
	tailLog  []*LogEntry // [snapLastIndex + 1, ...)
}

func NewRaftLog(snapLastIndex, snapLastTerm int, snapshot []byte, entries []*LogEntry) *RaftLog {
	log := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}
	log.tailLog = append(log.tailLog, &LogEntry{
		Term: snapLastTerm,
	})
	log.tailLog = append(log.tailLog, entries...)
	return log
}
