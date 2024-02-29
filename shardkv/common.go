package shardkv

import (
	"fmt"
	"log"
	"time"
)

const Debug = 0

const (
	ClientRequestTimeout = 500 * time.Millisecond
	FetchConfigTimeout   = 50 * time.Millisecond
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OpReply struct {
	Value string
	Err   Err
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

func getOperationType(v string) OperationType {
	switch v {
	case "Get":
		return OpGet
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("invalid operation type: %s", v))
	}
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}
