package pbservice

import (
	"hash/fnv"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	PutId int32
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type SyncPutArgs struct {
	Key string
	Value string
	PutId int32
}

type SyncPutReply struct {
	Err error
}

type SyncDBArgs struct {
	Store map[string]string
	GetRequestsProcessed map[int32]bool
	PutRequestsProcessed map[int32]string
}

type SyncDBReply struct {
	Err	error
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	GetId int32
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
