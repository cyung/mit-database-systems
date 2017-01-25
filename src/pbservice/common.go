package pbservice

import "viewservice"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Option string
	FromPrimary bool
	Id int64
}

type PutAppendReply struct {
	Err Err
	View *viewservice.View
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	Err   Err
	Value string
	View *viewservice.View
}

type CopyArgs struct {
	Db map[string]string
}

type CopyReply struct {
	Err Err
}


// Your RPC definitions here.
