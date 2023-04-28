package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout = "ErrTimeout"
)

type Err string

type OperationArgs struct {
	Key string
	Value string
	Op string
	ClientId int64
	MessageId int
}

type OperationReply struct {
	Err Err
	Value string
}