package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type OperationArgs struct {
	Key       string
	Value     string
	Method    string
	ClientId  int64
	MessageId int
}

type OperationReply struct {
	Err   Err
	Value string
}

type MigrationArgs struct {
	Shard     map[string]string
	ShardList []int
}

type MigrationReply struct {
	Err Err
}
