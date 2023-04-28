package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clientId int64
	messageId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.messageId = 1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.Operation(key, "", "Get")
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.Operation(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Operation(key string, value string, op string) string {
	args := OperationArgs{
			Key: key, 
			Value: value, 
			Op: op,
			ClientId: ck.clientId,
			MessageId: ck.messageId,
		}
	reply := OperationReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Operation", &args, &reply)
		if ok && reply.Err == OK {
			ck.messageId++
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}
