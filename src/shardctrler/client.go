package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
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
	ck.clientId = nrand()
	ck.messageId = 1
	return ck
}

func (ck *Clerk) Operation(args *OperationArgs) Config {
	for {
		// try each known server
		for _, srv := range ck.servers {
			var reply OperationReply
			ok := srv.Call("ShardCtrler.Operation", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.messageId++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Query(num int) Config {
	args := OperationArgs{
		ClientId:  ck.clientId,
		MessageId: ck.messageId,
		Method:    "Query",
		QueryNum:  num,
	}
	return ck.Operation(&args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := OperationArgs{
		ClientId:    ck.clientId,
		MessageId:   ck.messageId,
		Method:      "Join",
		JoinServers: servers,
	}
	ck.Operation(&args)
}

func (ck *Clerk) Leave(gids []int) {
	args := OperationArgs{
		ClientId:  ck.clientId,
		MessageId: ck.messageId,
		Method:    "Leave",
		LeaveGIDs: gids,
	}
	ck.Operation(&args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := OperationArgs{
		ClientId:  ck.clientId,
		MessageId: ck.messageId,
		Method:    "Move",
		MoveShard: shard,
		MoveGID:   gid,
	}
	ck.Operation(&args)
}
