package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const RequestTimeout = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	configs       []Config // indexed by config num
	notifyCh      map[int]chan OperationArgs
	clientRequest map[int64]int
}

func (sc *ShardCtrler) Operation(args *OperationArgs, reply *OperationReply) {
	clientId := args.ClientId
	messageId := args.MessageId
	queryNum := args.QueryNum

	// Start replicating this operation in this Raft group
	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// Wait for this request to be replicated (and committed) on this channel
	notifyCh := make(chan OperationArgs)
	sc.mu.Lock()
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	select {
	case appliedCommand := <-notifyCh:
		// message at this index may not be replicated
		// another message gets replicated
		// this case happens when this server is no longer a leader due to crash or network partition
		if clientId != appliedCommand.ClientId || messageId != appliedCommand.MessageId {
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		}
		sc.mu.RLock()
		if queryNum == -1 || queryNum >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[queryNum]
		}
		sc.mu.RUnlock()
		reply.Err = OK
	case <-time.After(RequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		close(notifyCh)
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) applyRoutine() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			command, _ := msg.Command.(OperationArgs)
			clientId := command.ClientId
			messageId := command.MessageId
			method := command.Method
			sc.mu.Lock()
			// Duplicate client request detection. Only handle new request
			if sc.clientRequest[clientId] < messageId {
				oldConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{
					Num:    oldConfig.Num + 1,
					Groups: make(map[int][]string),
				}

				// copy replica groups from old configuration
				for gid, servers := range oldConfig.Groups {
					newConfig.Groups[gid] = make([]string, len(servers))
					// deep copy slice in Go
					copy(newConfig.Groups[gid], servers)
				}

				// copy shard assignments from old configuration
				for shard, gid := range oldConfig.Shards {
					newConfig.Shards[shard] = gid
				}

				switch method {
				case "Join":
					// Add new replica groups to new config
					for gid, servers := range command.JoinServers {
						newConfig.Groups[gid] = servers
					}
				case "Leave":
					// Delete leaving replica groups from new config
					for _, gid := range command.LeaveGIDs {
						delete(newConfig.Groups, gid)
					}
				case "Move":
					newConfig.Shards[command.MoveShard] = command.MoveGID
				}

				// Rebalance shards across replica groups
				if method == "Join" || method == "Leave" {
					gids := make([]int, 0)
					for gid := range newConfig.Groups {
						gids = append(gids, gid)
					}
					if len(gids) == 0 {
						// all replica groups have left
						for shard := range newConfig.Shards {
							newConfig.Shards[shard] = 0
						}
					} else {
						// Sort to guarantee same config on every server
						// since go map iteration order is not deterministic
						sort.Ints(gids)
						for shard := range newConfig.Shards {
							newConfig.Shards[shard] = gids[shard%len(gids)]
						}
					}
				}

				if method != "Query" {
					sc.configs = append(sc.configs, newConfig)
				}
				sc.clientRequest[clientId] = messageId
			}

			// Notify a waiting RPC that this client request is committed
			if notifyCh, ok := sc.notifyCh[msg.CommandIndex]; ok {
				notifyCh <- command
			}
			sc.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(OperationArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.notifyCh = make(map[int]chan OperationArgs)
	sc.clientRequest = make(map[int64]int)

	go sc.applyRoutine()

	return sc
}
