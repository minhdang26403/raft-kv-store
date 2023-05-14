package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	// "fmt"
	"sync"
	"sync/atomic"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.

	configs       []Config // indexed by config num
	notifyCh      map[int]chan int
	clientRequest map[int64]int
}

type Op struct {
	// Your data here.
	Config    Config
	ClientId  int64
	MessageId int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	newConfig := sc.cloneLatestConfig()
	gids := make([]int, 0)
	for gid := range newConfig.Groups {
		gids = append(gids, gid)
	}

	// add new replica groups and assign shards to these
	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = make([]string, len(servers))
		copy(newConfig.Groups[gid], servers)
		gids = append(gids, gid)
	}

	i := 0
	for shard := range newConfig.Shards {
		newConfig.Shards[shard] = gids[i%len(gids)]
		i++
	}

	operation := Op{
		Config:    newConfig,
		ClientId:  args.ClientId,
		MessageId: args.MessageId,
	}
	index, _, _ := sc.rf.Start(operation)

	notifyCh := make(chan int)
	sc.mu.Lock()
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	// wait for this config replicated on majority of servers
	<-notifyCh
	go func() {
		sc.mu.Lock()
		close(notifyCh)
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	leaveGID := make(map[int]int)
	for _, gid := range args.GIDs {
		leaveGID[gid] = 0
	}

	// replica groups remain
	newGID := make([]int, 0)

	sc.mu.Lock()
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: oldConfig.Num + 1, Groups: make(map[int][]string)}

	// copy replica groups from old configuration
	for gid, servers := range oldConfig.Groups {
		// ignore replica groups that will leave
		if _, ok := leaveGID[gid]; ok {
			continue
		}
		newConfig.Groups[gid] = make([]string, len(servers))
		copy(newConfig.Groups[gid], servers)
		newGID = append(newGID, gid)
	}

	if len(newGID) == 0 {
		for shard := range newConfig.Shards {
			newConfig.Shards[shard] = 0
		}
	} else {
		i := 0
		// reassign shards
		for shard, gid := range oldConfig.Shards {
			if _, ok := leaveGID[gid]; ok {
				// shard belongs to leaving replica group
				newConfig.Shards[shard] = newGID[i%len(newGID)]
				i++
			} else {
				newConfig.Shards[shard] = gid
			}
		}
	}

	operation := Op{
		Config:    newConfig,
		ClientId:  args.ClientId,
		MessageId: args.MessageId,
	}
	index, _, _ := sc.rf.Start(operation)

	notifyCh := make(chan int)
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	// wait for this config replicated on majority of servers
	<-notifyCh

	go func() {
		sc.mu.Lock()
		close(notifyCh)
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	newConfig := sc.cloneLatestConfig()

	newConfig.Shards[args.Shard] = args.GID

	operation := Op{
		Config:    newConfig,
		ClientId:  args.ClientId,
		MessageId: args.MessageId,
	}
	index, _, _ := sc.rf.Start(operation)

	notifyCh := make(chan int)
	sc.mu.Lock()
	sc.notifyCh[index] = notifyCh
	sc.mu.Unlock()

	// wait for this config replicated on majority of servers
	<-notifyCh

	go func() {
		sc.mu.Lock()
		close(notifyCh)
		delete(sc.notifyCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	configNum := args.Num
	sc.mu.Lock()
	defer sc.mu.Unlock()
	latestConfig := sc.configs[len(sc.configs)-1]
	if configNum == -1 || configNum > latestConfig.Num {
		reply.Config = latestConfig
	} else {
		reply.Config = sc.configs[configNum]
	}
}

func (sc *ShardCtrler) applyRoutine() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			command, _ := msg.Command.(Op)
			sc.mu.Lock()
			// Duplicate detection. Only handle new request
			if sc.clientRequest[command.ClientId] != command.MessageId {
				sc.clientRequest[command.ClientId] = command.MessageId
				idx := command.Config.Num
				if idx < len(sc.configs) {
					sc.configs[idx] = command.Config
				} else {
					sc.configs = append(sc.configs, command.Config)
				}
			}

			if notifyCh, ok := sc.notifyCh[msg.CommandIndex]; ok {
				notifyCh <- 0
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) cloneLatestConfig() Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: oldConfig.Num + 1, Groups: make(map[int][]string)}

	// copy replica groups from old configuration
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = make([]string, len(servers))
		copy(newConfig.Groups[gid], servers)
	}

	// copy shard assignments from old configuration
	for shard, gid := range oldConfig.Shards {
		newConfig.Shards[shard] = gid
	}
	return newConfig
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
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

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.notifyCh = make(map[int]chan int)
	sc.clientRequest = make(map[int64]int)

	go sc.applyRoutine()

	return sc
}
