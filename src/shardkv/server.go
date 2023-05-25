package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const RequestTimeout = 500 * time.Millisecond

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big
	applyCh      chan raft.ApplyMsg
	dead         int32
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd

	kvStore       [shardctrler.NShards]Shard
	notifyCh      map[int]chan interface{}
	clientRequest map[int64]int
	mck           *shardctrler.Clerk
	config        shardctrler.Config
	lastConfig    shardctrler.Config
}

type Command struct {
	Key       string
	Value     string
	Method    string
	ClientId  int64
	MessageId int
}

type Configuration struct {
	Config shardctrler.Config
}

type PullShard struct {
	State  map[int]Shard
	Client map[int64]int
	Num    int
}

type DeleteShard struct {
	ShardList []int
	Num       int
}

const (
	Default    = 0
	Pull       = 1
	Push       = 2
	Collection = 3
)

type Shard struct {
	Status int
	State  map[string]string
}

func (shard *Shard) Get(key string) string {
	return shard.State[key]
}

func (shard *Shard) Put(key string, value string) {
	shard.State[key] = value
}

func (shard *Shard) Append(key string, value string) {
	shard.State[key] += value
}

func (shard *Shard) Copy() map[string]string {
	copy := make(map[string]string)
	for key, value := range shard.State {
		copy[key] = value
	}
	return copy
}

type Snapshot struct {
	ServiceState [shardctrler.NShards]Shard
	ClientState  map[int64]int
	Config       shardctrler.Config
	LastConfig   shardctrler.Config
}

func (kv *ShardKV) isStaleShard(shard int) bool {
	// shard migrated to another replica group
	if kv.config.Shards[shard] != kv.gid {
		return true
	}
	shardStatus := kv.kvStore[shard].Status
	if shardStatus == Pull || shardStatus == Push {
		return true
	}
	return false
}

func (kv *ShardKV) snapshot(index int) {
	snapshot := Snapshot{
		ServiceState: kv.kvStore,
		ClientState:  kv.clientRequest,
		Config:       kv.config,
		LastConfig:   kv.lastConfig,
	}
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(snapshot)
	kv.rf.Snapshot(index, writer.Bytes())
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	var snapshot Snapshot
	if decoder.Decode(&snapshot) != nil {
		log.Fatal("Decoding error")
	}

	kv.kvStore = snapshot.ServiceState
	kv.clientRequest = snapshot.ClientState
	kv.config = snapshot.Config
	kv.lastConfig = snapshot.LastConfig
}

func (kv *ShardKV) applyRoutine() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			switch command := msg.Command.(type) {
			case Command:
				kv.applyCommand(command)
			case Configuration:
				kv.applyConfiguration(command)
			case PullShard:
				kv.applyPullShard(command)
			case DeleteShard:
				kv.applyDeleteShard(command)
			}

			if notifyCh, ok := kv.notifyCh[msg.CommandIndex]; ok {
				notifyCh <- msg.Command
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.snapshot(msg.CommandIndex)
			}
			kv.mu.Unlock()
		}

		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readPersist(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) configureRoutine() {
	for !kv.killed() {
		kv.mu.RLock()
		// ensure all shards in current config are migrated
		// before switching to another config
		defaultShard := true
		for shard := range kv.kvStore {
			if kv.kvStore[shard].Status != Default {
				defaultShard = false
				break
			}
		}
		num := kv.config.Num
		kv.mu.RUnlock()

		_, isLeader := kv.rf.GetState()
		if isLeader && defaultShard {
			nextConfig := kv.mck.Query(num+1)
			if num+1 == nextConfig.Num {
				command := Configuration{
					Config: nextConfig,
				}
				// replicate new configuration in this replica group
				index, _, isLeader := kv.rf.Start(command)
				if isLeader {
					kv.consensus(index)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pullShardRoutine() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.RLock()
			gidShardList := make(map[int][]int)
			for shard := range kv.kvStore {
				if kv.kvStore[shard].Status == Pull {
					gid := kv.lastConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.RUnlock()

			// Pull shards from other replica groups
			var waitGroup sync.WaitGroup
			for gid, shardList := range gidShardList {
				waitGroup.Add(1)
				go kv.sendPullShard(&waitGroup, gid, shardList)
			}
			waitGroup.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) deleteShardRoutine() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.RLock()
			gidShardList := make(map[int][]int)
			for shard := range kv.kvStore {
				if kv.kvStore[shard].Status == Collection {
					gid := kv.lastConfig.Shards[shard]
					gidShardList[gid] = append(gidShardList[gid], shard)
				}
			}
			kv.mu.RUnlock()

			// Delete shards not in current config
			var waitGroup sync.WaitGroup
			for gid, shardList := range gidShardList {
				waitGroup.Add(1)
				go kv.sendDeleteShard(&waitGroup, gid, shardList)
			}
			waitGroup.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) heartbeatRoutine() {
	for !kv.killed() {
		index, _, isLeader := kv.rf.Start(Command{})
		if isLeader {
			kv.consensus(index)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (kv *ShardKV) applyCommand(command Command) {
	key := command.Key
	value := command.Value
	method := command.Method
	clientId := command.ClientId
	messageId := command.MessageId
	shard := key2shard(key)

	if !kv.isStaleShard(shard) && kv.clientRequest[clientId] < messageId {
		switch method {
		case "Put":
			kv.kvStore[shard].Put(key, value)
		case "Append":
			kv.kvStore[shard].Append(key, value)
		}
		kv.clientRequest[clientId] = messageId
	}
}

func (kv *ShardKV) applyConfiguration(command Configuration) {
	nextConfig := command.Config
	if nextConfig.Num != kv.config.Num+1 {
		return
	}

	for shard, gid := range nextConfig.Shards {
		sourceGid := kv.config.Shards[shard]
		if gid == kv.gid && sourceGid != kv.gid && sourceGid != 0 {
			kv.kvStore[shard].Status = Pull
		}
		if gid != kv.gid && sourceGid == kv.gid && gid != 0 {
			kv.kvStore[shard].Status = Push
		}
	}
	kv.lastConfig = kv.config
	kv.config = nextConfig
}

func (kv *ShardKV) applyPullShard(command PullShard) {
	state := command.State
	client := command.Client
	num := command.Num
	if num != kv.config.Num {
		return
	}

	for shard := range state {
		if kv.kvStore[shard].Status == Pull {
			for key, value := range state[shard].State {
				kv.kvStore[shard].Put(key, value)
			}
			kv.kvStore[shard].Status = Collection
		}
	}

	for clientId, messageId := range client {
		if kv.clientRequest[clientId] < messageId {
			kv.clientRequest[clientId] = messageId
		}
	}
}

func (kv *ShardKV) applyDeleteShard(command DeleteShard) {
	shardList := command.ShardList
	num := command.Num
	if num != kv.config.Num {
		return
	}

	for _, shard := range shardList {
		if kv.kvStore[shard].Status == Collection {
			kv.kvStore[shard].Status = Default
		}

		if kv.kvStore[shard].Status == Push {
			kv.kvStore[shard] = Shard{
				Status: Default,
				State:  make(map[string]string),
			}
		}
	}
}

func (kv *ShardKV) sendPullShard(waitGroup *sync.WaitGroup, gid int, shardList []int) {
	kv.mu.RLock()
	args := PullShardArgs{
		ShardList: shardList,
		Num:       kv.config.Num,
	}
	servers := kv.lastConfig.Groups[gid]
	kv.mu.RUnlock()

	for _, srv := range servers {
		var reply PullShardReply
		ok := kv.make_end(srv).Call("ShardKV.PullShard", &args, &reply)

		if ok && reply.Err == OK {
			command := PullShard{
				State:  reply.State,
				Client: reply.Client,
				Num:    reply.Num,
			}
			index, _, isLeader := kv.rf.Start(command)
			if isLeader {
				kv.consensus(index)
			}
			break
		}
	}
	waitGroup.Done()
}

func (kv *ShardKV) sendDeleteShard(waitGroup *sync.WaitGroup, gid int, shardList []int) {
	kv.mu.RLock()
	args := DeleteShardArgs{
		ShardList: shardList,
		Num:       kv.config.Num,
	}
	servers := kv.lastConfig.Groups[gid]
	kv.mu.RUnlock()

	for _, srv := range servers {
		var reply DeleteShardReply
		ok := kv.make_end(srv).Call("ShardKV.DeleteShard", &args, &reply)

		if ok && reply.Err == OK {
			command := DeleteShard{
				Num:       args.Num,
				ShardList: args.ShardList,
			}

			index, _, isLeader := kv.rf.Start(command)
			if isLeader {
				kv.consensus(index)
			}
			break
		}
	}
	waitGroup.Done()
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.config.Num < args.Num {
		reply.Err = ErrFuture
		kv.mu.RUnlock()
		return
	}

	shardList := args.ShardList
	state := make(map[int]Shard)
	for _, shard := range shardList {
		state[shard] = Shard{
			State: kv.kvStore[shard].Copy(),
		}
	}

	client := make(map[int64]int)
	for clientId, messageId := range kv.clientRequest {
		client[clientId] = messageId
	}

	reply.State = state
	reply.Client = client
	reply.Num = kv.config.Num
	reply.Err = OK
	kv.mu.RUnlock()
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.config.Num > args.Num {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	num := args.Num
	shardList := args.ShardList
	command := DeleteShard{
		Num:       num,
		ShardList: shardList,
	}
	index, _, isLeader := kv.rf.Start(command)
	if isLeader {
		_, reply.Err = kv.consensus(index)
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) Operation(args *OperationArgs, reply *OperationReply) {
	shard := key2shard(args.Key)
	kv.mu.RLock()
	if kv.isStaleShard(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	index, _, isLeader := kv.rf.Start(
		Command{
			Key:       args.Key,
			Value:     args.Value,
			Method:    args.Method,
			ClientId:  args.ClientId,
			MessageId: args.MessageId,
		},
	)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	appliedCommand, err := kv.consensus(index)
	if err == ErrTimeout {
		reply.Err = ErrTimeout
		return
	}

	command, ok := appliedCommand.(Command)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	if args.ClientId != command.ClientId || args.MessageId != command.MessageId {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	if kv.isStaleShard(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	if command.Method == "Get" {
		reply.Value = kv.kvStore[shard].Get(command.Key)
	}
	kv.mu.RUnlock()
	reply.Err = OK
}

func (kv *ShardKV) consensus(index int) (interface{}, Err) {
	notifyCh := make(chan interface{})
	kv.mu.Lock()
	kv.notifyCh[index] = notifyCh
	kv.mu.Unlock()
	defer kv.closeChannel(index)

	select {
	case command := <-notifyCh:
		return command, OK
	case <-time.After(500 * time.Millisecond):
		return nil, ErrTimeout
	}
}

func (kv *ShardKV) closeChannel(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if notifyCh, ok := kv.notifyCh[index]; ok {
		close(notifyCh)
		delete(kv.notifyCh, index)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(Configuration{})
	labgob.Register(PullShard{})
	labgob.Register(DeleteShard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	for index := range kv.kvStore {
		kv.kvStore[index] = Shard{
			Status: Default,
			State:  make(map[string]string),
		}
	}
	kv.notifyCh = make(map[int]chan interface{})
	kv.clientRequest = make(map[int64]int)

	if kv.maxraftstate != -1 {
		kv.readPersist(kv.persister.ReadSnapshot())
	}

	go kv.applyRoutine()
	go kv.configureRoutine()
	go kv.pullShardRoutine()
	go kv.deleteShardRoutine()
	go kv.heartbeatRoutine()

	return kv
}
