package shardkv

import (
	"bytes"
	"fmt"
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

type Op struct {
	Key       string
	Value     string
	Method    string
	ClientId  int64
	MessageId int
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	mck          *shardctrler.Clerk
	applyCh      chan raft.ApplyMsg
	dead         int32
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	isLeader     bool

	config        shardctrler.Config
	kvStore       map[string]string
	notifyCh      map[int]chan Op
	clientRequest map[int64]int
	shards        map[int]struct{} // Set of shards that this group owns
}

type Snapshot struct {
	ServiceState map[string]string
	ClientState  map[int64]int
}

func (kv *ShardKV) Operation(args *OperationArgs, reply *OperationReply) {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	kv.isLeader = isLeader
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	fmt.Printf("leader: %d\n", kv.me)
	shard := key2shard(args.Key)
	if _, ok := kv.shards[shard]; !ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	
	kv.mu.Unlock()
	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Method:    args.Method,
		ClientId:  args.ClientId,
		MessageId: args.MessageId,
	}
	index, _, _ := kv.rf.Start(command)

	notifyCh := make(chan Op)
	kv.mu.Lock()
	kv.notifyCh[index] = notifyCh
	kv.mu.Unlock()

	select {
	case appliedCommand := <-notifyCh:
		if args.ClientId != appliedCommand.ClientId || args.MessageId != appliedCommand.MessageId {
			reply.Err = ErrWrongLeader
			return
		}
		if appliedCommand.Method == "Get" {
			reply.Value = appliedCommand.Value
		}
		reply.Err = OK
	case <-time.After(RequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		close(notifyCh)
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
	index, _, isLeader := kv.rf.Start(*args)
	kv.mu.Lock()
	kv.isLeader = isLeader
	kv.mu.Unlock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyCh := make(chan Op)
	kv.mu.Lock()
	kv.notifyCh[index] = notifyCh
	kv.mu.Unlock()

	select {
	case <-notifyCh:
		reply.Err = OK
	case <-time.After(RequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		close(notifyCh)
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) replicationRoutine() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			switch msg.Command.(type) {
			case Op:
				command := msg.Command.(Op)
				if kv.clientRequest[command.ClientId] < command.MessageId {
					switch command.Method {
					case "Put":
						kv.kvStore[command.Key] = command.Value
					case "Append":
						kv.kvStore[command.Key] += command.Value
					}
				}
				command.Value = kv.kvStore[command.Key]
				if notifyCh, ok := kv.notifyCh[msg.CommandIndex]; ok {
					notifyCh <- command
				}
			case MigrationArgs:
				command := msg.Command.(MigrationArgs)
				for key, value := range command.Shard {
					kv.kvStore[key] = value
				}
				for _, shard := range command.ShardList {
					kv.shards[shard] = struct{}{}
				}
			}

			if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > 0.8*float32(kv.maxraftstate) {
				snapshot := Snapshot{
					ServiceState: kv.kvStore,
					ClientState:  kv.clientRequest,
				}
				writer := new(bytes.Buffer)
				encoder := labgob.NewEncoder(writer)
				encoder.Encode(snapshot)
				kv.rf.Snapshot(msg.CommandIndex, writer.Bytes())
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

func (kv *ShardKV) migrationRoutine() {
	for !kv.killed() {
		kv.mu.Lock()
		oldConfig := kv.config
		newConfig := kv.mck.Query(-1)
		if newConfig.Num > oldConfig.Num {
			sendTo := make(map[int]map[string]string)
			for shard := 0; shard < shardctrler.NShards; shard++ {
				oldGid := oldConfig.Shards[shard]
				newGid := newConfig.Shards[shard]
				// // Gain shard
				// if newGid == kv.gid {
				// 	kv.shards[shard] = struct{}{}
				// }
				// Lose shard
				if oldGid == kv.gid && newGid != kv.gid {
					sendTo[newGid] = make(map[string]string)
					delete(kv.shards, shard)
				}
			}
			kv.config = newConfig
			fmt.Println(newConfig)
			fmt.Printf("is leader: %t\n", kv.isLeader)
			if kv.isLeader {
				for key, value := range kv.kvStore {
					shard := key2shard(key)
					gid := newConfig.Shards[shard]
					if _, ok := sendTo[gid]; ok {
						sendTo[gid][key] = value
					}
				}
				kv.mu.Unlock()
				fmt.Println(sendTo)
				for gid, data := range sendTo {
					servers := newConfig.Groups[gid]
					shards := make([]int, 0)
					for shard := 0; shard < shardctrler.NShards; shard++ {
						if newConfig.Shards[shard] == gid {
							shards = append(shards, shard)
						}
					}
					args := MigrationArgs{
						Shard: data,
						ShardList: shards,
					}
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply MigrationReply
						ok := srv.Call("ShardKV.Migration", &args, &reply)
						if ok && reply.Err == OK {
							break
						}
						// ... not ok, or ErrWrongLeader
					}
				}
			} else {
				kv.mu.Unlock()
			}
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	labgob.Register(Op{})
	labgob.Register(Snapshot{})
	labgob.Register(MigrationArgs{})

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
	kv.config = kv.mck.Query(-1)
	kv.kvStore = make(map[string]string)
	kv.notifyCh = make(map[int]chan Op)
	kv.clientRequest = make(map[int64]int)
	kv.shards = make(map[int]struct{})
	_, isLeader := kv.rf.GetState()
	kv.isLeader = isLeader

	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.config.Shards[shard] == kv.gid {
			kv.shards[shard] = struct{}{}
		}
	}

	if kv.maxraftstate != -1 {
		kv.readPersist(kv.persister.ReadSnapshot())
	}

	go kv.replicationRoutine()
	go kv.migrationRoutine()

	fmt.Printf("")

	return kv
}
