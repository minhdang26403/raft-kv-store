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
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	dead         int32
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	kvStore       map[string]string
	notifyCh      map[int]chan Op
	clientRequest map[int64]int
}

type Snapshot struct {
	ServiceState map[string]string
	ClientState  map[int64]int
}

func (kv *ShardKV) Operation(args *OperationArgs, reply *OperationReply) {
	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Method:    args.Method,
		ClientId:  args.ClientId,
		MessageId: args.MessageId,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

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

func (kv *ShardKV) replicationRoutine() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			command, ok := msg.Command.(Op)
			if !ok {
				log.Fatal("Command must be of Op type")
			}
			kv.mu.Lock()
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.kvStore = make(map[string]string)
	kv.notifyCh = make(map[int]chan Op)
	kv.clientRequest = make(map[int64]int)

	if kv.maxraftstate != -1 {
		kv.readPersist(kv.persister.ReadSnapshot())
	}

	go kv.replicationRoutine()

	return kv
}
