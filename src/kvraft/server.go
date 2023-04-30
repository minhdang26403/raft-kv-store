package kvraft

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

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const RequestTimeout = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Op string
	ClientId int64
	MessageId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	persister *raft.Persister
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	kvStore map[string]string
	notifyCh map[int]chan Op
	clientRequest map[int64]int
	// Your definitions here.
}

type Snapshot struct {
	ServiceState map[string]string
	ClientState map[int64]int
}

func (kv *KVServer) Operation(args *OperationArgs, reply *OperationReply) {
	command := Op{
		Key: args.Key,
		Value: args.Value,
		Op: args.Op,
		ClientId: args.ClientId,
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
	case appliedCommand := <- notifyCh:
		if appliedCommand.Op == "Get" {
			reply.Value = appliedCommand.Value
		}

		if args.ClientId != appliedCommand.ClientId || args.MessageId != appliedCommand.MessageId {
			reply.Err = ErrWrongLeader
			return
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

func (kv *KVServer) applyRoutine() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			command, ok := msg.Command.(Op)
			if !ok {
				log.Fatal("Command must be of Op type")
			}
			kv.mu.Lock()
			// Duplicate detection. Only handle new request
			if kv.clientRequest[command.ClientId] != command.MessageId {
				switch command.Op {
				case "Put":
					kv.kvStore[command.Key] = command.Value
				case "Append":
					kv.kvStore[command.Key] += command.Value
				}
				kv.clientRequest[command.ClientId] = command.MessageId
			}
			command.Value = kv.kvStore[command.Key]

			if notifyCh, ok := kv.notifyCh[msg.CommandIndex]; ok {
				notifyCh <- command
			}

			if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > 0.8 * float32(kv.maxraftstate) {
				snapshot := Snapshot{
					ServiceState: kv.kvStore,
					ClientState: kv.clientRequest,
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

func (kv *KVServer) readPersist(data []byte) {
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Snapshot{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.kvStore = make(map[string]string)
	kv.notifyCh = make(map[int]chan Op)
	kv.clientRequest = make(map[int64]int)

	if maxraftstate != -1 {
		kv.readPersist(kv.persister.ReadSnapshot())
	}

	go kv.applyRoutine()

	return kv
}
