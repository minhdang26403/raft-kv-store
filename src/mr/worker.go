package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := RequestReply{}
		if ok := call("Coordinator.RequestTask", &RequestArgs{}, &reply); !ok {
			break
		}
		switch reply.Task {
		case MapTask:
			runMapTask(mapf, &reply)
		case ReduceTask:
			runReduceTask(reducef, &reply)
		case WaitTask:
			time.Sleep(time.Second)
		case NoTask:
			break
		default:
			log.Fatalf("Unexpected task type %v", reply.Task)
		}
	}
}

func runMapTask(mapf func(string, string) []KeyValue, reply *RequestReply) {
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// Call user-defined `Map` function, maybe crash or slow
	kva := mapf(filename, string(content))

	// Put kv pairs into the corresponding bucket of each `Reduce` task
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	// Write these kv pairs into intermediate files
	tempFiles := make([]*os.File, reply.NReduce)
	id := reply.TaskID
	for bucket := 0; bucket < reply.NReduce; bucket++ {
		iname := fmt.Sprintf("mr-%d-%d", id, bucket)
		ifile, _ := os.CreateTemp(".", iname)
		tempFiles[bucket] = ifile
		enc := json.NewEncoder(ifile)
		for _, kv := range intermediate[bucket] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to an intermediate file %v", iname)
			}
		}
		ifile.Close()
	}

	for i, file := range tempFiles {
		os.Rename(file.Name(), fmt.Sprintf("mr-%d-%d", id, i))
	}

	call("Coordinator.FinishTask", &ReportArgs{TaskID: id}, &ReportReply{})
}

func runReduceTask(reducef func(string, []string) string, reply *RequestReply) {
	var intermediate []KeyValue
	id := reply.TaskID

	for i := 0; i < reply.NMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, id)
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}

	// sort all intermediate data of this `Reduce` partition by keys
	sort.Sort(ByKey(intermediate))
	ofile, _ := os.CreateTemp(".", "mr-temp-")

	// Do `Reduce` work
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// Call user-defined `Reduce` function
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()

	// Rename (atomically commit) the intermediate output file into a final one
	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", id))

	call("Coordinator.FinishTask", &ReportArgs{TaskID: id}, &ReportReply{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// server was closed
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
