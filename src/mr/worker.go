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

	RunMapTask(mapf)
	RunReduceTask(reducef)
}

// Get a Map task from the coordinator and run if there is one.
// Return if there is no more Map task
func RunMapTask(mapf func(string, string) []KeyValue) {
	for {
		reply := MapReply{}
		if ok := call("Coordinator.GetMapTask", &MapArgs{}, &reply); !ok {
			break
		}
		// Read assigned input split
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
		tempFiles := make([]string, reply.NReduce)
		for bucket := 0; bucket < reply.NReduce; bucket++ {
			ifile, _ := os.CreateTemp("", fmt.Sprintf("mr-%d-%d-", reply.TaskNumber, bucket))
			tempFiles[bucket] = ifile.Name()
			enc := json.NewEncoder(ifile)
			for _, kv := range intermediate[bucket] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write to an intermediate file %v", tempFiles[bucket])
				}
			}
			ifile.Close()
		}

		// Tell the coordinator that this task is complete and send the location of intermediate files
		ok := call("Coordinator.CompleteMapTask",
			&MapArgs{TaskNumber: reply.TaskNumber, IntermediateFiles: tempFiles},
			&MapReply{})
		// Clean up all the intermediate files if this task is already completed by another worker
		if !ok {
			for _, ifilename := range tempFiles {
				os.Remove(ifilename)
			}
		}
	}
}

// Get a Reduce task from the coordinator and run if there is one.
// Return if there is no more Reduce task
func RunReduceTask(reducef func(string, []string) string) {
	for {
		args := ReduceArgs{}
		reply := ReduceReply{}
		if ok := call("Coordinator.GetReduceTask", &args, &reply); !ok {
			break
		}

		var intermediate []KeyValue
		// save temporary filenames to delete later
		tempFiles := make([]string, len(reply.IntermediateFiles))
		copy(tempFiles, reply.IntermediateFiles)
		// Read all kv pairs of this `Reduce` partition
		for _, iname := range tempFiles {
			ifile, err := os.Open(iname)
			if err != nil {
				fmt.Println(tempFiles)
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
		ofile, _ := os.CreateTemp("", "mr-temp-")

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

		// Tell the coordinator that this task is complete
		taskNum := reply.TaskNumber
		ok := call("Coordinator.CompleteReduceTask", &ReduceArgs{TaskNumber: taskNum}, &ReduceReply{})
		if !ok {
			// Remove the intermediate output file if this task is completed by another worker
			os.Remove(ofile.Name())
		} else {
			// Remove all the intermediate files read from Map task
			for _, iname := range tempFiles {
				os.Remove(iname)
			}
			// Rename (atomically commit) the intermediate output file into the final one
			curDir, _ := os.Getwd()
			os.Rename(ofile.Name(), fmt.Sprintf("%s/mr-out-%d", curDir, taskNum))
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	
	return false
}
