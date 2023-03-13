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

	// Your worker implementation here.
	RunMapTask(mapf)
	RunReduceTask(reducef)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// Get a Map task from the coordinator and run if there is one.
// Return if there is no more Map task
func RunMapTask(mapf func(string, string) []KeyValue) {
	for {
		args := MapArgs{}
		reply := MapReply{}
		if ok := call("Coordinator.GetMapTask", &args, &reply); !ok {
			break
		}
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
		// Call user-defined `Map` function
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
			ifile, _ := ioutil.TempFile("", fmt.Sprintf("mr-%d-%d-", reply.TaskNumber, bucket))
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
		ok := call("Coordinator.CompleteMapTask",
			&MapArgs{TaskNumber: reply.TaskNumber, IntermediateFiles: tempFiles},
			&MapReply{})
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
		for i, iname := range reply.IntermediateFiles {
			ifile, err := os.Open(iname)
			tempFiles[i] = iname
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
		ofile, _ := ioutil.TempFile("", "mr-temp-")

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
		ok := call("Coordinator.CompleteReduceTask", &ReduceArgs{TaskNumber: reply.TaskNumber}, &ReduceReply{})
		if !ok {
			os.Remove(ofile.Name())
		} else {
			for _, iname := range tempFiles {
				os.Remove(iname)
			}
			curDir, _ := os.Getwd()
			os.Rename(ofile.Name(), fmt.Sprintf("%s/mr-out-%d", curDir, reply.TaskNumber))
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println(rpcname)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
