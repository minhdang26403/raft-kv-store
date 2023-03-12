package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	idle = iota
	running
	completed
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	mu      sync.Mutex
	phase   int // the phase of MapReduce (0 is Map phase, 1 is Reduce phase)

	numMapGiven   int // the number of Map tasks assigned
	numMapRunning int // the number of Map tasks running
	mapPhase      sync.Cond
	mapTaskState  []int

	numReduceGiven   int // the number of Reduce tasks assigned
	numReduceRunning int // the number of Reduce tasks running
	reducePhase      sync.Cond
	reduceTaskState  []int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetMapTask(args *MapArgs, reply *MapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskNumber := c.numMapGiven
	if c.numMapGiven == len(c.files) {
		if c.numMapRunning == 0 {
			return fmt.Errorf("Map phase done")
		}
		c.mapPhase.Wait()
		if c.numMapGiven == len(c.files) && c.numMapRunning == 0 {
			return fmt.Errorf("Map phase done")
		}
		for i := 0; i < len(c.files); i++ {
			if c.mapTaskState[i] == idle {
				taskNumber = i
				break
			}
		}
	}
	reply.Filename = c.files[taskNumber]
	reply.TaskNumber = taskNumber
	reply.NReduce = c.nReduce
	c.numMapGiven++
	c.numMapRunning++
	return nil
}

func (c *Coordinator) CompleteMapTask(args *MapArgs, reply *MapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTaskState[args.TaskNumber] == completed {
		return nil
	}
	c.numMapRunning--
	c.mapTaskState[args.TaskNumber] = completed
	if c.numMapGiven == len(c.files) && c.numMapRunning == 0 {
		c.mapPhase.Broadcast()
		c.phase = 1
	}
	return nil
}

func (c *Coordinator) GetReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskNumber := c.numReduceGiven
	if c.numReduceGiven == c.nReduce {
		if c.numReduceRunning == 0 {
			return fmt.Errorf("Reduce phase done")
		}
		c.reducePhase.Wait()
		if c.numReduceGiven == c.nReduce && c.numReduceRunning == 0 {
			return fmt.Errorf("Reduce phase done")
		}
		for i := 0; i < len(c.files); i++ {
			if c.reduceTaskState[i] == idle {
				taskNumber = i
				break
			}
		}
	}

	reply.NMap = len(c.files)
	reply.TaskNumber = taskNumber
	c.numReduceGiven++
	c.numReduceRunning++

	return nil
}

func (c *Coordinator) CompleteReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceTaskState[args.TaskNumber] == completed {
		return nil
	}
	c.numReduceRunning--
	c.reduceTaskState[args.TaskNumber] = completed
	if c.numReduceGiven == c.nReduce && c.numReduceRunning == 0 {
		c.reducePhase.Broadcast()
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.numReduceGiven == c.nReduce && c.numReduceRunning == 0 {
		ret = true
	}

	return ret
}

func (c *Coordinator) CheckCrashWorkers() {
	for c.phase == 0 {
		time.Sleep(10 * time.Second)
		for i := 0; i < len(c.files); i++ {
			if c.mapTaskState[i] == running {
				c.numMapGiven--
				c.numMapRunning--
				c.mapTaskState[i] = idle
			}
		}
	}

	for !c.Done() {
		time.Sleep(10 * time.Second)
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskState[i] == running {
				c.numReduceGiven--
				c.numReduceRunning--
				c.reduceTaskState[i] = idle
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files,
		nReduce:         nReduce,
		mapTaskState:    make([]int, len(files)),
		reduceTaskState: make([]int, nReduce),
	}
	c.mapPhase = *sync.NewCond(&c.mu)
	c.reducePhase = *sync.NewCond(&c.mu)
	// Your code here.

	c.server()
	return &c
}
