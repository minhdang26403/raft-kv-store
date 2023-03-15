package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// State of workers
const (
	idle = iota
	running
	complete
)

// Phase of the MapReduce computation
const (
	Map = iota
	Reduce
	Done
)

const TimeOut = 10 * time.Second

type TaskStat struct {
	state int
	startTime time.Time
}

type Coordinator struct {
	files   []string   // input files to process
	nMap    int        // number of input splits
	nReduce int        // number of `Reduce` partitions
	mu      sync.Mutex // lock to protect the internal state of the coordinator
	phase   int        // the phase of MapReduce

	numMapComplete int // the number of complete Map tasks
	mapTasks   []TaskStat

	numReduceComplete int // the number of complete Reduce tasks
	reduceTasks   []TaskStat
}

// RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case Map:
		reply.Task = MapTask
		reply.NReduce = c.nReduce
		for i, task := range c.mapTasks {
			now := time.Now()
			if task.state == running && task.startTime.Add(TimeOut).Before(now) {
				task.state = idle
			}
			if task.state == idle {
				reply.TaskID = i
				reply.Filename = c.files[i]
				c.mapTasks[i].state = running
				c.mapTasks[i].startTime = now
				return nil
			}
		}
		reply.Task = WaitTask
	case Reduce:
		reply.Task = ReduceTask
		reply.NMap = c.nMap
		for i, task := range c.reduceTasks {
			now := time.Now()
			if task.state == running && task.startTime.Add(TimeOut).Before(now) {
				task.state = idle
			}
			if task.state == idle {
				reply.TaskID = i
				c.reduceTasks[i].state = running
				c.reduceTasks[i].startTime = now
				return nil
			}
		}
		reply.Task = WaitTask
	case Done:
		reply.Task = NoTask
	}
	return nil
}

func (c *Coordinator) FinishTask(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case Map:
		c.mapTasks[args.TaskID].state = complete
		c.numMapComplete++
		if c.numMapComplete == c.nMap {
			c.phase = Reduce
		}
	case Reduce:
		c.reduceTasks[args.TaskID].state = complete
		c.numReduceComplete++
		if c.numReduceComplete == c.nReduce {
			c.phase = Done
		}
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:           files,
		nMap:            len(files),
		nReduce:         nReduce,
		phase:           Map,
		mapTasks:    make([]TaskStat, len(files)),
		reduceTasks: make([]TaskStat, nReduce),
	}

	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i].state = idle
	}

	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i].state = idle
	}

	c.server()
	return &c
}
