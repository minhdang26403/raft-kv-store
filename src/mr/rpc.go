package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	NoTask
)

type RequestArgs struct {
}

type RequestReply struct {
	Filename string
	Task     TaskType
	TaskID   int
	NReduce  int
	NMap     int
}

type ReportArgs struct {
	TaskID int
}

type ReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
