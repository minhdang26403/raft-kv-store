package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State uint8

const (
	follower State = iota
	candidate
	leader
)

func (s State) String() string {
	if s == follower {
		return "F"
	} else if s == candidate {
		return "C"
	} else if s == leader {
		return "L"
	} else {
		panic("invalid state " + string(rune(s)))
	}
}

func (log LogEntry) String() string {
	return fmt.Sprintf("%d", log.Term)
}

const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 600
)

func getElectionTimeout() time.Duration {
	// randomized election timeout in the range of 300-600ms
	ms := ElectionTimeoutMin + (rand.Int63() % (ElectionTimeoutMax - ElectionTimeoutMin))
	return time.Duration(ms) * time.Millisecond
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state           State
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	voteCount       int
	applyCh         chan ApplyMsg
	applyCond       *sync.Cond

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// sender is behind
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		// a candidate from a newer term asking for vote
		// => reset vote (?)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	candidateLastLogIndex := args.LastLogIndex
	candidateLastLogTerm := args.LastLogTerm
	voterLastLogIndex := len(rf.log) - 1
	voterLastLogTerm := rf.log[voterLastLogIndex].Term

	if candidateLastLogTerm < voterLastLogTerm {
		return
	}

	if candidateLastLogTerm == voterLastLogTerm && candidateLastLogIndex < voterLastLogIndex {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// reset election timer when granting vote to candidate
		rf.lastHeartbeat = time.Now()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		// sender is behind
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.Debug(dLog, "Check 2 failed: Follower's log: %v (%d)", rf.log, args.PrevLogTerm)
		return
	}

	if len(args.Entries) > 0 {
		if args.PrevLogIndex+1 < len(rf.log) && rf.log[args.PrevLogIndex+1].Term != args.Entries[0].Term {
			rf.log = rf.log[:args.PrevLogIndex+1]
		}
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+1+i < len(rf.log) && args.Entries[i].Term == rf.log[args.PrevLogIndex+1+i].Term {
				continue
			}
			rf.log = append(rf.log, args.Entries[i])
		}
	}
	// rf.Debug(dLog, "Follower's log: %s", rf.log)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.Debug(dCommit, "Follower committed %d", rf.commitIndex)
		rf.Debug(dLog, "Follower's log: %v", rf.log)
		rf.Debug(dLog, "Args from leader: %v", args)
		rf.applyCond.Signal()
	}

	rf.state = follower
	// reset election timer when hearing from CURRENT leader
	rf.lastHeartbeat = time.Now()
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int) {
	rf.mu.Lock()
	// state may change at the time we acquire the lock again
	if rf.state != candidate {
		rf.mu.Unlock()
		return
	}

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{Term: rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}
	// state change at the time processing the RPC reply
	if rf.state != candidate || rf.currentTerm != args.Term {
		return
	}

	if reply.VoteGranted {
		rf.voteCount += 1
		if rf.voteCount > len(rf.peers)/2 {
			rf.state = leader
			numServer := len(rf.peers)
			for i := 0; i < numServer; i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			// rf.Debug(dLeader, "New leader's log: %v", rf.log)

			for id := range rf.peers {
				if id == rf.me {
					continue
				}
				go rf.sendAppendEntries(id)
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	// state may change at the time we acquire the lock again
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[server] == 0 {
		rf.mu.Unlock()
		return
	}
	var entries []LogEntry
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	lastLogIndex := len(rf.log) - 1

	if lastLogIndex >= rf.nextIndex[server] {
		// rf.Debug(dLeader, "Leader's log: %v", rf.log)
		// rf.Debug(dLeader, "Sent next entries starting at %d", rf.nextIndex[server])
		for i := rf.nextIndex[server]; i <= lastLogIndex; i++ {
			entries = append(entries, rf.log[i])
		}
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
	}
	// check that the term hasn't changed since sending the RPC
	if rf.state != leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		rf.nextIndex[server] = lastLogIndex + 1
		rf.matchIndex[server] = lastLogIndex
		// Test if log entries in current term are safely replicated
		for N := lastLogIndex; N > rf.commitIndex; N-- {
			replicateCount := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					replicateCount++
				}
			}
			if replicateCount > len(rf.peers)/2 {
				rf.commitIndex = N
				rf.Debug(dLeader, "Leader committed %d", rf.commitIndex)
				rf.Debug(dLog, "Leader's log: %v", rf.log)
				rf.applyCond.Signal()
				break
			}
		}
	} else {
		rf.Debug(dLeader, "sendAppendEntries: Found a conflicting entry at %d for server %d",
			rf.nextIndex[server] - 1, server)
		rf.nextIndex[server]--
		go rf.sendAppendEntries(server)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == leader

	// Issues AppendEntries immediately or later ???
	if isLeader {
		entry := LogEntry{Command: command, Term: rf.currentTerm}
		rf.Debug(dLeader, "Leader received new log entry: %v", entry)
		rf.log = append(rf.log, entry)
		// rf.Debug(dLeader, "Leader's log: %v", rf.log)
		rf.matchIndex[rf.me] = index
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionRoutine() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != leader && time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			rf.state = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			// reset election timer
			rf.electionTimeout = getElectionTimeout()
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()

			for id := range rf.peers {
				if id == rf.me {
					continue
				}
				go rf.sendRequestVote(id)
			}
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) replicationRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == leader {
			rf.mu.Unlock()
			for id := range rf.peers {
				if id == rf.me {
					continue
				}
				go rf.sendAppendEntries(id)
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) applyRoutine() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		queue := make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			queue = append(queue, msg)
		}
		rf.mu.Unlock()

		for _, msg := range queue {
			rf.applyCh <- msg
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())
	rf.state = follower
	rf.electionTimeout = getElectionTimeout()
	rf.lastHeartbeat = time.Now()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// persistent state on all servers
	rf.currentTerm = 0
	rf.votedFor = -1
	// log's indexing starts from 1
	rf.log = append(rf.log, LogEntry{})

	numServer := len(rf.peers)
	rf.nextIndex = make([]int, numServer)
	rf.matchIndex = make([]int, numServer)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start election goroutine
	go rf.electionRoutine()

	// start heartbeat goroutine to periodically send heartbeat messages
	// if this server is a leader
	go rf.replicationRoutine()

	go rf.applyRoutine()

	return rf
}
