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
	"6.5840/labgob"
	"bytes"
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	XCommitIndex int //commit index at destination server
	XTerm        int // term of conflicting entry
	XIndex       int // index of first entry with XTerm
	XLen         int // length of log
}

type InstallSnapshotArgs struct {
	Term              int //leader’s term
	LeaderId          int //so follower can redirect clients
	LastIncludedIndex int //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int //term of lastIncludedIndex
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int //CurrentTerm, for leader to update itself
}

type StateNode int

const (
	LEADER    StateNode = 0
	CANDIDATE StateNode = 1
	FOLLOWER  StateNode = 2
)

type LogEntry struct {
	Term         int
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's State
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted State
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server must maintain.
	applyChan       chan ApplyMsg
	LastHeartbeatTs int64
	State           StateNode
	CurrentTerm     int
	VotedFor        map[int]int
	Logs            []LogEntry

	CommitIndex int //index of highest log entry known to be committed, from 0
	LastApplied int //index of highest log entry applied to State machine, from 0

	nextIndex  map[int]int
	matchIndex map[int]int

	maxTermReplyVote int   // max term that receive from others
	terms            []int // terms in current log

	LastIncludedIndex int //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int //term of lastIncludedIndex
	//SnapshotSaved       []byte
	//snapshotIndex       []int
	//snapshotCommitIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = rf.State == LEADER
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor[rf.CurrentTerm])
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.LastApplied)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor[rf.CurrentTerm])
	e.Encode(rf.Logs)
	e.Encode(rf.CommitIndex)
	e.Encode(rf.LastApplied)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted State.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any State?
		return
	}
	// Your code here (2C).

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var commitIndex int
	var lastApplied int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		//error...
		Debug(dError, "%d Error readPersist null!", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = make(map[int]int)
		rf.VotedFor[currentTerm] = votedFor
		rf.Logs = logs
		rf.CommitIndex = commitIndex
		rf.LastApplied = lastApplied
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm

		//rf.CommitIndex = commitIndex
		//rf.LastApplied = lastApplied

		//if rf.LastApplied < rf.CommitIndex {
		//	maxIndex := rf.CommitIndex
		//	if maxIndex > len(rf.Logs) {
		//		maxIndex = len(rf.Logs)
		//	}
		//	stableLogs := rf.Logs[rf.LastApplied:maxIndex]
		//	if len(stableLogs) > 0 {
		//		Debug(dCommit, "%d apply stableLogs=(%v), (%d,%d)", rf.me, stableLogs, rf.CommitIndex, maxIndex)
		//		for _, stableLog := range stableLogs {
		//			applyMsg := &ApplyMsg{CommandValid: true, Command: stableLog.Command, CommandIndex: stableLog.CommandIndex}
		//			rf.applyChan <- *applyMsg
		//			rf.LastApplied++
		//		}
		//		Debug(dCommit, "%d apply done stableLogs=(%v), (%d, %d)", rf.me, stableLogs, rf.CommitIndex, rf.LastApplied)
		//	}
		//	rf.LastApplied = maxIndex
		//	rf.persist()
		//} else {
		//	Debug(dCommit, "%d Nothing left to apply", rf.me)
		//}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func(indexSnapshot int, dataSnapshot []byte) {
		rf.mu.Lock()
		Debug(dSnap, "%d snapshot index=%d, rf=%v", rf.me, indexSnapshot, rf)
		if indexSnapshot <= rf.LastIncludedIndex {
			rf.mu.Unlock()
			return
		}
		tmpLastIncludedTerm := rf.Logs[rf.getLogIndexFrom0(indexSnapshot)].Term
		tmpLastIncludedIndex := indexSnapshot

		rf.Logs = append(make([]LogEntry, 0), rf.Logs[rf.getLogIndexFrom1(indexSnapshot):]...)
		rf.LastIncludedTerm = tmpLastIncludedTerm
		rf.LastIncludedIndex = tmpLastIncludedIndex

		Debug(dSnap, "%d done snapshot index=%d, rf=%v", rf.me, indexSnapshot, rf)
		rf.persistStateAndSnapshot(dataSnapshot)
		rf.mu.Unlock()
	}(index, snapshot)

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
	Term         int
	VoteGranted  bool
	LeaderId     int // current leader id
	LeaderTerm   int // current leader term at that server
	LastLogIndex int // last log index
	LastLogTerm  int // last log term
}

// example RequestVote RPC handler.
// handling RequestVote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	Debug(dVote, "%d (%v) receive RequestVote from %d (args=%v)", rf.me, rf, args.CandidateId, args)

	yes := args.Term >= rf.CurrentTerm
	if yes {
		votedId, exist := rf.VotedFor[args.Term]
		yes = !exist || args.CandidateId == votedId
	}

	if yes {
		if rf.LastIncludedIndex != 0 || len(rf.Logs) != 0 {
			lastLogTerm := rf.LastIncludedTerm
			lastLogIndex := rf.LastIncludedIndex
			if len(rf.Logs) != 0 {
				lastLogIndex = rf.Logs[len(rf.Logs)-1].CommandIndex
				lastLogTerm = rf.Logs[len(rf.Logs)-1].Term
			}
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				yes = true
			} else {
				yes = false
			}
		}
	}

	if yes {
		rf.VotedFor[args.Term] = args.CandidateId
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.LastHeartbeatTs = time.Now().UnixMilli()
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		Debug(dVote, "%d (%v) vote YES RequestVote from %d (args=%v)", rf.me, rf, args.CandidateId, args)

	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		if args.Term > rf.CurrentTerm {
			rf.State = FOLLOWER
			rf.CurrentTerm = args.Term
			Debug(dVote, "%d back to follower", rf.me)
		}
		Debug(dVote, "%d (%v) reject RequestVote from %d (args=%v)", rf.me, rf, args.CandidateId, args)
	}
}

// AppendEntries RPC handler.
// handling AppendEntries
func (rf *Raft) ReceiveAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	Debug(dLog, "%d (CurrentTerm=%d, State=%d, voteFor=%d, CommitIndex=%d, LastApplied=%d, lastIncludedIndex=%d, lastIncludedTerm=%d, Logs=%v) receive AppendAntries from %d (term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, LogEntries=%v, LeaderCommit=%d)",
		rf.me, rf.CurrentTerm, rf.State, rf.VotedFor, rf.CommitIndex, rf.LastApplied, rf.LastIncludedIndex, rf.LastIncludedTerm,
		rf.Logs, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LogEntries, args.LeaderCommit)

	accept := false

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = FOLLOWER
		if len(args.LogEntries) == 0 {
			v, has := rf.VotedFor[args.Term]
			if !has || v == args.LeaderId {
				rf.LastHeartbeatTs = time.Now().UnixMilli()
				rf.VotedFor[args.Term] = args.LeaderId
			}
		}
	} else if (len(rf.Logs) != 0 || rf.LastIncludedIndex != 0) && args.Term == rf.CurrentTerm && rf.VotedFor[rf.CurrentTerm] == args.LeaderId {
		lastTerm := rf.LastIncludedTerm
		lastIndex := rf.LastIncludedIndex
		if len(rf.Logs) != 0 {
			lastTerm = rf.Logs[len(rf.Logs)-1].Term
			lastIndex = rf.Logs[len(rf.Logs)-1].CommandIndex
		}

		if len(args.LogEntries) == 0 {
			if args.PrevLogTerm > lastTerm || (args.PrevLogTerm == lastTerm && args.PrevLogIndex >= lastIndex) {
				rf.LastHeartbeatTs = time.Now().UnixMilli()
			}
		} else {
			argsLastLogEntry := args.LogEntries[len(args.LogEntries)-1]
			if argsLastLogEntry.Term > lastTerm || (argsLastLogEntry.Term == lastTerm && argsLastLogEntry.CommandIndex >= lastIndex) {
				rf.LastHeartbeatTs = time.Now().UnixMilli()
			}
		}
	}
	if args.PrevLogIndex < rf.LastIncludedIndex {
		accept = false
	} else if args.Term >= rf.CurrentTerm {
		if rf.getLogIndexLogicalFrom1(len(rf.Logs)) < args.PrevLogIndex {
			accept = false
		} else if len(rf.Logs) != 0 || rf.LastIncludedIndex != 0 {
			if args.PrevLogIndex != 0 {
				curPrevIndexEntryTerm := rf.LastIncludedTerm
				if args.PrevLogIndex > rf.LastIncludedIndex {
					curPrevIndexEntryTerm = rf.Logs[rf.getLogIndexFrom0(args.PrevLogIndex)].Term
				}
				if args.PrevLogTerm == curPrevIndexEntryTerm { // same current previous entry
					accept = true
					if args.Term == rf.CurrentTerm {
						if rf.State != FOLLOWER {
							rf.State = FOLLOWER
							rf.VotedFor[args.Term] = args.LeaderId
							Debug(dLog, "%d back to follower due to equal current term of %v", rf.me, args)
						}
					}
				}
			} else {
				accept = true
			}

			if accept {
				if rf.CommitIndex > args.PrevLogIndex {
					if len(args.LogEntries) == 0 {
						accept = false
					}
					for i := 0; i+args.PrevLogIndex < rf.CommitIndex && i < len(args.LogEntries) && i+args.PrevLogIndex < rf.getLogIndexLogicalFrom1(len(rf.Logs)); i++ {
						if args.LogEntries[i].Term != rf.Logs[rf.getLogIndexFrom1(i+args.PrevLogIndex)].Term {
							accept = false
							reply.XIndex = rf.getLogIndexLogicalFrom1(len(rf.Logs))
							reply.XTerm = 0
							Debug(dWarn, "CommitIndex > PrevLogIndex %d (CurrentTerm=%d, State=%d, voteFor=%d, CommitIndex=%d, LastApplied=%d, lastIncludedIndex=%d, lastIncludedTerm=%d, Logs=%v) receive AppendAntries from %d (term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, LogEntries=%v, LeaderCommit=%d)",
								rf.me, rf.CurrentTerm, rf.State, rf.VotedFor, rf.CommitIndex, rf.LastApplied, rf.LastIncludedIndex, rf.LastIncludedTerm,
								rf.Logs, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LogEntries, args.LeaderCommit)
							break
						}
					}
				}
			}
		} else {
			accept = true
			if args.Term == rf.CurrentTerm && rf.State != FOLLOWER {
				rf.State = FOLLOWER
				rf.VotedFor[args.Term] = args.LeaderId
				Debug(dLog, "%d back to follower due to equal current term of %v", rf.me, args)
			}
		}
	} else { //args.Term < rf.CurrentTerm
		//another server establishes itself as leader
		Debug(dLog, "%d another server establishes itself as leader %v", rf.me, args)
		accept = false
	}

	if accept {
		i := 0
		for i = 0; i+args.PrevLogIndex < rf.getLogIndexLogicalFrom1(len(rf.Logs)) && i < len(args.LogEntries); i++ {
			if args.LogEntries[i].Term != rf.Logs[rf.getLogIndexFrom1(i+args.PrevLogIndex)].Term {
				rf.Logs = rf.Logs[0:rf.getLogIndexFrom1(i+args.PrevLogIndex)]
				Debug(dWarn, "Conflict log %d (CurrentTerm=%d, State=%d, voteFor=%d, CommitIndex=%d, LastApplied=%d, lastIncludedIndex=%d, lastIncludedTerm=%d, Logs=%v) receive AppendAntries from %d (term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, LogEntries=%v, LeaderCommit=%d)",
					rf.me, rf.CurrentTerm, rf.State, rf.VotedFor, rf.CommitIndex, rf.LastApplied, rf.LastIncludedIndex, rf.LastIncludedTerm,
					rf.Logs, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LogEntries, args.LeaderCommit)
				break
			}
		}

		if i < len(args.LogEntries) {
			newEntries := args.LogEntries[i:]
			rf.Logs = append(rf.Logs, newEntries...)
		}
	}

	if accept {
		rf.State = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.LastHeartbeatTs = time.Now().UnixMilli()
		rf.VotedFor[args.Term] = args.LeaderId

		reply.Term = rf.CurrentTerm
		reply.Success = true
		reply.XLen = rf.getLogIndexLogicalFrom1(len(rf.Logs))
		if reply.XLen > args.PrevLogIndex+len(args.LogEntries) {
			reply.XLen = args.PrevLogIndex + len(args.LogEntries)
		}
		for k, _ := range rf.VotedFor {
			if k > args.Term {
				delete(rf.VotedFor, k)
			}
		}
		if rf.CommitIndex < args.LeaderCommit {
			rf.CommitIndex = min(args.LeaderCommit, rf.getLogIndexLogicalFrom1(len(rf.Logs)))
			rf.Commit()
		}

		Debug(dLog, "%d (CurrentTerm=%d, State=%d, voteFor=%d, CommitIndex=%d, LastApplied=%d, lastIncludedIndex=%d, lastIncludedTerm=%d, Logs=%v) apply AppendAntries from %d (term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, LogEntries=%v, LeaderCommit=%d)",
			rf.me, rf.CurrentTerm, rf.State, rf.VotedFor, rf.CommitIndex, rf.LastApplied, rf.LastIncludedIndex, rf.LastIncludedTerm,
			rf.Logs, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LogEntries, args.LeaderCommit)
	} else {
		reply.Success = false
		reply.XLen = rf.getLogIndexLogicalFrom1(len(rf.Logs))
		reply.Term = rf.CurrentTerm
		if args.PrevLogIndex == 0 {
			reply.XIndex = 0
			reply.XTerm = 0
		} else if rf.getLogIndexLogicalFrom1(len(rf.Logs)) < args.PrevLogIndex {
			reply.XIndex = rf.getLogIndexLogicalFrom1(len(rf.Logs))
			reply.XTerm = 0
		} else if args.PrevLogIndex > rf.LastIncludedIndex {
			curPrevIndexEntry := rf.Logs[rf.getLogIndexFrom0(args.PrevLogIndex)]
			reply.XTerm = curPrevIndexEntry.Term

			xIndex := args.PrevLogIndex
			for {
				if xIndex <= 0 || rf.Logs[rf.getLogIndexFrom0(xIndex)].Term != reply.XTerm {
					break
				}
				xIndex--
			}
			reply.XIndex = xIndex + 1
		}
		Debug(dLog, "%d (CurrentTerm=%d, State=%d, voteFor=%d, CommitIndex=%d, LastApplied=%d, lastIncludedIndex=%d, lastIncludedTerm=%d, Logs=%v) reject AppendAntries from %d (term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, LogEntries=%v, LeaderCommit=%d)",
			rf.me, rf.CurrentTerm, rf.State, rf.VotedFor, rf.CommitIndex, rf.LastApplied, rf.LastIncludedIndex, rf.LastIncludedTerm,
			rf.Logs, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LogEntries, args.LeaderCommit)
	}
	return
}

func (rf *Raft) Commit() {
	if rf.LastApplied < rf.CommitIndex {
		Debug(dCommit, "%d needApplyStateMachine=(%d,%d]", rf.me, rf.LastApplied, rf.CommitIndex)
		for rf.LastApplied < rf.CommitIndex {
			rf.LastApplied++
			applyEntry := rf.getLogEntry(rf.LastApplied)
			applyMsg := &ApplyMsg{CommandValid: true, Command: applyEntry.Command, CommandIndex: applyEntry.CommandIndex}
			Debug(dCommit, "%d applyMsg=%v", rf.me, applyMsg)
			rf.applyChan <- *applyMsg
		}
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveAppendEntries", args, reply)
	return ok
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
	currentTerm := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm, isLeader = rf.GetState()
	if !isLeader {
		return index, currentTerm, isLeader
	}

	index = rf.getIndexForIncomingCommand()
	newEntry := &LogEntry{Term: rf.CurrentTerm, Command: command, CommandIndex: index}
	rf.Logs = append(rf.Logs, *newEntry)
	rf.persist()
	Debug(dLeader, "%d (term=%d, isLeader=%v (%d) init command(command=%v)", rf.me, currentTerm, isLeader, len(rf.peers), command)
	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}
		go func(destId int, term int) {
			// prepare params
			rf.mu.Lock()
			if rf.State != LEADER || rf.CurrentTerm != term {
				rf.mu.Unlock()
				return
			}
			args := &AppendEntriesArgs{Term: term, LeaderId: rf.me, LeaderCommit: rf.CommitIndex}
			if rf.nextIndex[destId]-1 >= rf.getLogIndexLogicalFrom1(len(rf.Logs)) {
				Debug(dDrop, "%d -> %d send appendEntry emptylog [%d,%d] %v", rf.me, destId, rf.nextIndex[destId]-1, rf.getLogIndexLogicalFrom1(len(rf.Logs)), rf.Logs)
				rf.mu.Unlock()
				return
			}
			appendLogs := rf.Logs[rf.getLogIndexFrom0(rf.nextIndex[destId]):len(rf.Logs)]
			args.LogEntries = appendLogs
			args.PrevLogIndex = rf.nextIndex[destId] - 1
			args.PrevLogTerm = rf.LastIncludedTerm
			if args.PrevLogIndex > rf.LastIncludedIndex {
				args.PrevLogTerm = rf.Logs[rf.getLogIndexFrom0(args.PrevLogIndex)].Term
			}
			reply := &AppendEntriesReply{}
			Debug(dLeader, "%d -> %d send appendEntry(Term=%d, LeaderId=%d, PrevLogIndex=%d, PrevLogTerm=%d, LogEntries=%v, LeaderCommit=%d)",
				rf.me, destId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LogEntries, args.LeaderCommit)
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(destId, args, reply)
			if !ok {
				return
			}
			rf.processAppendEntryResponse(destId, reply)
		}(serverId, currentTerm)
	}

	DPrintf("%d (term=%d, isLeader=%v) end command(command=%v, index=%d)", rf.me, currentTerm, isLeader, command, index)
	return index, currentTerm, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 150 and 500
		// milliseconds.
		ms := rand.Intn(200) + 300
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		_, isLeader := rf.GetState()
		if !isLeader {
			currentTs := time.Now().UnixMilli()
			if currentTs-rf.LastHeartbeatTs < 200 {
				Debug(dInfo, "%d (term=%d, State=%d, lastHeartbeat=%d, delta=%d) still valid in timeout", rf.me, rf.CurrentTerm, rf.State, rf.LastHeartbeatTs, currentTs-rf.LastHeartbeatTs)
			} else {
				Debug(dInfo, "%d (term=%d, State=%d, lastHeartbeat=%d, delta=%d) invalid timeout", rf.me, rf.CurrentTerm, rf.State, rf.LastHeartbeatTs, currentTs-rf.LastHeartbeatTs)

				rf.State = CANDIDATE
				rf.CurrentTerm++
				if rf.maxTermReplyVote > rf.CurrentTerm {
					rf.CurrentTerm = rf.maxTermReplyVote + 1
				}
				rf.VotedFor[rf.CurrentTerm] = rf.me
				votes := 1
				canFinish := false

				// prepare request params
				voteArgs := &RequestVoteArgs{}
				voteArgs.Term = rf.CurrentTerm
				voteArgs.CandidateId = rf.me
				if rf.LastIncludedIndex != 0 || len(rf.Logs) > 0 {
					voteArgs.LastLogIndex = rf.LastIncludedIndex
					voteArgs.LastLogTerm = rf.LastIncludedTerm
					if len(rf.Logs) != 0 {
						voteArgs.LastLogIndex = rf.getLogIndexLogicalFrom1(len(rf.Logs))
						voteArgs.LastLogTerm = rf.Logs[len(rf.Logs)-1].Term
					}
				}

				for serverId := range rf.peers {
					if serverId == rf.me {
						continue
					}

					go func(destId int, args *RequestVoteArgs, reply *RequestVoteReply) {
						Debug(dVote, "%d start sending RequestVote %v", rf.me, args)
						if rf.State != CANDIDATE || args.Term != rf.CurrentTerm {
							return
						}
						ok := rf.sendRequestVote(destId, args, reply)
						if !ok {
							return
						}

						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.State != CANDIDATE || reply.Term < rf.CurrentTerm {
							return
						}
						Debug(dVote, "%d vote %d %v", destId, rf.me, reply)
						if reply.VoteGranted && !canFinish {
							votes++
						} else if !reply.VoteGranted && !canFinish {
							if reply.Term > rf.CurrentTerm {
								canFinish = true
								rf.State = FOLLOWER
								rf.CurrentTerm = reply.Term
								Debug(dDrop, "%d become follower after receive Vote of %d %v %v", rf.me, destId, reply, rf)
							}
						}
						if votes <= len(rf.peers)/2 || canFinish {
							return
						}

						canFinish = true
						rf.State = LEADER
						rf.maxTermReplyVote = -1
						rf.CommitIndex = rf.LastApplied
						rf.VotedFor[args.Term] = rf.me
						for dId := range rf.peers {
							rf.nextIndex[dId] = rf.LastIncludedIndex + len(rf.Logs) + 1
						}
						for dId := range rf.peers {
							rf.matchIndex[dId] = 0
						}
						for k, _ := range rf.VotedFor {
							if k > rf.CurrentTerm {
								delete(rf.VotedFor, k)
							}
						}
						Debug(dLeader, "%d got enough votes (%d) => %d now is leader (%v)", rf.me, votes, rf.me, rf)

						rf.persist()
						go rf.sendHeartbeat()
					}(serverId, voteArgs, &RequestVoteReply{})
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		currentTerm, isLeader := rf.GetState()
		if isLeader {
			Debug(dLeader, "%d start sending heartbeat %v", rf.me, rf)
			for serverId := range rf.peers {
				if serverId == rf.me {
					continue
				}
				go func(destId int, term int) {
					// prepare heartbeat params
					rf.mu.Lock()
					if rf.State != LEADER || rf.CurrentTerm != term {
						rf.mu.Unlock()
						return
					}
					args := &AppendEntriesArgs{Term: term, LeaderId: rf.me, LeaderCommit: rf.CommitIndex}
					appendLogs := rf.Logs[rf.getLogIndexFrom0(rf.nextIndex[destId]):len(rf.Logs)]
					args.LogEntries = appendLogs
					args.PrevLogIndex = rf.nextIndex[destId] - 1
					args.PrevLogTerm = rf.LastIncludedTerm
					if args.PrevLogIndex > rf.LastIncludedIndex {
						args.PrevLogTerm = rf.Logs[rf.getLogIndexFrom0(args.PrevLogIndex)].Term
					}
					reply := &AppendEntriesReply{}
					rf.mu.Unlock()

					ok := rf.sendAppendEntries(destId, args, reply)
					if !ok {
						return
					}
					rf.processAppendEntryResponse(destId, reply)
				}(serverId, currentTerm)
			}
		}
		rf.mu.Unlock()

		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) processAppendEntryResponse(destId int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State == LEADER && reply.Term > rf.CurrentTerm {
		rf.State = FOLLOWER
		rf.CurrentTerm = reply.Term
		rf.persist()
		Debug(dDrop, "%d become follower after receive APC of %d %v %v", rf.me, destId, reply, rf)
	}

	if rf.State != LEADER || reply.Term != rf.CurrentTerm {
		Debug(dDrop, "%d drop receive APC of %d %v %v", rf.me, destId, reply, rf)
		return
	}

	if rf.matchIndex[destId] == rf.getLogIndexLogicalFrom1(len(rf.Logs)) {
		return
	}

	if reply.Success {
		Debug(dLog, "%d receive success APC of %d %v", rf.me, destId, reply)
		if reply.XLen <= rf.matchIndex[destId] && rf.matchIndex[destId] != 0 {
			return
		}

		if reply.XLen >= rf.nextIndex[destId]-1 {
			rf.nextIndex[destId] = reply.XLen + 1
			rf.matchIndex[destId] = reply.XLen
			Debug(dLog, "%d update nextIndex[%d]=%d", rf.me, destId, rf.nextIndex[destId])
		}
		//check, update CommitIndex, save stable log
		matchIndexCountNServer := make(map[int]int)
		for _, nrep := range rf.matchIndex {
			if nrep == 0 {
				continue
			}
			if _, exist := matchIndexCountNServer[nrep]; !exist {
				count := 0
				for _, val := range rf.matchIndex {
					if val >= nrep {
						count++
					}
				}
				matchIndexCountNServer[nrep] = count
			}
		}
		Debug(dLog, "%d rf.matchIndex[%v]", rf.me, rf.matchIndex)
		Debug(dLog, "%d matchIndexCountNServer[%v]", rf.me, matchIndexCountNServer)

		maxIndex := 0
		for Nindex, count := range matchIndexCountNServer {
			if Nindex > maxIndex && count >= (len(rf.peers)-1)/2 && Nindex > rf.CommitIndex {
				if rf.getLogIndexLogicalFrom1(len(rf.Logs)) >= Nindex && rf.Logs[rf.getLogIndexFrom0(Nindex)].Term == rf.CurrentTerm {
					maxIndex = Nindex
				} else if rf.getLogIndexLogicalFrom1(len(rf.Logs)) >= Nindex && count == len(rf.peers)-1 {
					//that entry is stored on every server
					maxIndex = Nindex
				}
			}
		}
		if maxIndex > 0 {
			rf.CommitIndex = maxIndex
			rf.Commit()
			rf.persist()
		}
	} else if !reply.Success {
		tmpNextIndex := rf.nextIndex[destId]
		Debug(dLog, "%d receive reject APC of %d %v", rf.me, destId, reply)
		if reply.XTerm == 0 {
			rf.nextIndex[destId] = reply.XIndex + 1
			Debug(dLog, "%d update nextIndex[%d]=%d", rf.me, destId, rf.nextIndex[destId])
			return
		}
		lastEntryForXTerm := rf.getLogIndexFrom1(reply.XIndex)
		for lastEntryForXTerm < len(rf.Logs) && rf.Logs[lastEntryForXTerm].Term == reply.XTerm {
			lastEntryForXTerm++
		}
		tmpNextIndex = lastEntryForXTerm
		if tmpNextIndex > reply.XLen+1 {
			tmpNextIndex = reply.XLen + 1
		}
		if tmpNextIndex <= 0 {
			tmpNextIndex = 1
		}
		if tmpNextIndex < rf.nextIndex[destId] {
			rf.nextIndex[destId] = tmpNextIndex
		}
		Debug(dLog, "%d update nextIndex[%d]=%d", rf.me, destId, rf.nextIndex[destId])
	}
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	return rf.Logs[index-rf.LastIncludedIndex-1]
}

func (rf *Raft) getLogIndexFrom0(index int) int {
	realIndex := index - rf.LastIncludedIndex - 1
	if realIndex < 0 {
		realIndex = 0
	}
	return realIndex
}

func (rf *Raft) getLogIndexFrom1(index int) int {
	realIndex := index - rf.LastIncludedIndex
	return realIndex
}

func (rf *Raft) getLogIndexLogicalFrom1(index int) int {
	return index + rf.LastIncludedIndex
}

func (rf *Raft) getIndexForIncomingCommand() int {
	if len(rf.Logs) == 0 {
		return rf.LastIncludedIndex + 1
	}
	return rf.Logs[len(rf.Logs)-1].CommandIndex + 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent State, and also initially holds the most
// recent saved State, if any. applyCh is a channel on which the
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
	rf.State = FOLLOWER
	rf.CurrentTerm = 0
	rf.VotedFor = make(map[int]int)
	rf.applyChan = applyCh
	rf.maxTermReplyVote = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.LastApplied = 0
	rf.CommitIndex = 0

	// initialize from State persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	Debug(dClient, "%d Started (State=%d, CurrentTerm=%d)", rf.me, rf.State, rf.CurrentTerm)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
