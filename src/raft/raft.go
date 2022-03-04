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

	"bytes"
	"context"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	TermNumber int
	Command    interface{}
	// index      int
}
type LeaderState struct {
	// nextIndex map[int]int
	// matchIndex map[int]int
}

type Volatile struct {
	// commitIndex int
	// lastApplied int
	// leaderState *LeaderState
}
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	volatile          Volatile
	leaderId          int
	logs              []Log
	votedFor          int
	currentTerm       int
	status            int // 0 follower 1 candidate 2 leader
	nextIndex         []int
	matchIndex        []int
	lastApplied       int
	commitIndex       int
	startTime         time.Time
	electionTimeout   time.Duration
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	applyCh           chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func CutLog(a []Log, term int) []Log {
	tmp := make([]Log, 1)
	tmp[0].TermNumber = term
	tmp = append(tmp, a...)
	return tmp
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.status == 2 {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	data := w.Bytes()

	rf.persister.SaveRaftState(data)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}
func (rf *Raft) Convert(index int) int { // 我们每次在log前插入了一个空log entry，并且我们需要保证这个log entry不被执行
	return index - rf.lastIncludedIndex

}

func (rf *Raft) Reconvert(index int) int {
	return index + rf.lastIncludedIndex
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Log
	var votedFor int
	var currentTerm int
	if d.Decode(&logs) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&currentTerm) != nil {
		DPrintf("rf[%v] decode err", rf.me)
	} else {
		rf.logs = logs
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
	}
}
func (rf *Raft) readSnap(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snap []byte
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&snap) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		DPrintf("rf[%v] decode err", rf.me)
	} else {
		rf.snapshot = snap
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("rf[%v] read snap lastIndexedIndex %v lastInclydeTerm %v commitIndex %v lastapplied %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.commitIndex, rf.lastApplied)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// simply return true
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 意思是服务器定期会调用这个函数，index之前（包括index）对应的log都不在被需要
	// 在做快照的时候，可以在log最前插入一个空log entry，并且这个log entry的term为lastIncludeTerm
	if index <= rf.lastIncludedIndex {
		return // 说明是旧的快照
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("rf[%v] doing snap index %v", rf.me, index)
	DPrintf("rf[%v] logs %v", rf.me, rf.logs)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	state := w.Bytes()

	logs := make([]Log, 1)
	logs[0].TermNumber = rf.logs[rf.Convert(index)].TermNumber
	rf.logs = append(logs, rf.logs[rf.Convert(index)+1:len(rf.logs)]...) // 切除log

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[rf.Convert(index)].TermNumber
	rf.snapshot = snapshot

	w = new(bytes.Buffer)
	e = labgob.NewEncoder(w)
	e.Encode(snapshot)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	snap := w.Bytes()
	// rf.persister.SaveStateAndSnapshot(data, snap)

	rf.persister.SaveStateAndSnapshot(state, snap)
	DPrintf("rf[%v] done snap", rf.me)
	DPrintf("rf[%v] logs %v", rf.me, rf.logs)
}

type InstallsnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapShot          []byte
}
type InstallsnapshotReply struct {
	Term int
}

func (rf *Raft) Installsnapshot(args *InstallsnapshotArgs, reply *InstallsnapshotReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.status = 0
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	} else {
		rf.startTime = time.Now()
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	reply.Term = rf.currentTerm

	msg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		Snapshot:      args.SnapShot,
	}
	rf.applyCh <- msg
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	if len(rf.logs) <= rf.Convert(args.LastIncludedIndex) || rf.logs[rf.Convert(args.LastIncludedIndex)].TermNumber != args.LastIncludedTerm {
		rf.logs = CutLog([]Log{}, args.LastIncludedTerm)
	} else {
		rf.logs = CutLog(rf.logs[rf.Convert(args.LastIncludedIndex)+1:len(rf.logs)], args.LastIncludedTerm)
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	DPrintf("rf[%v] install snap, now lastIndex %v", rf.me, rf.lastIncludedIndex)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	HasRequest  bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if currentTerm < args.Term { // 只要currentTerm 小于 arg Term一定会投票
		// t := rf.currentTerm
		// rf.currentTerm = args.Term // 修改当前current term
		// // DPrintf("rf[%d] %d vote rf[%d] %d", rf.me, t, args.CandidateId, args.Term)
		// if rf.status != 0 {        //如果它不是follower，那么一定没有投过票。
		// 	DPrintf("rf[%d]  at term %d vote rf[%d] at term %d", rf.me, t, args.CandidateId, args.Term)
		// 	rf.status = 0
		// 	rf.votedFor = args.CandidateId
		// 	reply.VoteGranted = true
		// 	// return
		// } else if rf.votedFor == -1 { // 还没投过票
		// 	DPrintf("rf[%d]  at term %d vote rf[%d] at term %d", rf.me, t, args.CandidateId, args.Term)
		// 	rf.votedFor = args.CandidateId
		// 	reply.VoteGranted = true
		// } else {
		// 	reply.VoteGranted = false
		// }
		// 确保给up to date投票
		rf.currentTerm = args.Term // 升级任期
		rf.status = 0
		// rf.startTime = time.Now()
		rf.votedFor = -1
		rf.persist()
	}
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // rf.votedFor == args.CandidateId是因为可能crash，crash恢复之后还投回原来的candidate
		// reply.VoteGranted = true
		// rf.votedFor = args.CandidateId
		// lastLogTerm := rf.logs[len(rf.logs)-1].TermNumber
		// lastLogIndex := len(rf.logs) -1
		// if lastLogTerm > args.Term || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex){
		// 	return
		// }
		// rf.votedFor = args.CandidateId
		// reply.VoteGranted = true
		// rf.startTime = time.Now() // 投票出去才会重置时间
		// rf.persist()
		if rf.logs[len(rf.logs)-1].TermNumber < args.LastLogTerm {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.startTime = time.Now() // 投票出去才会重置时间
			rf.persist()

		} else if rf.logs[len(rf.logs)-1].TermNumber == args.LastLogTerm && rf.Reconvert(len(rf.logs)-1) <= args.LastLogIndex { // 这里的index用了等于
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.startTime = time.Now() // 投票出去才会重置时间
			rf.persist()
		}
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ctx context.Context) int {
	ch := make(chan bool)
	go func() {
		ch <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()
	for {
		select {
		case <-ctx.Done():
			return 0 // 响应超时
		case t := <-ch:
			if !t { // 响应超时
				return 1
			} else {
				return 2
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	XTerm   int
	XIndex  int
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.XTerm = 0
	DPrintf("rf[%v] is follower logs %v", rf.me, rf.logs)
	if rf.currentTerm <= args.Term {
		// DPrintf("rf[%v] return follower , term %v arg.Term %v", rf.me, rf.currentTerm, args.Term)
		DPrintf("rf[%v] is follower, currentTerm %v has log length %v, last log term is %v, commitIndex is %v, applyIndex is %v", rf.me, rf.currentTerm, rf.Reconvert(len(rf.logs)), rf.logs[len(rf.logs)-1].TermNumber, rf.commitIndex, rf.lastApplied)
		if rf.currentTerm < args.Term { // term 提升
			rf.votedFor = -1 // 这一轮没有投票，所以是-1
			rf.currentTerm = args.Term
			rf.persist()
		}
		rf.status = 0 // 转换为follower
		rf.leaderId = args.LeaderId
		reply.Term = rf.currentTerm
		// t := rf.startTime
		rf.startTime = time.Now()
		// DPrintf("rf[%d] have new start time, since time %v", rf.me, time.Since(rf.startTime))
		if args.PrevLogIndex < rf.lastIncludedIndex {
			reply.Term = -1
			reply.XIndex = rf.Reconvert(1)
			return
		}
		if args.PrevLogIndex >= rf.Reconvert(len(rf.logs)) { // 表明当前log还很短
			reply.XTerm = -1
			reply.XIndex = rf.Reconvert(len(rf.logs))
			reply.Success = false
		} else if rf.logs[rf.Convert(args.PrevLogIndex)].TermNumber != args.PreLogTerm {
			reply.XTerm = rf.logs[rf.Convert(args.PrevLogIndex)].TermNumber
			reply.XIndex = rf.Reconvert(1)
			for i := rf.Convert(args.PrevLogIndex); i >= 1; i-- { // 不应该遍历到0，因为0是空command
				if rf.logs[i].TermNumber != rf.logs[rf.Convert(args.PrevLogIndex)].TermNumber {
					reply.XIndex = rf.Reconvert(i)
					break
				}
			}
		} else { // 成功hit！
			last := rf.logs[rf.Convert(args.PrevLogIndex+1):len(rf.logs)]
			DPrintf("rf[%v] want to append entries %v", rf.me, args.Entries)
			// 有个难点如何判断旧的rpc
			if len(last) == len(args.Entries) && len(last) != 0 {
				flag := true
				for i := 0; i < len(last); i++ {
					if last[i].TermNumber != args.Entries[i].TermNumber {
						flag = false
						break
					}
				}
				if flag {
					// rf.commitIndex = int(math.Min(float64(len(rf.logs)), float64(args.LeaderCommit)))
					reply.XTerm = -2 // 说明已经append过了
					reply.XIndex = rf.Reconvert(len(rf.logs))

					// reply.Success = true
					return
				}
			}
			rf.logs = rf.logs[0:rf.Convert(args.PrevLogIndex+1)]
			DPrintf("rf[%v] is follower, append entries %v", rf.me, args.Entries)
			rf.logs = append(rf.logs, args.Entries...)
			reply.XIndex = rf.Reconvert(len(rf.logs))
			rf.persist()
			reply.Success = true
			rf.commitIndex = int(math.Min(float64(rf.Reconvert(len(rf.logs))), float64(args.LeaderCommit)))
		}
		DPrintf("rf[%v] send Xindex %v to rf[%v]", rf.me, reply.XIndex, args.LeaderId)
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		// reply.XTerm = -3
	}
}

func (rf *Raft) sendInstallsnapshot(server int, args *InstallsnapshotArgs, reply *InstallsnapshotReply) {
	ch := make(chan bool)
	go func() {
		ch <- rf.peers[server].Call("Raft.Installsnapshot", args, reply)
	}()
	c, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)
	select {
	case <-c.Done():
		return
	case t := <-ch:
		if !t {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.status = 0
			rf.votedFor = -1
			rf.persist()
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

}
func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ch := make(chan bool)
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*30)

	go func() {
		// DPrintf("rf[%v] send entries %v",rf.me, args.Entries)
			ch <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()

	select {
	case <-ctx.Done():
		return
	case t := <-ch: // 确保完成了rpc
		if !t {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm < reply.Term {
			rf.status = 0
			rf.currentTerm = reply.Term
			rf.persist()
			return
		} else if rf.status != 2 || args.Term != reply.Term || rf.currentTerm != reply.Term { // 不再是leader或者这个rpc不在任期内
			// time.Sleep(10*time.Millisecond)
			return
		}
		if reply.XTerm == -2 && reply.XIndex == len(args.Entries)+rf.nextIndex[server] {
			rf.nextIndex[server] = reply.XIndex
			rf.matchIndex[server] = reply.XIndex - 1
		} else if reply.Success {
			// DPrintf("rf[%v] server %v nextIndex %v has append entries %v")
			// DPrintf()
			if reply.XIndex == len(args.Entries)+rf.nextIndex[server] {
				rf.nextIndex[server] = reply.XIndex
				rf.matchIndex[server] = reply.XIndex - 1
			}
		} else { // 如果返回失败说明，log不匹配
			rf.setNextIndex(server, args, reply)
		}
		return
	}
}

func (rf *Raft) setNextIndex(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.XTerm == -1 {
		rf.nextIndex[server] = reply.XIndex
	} else if reply.XTerm >= 0 {
		hit := false
		var i int
		for i = len(rf.logs) - 1; i >= 1; i-- { // 0 是空log应该从1开始遍历
			if rf.logs[i].TermNumber == reply.XTerm {
				hit = true
				break
			} else if rf.logs[i].TermNumber < reply.XTerm {
				break
			}
		}
		if hit {
			rf.nextIndex[server] = rf.Reconvert(i)
		} else {
			rf.nextIndex[server] = reply.XIndex
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) { // 这个函数用于添加log
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == 2 && rf.dead != 1 {
		isLeader = true
		index = rf.Reconvert(len(rf.logs))
		DPrintf("rf[%v] write command %v to log index %v", rf.me, command, index)
		rf.logs = append(rf.logs, Log{rf.currentTerm, command})
		rf.persist()
		term = rf.currentTerm
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 5) // 确保能抢占到锁
		rf.mu.Lock()
		t := rf.startTime
		rf.mu.Unlock()
		time.Sleep(rf.electionTimeout - time.Since(t))
		rf.mu.Lock() // 在AttemptElection中解锁
		d := time.Since(rf.startTime)
		if d > rf.electionTimeout && rf.status != 2 { // 超时了，试图进行选举
			DPrintf("rf[%d] since time %v timeout%v", rf.me, d, rf.electionTimeout)
			// rf.votedFor = -1
			go rf.AttempElection()
		} else {
			rf.mu.Unlock()
		}

		// if
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) heartsbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		mpp := map[int]int{}
		min := 0
		for server, v := range rf.matchIndex {
			if server != rf.me {
				mpp[v]++
			}
		}
		for k, v := range mpp {
			if v >= len(rf.peers)/2 && k > min {
				min = k
			}
		}
		if min > rf.commitIndex && rf.logs[rf.Convert(min)].TermNumber == rf.currentTerm {
			rf.commitIndex = min
		}
		status := rf.status
		if status == 2 {
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				if rf.nextIndex[server] <= rf.lastIncludedIndex { // leader 已经没有之前的log了。
					// DPrintf("rf[%v] get XIndex %v from rf[%v]", rf.me, reply.XIndex, server)
					args := InstallsnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.leaderId,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						SnapShot:          rf.snapshot,
					}
					go rf.sendInstallsnapshot(server, &args, &InstallsnapshotReply{Term: rf.currentTerm})
					continue
				}
				entries := []Log{}

				// var end = len(rf.logs)
				// if len(rf.logs) - rf.Convert(rf.nextIndex[server]) > 100{

				// }
				t := make([]Log, len(rf.logs[rf.Convert(rf.nextIndex[server]):len(rf.logs)]))
				copy(t, rf.logs[rf.Convert(rf.nextIndex[server]):len(rf.logs)])
				entries = t
				PrevLogIndex := rf.nextIndex[server] - 1
				PreLogTerm := rf.logs[rf.Convert(rf.nextIndex[server])-1].TermNumber
				// entries =  rf.logs[rf.nextIndex[server]:len(rf.logs)] // 一次性全打包出去

				DPrintf("rf[%v] send %v to rf[%v] rf.nextIndex + len(entries) = %v len(rf.logs) %v lastIndex %v", rf.me, entries, server, rf.nextIndex[server]+len(entries), rf.Reconvert(len(rf.logs)), rf.lastIncludedIndex)
				DPrintf("rf[%v] logs %v", rf.me, rf.logs)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: PrevLogIndex,
					PreLogTerm:   PreLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				i := rf.currentTerm
				go rf.sendHeartBeat(server, &args, &AppendEntriesReply{Term: i, Success: false, XIndex: 0, XTerm: -110})

			}
		} else {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		DPrintf("rf[%v] is leader, currentTerm %v has log length %v, last log term is %v, commitIndex is %v, applyIndex is %v", rf.me, rf.currentTerm, rf.Reconvert(len(rf.logs)), rf.logs[len(rf.logs)-1].TermNumber, rf.commitIndex, rf.lastApplied)

		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) AttempElection() {
	cond := sync.NewCond(&sync.Mutex{})
	DPrintf("rf[%v] start elect at term %v", rf.me, rf.currentTerm+1)
	rf.status = 1
	count := 1
	rf.currentTerm++
	currentTerm := rf.currentTerm
	term := currentTerm
	sum := 1
	rf.votedFor = rf.me
	rf.electionTimeout = randomizeTimeout()
	// d := rf.electionTimeout
	rf.startTime = time.Now()
	// alive := 0
	rf.persist()
	rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rva := RequestVoteArgs{Term: currentTerm, CandidateId: rf.me, LastLogIndex: rf.Reconvert(len(rf.logs)) - 1, LastLogTerm: rf.logs[len(rf.logs)-1].TermNumber}
			rvr := RequestVoteReply{Term: currentTerm, VoteGranted: false, HasRequest: false}
			c, cancel := context.WithTimeout(context.Background(), time.Millisecond*50) // 争取在规定时间内完成投票
			defer cancel()
			t := rf.sendRequestVote(server, &rva, &rvr, c)
			cond.L.Lock()
			defer cond.L.Unlock()
			sum++
			cond.Broadcast()
			if t == 0 || t == 1 {
				return // 没来得及响应
			}
			if rvr.Term > term {
				term = rvr.Term
			}
			if rvr.VoteGranted {
				count++
			}

		}(server)
	}

	half := int(math.Ceil(float64(len(rf.peers)) / 2))
	cond.L.Lock()
	for currentTerm == term && sum != len(rf.peers) && count <= half {
		// DPrintf("%v %v %v", term, sum, count)
		cond.Wait()
	}
	// DPrintf("%v %v get count", rf.me, count)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("rf[%v]: count:%v sum %v status:%v term:%v currentTerm:%v rf.currentTerm%v lastIndex %v lastApply %v commitIndex %v", rf.me, count, sum, rf.status, term, currentTerm, rf.currentTerm, rf.logs[len(rf.logs)-1].TermNumber, rf.lastApplied, rf.commitIndex)

	if count >= half && rf.status == 1 && term == rf.currentTerm && term == currentTerm { // 保证还在选举期间

		rf.status = 2
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.Reconvert(len(rf.logs))
		}
		rf.matchIndex = make([]int, len(rf.peers)) // 这里初始化了 leader的nextIndex和matchIndex
		// rf.logs = append(rf.logs, Log{rf.currentTerm, true}) // no-hup
		go rf.heartsbeats()
		DPrintf("%v become leader at term[%v]", rf.me, rf.currentTerm)
	}
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.persist()
	}
	cond.L.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.dead = 0
	rf.leaderId = -1
	rf.volatile = Volatile{}
	rf.votedFor = -1
	rf.status = 0 // 初始化为follower
	rf.electionTimeout = randomizeTimeout()
	rf.startTime = time.Now()
	rand.Seed(time.Now().Unix())
	// 这里初始化2b的log
	rf.logs = make([]Log, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnap(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Apply(applyCh)
	// go rf.shutdown()
	return rf
}

func (rf *Raft) Apply(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		ApplyMsgs := []ApplyMsg{}
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				entry := rf.logs[rf.Convert(i)]
				msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: i}
				ApplyMsgs = append(ApplyMsgs, msg)
			}
			// rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		// DPrintf("rf[%v] %v",rf.me, len(ApplyMsgs))
		for _, msg := range ApplyMsgs {
			// DPrintf("rf[%v], has log length %v, last log term is %v, commitIndex is %v, applyIndex is %v", rf.me, len(rf.logs), rf.logs[len(rf.logs)-1].TermNumber, rf.commitIndex, rf.lastApplied)
			DPrintf("rf[%d] apply command %v at applyIndex %v", rf.me, msg.Command, rf.lastApplied+1)
			applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied++
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func randomizeTimeout() time.Duration {
	rand.Seed(time.Now().UnixMicro())
	return time.Millisecond * time.Duration(rand.Intn(300)+200)
}
