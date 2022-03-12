package raft

import (
	//	"bytes"

	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	LEADER    = 2
	CANDIDATE = 1
	FOLLOWER  = 0
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Entry struct {
	Term    int
	Command interface{}
	Index   int
}
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// need Persisent
	logs        []Entry
	votedFor    int
	currentTerm int

	// volatile for every servers
	lastApplied int
	commitIndex int
	status      int

	// volatile for leader
	nextIndex  []int
	matchIndex []int

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}


func (rf *Raft) changeStatus(status int) {
	if status == LEADER {
		for peer := range rf.peers {
			rf.nextIndex[peer] = rf.lastEntry().Index + 1
			rf.matchIndex[peer] = 0
		}
	}
	rf.status = status
}

// 这里我们设置好rf获得log的函数，防止切断日志后不连续出错

func (rf *Raft) oneEntry(Index int) Entry {
	if Index == -1 {
		return rf.lastEntry()
	}
	start := rf.firstEntry().Index
	if Index < start || Index > rf.lastEntry().Index {
		panic("oneEntry try to get Entry, but out of range")
	}
	return rf.logs[Index-start]
}

func (rf *Raft) sliceLog(startIndex, endIndex int) []Entry {
	// 前闭后开
	if startIndex == 0 {
		startIndex = rf.firstEntry().Index
	}
	start := startIndex - rf.firstEntry().Index
	if endIndex == -1 {
		endIndex = rf.lastEntry().Index + 1
	}

	end := endIndex - rf.firstEntry().Index
	tmp := make([]Entry, len(rf.logs[start:end]))
	copy(tmp, rf.logs[start:end])
	return tmp
}
func (rf *Raft) cutLog(IncludeIndex, IncludeTerm int) {
	if IncludeIndex <= rf.firstEntry().Index {
		return
	}
	entries := make([]Entry, 1)

	if IncludeTerm == -1 {
		// KV server调用snapshot截断log
		entries[0] = rf.oneEntry(IncludeIndex)
		entries[0].Command = nil
		rf.logs = append(entries, rf.sliceLog(IncludeIndex+1, -1)...)
		return
	}

	entries[0] = Entry{Index: IncludeIndex, Term: IncludeTerm}
	if IncludeIndex > rf.lastEntry().Index {
		// 安装Snapshot 可能需要制定Term
		rf.logs = entries
		return
	}

	if rf.oneEntry(IncludeIndex).Term == IncludeTerm {
		entries = append(entries, rf.sliceLog(IncludeIndex+1, -1)...)
	}
	rf.logs = entries

}

func (rf *Raft) firstEntry() Entry {
	return rf.logs[0]
}

func (rf *Raft) lastEntry() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A)

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.status == LEADER
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.sliceLog(0, -1))
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

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Entry
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
	DPrintf("kv[%v] doing read snapshot", rf.me)
	msg := ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		SnapshotTerm: rf.firstEntry().Term,
		SnapshotIndex: rf.firstEntry().Index,
		Snapshot: data,
	}
	rf.mu.Lock()
	go func(){
		defer rf.mu.Unlock()
		rf.applyCh <- msg
		// rf.commitIndex = rf.firstEntry().Index
		// rf.lastApplied = rf.commitIndex
	}()

	DPrintf("rf[%v] now Term done read snap%v  commitIndex %v lastApplied %v the log is %v", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// 意思是服务器定期会调用这个函数，index之前（包括index）对应的log都不在被需要
	// 在做快照的时候，可以在log最前插入一个空log entry，并且这个log entry的term为lastIncludeTerm
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.firstEntry().Index {
		return // 说明是旧的快照
	}

	// DPrintf("rf[%v] logs %v", rf.me, rf.logs)
	DPrintf("rf[%v] is saving snapshot index %v logs %v", rf.me, index, rf.logs)
	rf.cutLog(index, -1)
	rf.persist()
	DPrintf("rf[%v] has done snapshot index %v logs %v", rf.me, index, rf.logs)
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)

}

type InstallsnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapShot          []byte
}

func (rf *Raft) getInstallSnapshotArgs() *InstallsnapshotArgs {
	args := &InstallsnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.firstEntry().Index,
		LastIncludedTerm:  rf.firstEntry().Term,
		SnapShot:          rf.persister.ReadSnapshot(),
	}
	return args

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
	DPrintf("rf[%v] get snapshot lastIndex %v lastTerm %v Term %v currentTerm %v firstIndex %v", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, args.Term, rf.currentTerm, rf.firstEntry().Index)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeStatus(FOLLOWER)
		rf.persist()
	}

	rf.status = FOLLOWER
	rf.electionTimer.Reset(randomizeTimeout())

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		CommandValid:  false,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		Snapshot:      args.SnapShot,
	}

	go func() {
		rf.applyCh <- msg
	}()

}
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// simply return true
	if rf.killed(){
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	rf.cutLog(lastIncludedIndex, lastIncludedTerm)
	rf.persist()

	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	rf.lastApplied = rf.firstEntry().Index
	rf.commitIndex = rf.lastApplied
	DPrintf("rf[%v] now commitedIndex %v lastApplied %v  lastIncludeTerm %v lastIncludedIndex %v  log is %v ", rf.me, rf.commitIndex, rf.lastApplied, lastIncludedTerm, lastIncludedIndex, rf.logs)

	return true
}

func (rf *Raft) sendInstallsnapshot(server int, args *InstallsnapshotArgs, reply *InstallsnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Installsnapshot", args, reply)
	return ok
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

func (rf *Raft) GetRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastEntry().Index,
		LastLogTerm:  rf.lastEntry().Term,
	}
	return args
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}

	if args.Term < rf.currentTerm || (rf.votedFor != -1 && rf.votedFor != args.CandidateId) { // 比当前term还小则忽略
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	reply.Term = rf.currentTerm

	if rf.isUptoDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(randomizeTimeout())
		rf.persist()
		DPrintf("rf[%v] vote for rf[%v] at term %v args %v lastEntry %v", rf.me, args.CandidateId, rf.currentTerm, args, rf.lastEntry())
		return
	}
	reply.VoteGranted = false

}

func (rf *Raft) isUptoDate(Index, Term int) bool {
	lastIndex, lastTerm := rf.lastEntry().Index, rf.lastEntry().Term
	if lastTerm < Term || (lastTerm == Term && lastIndex <= Index) {
		return true
	}
	return false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Election() {
	args := rf.GetRequestVoteArgs()
	Votes := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == reply.Term && rf.status == CANDIDATE && rf.currentTerm == args.Term{
					if reply.VoteGranted {
						Votes++
						// DPrintf("rf[%v] get a vote in term %v", rf.me, rf.currentTerm)
						if Votes > len(rf.peers)/2 {
							rf.changeStatus(LEADER)
							DPrintf("rf[%v] become leader in term %v", rf.me, rf.currentTerm)
							rf.BroadcastHeartbeat(true)
						}
					}
				} else if reply.Term > rf.currentTerm {
					rf.changeStatus(FOLLOWER)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
				}
			}
		}(peer)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

func (rf *Raft) GetAppendEntriesArgs(peer int) *AppendEntriesArgs {

	prevLogIndex := rf.nextIndex[peer] - 1

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.oneEntry(prevLogIndex).Term,
		Entries:      rf.sliceLog(prevLogIndex+1, -1),
		LeaderCommit: rf.commitIndex,
	}
	return args
}

// log复制部分

type AppendEntriesReply struct {
	XTerm   int
	XIndex  int // 这个XIndex 是FOLLOWER期待的下一个日志log Index
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("rf[%v] is follower Term %v replyXindex %v commitIndex %v lastApplied %v the log is %v", rf.me, rf.currentTerm, reply.XIndex, rf.commitIndex, rf.lastApplied, rf.logs)
	if args.Term < rf.currentTerm {
		// 过时的AppendEntries，直接返回false
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeStatus(FOLLOWER)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	rf.changeStatus(FOLLOWER)
	rf.electionTimer.Reset(randomizeTimeout())
	if args.PrevLogIndex < rf.firstEntry().Index {

		//当前日志比较靠前，没有了这个prevlog，无法进行比较
		reply.Success = false
		reply.XIndex = rf.firstEntry().Index + 1
		reply.XTerm = -1
		return
	}

	if args.PrevLogIndex > rf.lastEntry().Index {

		// 当前日志太短了，或者太后了，没有了这个prevLog
		reply.XIndex = rf.lastEntry().Index + 1
		reply.Success = false
		reply.XTerm = -1
		return
	}

	if rf.oneEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// 日志出现了不匹配
		reply.XTerm = rf.oneEntry(args.PrevLogIndex).Term
		reply.Success = false
		i := args.PrevLogIndex
		for i >= rf.firstEntry().Index && rf.oneEntry(i).Term == rf.oneEntry(args.PrevLogIndex).Term {
			i--
		}
		reply.XIndex = i + 1
		return
	}

	// 日志修复
	lastIndex := rf.lastEntry().Index
	for i, entry := range args.Entries {
		if entry.Index <= lastIndex && rf.oneEntry(entry.Index).Term == entry.Term {
			continue
		}
		// 这里是一个易错点
		if entry.Index <= rf.firstEntry().Index{
			panic("wrong entry index.")
		}
		rf.logs = append(rf.sliceLog(0, entry.Index), args.Entries[i:len(args.Entries)]...)
		break
	}
	rf.persist()
	rf.commitIndex = int(math.Min(float64(rf.lastEntry().Index), float64(args.LeaderCommit)))
	rf.applyCond.Signal()
	reply.Success = true
	reply.XIndex = rf.lastEntry().Index + 1
	
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.changeStatus(FOLLOWER)
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	if reply.Term < rf.currentTerm || rf.status != LEADER || reply.Term == 0 || rf.currentTerm != args.Term{
		return
	}
	if reply.Success {
		if rf.matchIndex[server] < len(args.Entries) + args.PrevLogIndex { // matchIndex 只能被提升
			rf.nextIndex[server] = len(args.Entries) + args.PrevLogIndex + 1
			// DPrintf("rf[%v] is leader now the matchIndex of rf[%v] is %v ", rf.me, server, reply.XIndex -1)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			if rf.nextIndex[server] > rf.lastEntry().Index+1 {
				DPrintf("rf[%v] get wong XIndex %v lastIndex %v log %v", rf.me, reply.XIndex, rf.lastEntry().Index, rf.logs)
				panic("the XIndex seem worng")
			}
			if rf.matchIndex[server] > rf.lastEntry().Index {
				DPrintf("rf[%v] worng macthIndex %v to rf[%v] lastIndex %v term %v replyXIndex %v XTerm %v", rf.me, rf.matchIndex[server], server, rf.lastEntry().Index, rf.currentTerm, reply.XIndex, reply.XTerm)
				panic("worng matchIndex")
			}
			rf.checkN()
		}
	} else if reply.XIndex > rf.matchIndex[server]{
		if reply.XIndex <= rf.firstEntry().Index {
			rf.nextIndex[server] = rf.firstEntry().Index
		} else if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XIndex
		} else {
			var i = reply.XIndex
			for i >= rf.firstEntry().Index && (rf.oneEntry(i).Term > reply.XTerm) {
				i--
			}
			rf.nextIndex[server] = i
		}

	}
	if rf.matchIndex[server] > rf.nextIndex[server] {
		DPrintf("rf[%v] worng macthIndex %v to rf[%v] lastIndex %v term %v replyXIndex %v XTerm %v", rf.me, rf.matchIndex[server], server, rf.lastEntry().Index, rf.currentTerm, reply.XIndex, reply.XTerm)
		panic("worng matchIndex")
	}
}

func (rf *Raft) checkN() {
	major := rf.lastEntry().Index
	for ; major > rf.firstEntry().Index; major-- {
		num := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= major {
				num++
			}
		}
		if num > len(rf.peers)/2 {
			break
		}
	}

	if major > rf.commitIndex && rf.oneEntry(major).Term == rf.currentTerm {
		rf.commitIndex = major
		rf.applyCond.Signal()
	}

}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.status != LEADER {
		rf.mu.RUnlock()
		return
	}
	// defer rf.mu.RUnlock()
	PrevLogIndex := rf.nextIndex[peer] - 1

	if PrevLogIndex < rf.firstEntry().Index {
		// 安装shotsnap
		args := rf.getInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallsnapshotReply)
		DPrintf("rf[%v] send snapshot Index %v Term %v to rf[%v]", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, peer)
		if rf.sendInstallsnapshot(peer, args, reply) {
			rf.mu.Lock()
			DPrintf("rf[%v] get snapshot reply from rf[%v] try to handle it replyTerm %v matchIndex %v lastInclude %v", rf.me, peer, reply.Term,rf.matchIndex[peer], args.LastIncludedIndex)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.status = FOLLOWER
				rf.persist()
			}else if reply.Term == rf.currentTerm && rf.status == LEADER && rf.currentTerm == args.Term{
				if rf.matchIndex[peer] < args.LastIncludedIndex{
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1
					if rf.matchIndex[peer] > rf.lastEntry().Index {
						DPrintf("rf[%v] worng macthIndex %v to rf[%v] args %v", rf.me, rf.matchIndex[peer], peer, args)
						panic("worng matchIndex")
					}
					rf.checkN()
				}
			}
			DPrintf("rf[%v] get snapshot reply from rf[%v] has handle it replyTerm %v matchIndex %v lastInclude %v", rf.me, peer, reply.Term,rf.matchIndex[peer], args.LastIncludedIndex)
			rf.mu.Unlock()
		}
	} else {
		args := rf.GetAppendEntriesArgs(peer)
		rf.mu.RUnlock()
		DPrintf("rf[%v] send entries %v to rf[%v]", rf.me, args.Entries, peer)
		reply := new(AppendEntriesReply)
		if rf.sendHeartBeat(peer, args, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, args, reply)
			if rf.status == LEADER{
				DPrintf("rf[%v] is leader now Term %v the commitIndex is %v lastApplied is %v log %v", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs)
			}
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.replicateOneRound(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.status == LEADER && rf.matchIndex[peer] < rf.lastEntry().Index
}

//
func (rf *Raft) Start(command interface{}) (int, int, bool) { // 这个函数用于添加log
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == LEADER {
		entry := Entry{
			Index:   rf.lastEntry().Index + 1,
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.logs = append(rf.logs, entry)
		rf.persist()
		go rf.BroadcastHeartbeat(false)
		index = entry.Index
		term = entry.Term
		isLeader = true
		DPrintf("rf[%v] write command %v to index %v term %v now log %v", rf.me, command, index, term, rf.logs)
	}

	return index, term, isLeader
}

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
	DPrintf("rf[%v] start!", rf.me)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Entry, 1)
	rf.logs[0] = Entry{Index: 0, Term: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = FOLLOWER

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.electionTimer = time.NewTimer(randomizeTimeout())
	rf.heartbeatTimer = time.NewTimer(StabelTimeout())

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.replicatorCond = make([]*sync.Cond, len(rf.peers))
	for peer := range rf.peers {
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(peer)
		}
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnap(persister.ReadSnapshot())
	// rf.readSnap(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.Apply()
	// go rf.shutdown()
	DPrintf("rf[%v] end start", rf.me)
	return rf
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.status == LEADER { // leader不需要这个选举超时
				rf.electionTimer.Reset(randomizeTimeout())
				rf.mu.Unlock()
				break
			}
			rf.status = CANDIDATE
			rf.currentTerm++
			// DPrintf("rf[%v] become election term %v", rf.me, rf.currentTerm)
			rf.Election()
			rf.electionTimer.Reset(randomizeTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.heartbeatTimer.Reset(StabelTimeout())
			rf.mu.Lock()
			if rf.status == LEADER {
				rf.BroadcastHeartbeat(true)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Apply() {

	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		// DPrintf("rf[%v] rf.lastA")
		entries := rf.sliceLog(rf.lastApplied+1, rf.commitIndex+1)
		DPrintf("rf[%v] start to apply %v, commitIndex %v applyIndex %v", rf.me, entries, rf.commitIndex, rf.lastApplied)
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  entry.Index,
				CommandTerm:   entry.Term,
				SnapshotValid: false,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(commitIndex)))
		DPrintf("rf[%v] done apply %v, now lastApplied is %v", rf.me, entries, rf.lastApplied)
		rf.mu.Unlock()
	}
}

func randomizeTimeout() time.Duration {
	rand.Seed(time.Now().UnixMicro())
	return time.Millisecond * time.Duration(rand.Intn(200)+200)
}

func StabelTimeout() time.Duration {
	return 100 * time.Millisecond
}
