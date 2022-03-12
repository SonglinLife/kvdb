package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
	Key      string
	Value    string
	OpType   int
}

type KVServer struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	data         map[string]string
	done         map[int64]int
	notify       map[int64]chan string
	maxraftstate int // snapshot if log grows this big
	lastApplied  int
	Term         int
	// chanMap map[int64](chan)
	// Your definitions here.
}

func (kv *KVServer) Dup(clientId int64, CommandId int) bool {
	return kv.done[clientId] >= CommandId
}

func (kv *KVServer) makeChan(index, term int) chan string {
	i := int64(index)*100000 + int64(term)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch := make(chan string)
	kv.notify[i] = ch
	return ch
}
func (kv *KVServer) getChan(index, term int) (chan string, bool) {
	i := int64(index)*100000 + int64(term)

	ch, ok := kv.notify[i]
	return ch, ok
}

func (kv *KVServer) removeChan(index, term int) {
	i := int64(index)*100000 + int64(term)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notify, i)
}

func (kv *KVServer) ClientRequest(args *ClientRequestArgs, reply *ClientRequestReply) {
	if kv.killed() {
		return
	}
	kv.mu.Lock()
	if args.OpType != GET && kv.Dup(args.ClientId, args.SequenceNum) {
		reply.Err = OK
		reply.Response = ""
	}
	kv.mu.Unlock()
	op := Op{
		ClientId: args.ClientId,
		Seq:      args.SequenceNum,
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.OpType,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	ch := kv.makeChan(index, term)
	select {
	case res := <-ch:
		reply.Err = OK
		reply.Response = res
	case <-time.After(100 * time.Millisecond):
	}
	go func() {
		kv.removeChan(index, term)
	}()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = map[string]string{}
	kv.done = map[int64]int{}
	kv.notify = map[int64]chan string{}
	kv.lastApplied = 0
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.Apply()
	return kv
}

func (kv *KVServer) Apply() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				command := msg.Command
				op, _ := command.(Op)
				var res = ""
				if op.OpType != GET && !kv.Dup(op.ClientId, op.Seq) {
					if op.OpType == PUT {
						kv.data[op.Key] = op.Value
					} else {
						kv.data[op.Key] += op.Value
					}
					kv.done[op.ClientId] = op.Seq
				}
				if op.OpType == GET {
					res = kv.data[op.Key]
				}
				if currentTerm, isleader := kv.rf.GetState(); isleader && currentTerm == msg.CommandTerm { //保证读请求还是当前leader处理
					go func(index, term int) {
						kv.mu.RLock()
						defer kv.mu.RUnlock()
						ch, ok := kv.getChan(index, term)
						if ok {
							select {
							case ch <- res:
							case <-time.After(3 * time.Millisecond):
							}
						}
						// select
					}(msg.CommandIndex, msg.CommandTerm)
				}
				kv.lastApplied = msg.CommandIndex
				kv.Snapshot()
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				if !kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.mu.Unlock()
					continue
				}
				r := bytes.NewBuffer(msg.Snapshot)
				d := labgob.NewDecoder(r)
				var data map[string]string
				var done map[int64]int
				var lastApplied int
				if d.Decode(&data) != nil ||
					d.Decode(&lastApplied) != nil || d.Decode(&done) != nil {
					DPrintf("kv[%v] decode err", kv.me)
				} else {
					kv.data = data
					kv.lastApplied = lastApplied
					kv.done = done
				}
				kv.mu.Unlock()
			}
		case <-time.After(200 * time.Microsecond):
		}
	}
}

func (kv *KVServer) Snapshot() {
	length := kv.rf.GetRaftStateSize()
	if kv.maxraftstate != -1 && length > kv.maxraftstate/3 {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.data)
		e.Encode(kv.lastApplied)
		e.Encode(kv.done)
		data := w.Bytes()
		kv.rf.Snapshot(kv.lastApplied, data)
	}
}
