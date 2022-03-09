package kvraft

import (
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
	Command  int
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	data         map[string]string
	done         map[int64]lastOp
	maxraftstate int // snapshot if log grows this big
	lastApply    int
	Term         int
	// chanMap map[int64](chan)
	// Your definitions here.
}
type lastOp struct {
	k        int
	Response string
}

func (kv *KVServer) ClientRequest(args *ClientRequestArgs, reply *ClientRequestReply) {
	// Your code here.
	DPrintf("kv[%v] get request from client[%v]", kv.me, args.ClientId)
	reply.ServerId = -1
	if kv.killed() {
		return
	}
	reply.ServerId = kv.me
	kv.mu.Lock()
	if kv.done[args.ClientId].k >= args.SequenceNum && kv.rf.IsLeader() {
		DPrintf("kv[%v] is leader, handle with request key %v value %v command %v CommandId %v from client[%v]", kv.me, args.Key, args.Value, args.Command, args.SequenceNum, args.ClientId)
		reply.Response = kv.done[args.ClientId].Response
		kv.mu.Unlock()
		reply.LeaderHint = kv.me
		reply.Status = 1
		DPrintf("kv[%v] CommandId %v return key %v value %v to client[%v]", kv.me, args.SequenceNum, args.Key, reply.Response, args.ClientId)
		return
	}
	kv.mu.Unlock()
	op := Op{
		ClientId: args.ClientId,
		Seq:      args.SequenceNum,
		Key:      args.Key,
		Value:    args.Value,
		Command:  args.Command,
	}
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	kv.Term = term
	kv.mu.Unlock()
	if !isLeader {
		reply.LeaderHint = kv.rf.LeaderId()
		// DPrintf("kv[%v] is not leader, should contact with kv[%v], reply %v", kv.me, reply.LeaderHint, reply.ServerId)
		return
	}
	DPrintf("kv[%v] is leader, handle with request key %v value %v command %v CommandId %v from client[%v]", kv.me, args.Key, args.Value, args.Command, args.SequenceNum, args.ClientId)
	for !kv.killed() && kv.rf.IsLeader() {
		kv.mu.Lock()
		if kv.done[args.ClientId].k >= args.SequenceNum {
			reply.Response = kv.done[args.ClientId].Response
			kv.mu.Unlock()
			reply.LeaderHint = kv.me
			reply.Status = 1
			DPrintf("kv[%v] CommandId %v return key %v value %v to client[%v]", kv.me, args.SequenceNum, args.Key, reply.Response, args.ClientId)
			return
		} else {
			kv.mu.Unlock()
			time.Sleep(2 * time.Millisecond) // 等待
		}
	}
	reply.LeaderHint = -1

	// clientId := args.ClientId
	// seq := args.SequenceNum
	// if _,ok := kv.done[clientId];!ok{
	// 	kv.done[clientId] = 0
	// }
	// if kv.done[clientId] >= seq{

	// }
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
	kv.done = map[int64]lastOp{}
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.Apply()
	return kv
}

func (kv *KVServer) Apply() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		if !msg.CommandValid {
			continue
		}
		command := msg.Command
		if op, ok := command.(Op); ok {
			kv.mu.Lock()
			v := kv.done[op.ClientId].k
			if v+1 == op.Seq {
				c := op.Command
				cli := op.ClientId
				var rs string = ""
				if c == 1 {
					kv.data[op.Key] = op.Value
				} else if c == 2 {
					kv.data[op.Key] += op.Value
				} else {
					rs = kv.data[op.Key]
				}
				kv.done[cli] = lastOp{
					v + 1,
					rs,
				}
				DPrintf("kv[%v] done client[%v] commandId %v command %v now key %v value is %v ", kv.me, op.ClientId, op.Seq, op.Command, op.Key, kv.data[op.Key])
			}

			kv.mu.Unlock()
		}
	}
}
