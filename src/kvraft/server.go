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

const Debug = true

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
	notify       map[int] chan not
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
type not struct{
	res string
	term int
}

func (kv *KVServer) Dup(clientId int64, CommandId int) bool {
	return kv.done[clientId].k >= CommandId
}

func (kv *KVServer) getChan(index int) (chan not, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch := make(chan not)
	kv.notify[index] = ch
	return ch, false
}

func (kv *KVServer) removeChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notify, index)
}

func (kv *KVServer) ClientRequest(args *ClientRequestArgs, reply *ClientRequestReply) {
	if kv.killed() {
		return
	}
	DPrintf("kv[%v] get request from client[%v]", kv.me, args.ClientId)
	_, isleader := kv.rf.GetState()
	kv.mu.Lock()
	DPrintf("kv[%v] get lock, look request key %v value %v command %v CommandId %v from client[%v]", kv.me, args.Key, args.Value, args.Command, args.SequenceNum, args.ClientId)
	
	if args.Command != 0 && kv.Dup(args.ClientId, args.SequenceNum) && isleader{
		reply.Status = 1
		reply.Response = ""
		kv.mu.Unlock()
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
	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		DPrintf("kv[%v] is not leader, should contact with kv[%v], reply %v", kv.me, reply.LeaderHint, reply.ServerId)
		return
	}
	DPrintf("kv[%v] is leader, handle with request key %v value %v command %v CommandId %v from client[%v]", kv.me, args.Key, args.Value, args.Command, args.SequenceNum, args.ClientId)
	ch, _ := kv.getChan(index)
	select {
	case res := <-ch:
		if res.term == term{
			reply.Response = res.res
			reply.Status = 1
			DPrintf("kv[%v] CommandId %v return key %v value %v to client[%v]", kv.me, args.SequenceNum, args.Key, res.res, args.ClientId)
		}	
	case <-time.After(80 * time.Millisecond):
	}
	go kv.removeChan(index)

	// Your code here.
	// DPrintf("kv[%v] get request from client[%v]", kv.me, args.ClientId)
	// reply.ServerId = -1
	// if kv.killed() {
	// 	return
	// }
	// reply.ServerId = kv.me
	// kv.mu.Lock()
	// if kv.done[args.ClientId].k == args.SequenceNum {
	// 	DPrintf("kv[%v] is leader, handle with request key %v value %v command %v CommandId %v from client[%v]", kv.me, args.Key, args.Value, args.Command, args.SequenceNum, args.ClientId)
	// 	reply.Response = kv.done[args.ClientId].Response
	// 	kv.mu.Unlock()
	// 	reply.LeaderHint = kv.me
	// 	reply.Status = 1
	// 	DPrintf("kv[%v] CommandId %v return key %v value %v to client[%v]", kv.me, args.SequenceNum, args.Key, reply.Response, args.ClientId)
	// 	return
	// }else if kv.done[args.ClientId].k > args.SequenceNum{
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()
	// op := Op{
	// 	ClientId: args.ClientId,
	// 	Seq:      args.SequenceNum,
	// 	Key:      args.Key,
	// 	Value:    args.Value,
	// 	Command:  args.Command,
	// }
	// _, term, isLeader := kv.rf.Start(op)
	// kv.mu.Lock()
	// kv.Term = term
	// kv.mu.Unlock()
	// if !isLeader {
	// 	reply.LeaderHint = kv.rf.LeaderId()
	// 	// DPrintf("kv[%v] is not leader, should contact with kv[%v], reply %v", kv.me, reply.LeaderHint, reply.ServerId)
	// 	return
	// }
	// DPrintf("kv[%v] is leader, handle with request key %v value %v command %v CommandId %v from client[%v]", kv.me, args.Key, args.Value, args.Command, args.SequenceNum, args.ClientId)
	// for !kv.killed() && kv.rf.IsLeader() {
	// 	kv.mu.Lock()
	// 	DPrintf("kv[%v] handle request client %v commandId %v get lock", kv.me, args.ClientId, args.SequenceNum)
	// 	if kv.done[args.ClientId].k == args.SequenceNum {
	// 		reply.Response = kv.done[args.ClientId].Response
	// 		kv.mu.Unlock()
	// 		reply.LeaderHint = kv.me
	// 		reply.Status = 1
	// 		DPrintf("kv[%v] CommandId %v return key %v value %v to client[%v]", kv.me, args.SequenceNum, args.Key, reply.Response, args.ClientId)
	// 		return
	// 	} else if kv.done[args.ClientId].k < args.SequenceNum{
	// 		kv.mu.Unlock()
	// 		time.Sleep(2 * time.Millisecond) // 等待
	// 	}else{
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
	// reply.LeaderHint = -1

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
	kv.notify = map[int]chan not{}
	kv.lastApply = 0
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	go kv.Apply()
	return kv
}

func (kv *KVServer) Apply() {

	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApply {
				kv.mu.Unlock()
				continue
			}
			kv.lastApply = msg.CommandIndex
			command := msg.Command
			op, _ := command.(Op)
			var res = ""
			DPrintf("kv[%v] got msg index %v term %v client %v commandId %v command %v key %v value %v", kv.me, msg.CommandIndex, msg.CommandTerm, op.ClientId, op.Seq,op.Command, op.Key, op.Value)
			if op.Command != 0 && !kv.Dup(op.ClientId, op.Seq) {
				if op.Command == 1 {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] += op.Value
				}
				kv.done[op.ClientId] = lastOp{op.Seq, res}
				DPrintf("kv[%v] done client[%v] commandId %v command %v now key %v value is %v ", kv.me, op.ClientId, op.Seq, op.Command, op.Key, kv.data[op.Key])
			}
			if op.Command == 0 {
				res = kv.data[op.Key]
				DPrintf("kv[%v] done client[%v] commandId %v command %v now key %v value is %v ", kv.me, op.ClientId, op.Seq, op.Command, op.Key, kv.data[op.Key])
			}
			if currentTerm, isleader := kv.rf.GetState(); isleader && currentTerm == msg.CommandTerm { //保证读请求还是当前leader处理
				ch, ok := kv.notify[msg.CommandIndex]
				if ok {
					select {
					case ch <- not{res: res, term: msg.CommandTerm}:
					case <-time.After(1 * time.Millisecond):
					}
				}
				// ch <- res
				
			}
			
			kv.mu.Unlock()
		}

	}
}

//
// if op, ok := command.(Op); ok {
// 	kv.mu.Lock()
// 	v := kv.done[op.ClientId].k
// 	if v+1 == op.Seq {
// 		c := op.Command
// 		cli := op.ClientId
// 		var rs string = ""
// 		if c == 1 {
// 			kv.data[op.Key] = op.Value
// 		} else if c == 2 {
// 			kv.data[op.Key] += op.Value
// 		} else {
// 			rs = kv.data[op.Key]
// 		}
// 		kv.done[cli] = lastOp{
// 			v + 1,
// 			rs,
// 		}
// 		DPrintf("kv[%v] done client[%v] commandId %v command %v now key %v value is %v ", kv.me, op.ClientId, op.Seq, op.Command, op.Key, kv.data[op.Key])
// 	}

// 	kv.mu.Unlock()
// }
