package kvraft

import (
	"context"
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers           []*labrpc.ClientEnd
	contactServer     int // 上一个leader
	uuid              int64
	lastExe           int
	mu                sync.Mutex
	serverMap         map[int]int
	serverIndexAddMap map[int]int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	DPrintf("start!")
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.contactServer = 0
	ck.uuid = nrand() // 给每一个客户端一个唯一的uuid
	ck.lastExe = 0
	ck.serverMap = map[int]int{}
	ck.serverIndexAddMap = map[int]int{}
	return ck
}

func (ck *Clerk) FindLeader(reply *ClientRequestReply, index int) (int, int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	i := reply.LeaderHint
	if i == -1 || ck.serverMap[i] == 0 { // server不知道谁是leader，或者client不知道leader address
		ck.contactServer = (index + 1) % len(ck.servers)
	} else {
		ck.contactServer = ck.serverMap[i] - 1
	}
	// DPrintf("client[%v] now request to kv[%v]", ck.uuid, ck.contactServer)
	return ck.contactServer, ck.serverIndexAddMap[ck.contactServer] - 1
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Request(args *ClientRequestArgs, Reply *ClientRequestReply) {
	// You will have to modify this function.
	contactServer, address := ck.GetContactServer()
	for {
		reply := new(ClientRequestReply)
		*reply = *Reply
		ctx, _ := context.WithTimeout(context.Background(), 30*time.Millisecond)
		ch := make(chan bool)
		go func(contact, add int) {
			DPrintf("client[%v] send request to kv index %v", ck.uuid, contact)
			ch <- ck.servers[contact].Call("KVServer.ClientRequest", args, reply)
		}(contactServer, address)
		select {
		case <-ctx.Done():
			ck.mu.Lock()
			ck.contactServer = (ck.contactServer + 1) % len(ck.servers)
			contactServer = ck.contactServer
			address = ck.serverIndexAddMap[contactServer] - 1
			ck.mu.Unlock()
		case t := <-ch:
			ck.mu.Lock()
			// DPrintf("set map serveradd %v", reply.ServerId)
			if reply.ServerId != -1 && ck.serverIndexAddMap[reply.ServerId] == 0 { // 如果得到了server的add
				ck.serverMap[reply.ServerId] = contactServer + 1
				ck.serverIndexAddMap[contactServer] = reply.ServerId + 1
				// DPrintf("ck[%v] serverIndexmap %v, serverMap %v", ck.uuid, ck.serverIndexAddMap, ck.serverMap)
			}
			ck.mu.Unlock()

			if !t || reply.Status == 0 {
				// contactServer, address = ck.FindLeader(reply, contactServer)
				ck.mu.Lock()
				ck.contactServer  = (ck.contactServer + 1) % len(ck.servers)
				contactServer = ck.contactServer
				address = ck.serverIndexAddMap[contactServer] - 1
				ck.mu.Unlock()
			} else {
				DPrintf("ck[%v] command %v ok", ck.uuid, args.SequenceNum)
				*Reply = *reply
				return
			}
		}
	}
}

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := ck.GetArgs(key, "", 0)
	reply := ck.GetReply()
	ck.Request(&args, &reply)
	return reply.Response
}

func (ck *Clerk) Put(key string, value string) {
	args := ck.GetArgs(key, value, 1)
	reply := ck.GetReply()
	ck.Request(&args, &reply)
}
func (ck *Clerk) Append(key string, value string) {
	args := ck.GetArgs(key, value, 2)
	reply := ck.GetReply()
	ck.Request(&args, &reply)
}

func (ck *Clerk) GetArgs(key, value string, command int) ClientRequestArgs {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	i := ck.lastExe + 1
	ck.lastExe++
	return ClientRequestArgs{
		ClientId:    ck.uuid,
		SequenceNum: i,
		Command:     command,
		Value:       value,
		Key:         key,
	}
}

func (ck *Clerk) GetReply() ClientRequestReply {
	status := 0
	res := ""
	leader := 0
	serverid := 0
	return ClientRequestReply{
		Status:     status,
		Response:   res,
		LeaderHint: leader,
		ServerId:   serverid,
	}
}

func (ck *Clerk) GetContactServer() (int, int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.contactServer, ck.serverIndexAddMap[ck.contactServer] - 1
}
