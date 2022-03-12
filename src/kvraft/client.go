package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	contactServer int // 上一个leader
	uuid          int64
	lastExe       int

	// serverMap         map[int]int
	// serverIndexAddMap map[int]int
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
	// ck.serverMap = map[int]int{}
	// ck.serverIndexAddMap = map[int]int{}
	return ck
}

func (ck *Clerk) changeLeader() int {
	ck.contactServer = (ck.contactServer + 1) % len(ck.servers)
	return ck.contactServer
}

func (ck *Clerk) sendRequest(args *ClientRequestArgs) string {
	// You will have to modify this function.
	server := ck.contactServer
	reply := new(ClientRequestReply)
	for !ck.servers[server].Call("KVServer.ClientRequest", args, reply) || reply.Err != OK {
		reply = new(ClientRequestReply)
		server = ck.changeLeader()
	}
	return reply.Response
}

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := ck.GetArgs(key, "", GET)
	reply := ck.sendRequest(args)
	return reply
}

func (ck *Clerk) Put(key string, value string) {
	args := ck.GetArgs(key, value, PUT)
	ck.sendRequest(args)
}
func (ck *Clerk) Append(key string, value string) {
	args := ck.GetArgs(key, value, APPEND)
	ck.sendRequest(args)
}

func (ck *Clerk) GetArgs(key, value string, OpType int) *ClientRequestArgs {
	i := ck.lastExe + 1
	ck.lastExe++
	return &ClientRequestArgs{
		ClientId:    ck.uuid,
		SequenceNum: i,
		OpType:      OpType,
		Key:         key,
		Value:       value,
	}
}
