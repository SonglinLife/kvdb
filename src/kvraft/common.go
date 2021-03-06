package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	GET            = 0
	PUT            = 1
	APPEND         = 2
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	UUID      int64
	CommandID int

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err      Err
	LeaderID int
}

type GetArgs struct {
	Key       string
	UUID      int64
	CommandID int

	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
}

type ClientRequestArgs struct {
	ClientId    int64
	SequenceNum int
	Key         string
	Value       string
	OpType      int // 0 get 1 put 2 append

}

type ClientRequestReply struct {
	Err      string
	Response string
}
