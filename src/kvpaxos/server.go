package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string
	Key string
	Value string
	Id int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	debug bool
	db map[string]string // database for key/value
	requests map[int64]bool
	latest int
}


// code from lab 3 handout
func (kv *KVPaxos) WaitForAgreement(seq int) interface{} {
	to := 10 * time.Millisecond
	for {
		status, value := kv.px.Status(seq)
		if status == paxos.Decided {
			return value
		}

		time.Sleep(to)
		if to < 10 * time.Millisecond {
			to *= 2
		}
	}
}

func printOp(op Op) {
	fmt.Printf("###Op=%v, Key=%v, Value=%v\n", op.Type, op.Key, op.Value)
}

func (kv *KVPaxos) Update() {
	for {
		status, value := kv.px.Status(kv.latest+1)

		if status != paxos.Decided {
			return
		}


		op := value.(Op)
		kv.Execute(op)
		kv.px.Done(kv.latest)

		kv.mu.Lock()
		kv.latest++
		kv.mu.Unlock()
	}
}

func (kv *KVPaxos) Execute(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if op.Type == "Get" {
		// do nothing
	} else if op.Type == "Put" {
		kv.db[op.Key] = op.Value
	} else if op.Type == "Append" {
		val, ok := kv.db[op.Key]
		if ok {
			kv.db[op.Key] = val + op.Value
		} else {
			kv.db[op.Key] = op.Value
		}
	}

	kv.requests[op.Id] = true
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	if kv.debug {
		fmt.Printf("Get(%v)\n", args.Key)
	}

	op := Op{
		Type: "Get",
		Key: args.Key,
		Value: "",
		Id: args.Id,
	}

	xid := kv.latest+1

	kv.Update()
	kv.px.Start(xid, op)
	value := kv.WaitForAgreement(xid)
	agreed_op := value.(Op)

	for agreed_op.Id != op.Id {
		xid++
		kv.px.Start(xid, op)
		value = kv.WaitForAgreement(xid)
		agreed_op = value.(Op)
	}

	kv.Update()

	kv.mu.Lock()
	reply.Value = kv.db[args.Key]
	kv.mu.Unlock()

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	if kv.debug {
		fmt.Printf("%s(%v, %v)\n", args.Op, args.Key, args.Value)
	}

	_, ok := kv.requests[args.Id]
	if ok {
		return nil
	}

	op := Op{
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		Id: args.Id,
	}

	xid := kv.latest+1

	kv.Update()
	kv.px.Start(xid, op)
	value := kv.WaitForAgreement(xid)
	agreed_op := value.(Op)

	for agreed_op.Id != op.Id {
		xid++
		kv.px.Start(xid, op)
		value = kv.WaitForAgreement(xid)
		agreed_op = value.(Op)
	}


	kv.Update()
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	// kv.debug = true
	kv.db = make(map[string]string)
	kv.requests = make(map[int64]bool)
	kv.latest = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
