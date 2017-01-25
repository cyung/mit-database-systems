package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strings"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Type string
	Key string
	Value string
	Hash int64 // unique hash to find duplicate ops
	Config shardmaster.Config
	Shard int
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	debug bool
	db map[string]string
	requests map[int64]bool
	latest int
	conf shardmaster.Config
}

func (kv *ShardKV) addPaxosInstance(op Op) {
	// keep updating until op has been successfully added
	for {
		if _, ok := kv.requests[op.Hash]; ok {
			return
		}

		kv.px.Start(kv.latest, op)

		to := 10 * time.Millisecond
		for {
			status, _ := kv.px.Status(kv.latest)
			if status == paxos.Decided{
				break
			}
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}

		kv.update()
	}
}

func (kv *ShardKV) update() {
	if kv.debug {
		// fmt.Printf("%v: Updating...\n", kv.gid)
	}

	status, v := kv.px.Status(kv.latest)
	if status != paxos.Decided {
		return
	}

	op := v.(Op)
	if op.Type == "GET" {
		// do nothing
	} else if op.Type == "PUT" {
		kv.put(op)
	} else if op.Type == "APPEND" {
		kv.append(op)
	} else if op.Type == "RECONFIG" {
		kv.reconfigure(op)
	} else if op.Type == "GETSHARD" {
		// do nothing
	}

	kv.px.Done(kv.latest)
	kv.requests[op.Hash] = true
	kv.latest++

	if kv.debug {
		// fmt.Printf("%v: Updated... %v\n", kv.gid, kv.latest)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.debug {
		fmt.Printf("%v: GET(%v)\n", kv.gid, args.Key)
	}

	op := Op{
		Type: "GET",
		Key: args.Key,
		Hash: args.Hash,
	}

	if kv.gid != kv.conf.Shards[key2shard(op.Key)] {
		reply.Err = ErrWrongGroup
		return nil
	}

	kv.addPaxosInstance(op)

	reply.Value = kv.db[op.Key]
	reply.Err = OK
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.debug {
		fmt.Printf("%v: %v(%v, %v)\n", kv.gid, args.Op, args.Key, args.Value)
	}

	if _, ok := kv.requests[args.Hash]; ok {
		return nil
	}

	op := Op{
		Type: strings.ToUpper(args.Op),
		Key: args.Key,
		Value: args.Value,
		Hash: args.Hash,
	}

	kv.addPaxosInstance(op)
	reply.Err = OK
	return nil
}

func (kv *ShardKV) put(op Op) {
	kv.db[op.Key] = op.Value
}

func (kv *ShardKV) append(op Op) {
	kv.db[op.Key] += op.Value
}

func (kv *ShardKV) Reconfigure(op Op) bool {
	if kv.debug {
		fmt.Printf("%v: Reconfiguring...\n", kv.gid)
	}
	
	if ok := kv.reconfigure(op); ok {
		kv.addPaxosInstance(op)
	} else {
		if kv.debug {
			fmt.Printf("%v: Failed to configure\n", kv.gid)
		}
		return false
	}

	return true
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	if kv.conf.Num < args.Config.Num {
		reply.Err = "Config num is behind"
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.debug {
		fmt.Printf("%v: GetShard(%v)\n", kv.gid, args.Shard)
	}

	op := Op{
		Type: "GETSHARD",
		Shard: args.Shard,
		Hash: args.Hash,
	}

	// need to add to paxos log because checking the config num is not
	// enough since a server can be very far ahead
	kv.addPaxosInstance(op)

	// iterate through db and find all keys for shard
	reply.Db = make(map[string]string)

	for k, v := range kv.db {
		if key2shard(k) == args.Shard {
			reply.Db[k] = v
		}
	}
	reply.Err = OK

	return nil
}

func (kv *ShardKV) needsReconfigure(shard int, new_gid int64) bool {
	old_gid := kv.conf.Shards[shard] // previous shard holder

	// if not holder of shard in new config
	if new_gid != kv.gid {
		return false
	}

	// if holder, but was also previously holder
	if old_gid == new_gid {
		return false
	}

	return true
}

func (kv *ShardKV) reconfigure(op Op) bool {
	shard_data := make(map[string]string)
	// figure out shard changes
	if kv.debug {
		fmt.Printf("%v: shards     %v\n", kv.gid, kv.conf.Shards)
		fmt.Printf("%v: new shards %v\n", kv.gid, op.Config.Shards)
		fmt.Printf("%v: current config num %v to %v\n", kv.gid, kv.conf.Num, op.Config.Num)
	}
	for shard, new_gid := range op.Config.Shards {

		// if new config has different shard config than previous
		if kv.needsReconfigure(shard, new_gid) {
			found := false

			// iterate through servers in old group and get data
			old_gid := kv.conf.Shards[shard] // previous shard holder
			if old_gid == 0 {
				break
			}
			for _, server := range kv.conf.Groups[old_gid] {
				args := &GetShardArgs{
					Shard: shard,
					Config: kv.conf,
					Hash: nrand(),
				}

				var reply GetShardReply

				ok := call(server, "ShardKV.GetShard", args, &reply)

				// server will reject GetShard if not up-to-date with sent config
				if ok && reply.Err == OK {
					if kv.debug {
						fmt.Printf("%v: Acquiring new data %v\n", kv.gid, reply.Db)
					}
					found = true
					for k, v := range reply.Db {
						shard_data[k] = v
					}
					// can break as soon as get valid request from one group
					break
				}
			}

			if !found {
				if kv.debug {
					fmt.Printf("%v: nothing found\n", kv.gid)
				}
				return false
			}
		}

	}

	kv.latest++
	kv.conf = op.Config
	// if sucessfully finishes, all shard data has been obtained
	for k, v := range shard_data {
		kv.db[k] = v
	}
	return true
}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	conf := kv.sm.Query(-1)
	for kv.conf.Num < conf.Num {
		op := Op{
			Type: "RECONFIG",
			Config: kv.sm.Query(kv.latest+1),
		}
		ok := kv.Reconfigure(op)
		// abandon tick if reconfigure fails to prevent deadlock
		if !ok {
			return
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	// kv.debug = true
	kv.db = make(map[string]string)
	kv.requests = make(map[int64]bool)

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
