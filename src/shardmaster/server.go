package shardmaster

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
import crand "crypto/rand"
import "math/big"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	debug bool
	configs []Config // indexed by config num
	current_config int
	pid int // current paxos id processed up to
	processed map[int64]bool // map of processed operations by hash
}


type Op struct {
	Type string
	Gid int64
	Servers []string
	Hash int64 // unique hash to find duplicate ops
	Shard int
	Num int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// create new config while retaining gids and shards from previous
func (sm *ShardMaster) getNextConfig() *Config {
	current := &sm.configs[sm.current_config]
	// var next Config
	// next.Num = current.Num + 1
	// next.Groups = map[int64][]string{}
	next := Config{
		Num: current.Num + 1,
		Groups: map[int64][]string{},
		Shards: [NShards]int64{},
	}

	// copy shards to new config
	for shard, gid := range current.Shards {
		next.Shards[shard] = gid
	}

	// copy groups to new config
	for gid, servers := range current.Groups {
		next.Groups[gid] = servers
	}

	sm.configs = append(sm.configs, next)

	sm.current_config++
	return &sm.configs[sm.current_config]
}

func (sm *ShardMaster) update() {
	if sm.debug {
		fmt.Println("Updating...")
	}

	status, v := sm.px.Status(sm.pid)
	if status != paxos.Decided {
		return
	}

	op := v.(Op)
	if op.Type == "JOIN" {
		sm.join(op)
	} else if op.Type == "LEAVE" {
		sm.leave(op)
	} else if op.Type == "MOVE" {
		sm.move(op)
	} else if op.Type == "QUERY" {

	}

	sm.px.Done(sm.pid)
	sm.processed[op.Hash] = true
	sm.pid++
}

func (sm *ShardMaster) addPaxosInstance(op Op) {
	// keep updating until op has been successfully added
	for {
		if _, ok := sm.processed[op.Hash]; ok {
			return
		}

		sm.px.Start(sm.pid, op)

		to := 10 * time.Millisecond
		for {
		  status, _ := sm.px.Status(sm.pid)
		  if status == paxos.Decided{
		    break
		  }
		  time.Sleep(to)
		  if to < 10 * time.Second {
		    to *= 2
		  }
		}

		sm.update()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type: "JOIN",
		Gid: args.GID,
		Servers: args.Servers,
		Hash: nrand(),
	}

	sm.addPaxosInstance(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type: "LEAVE",
		Gid: args.GID,
		Hash: nrand(),
	}

	sm.addPaxosInstance(op)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type: "MOVE",
		Gid: args.GID,
		Shard: args.Shard,
		Hash: nrand(),
	}

	sm.addPaxosInstance(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Type: "QUERY",
		Num: args.Num,
		Hash: nrand(),
	}

	sm.addPaxosInstance(op)

	if sm.debug {
		fmt.Printf("%v: %v(%v)\n", sm.me, op.Type, op.Gid)
	}

	if op.Num == -1 || op.Num > (len(sm.configs)-1) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[op.Num]
	}

	return nil
}

// get the GID with the largest number of shards
func (sm *ShardMaster) getLargestGid(conf *Config) (int64, int) {
	gids := make(map[int64]int)
	for _, gid := range conf.Shards {
		if _, ok := gids[gid]; !ok {
			gids[gid] = 1
		} else {
			gids[gid]++
		}
	}

	max := 0
	var largest_gid int64

	for gid, count := range gids {
		if count > max {
			max = count
			largest_gid = gid
		}
	}

	return largest_gid, max
}

// get the GID with the least number of shards
func (sm *ShardMaster) getSmallestGid(conf *Config) (int64, int) {
	gids := make(map[int64]int)
	for gid, _ := range conf.Groups {
		gids[gid] = 0
	}

	for _, gid := range conf.Shards {
		// don't add if it is a group that left
		if _, ok := gids[gid]; ok {
			gids[gid]++
		}
	}

	min := NShards
	var smallest_gid int64

	for gid, count := range gids {
		if count < min {
			min = count
			smallest_gid = gid
		}
	}

	return smallest_gid, min
}

func (sm *ShardMaster) isBalanced(conf *Config, max_shards int, min_shards int) bool {
	return !(max_shards > (min_shards+1))
}

func (sm *ShardMaster) balance(op Op) {
	if sm.debug {
		fmt.Println("Balancing...")
	}
	conf := sm.getNextConfig()
	if op.Type == "JOIN" {
		// don't do anything if group already exists
		if _, ok := conf.Groups[op.Gid]; ok {
			return
		}

		// if very first group, assign all shards
		if len(conf.Groups) == 0 {
			conf.Groups[op.Gid] = op.Servers
			for i := 0; i < NShards; i++ {
				conf.Shards[i] = op.Gid
			}
		} else {
			// iterate until largest shard count is close to min
			conf.Groups[op.Gid] = op.Servers
			largest_gid, max_shards := sm.getLargestGid(conf)
			smallest_gid, min_shards := sm.getSmallestGid(conf)
			for !sm.isBalanced(conf, max_shards, min_shards) {
				// take first found shard of largest group
				for shard, holder := range conf.Shards {
					if holder == largest_gid {
						conf.Shards[shard] = smallest_gid
						break
					}
				}
				largest_gid, max_shards = sm.getLargestGid(conf)
				smallest_gid, min_shards = sm.getSmallestGid(conf)
			}
		}
	} else { // if op is LEAVE
		// get all shards contained by leaving group
		shards := make([]int, 0, NShards)
		for shard, gid := range conf.Shards {
			if gid == op.Gid {
				shards = append(shards, shard)
			}
		}

		// delete group and begin rebalancing
		delete(conf.Groups, op.Gid)

		// get all gids remaining in config
		gids := make([]int64, 0, len(conf.Groups))
		for gid, _ := range conf.Groups {
			gids = append(gids, gid)
		}

		// redistribute shards of leaving group to smallest group
		for _, shard := range shards {
			smallest_gid, _ := sm.getSmallestGid(conf)
			conf.Shards[shard] = smallest_gid
		}
	}
	if sm.debug {
		fmt.Println("Done balancing...")
		fmt.Printf("%v, %v\n", conf.Shards, conf.Groups)
	}
}

func (sm *ShardMaster) join(op Op) {
	if sm.debug {
		fmt.Printf("%v: %v(%v)\n", sm.me, op.Type, op.Gid)
	}
	sm.balance(op);
	// conf.Groups[op.Gid] = op.Servers
}
func (sm *ShardMaster) leave(op Op) {
	if sm.debug {
		fmt.Printf("%v: %v(%v)\n", sm.me, op.Type, op.Gid)
	}
	sm.balance(op);
	// _ := sm.getNextConfig()

}
func (sm *ShardMaster) move(op Op) {
	if sm.debug {
		fmt.Printf("%v: %v(%v)\n", sm.me, op.Type, op.Gid)
	}
	conf := sm.getNextConfig()
	conf.Shards[op.Shard] = op.Gid
}


// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	// sm.debug = true
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.processed = make(map[int64]bool)

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
