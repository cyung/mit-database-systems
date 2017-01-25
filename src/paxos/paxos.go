package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"
import "math"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	debug bool
	instances map[int]*Instance
	peers_done map[string]int // map of max done value for each peer key
	me_name string
	max_instance int
}

type ProposalId struct {
	Proposal int64
	Who int
}

type Instance struct {
	xid int // id for instance of paxos
	n_p *ProposalId
	n_a *ProposalId
	v interface{}
	v_a interface{}
	decided bool
}

type PrepareArgs struct {
	Xid int
	N *ProposalId
}

type PrepareReply struct {
	PrepareOK bool
	N *ProposalId
	N_a *ProposalId
	V_a interface{}
	Done int
}

type AcceptArgs struct {
	Xid int
	N *ProposalId
	V interface{}
}

type AcceptReply struct {
	AcceptOK bool
	N *ProposalId
	Done int
}

type DecidedArgs struct {
	Xid int
	N *ProposalId
	V interface{}
}

type DecidedReply struct {
	DecidedOK bool
	Done int
}


func (pi *ProposalId) IsGreaterThan(other *ProposalId) bool {
	if pi.Proposal > other.Proposal {
		return true
	}

	return (pi.Proposal == other.Proposal) && (pi.Who > other.Who)
}

func (pi *ProposalId) IsEqual(other *ProposalId) bool {
	return (pi.Proposal == other.Proposal) && (pi.Who == other.Who)
}

func (pi *ProposalId) IsGreaterThanOrEqual(other *ProposalId) bool {
	return pi.IsGreaterThan(other) || pi.IsEqual(other)
}

func NullProposal() *ProposalId {
	return &ProposalId{-1, -1}
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// creates a paxos instance
func (px *Paxos) makeInstance(xid int, v interface{}) {
	px.instances[xid] = &Instance{
		xid: xid,
		n_p: NullProposal(),
		n_a: NullProposal(),
		v: v,
		v_a: nil,
		decided: false,
	}
}

func getProposalNum() int64 {
	// set a consistent date and subtract it from the current time to get
	// a monotonically increasing number
	now := time.Now()
	date := time.Date(2000, time.February, 1, 1, 1, 1, 1, time.UTC)
	duration := now.Sub(date)

	return duration.Nanoseconds()
}

func (px *Paxos) sendPrepare(xid int, v interface{}) bool {
	if px.debug {
		fmt.Printf("%d: SendPrepare(%v) to XID %d\n", px.me, v, xid)
	}

	px.setMaxInstance(xid)

	proposal := &ProposalId{
		Proposal: getProposalNum(),
		Who: px.me,
	}

	args := PrepareArgs{
		Xid: xid,
		N: proposal,
	}

	v_current := v
	count := 0
	min := NullProposal()

	for xid, server := range px.peers {
		var reply PrepareReply

		// skip RPC if server is self
		if xid == px.me {
			px.ReceivePrepare(&args, &reply)
		} else {
			ok := call(server, "Paxos.ReceivePrepare", &args, &reply)
			if !ok {
				if px.debug {
					fmt.Printf("%d: SendPrepare(%v) to XID %d NOT OK\n", px.me, v_current, xid)
				}
				continue
			}
		}

		if reply.PrepareOK {
			count++
			if reply.N_a.IsGreaterThan(min) {
				min.Proposal = reply.N_a.Proposal
				v_current = reply.V_a
			}
		}

		px.mu.Lock()
		px.peers_done[server] = reply.Done
		px.mu.Unlock()
	}

	if count > len(px.peers)/2 {
		return px.sendAccept(xid, proposal, v_current)
	} else {
		return false
	}
}

func (px *Paxos) sendAccept(xid int, proposal *ProposalId, v interface{}) bool {
	if px.debug {
		fmt.Printf("%d: SendAccept(%v) to XID %d\n", px.me, v, xid)
	}

	px.setMaxInstance(xid)
	args := AcceptArgs{
		Xid: xid,
		N: proposal,
		V: v,
	}

	count := 0

	for xid, server := range px.peers {
		var reply AcceptReply

		if xid == px.me {
			px.ReceiveAccept(&args, &reply)
		} else {
			ok := call(server, "Paxos.ReceiveAccept", &args, &reply)
			if !ok {
				if px.debug {
					fmt.Printf("%d: SendAccept(%v) to XID %d NOT OK\n", px.me, v, xid)
				}
				continue
			}
		}

		if reply.AcceptOK {
			count++
		}
		px.mu.Lock()
		px.peers_done[server] = reply.Done
		px.mu.Unlock()
	}

	if count > len(px.peers)/2 {
		return px.sendDecided(xid, proposal, v)
	} else {
		return false
	}
}

func (px *Paxos) sendDecided(xid int, proposal *ProposalId, v interface{}) bool {
	if px.debug {
		fmt.Printf("%d: SendDecided(%v) to XID %d\n", px.me, v, xid)
	}

	px.setMaxInstance(xid)
	args := DecidedArgs{
		Xid: xid,
		N: proposal,
		V: v,
	}

	for xid, server := range px.peers {
		var reply DecidedReply

		if xid == px.me {
			px.ReceiveDecided(&args, &reply)
		} else {
			ok := call(server, "Paxos.ReceiveDecided", &args, &reply)
			if !ok {
				if px.debug {
					fmt.Printf("%d: SendDecided(%v) to XID %d NOT OK\n", px.me, v, xid)
				}
				continue
			}
		}
		px.mu.Lock()
		px.peers_done[server] = reply.Done
		px.mu.Unlock()
	}

	return true
}

func (px *Paxos) ReceivePrepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.debug {
		fmt.Printf("%d: ReceivePrepared() at XID %d\n", px.me, args.Xid)
	}

	xid := args.Xid
	px.setMaxInstance(xid)

	_, ok := px.instances[xid]
	if !ok {
		px.makeInstance(xid, nil)
		px.instances[xid].n_p = args.N
		reply.PrepareOK = true
	} else if args.N.IsGreaterThan(px.instances[xid].n_p) {
		px.instances[xid].n_p = args.N
		reply.PrepareOK = true
	} else if px.debug {
		fmt.Printf("%d: Prepare NOT OK\n", px.me)
	}

	reply.N_a = px.instances[xid].n_a
	reply.V_a = px.instances[xid].v_a
	reply.Done = px.peers_done[px.me_name]

	return nil
}

func (px *Paxos) ReceiveAccept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.debug {
		fmt.Printf("%d: ReceiveAccept(%v) at XID %d\n", px.me, args.V, args.Xid)
	}

	xid := args.Xid
	px.setMaxInstance(xid)

	_, ok := px.instances[xid]
	if !ok {
		px.makeInstance(xid, nil)
		reply.AcceptOK = true
	}
	if args.N.IsGreaterThanOrEqual(px.instances[xid].n_p) {
		reply.AcceptOK = true
	}

	if reply.AcceptOK {
		px.instances[xid].n_p = args.N
		px.instances[xid].n_a = args.N
		px.instances[xid].v_a = args.V
	} else if px.debug {
		fmt.Printf("%d: Accept NOT OK\n", px.me)
	}

	reply.Done = px.peers_done[px.me_name]
	return nil
}

func (px *Paxos) ReceiveDecided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.debug {
		fmt.Printf("%d: ReceiveDecided() at XID %d\n", px.me, args.Xid)
	}

	xid := args.Xid
	px.setMaxInstance(xid)

	_, ok := px.instances[xid]
	if !ok {
		px.makeInstance(xid, nil)
	}

	px.instances[xid].n_p = args.N
	px.instances[xid].n_a = args.N
	px.instances[xid].v_a = args.V
	px.instances[xid].decided = true
	reply.DecidedOK = true
	reply.Done = px.peers_done[px.me_name]

	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go func() {
		if seq >= px.Min() {
			for {
				ok := px.sendPrepare(seq, v)
				if ok {
					break
				}
			}
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.peers_done[px.me_name] < seq {
		px.peers_done[px.me_name] = seq
	}
	px.Max()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max := 0

	for xid, _ := range px.instances {
		if xid > max {
			max = xid
		}
	}
	return px.max_instance
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	min := math.MaxInt32

	for _, xid := range px.peers_done {
		if xid < min {
			min = xid
		}
	}

	return min+1
}

func (px *Paxos) clearMemory() {
	min := -1
	for !px.isdead() {
		if px.debug {
			fmt.Println("CLEARING MEMORY")
		}
		old_min := px.Min()
		if min < old_min {
			for xid, _ := range px.instances {
				if xid < old_min {
					px.deleteFromMemory(xid)
				}
				min = old_min
			}

		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (px *Paxos) deleteFromMemory(xid int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.debug {
		fmt.Printf("%v: deleting %v from memory\n", px.me, xid)
	}

	delete(px.instances, xid)
}

func (px *Paxos) setMaxInstance(xid int) {
	if xid > px.max_instance {
		px.max_instance = xid
	}
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

	_, ok := px.instances[seq]
	if !ok {
		return Forgotten, nil
	}

	if px.instances[seq].decided {
		return Decided, px.instances[seq].v_a
	} else {
		return Pending, nil
	}
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	// px.debug = true
	px.instances = map[int]*Instance{}
	px.peers_done = make(map[string]int, len(peers))
	px.mu.Lock()
	for _, peer := range px.peers {
		px.peers_done[peer] = -1
	}
	px.mu.Unlock()
	px.me_name = px.peers[px.me]
	go px.clearMemory()

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
