package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	primaryDeadPings int
	backupDeadPings int
	currentView *View
	primaryACK bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	// if no views set yet
	if vs.currentView.Viewnum == 0 {
		vs.currentView.Primary = args.Me
		vs.currentView.Viewnum++
	} else if vs.currentView.Primary == args.Me {
		// if primary has crashed and rebooted before declared dead
		if (args.Viewnum == 0) && vs.primaryACK {
			vs.ReplacePrimary()
		}
		vs.primaryDeadPings = 0
		// if primary is up-to-date
		if vs.currentView.Viewnum == args.Viewnum {
			vs.primaryACK = true
		}
	} else if vs.currentView.Backup == args.Me {
		if vs.currentView.Viewnum == args.Viewnum {
			vs.backupDeadPings = 0
		}
	} else if vs.primaryACK {
		if vs.currentView.Backup == "" {
			vs.ReplaceBackup(args.Me)
		}
	}

	reply.View = *vs.currentView
	vs.mu.Unlock()
	return nil
}

func (vs *ViewServer) ReplacePrimary() {
	vs.currentView.Primary = vs.currentView.Backup
	vs.currentView.Backup = ""
	vs.currentView.Viewnum++
	vs.primaryDeadPings = 0
	vs.primaryACK = false
}

func (vs *ViewServer) ReplaceBackup(serverName string) {
	vs.currentView.Backup = serverName
	vs.primaryACK = false
	vs.currentView.Viewnum++
	vs.backupDeadPings = 0
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = *vs.currentView
	vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	if vs.currentView.Viewnum == 0 {
		vs.mu.Unlock()
		return
	}

	vs.primaryDeadPings++

	if (vs.currentView.Backup != "") {
		vs.backupDeadPings++
	}

	if (vs.primaryDeadPings >= DeadPings) && vs.primaryACK {
		vs.ReplacePrimary()
	}

	if (vs.backupDeadPings >= DeadPings) && vs.primaryACK {
		vs.currentView.Backup = ""
		vs.primaryACK = false
		vs.currentView.Viewnum++
		vs.backupDeadPings = 0
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = &View{Viewnum: 0, Primary:"", Backup: ""}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
