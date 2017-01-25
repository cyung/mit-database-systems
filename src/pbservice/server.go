package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	db map[string]string
	currentView *viewservice.View
	requests map[int64]bool
	commandNum int
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()

	if pb.me != pb.currentView.Primary {
		reply.View = pb.currentView
		pb.mu.Unlock()
		return errors.New("GET ERROR: Not primary server.")
	}

	val, ok := pb.db[args.Key]
	if ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}

	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	_, ok := pb.requests[args.Id]

	// if request already processed
	if ok {
		pb.mu.Unlock()
		return nil
	}

	// if handling forwarded request from primary
	if args.FromPrimary {
		if args.Option == "Put" {
			pb.db[args.Key] = args.Value
		} else if args.Option == "Append" {
			val, ok := pb.db[args.Key]
			if ok {
				pb.db[args.Key] = val + args.Value
			} else {
				pb.db[args.Key] = args.Value
			}
		}
	} else if pb.me != pb.currentView.Primary {
		pb.mu.Unlock()
		return errors.New("PUTAPPEND ERROR: Not primary server.")
	} else { // primary route
		// if there exists a backup
		if pb.currentView.Backup != "" {
			args.FromPrimary = true
			ok = call(pb.currentView.Backup, "PBServer.PutAppend", args, reply)

			if !ok {
				pb.mu.Unlock()
				return errors.New("Unable to contact backup")
			}
		}

		if args.Option == "Put" {
			pb.db[args.Key] = args.Value
		} else if args.Option == "Append" {
			val, ok := pb.db[args.Key]
			if ok {
				pb.db[args.Key] = val + args.Value
			} else {
				pb.db[args.Key] = args.Value
			}
		}
	}

	pb.requests[args.Id] = true
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) CopyData(args *CopyArgs, reply *CopyReply) error {
	pb.mu.Lock()
	pb.db = args.Db
	pb.mu.Unlock()
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	reply, ok := pb.vs.Ping(pb.currentView.Viewnum)

	// reset to idle if ping fails
	if ok != nil {
		pb.currentView.Viewnum = 0
		pb.currentView.Primary = ""
		pb.currentView.Backup = ""
		pb.mu.Unlock()
		return
	}

	// do nothing if view is up-to-date
	if pb.currentView.Viewnum == reply.Viewnum {
		pb.mu.Unlock()
		return
	}

	// TWO SCENARIOS
	// 1. Switched from backup to primary, and backup is now ""
	// 2. backup was changed from "" to Y
	if pb.me == reply.Primary {
		if reply.Backup != "" {
			args := &CopyArgs{Db: pb.db}
			var copyReply CopyReply
			call(reply.Backup, "PBServer.CopyData", args, &copyReply)
		}
	} else if pb.me == reply.Backup {
		// changed from idle to backup
		// do nothing
	} else {
		// is an idle server
		// do nothing
	}

	pb.currentView = &reply
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.db = make(map[string]string)
	pb.currentView = &viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.requests = make(map[int64]bool)


	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
