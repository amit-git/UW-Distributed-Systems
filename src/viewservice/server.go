package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	currentView       *View
	nextView          *View
	primaryPingTime   int64
	backupPingTime    int64
	primaryAckViewnum uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.currentView.Primary == "" && vs.currentView.Backup == "" && vs.currentView.Viewnum == 0 {
		// initial state
		vs.currentView.Viewnum += 1
		vs.currentView.Primary = args.Me
	} else {
		// check if primary ping
		if vs.currentView.Primary == args.Me && vs.currentView.Viewnum == args.Viewnum {
			vs.primaryAckViewnum = args.Viewnum
			// can switch if nextView different
			if vs.nextView.Primary != "" &&
				(vs.currentView.Backup != vs.nextView.Backup || vs.currentView.Primary != vs.nextView.Primary) {
				vs.currentView = vs.nextView
			}
		} else if vs.currentView.Primary == args.Me && args.Viewnum == 0 {
			// primary restart - can not act as primary anymore
			if vs.primaryAckViewnum == vs.currentView.Viewnum {
				currentPrimary := vs.currentView.Primary
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = currentPrimary
				vs.currentView.Viewnum += 1
			}
		} else {
			// if new backup sends a ping
			if vs.currentView.Backup == "" && vs.currentView.Primary != args.Me {
				vs.nextView.Viewnum = vs.currentView.Viewnum + 1
				vs.nextView.Backup = args.Me
				vs.nextView.Primary = vs.currentView.Primary
			}
		}

	}

	// record ping timings
	if args.Me == vs.currentView.Primary {
		vs.primaryPingTime = time.Now().UnixNano()
	} else if args.Me == vs.currentView.Backup {
		vs.backupPingTime = time.Now().UnixNano()
	}

	reply.View = *vs.currentView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	reply.View = *vs.currentView
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	curTime := time.Now().UnixNano()

	// check backup dead ?
	backupDead := false
	if (curTime - vs.backupPingTime) >= (DeadPings * int64(PingInterval)) {
		vs.currentView.Backup = ""
		backupDead = true
	}

	// check primary dead ?
	if (curTime - vs.primaryPingTime) >= (DeadPings * int64(PingInterval)) {
		if !backupDead && vs.primaryAckViewnum == vs.currentView.Viewnum {
			log.Printf("Making backup -> primary %v\n", vs.currentView.Backup)
			currentPrimary := vs.currentView.Primary
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Backup = currentPrimary
			vs.currentView.Viewnum += 1
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = &View{Viewnum: 0}
	vs.nextView = &View{Viewnum: 0}

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
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
