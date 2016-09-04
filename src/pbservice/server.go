package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import (
	"strconv"
	"errors"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l                    net.Listener
	dead                 bool             // for testing
	unreliable           bool             // for testing
	me                   string
	vs                   *viewservice.Clerk
	done                 sync.WaitGroup
	finish               chan interface{}
					      // Your declarations here.
	view                 viewservice.View
	store                map[string]string
	putRequestsProcessed map[int32]string // value is the previous PUT value
	getRequestsProcessed map[int32]bool
	lock                 *sync.Mutex
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.lock.Lock()
	defer pb.lock.Unlock()

	if pb.view.Primary != pb.me || pb.dead {
		log.Printf("Rejecting PUT - %v Not a primary", pb.me)
		// not a primary
		reply.Err = "IllegalStateException"
		return errors.New("IllegalStateException")
	}

	//fmt.Printf("Serving PUT %v\n", args)

	if pb.putRequestsProcessed[args.PutId] != "" {
		reply.PreviousValue = pb.putRequestsProcessed[args.PutId]
		return nil
	}
	reply.PreviousValue = pb.store[args.Key]
	if args.DoHash {
		pb.store[args.Key] = strconv.Itoa(int(hash(pb.store[args.Key] + args.Value)))
	} else {
		pb.store[args.Key] = args.Value
	}

	if pb.view.Primary == pb.me && pb.view.Backup != "" {
		syncPutArgs := &SyncPutArgs{Key: args.Key, Value: pb.store[args.Key], PutId:args.PutId}
		syncPutReply := &SyncPutReply{}
		call(pb.view.Backup, "PBServer.SyncPut", syncPutArgs, syncPutReply)
	}

	pb.putRequestsProcessed[args.PutId] = reply.PreviousValue
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.lock.Lock()
	defer pb.lock.Unlock()

	fmt.Printf("Get on %v for %v (primary = %v) \n", pb.me, args.Key, pb.view.Primary)
	if pb.view.Primary != pb.me || pb.dead {
		log.Printf("Rejecting GET - %v Not a primary", pb.me)
		// not a primary
		reply.Err = "IllegalStateException"
		return errors.New("IllegalStateException")
	}

	if pb.getRequestsProcessed[args.GetId] {
		reply.Value = pb.store[args.Key]
		return nil
	}
	// log.Printf("GET %v\n", args)

	if pb.store[args.Key] != "" {
		reply.Value = pb.store[args.Key]
	} else {
		reply.Value = ""
	}
	pb.getRequestsProcessed[args.GetId] = true
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	//log.Printf("TICK %v\n", pb.me)
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		pb.lock.Lock()
		defer pb.lock.Unlock()
		if view.Primary == pb.me && pb.view.Backup != view.Backup {
			// send entire store to Backup
			syncDBArgs := &SyncDBArgs{View:view,
				Store:pb.store,
				GetRequestsProcessed:pb.getRequestsProcessed,
				PutRequestsProcessed:pb.putRequestsProcessed}
			syncDBReply := &SyncDBReply{}
			call(view.Backup, "PBServer.SyncDB", syncDBArgs, syncDBReply)
		}
		pb.view = view
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func (pb *PBServer) SyncPut(args *SyncPutArgs, reply *SyncPutReply) error {
	//log.Printf("SyncPUT %v\n", args)
	pb.lock.Lock()
	defer pb.lock.Unlock()
	pb.putRequestsProcessed[args.PutId] = pb.store[args.Key]
	pb.store[args.Key] = args.Value
	reply.Err = nil
	return nil
}
func (pb *PBServer) SyncDB(args *SyncDBArgs, reply *SyncDBReply) error {
	fmt.Printf("SyncDB received at %v from %v ::: keys %v\n", pb.me, args.View.Primary, len(args.Store))
	pb.lock.Lock()
	defer pb.lock.Unlock()

	pb.view = args.View

	pb.store = make(map[string]string)
	for k, v := range args.Store {
		pb.store[k] = v
	}

	pb.getRequestsProcessed = make(map[int32]bool)
	for k, v := range args.GetRequestsProcessed {
		pb.getRequestsProcessed[k] = v
	}

	pb.putRequestsProcessed = make(map[int32]string)
	for k, v := range args.PutRequestsProcessed {
		pb.putRequestsProcessed[k] = v
	}

	reply.Err = nil
	return nil
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.lock = &sync.Mutex{}
	pb.lock.Lock()
	defer pb.lock.Unlock()

	pb.store = make(map[string]string)
	pb.getRequestsProcessed = make(map[int32]bool)
	pb.putRequestsProcessed = make(map[int32]string)

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
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63() % 1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63() % 1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
