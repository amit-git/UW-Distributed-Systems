package pbservice

import "viewservice"
import "net/rpc"
import (
	"math/rand"
	"time"
)

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println("CALL ERROR " + err.Error())
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	primary := ck.vs.Primary()
	getArgs := &GetArgs{Key: key}
	getArgs.GetId = rand.Int31()
	getReply := &GetReply{}
	keepGoing := true
	for keepGoing {
		if call(primary, "PBServer.Get", getArgs, getReply) {
			if getReply.Err == "" {
				keepGoing = false
			}
		}
	}
	return getReply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// Your code here.
	putArgs := &PutArgs{Key: key, Value: value, DoHash: dohash}
	putArgs.PutId = rand.Int31()
	putReply := &PutReply{}
	keepGoing := true
	for keepGoing {
		primary := ck.vs.Primary()
		//fmt.Printf("Calling PUT -> %v :: %v\n", primary, putArgs)
		callOk := call(primary, "PBServer.Put", putArgs, putReply)
		if callOk && putReply.Err == "" {
			keepGoing = false
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	return putReply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
