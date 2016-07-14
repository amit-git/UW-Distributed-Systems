package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	lastWorkFinished bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	go func() {
		workerAddress := <-mr.registerChannel
		worker := &WorkerInfo{address: workerAddress, lastWorkFinished: true}
		mr.Workers[workerAddress] = worker
		mr.readyWorkersChan <- worker
	}()

	for i := 0; i < mr.nMap; i++ {
		w := <-mr.readyWorkersChan
		go func(jobNum int) {
			args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: jobNum, NumOtherPhase: mr.nReduce}
			var reply DoJobReply
			ok := call(w.address, "Worker.DoJob", args, &reply)
			w.lastWorkFinished = ok && reply.OK
			mr.readyWorkersChan <- w
		}(i)
	}

	for j := 0; j < mr.nReduce; j++ {
		w := <-mr.readyWorkersChan
		go func(jobNum int) {
			args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: jobNum, NumOtherPhase: mr.nMap}
			var reply DoJobReply
			ok := call(w.address, "Worker.DoJob", args, &reply)
			w.lastWorkFinished = ok && reply.OK
			mr.readyWorkersChan <- w
		}(j)
	}
	return mr.KillWorkers()
}
