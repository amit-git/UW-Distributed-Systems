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
	go func() {
		for {
			workerAddress := <-mr.registerChannel
			worker := &WorkerInfo{address: workerAddress, lastWorkFinished: true}
			mr.Workers[workerAddress] = worker
			fmt.Printf("Registered worker %v\n", worker.address)
			mr.readyWorkersChan <- worker
		}
	}()

	for i := 0; i < mr.nMap; {
		fmt.Printf("Waiting for ready worker\n")
		w := <-mr.readyWorkersChan
		if !w.lastWorkFinished {
			i--
			continue
		}

		fmt.Printf("Ready worker %v\n", w.address)
		go func(jobNum int, worker *WorkerInfo) {
			args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: jobNum, NumOtherPhase: mr.nReduce}
			var reply DoJobReply
			ok := call(worker.address, "Worker.DoJob", args, &reply)
			worker.lastWorkFinished = ok && reply.OK
			mr.readyWorkersChan <- worker
		}(i, w)
		i++
	}

	for j := 0; j < mr.nReduce; {
		w := <-mr.readyWorkersChan
		if !w.lastWorkFinished {
			j--
			continue
		}

		fmt.Printf("Submitting REDUCE #%v/%v\n", j, mr.nReduce)
		go func(jobNum int, worker *WorkerInfo) {
			args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: jobNum, NumOtherPhase: mr.nMap}
			var reply DoJobReply
			ok := call(worker.address, "Worker.DoJob", args, &reply)
			worker.lastWorkFinished = ok && reply.OK
			mr.readyWorkersChan <- worker
		}(j, w)
		j++
	}
	return mr.KillWorkers()
}
