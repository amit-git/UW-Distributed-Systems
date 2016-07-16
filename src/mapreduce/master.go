package mapreduce

import (
	"container/list"
	"errors"
	"fmt"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
	lastWorkFinished bool
	lastJobType      string
	lastJobNum       int
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

func (mr *MapReduce) getNextReduceJob() (int, error) {
	if len(mr.reduceJobsQueue) == 0 && len(mr.pendingReduceJobs) != 0 {
		return -1, errors.New("PendingReduce")
	}
	if len(mr.reduceJobsQueue) == 0 {
		return -1, errors.New("DoneReduce")
	}
	nextReduceJobNum := mr.reduceJobsQueue[0]
	if len(mr.reduceJobsQueue) > 1 {
		mr.reduceJobsQueue = mr.reduceJobsQueue[1:]
	} else {
		mr.reduceJobsQueue = []int{}
	}
	mr.pendingReduceJobs[nextReduceJobNum] = true
	return nextReduceJobNum, nil
}

func (mr *MapReduce) getNextMapJob() (int, error) {
	if len(mr.mapJobsQueue) == 0 && len(mr.pendingMapJobs) != 0 {
		return -1, errors.New("PendingMap")
	}
	if len(mr.mapJobsQueue) == 0 {
		return -1, errors.New("DoneMap")
	}
	nextMapJobNum := mr.mapJobsQueue[0]
	if len(mr.mapJobsQueue) > 1 {
		mr.mapJobsQueue = mr.mapJobsQueue[1:]
	} else {
		mr.mapJobsQueue = []int{}
	}
	mr.pendingMapJobs[nextMapJobNum] = true
	return nextMapJobNum, nil
}

func (mr *MapReduce) getNextJobToRun() (int, string, error) {
	nextMapJobNum, err := mr.getNextMapJob()
	if err != nil && err.Error() == "DoneMap" {
		// reduce step
		nextReduceJobNum, err := mr.getNextReduceJob()
		return nextReduceJobNum, Reduce, err
	}
	return nextMapJobNum, Map, err
}

func (mr *MapReduce) processMapWorkerResponse(w *WorkerInfo) {
	delete(mr.pendingMapJobs, w.lastJobNum)
	if !w.lastWorkFinished {
		// resubmit
		mr.mapJobsQueue = append(mr.mapJobsQueue, w.lastJobNum)
	}
}

func (mr *MapReduce) processReduceWorkerResponse(w *WorkerInfo) {
	delete(mr.pendingReduceJobs, w.lastJobNum)
	if !w.lastWorkFinished {
		// resubmit
		mr.reduceJobsQueue = append(mr.reduceJobsQueue, w.lastJobNum)
	}
}

func (mr *MapReduce) processWorkerResponse(w *WorkerInfo) {
	if w.lastJobType == Map {
		mr.processMapWorkerResponse(w)
	} else {
		mr.processReduceWorkerResponse(w)
	}
}

func (mr *MapReduce) scheduleJob(jobType string, jobNum int, workerAddr string) {
	args := &DoJobArgs{File: mr.file, Operation: JobType(jobType), JobNumber: jobNum}
	if jobType == Map {
		args.NumOtherPhase = mr.nReduce
	} else {
		args.NumOtherPhase = mr.nMap
	}
	var reply DoJobReply
	ok := call(workerAddr, "Worker.DoJob", args, &reply)
	if !ok {
		mr.failedWorkersChan <- &WorkerInfo{address: workerAddr, lastWorkFinished: false,
			lastJobNum: jobNum, lastJobType: jobType}
	} else {
		mr.readyWorkersChan <- &WorkerInfo{address: workerAddr, lastWorkFinished: reply.OK,
			lastJobNum: jobNum, lastJobType: jobType}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	allJobsDone := false
	for !allJobsDone {
		select {
		case workerAddress := <-mr.registerChannel:
			worker := &WorkerInfo{address: workerAddress, lastWorkFinished: true}
			mr.Workers[workerAddress] = worker
			fmt.Printf("Registered worker %v\n", worker.address)
			go func(wk *WorkerInfo) {
				mr.readyWorkersChan <- wk
			}(worker)
		case badWorker := <-mr.failedWorkersChan:
			mr.processWorkerResponse(badWorker)
		case w := <-mr.readyWorkersChan:
			mr.processWorkerResponse(w)
			nextJobNum, nextJobType, err := mr.getNextJobToRun()
			if err == nil {
				go func(jobType string, jobNum int, workerAddr string) {
					mr.scheduleJob(jobType, jobNum, workerAddr)
				}(nextJobType, nextJobNum, w.address)
			} else if err.Error() == "DoneReduce" {
				allJobsDone = true
			}
		default:
		}
	}
	return mr.KillWorkers()
}
