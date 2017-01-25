package mapreduce

import "container/list"
import "fmt"
import "sync"

type WorkerInfo struct {
	sync.Mutex
	address string
	ready bool
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
			// fmt.Println("Shut down one worker")
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func getWorkers(mr *MapReduce) {
	for mr.finishedAll == false {
		worker := <-mr.registerChannel
		if worker == "" {
			continue
		}
		mr.Workers[worker] = &WorkerInfo{address: worker, ready: true}
	}
	// fmt.Println("FINISHED GETTING WORKERS")
	mr.finished <- true
}

func performJob(worker *WorkerInfo, mr *MapReduce, job JobType, jobNum int) {
	var numOtherPhase int
	switch job {
	case "Map":
		numOtherPhase = mr.nReduce
	case "Reduce":
		numOtherPhase = mr.nMap
	}

	args := &DoJobArgs{
		mr.file,
		job,
		jobNum,
		numOtherPhase,
	}

	var reply DoJobReply

	ok := call(worker.address, "Worker.DoJob", args, &reply)


	if ok == false {
		// fmt.Printf("WORKER FAILURE HAS OCCURRED AT JOB %d\n", jobNum)
		mr.JobTable.ChangeStatus(jobNum, 0)
	} else {
		if job == "Map" {
			// fmt.Println("WAITING FOR LOCK")
			mr.JobTable.Lock()
			// fmt.Println("LOCKED")
			if mr.JobTable.table[jobNum] == 2 {
				mr.JobTable.Unlock()
				// fmt.Println("UNLOCKED")
				return
			} else {
				mr.JobTable.table[jobNum] = 2
				mr.opCount.Inc()
				mr.JobTable.Unlock()
				// fmt.Println("UNLOCKED")
			}
			// fmt.Printf("Job %d completed  |  mapCount = %d\n", jobNum, mr.opCount.counter)
			if mr.opCount.counter == mr.nMap {
				mr.finishedMap <- true
			}
		} else {
			mr.JobTable.Lock()
			if mr.JobTable.table[jobNum] == 2 {
				mr.JobTable.Unlock()
				return
			} else {
				mr.JobTable.table[jobNum] = 2
				mr.opCount.Inc()
			}
			mr.JobTable.Unlock()
			// fmt.Printf("Job %d completed  |  reduceCount = %d\n", jobNum, mr.opCount.counter)
			if mr.opCount.counter == mr.nReduce {
				mr.finishedReduce <- true
			}
		}

		worker.ready = true
	}
}

func runWorker(worker *WorkerInfo, mr *MapReduce, job JobType) {
	var jobCount int
	if job == "Map" {
		jobCount = mr.nMap
	} else {
		jobCount = mr.nReduce
	}

	if mr.opCount.counter >= jobCount {
		return
	}

	worker.Lock()
	if worker.ready == false {
		worker.Unlock()
		return
	}

	worker.ready = false
	worker.Unlock()

	mr.JobTable.Lock()
	for jobNum, status := range mr.JobTable.table {
		if status == 0 { // READY
			mr.JobTable.table[jobNum] = 1 // IN PROGRESS
			mr.JobTable.Unlock()
			performJob(worker, mr, job, jobNum)
			return
		}
	}
	mr.JobTable.Unlock()
}


func (mr *MapReduce) RunMaster() *list.List {
	go getWorkers(mr)

	mr.JobTable.Init(mr.nMap)

	for mr.opCount.counter < mr.nMap {
		for _, w := range mr.Workers {
			if mr.opCount.counter >= mr.nMap {
				break
			}
			go runWorker(w, mr, "Map")
		}
	}

	<-mr.finishedMap
	// fmt.Println("FINISHED MAP")
	mr.JobTable.Init(mr.nReduce)
	mr.opCount.Reset()

	for mr.opCount.counter < mr.nReduce {
		for _, w := range mr.Workers {
			if mr.opCount.counter >= mr.nReduce {
				break
			}
			go runWorker(w, mr, "Reduce")
		}
	}

	<-mr.finishedReduce
	// fmt.Println("FINISHED REDUCE")
	mr.finishedAll = true
	close(mr.registerChannel)
	mr.registerChannel = make(chan string)


	<-mr.finished
	return mr.KillWorkers()
}
