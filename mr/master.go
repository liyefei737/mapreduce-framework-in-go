package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"path/filepath"
	"sync"
	"time"
)

type Master struct {
	phase             string   // "map" or "reduce", or "done"  done means all map and reduce tasks are all finished
	splits            []string // paper refers to a input partition as a split, which is the input to a map function
	NReduce           int
	mapTasksStatus    map[int]*TaskStatus
	reduceTasksStatus map[int]*TaskStatus
	inProgressTasks   map[int]bool // a "set" of tasks(either all map tasks or reduce tasks) that are in progress
	mapTasksTodo      int
	reduceTasksTodo   int
	GetTaskLock       sync.Mutex
	ReportDoneLock    sync.Mutex
}

type TaskStatus struct {
	status    string // notStarted,inProgress, or finished
	startTime time.Time
}

//
// RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
//
func (master *Master) GetTask(args *WorkerMessage, reply *MasterMessage) error {
	master.GetTaskLock.Lock()
	defer master.GetTaskLock.Unlock()
	switch master.phase {
	case "map":
		log.Printf("Asking for a map task\n")
		for taskID, taskStatus := range master.mapTasksStatus {
			if taskStatus.status == "notStarted" {
				taskStatus.status = "inProgress"
				taskStatus.startTime = time.Now()
				master.inProgressTasks[taskID] = true

				reply.Task = "map"
				reply.File = master.splits[taskID]
				reply.TaskID = taskID
				reply.NReduce = master.NReduce
				return nil
			}
		}

	case "reduce":
		log.Printf("Asking for a reduce task\n")
		for taskID, taskStatus := range master.reduceTasksStatus {
			if taskStatus.status == "notStarted" {
				taskStatus.status = "inProgress"
				taskStatus.startTime = time.Now()
				master.inProgressTasks[taskID] = true

				reply.Task = "reduce"
				reply.TaskID = taskID
				return nil
			}
		}

	case "done":
		reply.Task = "done"
		return nil

	}

	reply.Task = "idle"
	return nil
}

func (master *Master) ReportDone(workerMssg *WorkerMessage, reply *MasterMessage) error {
	master.ReportDoneLock.Lock()
	defer master.ReportDoneLock.Unlock()

	_, isPresent := master.inProgressTasks[workerMssg.TaskID]

	// time-out checker already reset this task to notStarted
	if !isPresent {
		return errors.New("timeout error: task taken longer than 10s")
	}

	switch workerMssg.Task {
	case "map":
		startTime := master.mapTasksStatus[workerMssg.TaskID].startTime
		timeTaken := time.Now().Sub(startTime)
		if timeTaken.Seconds() > 10 {
			master.mapTasksStatus[workerMssg.TaskID].status = "notStarted"
			delete(master.inProgressTasks, workerMssg.TaskID)
			return errors.New("timeout error: task taken longer than 10s")
		} else {
			master.mapTasksStatus[workerMssg.TaskID].status = "finished"
			delete(master.inProgressTasks, workerMssg.TaskID)
			master.mapTasksTodo -= 1
			if master.mapTasksTodo == 0 {
				master.phase = "reduce"
			}
		}

	case "reduce":
		startTime := master.reduceTasksStatus[workerMssg.TaskID].startTime
		timeTaken := time.Now().Sub(startTime)
		if timeTaken.Seconds() > 10 {
			master.reduceTasksStatus[workerMssg.TaskID].status = "notStarted"
			delete(master.inProgressTasks, workerMssg.TaskID)
			return errors.New("timeout error: task taken longer than 10s")
		} else {
			master.reduceTasksStatus[workerMssg.TaskID].status = "finished"
			delete(master.inProgressTasks, workerMssg.TaskID)
			master.reduceTasksTodo -= 1
			if master.reduceTasksTodo == 0 {
				master.phase = "done"
			}
		}

	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (master *Master) server() {
	println("running master.server()")
	rpc.Register(master)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":12346")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (master *Master) Done() bool {
	if master.phase == "done" {
		log.Printf("MapReduce job succeeded!\n")

		// this loop is also important because we will need to have the master alive for some time to
		// send the done task to all the workers signalling normal termination
		//for i := 10; i > 0; i-- {
		//	log.Printf("Master exiting in %vs...\n", i)
		//	time.Sleep(1 * time.Second)
		//}
		//log.Printf("Master shutting down...\n")
		return true
	}

	return false
}

//
// create a Master. main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	absFilePaths := toAbsPaths(files)
	mapTasksStatus := map[int]*TaskStatus{}
	reduceTasksStatus := map[int]*TaskStatus{}

	// initialize map tasks' status
	for i := 0; i < len(absFilePaths); i++ {
		mapTasksStatus[i] = &TaskStatus{
			status:    "notStarted",
			startTime: time.Time{},
		}
	}

	// initialize reduce tasks' status
	for i := 0; i < nReduce; i++ {
		reduceTasksStatus[i] = &TaskStatus{
			status:    "notStarted",
			startTime: time.Time{},
		}
	}

	master := Master{
		phase:             "map", // first do map then reduce
		splits:            absFilePaths,
		NReduce:           nReduce,
		mapTasksStatus:    mapTasksStatus,
		reduceTasksStatus: reduceTasksStatus,
		inProgressTasks:   make(map[int]bool),
		mapTasksTodo:      len(absFilePaths),
		reduceTasksTodo:   nReduce,
	}

	// start a long running thread to check for timed-out tasks every 5 seconds
	go func(periodInSeconds int) {
		for {
			//log.Printf("Running clean up thread to look for timed-out tasks")
			for taskID := range master.inProgressTasks {
				timeElapsed := time.Now().Sub(mapTasksStatus[taskID].startTime)
				if timeElapsed.Seconds() > 10 {
					log.Printf("Found a timed-out task: \n")
					master.mapTasksStatus[taskID].status = "notStarted"
					delete(master.inProgressTasks, taskID)
				}
			}
			time.Sleep(time.Duration(periodInSeconds) * time.Second)
		}
	}(5)

	master.server()
	return &master
}

func toAbsPaths(files []string) []string {
	for i, _ := range files {
		absPath, err := filepath.Abs(files[i])
		if err != nil {
			log.Fatal(err)
		}
		files[i] = absPath
	}
	return files
}
