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
	phase              string   // "map" or "reduce"
	splits             []string // paper refers to a input partition as a split, which is the input to a map function
	NReduce            int
	mapTaskStatus      map[int]*MapStatus
	inProgressMapTasks map[int]bool // a "set" of map tasks that are in progress
	lock               sync.Mutex
	wg                 sync.WaitGroup
}

func (master *Master) setMapReply(reply *MasterMessage) {

	for taskID, taskStatus := range master.mapTaskStatus {
		if taskStatus.status == "notStarted" {
			taskStatus.status = "inProgress"
			taskStatus.startTime = time.Now()
			master.inProgressMapTasks[taskID] = true

			reply.Task = "map"
			reply.TaskID = taskID
			reply.NReduce = master.NReduce
			return
		}
	}

	// all tasks are either in inProgress(a worker is working on it) or finished state
	// we make this worker to run the first reduce, and block untill all other map tasks are finished
	master.wg.Wait()
	master.phase = "reduce"
	reply.Task = "skip"

}

//
// RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
//
func (master *Master) GetTask(args *WorkerMessage, reply *MasterMessage) error {
	//log.Printf("giving out a task\n")
	switch master.phase {
	case "map":
		master.lock.Lock()
		log.Printf("Asking for a map task\n")
		for taskID, taskStatus := range master.mapTaskStatus {
			if taskStatus.status == "notStarted" {
				taskStatus.status = "inProgress"
				taskStatus.startTime = time.Now()
				master.inProgressMapTasks[taskID] = true

				reply.Task = "map"
				reply.File = master.splits[taskID]
				reply.TaskID = taskID
				reply.NReduce = master.NReduce
				master.lock.Unlock()
				return nil
			}
		}
		master.lock.Unlock()

		// all tasks are either in inProgress(a worker is working on it) or finished state if we come here
		// we make this worker to run the first reduce, and block until all other map tasks are finished
		master.wg.Wait()
		master.phase = "reduce"
		return nil

	case "reduce":

		log.Printf("Asking for a reduce task\n")
		reply.Task = "reduce"

		//if !master.areAllMapTasksDone() {
		//	reply.Task = "skip"
		//} else {
		//	// give out reduce task as we know all map tasks are in finished state
		//	reply.Task = "reduce"
		//}

	}

	return nil
}

func (master *Master) ReportDone(args *WorkerMessage, reply *MasterMessage) error {

	_, isPresent := master.inProgressMapTasks[args.TaskID]

	// the long running thread that checks for tasks that ran over 10s has already reset the task status to notStarted
	if !isPresent {
		return errors.New("timeout error: task taken longer than 10s")
	}

	startTime := master.mapTaskStatus[args.TaskID].startTime
	timeTaken := time.Now().Sub(startTime)
	if timeTaken.Seconds() > 10 {
		master.mapTaskStatus[args.TaskID].status = "notStarted"
		delete(master.inProgressMapTasks, args.TaskID)
		return errors.New("timeout error: task taken longer than 10s")
	} else {
		master.mapTaskStatus[args.TaskID].status = "finished"
		delete(master.inProgressMapTasks, args.TaskID)
		master.wg.Done()
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
	//l, e := net.Listen("tcp", ":1234")
	//sockname := masterSock()
	//os.Remove(sockname)
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
	ret := false

	// Your code here.

	return ret
}

type MapStatus struct {
	status    string // notStarted,inProgress, or finished
	startTime time.Time
}

//
// create a Master. main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	absFilePaths := toAbsPaths(files)
	mapTaskStatus := map[int]*MapStatus{}

	// initialize map tasks's status
	for i := 0; i < len(absFilePaths); i++ {
		mapTaskStatus[i] = &MapStatus{
			status:    "notStarted",
			startTime: time.Time{},
		}
	}

	master := Master{
		phase:              "map", // first do map then reduce
		splits:             absFilePaths,
		NReduce:            nReduce,
		mapTaskStatus:      mapTaskStatus,
		inProgressMapTasks: make(map[int]bool),
	}

	// wait for all map tasks to complete before the first reduce task
	master.wg.Add(len(master.splits))

	// 0 not started, 1 in progress, 2 finished
	mapTasks := make(map[string]int)

	for i := 0; i < len(files); i++ {
		mapTasks[files[i]] = 0
	}

	// check for tasks overtime every 5 seconds
	go func() {
		for {
			//log.Printf("Running clean up thread to look for timed-out tasks")
			for taskID := range master.inProgressMapTasks {
				timeElapsed := time.Now().Sub(mapTaskStatus[taskID].startTime)
				if timeElapsed.Seconds() > 10 {
					log.Printf("Found a timed-out task: \n")
					master.mapTaskStatus[taskID].status = "notStarted"
					delete(master.inProgressMapTasks, taskID)
				}
			}
			time.Sleep(5 * time.Second)
		}
	}()

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
