package mr

//
// RPC message definitions.
// remember to capitalize all names.
//

import "os"
import "strconv"

type WorkerMessage struct {
	TaskID int
	Task   string
}

type MasterMessage struct {
	File    string
	Task    string
	TaskID  int
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
