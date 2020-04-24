package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(Map func(string, string) []KeyValue, Reduce func(string, []string) string) {

	// Your worker implementation here.

	for true {
		masterMssg := MasterMessage{}
		isSuccessful := call("Master.GetTask", WorkerMessage{}, &masterMssg)
		if !isSuccessful {
			log.Println("Error getting a task from master")
		}
		//fmt.Printf("geting task: %v\n", masterMssg)

		switch masterMssg.Task {
		case "map":
			fmt.Printf("Map on file: %v\n", masterMssg.File)
			fileString := prepareMapInput(masterMssg.File)
			tempFiles := []*os.File{}
			for i := 0; i < masterMssg.NReduce; i++ {
				file, _ := ioutil.TempFile(".", "*")
				tempFiles = append(tempFiles, file)
			}
			intermediate := Map(masterMssg.File, fileString)

			for _, kv := range intermediate {
				tempFiles[ihash(kv.Key)%masterMssg.NReduce].WriteString(fmt.Sprintf("%v %v", kv.Key, kv.Value))
			}
			doneMessage := WorkerMessage{
				Task:   "map",
				TaskID: masterMssg.TaskID,
			}
			isSuccessful := call("Master.ReportDone", &doneMessage, &masterMssg)
			if isSuccessful {
				//rename tempfiles to the correct names
				for i := 0; i < masterMssg.NReduce; i++ {
					os.Rename(tempFiles[i].Name(),
						"mr-"+strconv.Itoa(masterMssg.TaskID)+"-"+strconv.Itoa(i))
				}
			} else {
				log.Println("Error reporting a task result")
				//remove the temporary files generated
				for i := 0; i < masterMssg.NReduce; i++ {
					os.Remove(tempFiles[i].Name())
				}
			}
			for i := 0; i < masterMssg.NReduce; i++ {
				tempFiles[i].Close()
			}

		case "reduce":
			log.Printf("xxx")
		}

		time.Sleep(1 * time.Second)

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func prepareMapInput(fileName string) string {
	//time.Sleep(8 * time.Second)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	return string(content)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	c, err := rpc.DialHTTP("tcp", "localhost:12346")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
