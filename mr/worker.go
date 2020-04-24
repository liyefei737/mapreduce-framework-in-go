package mr

import (
	"fmt"
	"io/ioutil"
	"os"
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

			intermediate := Map(masterMssg.File, fileString)
			for i, kv := range intermediate {
				if i == 5 {
					break
				}
				fmt.Printf("%v : %v reduce task: %v\n", kv.Key, kv.Value, ihash(kv.Key)%masterMssg.NReduce)
			}
			doneMssg := WorkerMessage{
				Task:   "map",
				TaskID: masterMssg.TaskID,
			}
			isSuccessful := call("Master.ReportDone", &doneMssg, &masterMssg)
			if !isSuccessful {
				log.Println("Error reporting a task result")
				//delete the temporary files generated
			}
			//time.Sleep(time.Minute)
		case "reduce":
			log.Printf("xxx")
		}

		time.Sleep(1 * time.Second)

	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func prepareMapInput(fileName string) string {
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
