package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

	for true {

		masterMssg := MasterMessage{}
		isSuccessful := call("Master.GetTask", WorkerMessage{}, &masterMssg)

		if !isSuccessful {
			log.Println("Error getting a task from master")
		}

		//fmt.Printf("geting task: %v\n", masterMssg)

		switch masterMssg.Task {
		case "map":
			fmt.Printf("Running map task for file: %v\n", masterMssg.File)
			fileString := prepareMapInput(masterMssg.File)
			tempFiles := []*os.File{}
			for i := 0; i < masterMssg.NReduce; i++ {
				file, _ := ioutil.TempFile(".", "*")
				tempFiles = append(tempFiles, file)
			}
			intermediate := Map(masterMssg.File, fileString)

			for _, kv := range intermediate {
				tempFiles[ihash(kv.Key)%masterMssg.NReduce].WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
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
			reduceTaskID := masterMssg.TaskID // TaskID is the reduce task number
			log.Printf("Working on Reduce Task: %v\n", reduceTaskID)
			ofile, _ := ioutil.TempFile(".", "*")

			files, _ := ioutil.ReadDir(".")

			for _, file := range files {
				if strings.HasSuffix(file.Name(), strconv.Itoa(reduceTaskID)) {
					intermediate := []KeyValue{}
					intermediate = deserialize(file.Name(), intermediate)
					//
					// call Reduce on each distinct key in intermediate[],
					// and print the result to output file (ofile).
					//
					i := 0
					for i < len(intermediate) {
						j := i + 1
						for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, intermediate[k].Value)
						}
						output := Reduce(intermediate[i].Key, values)

						// this is the correct format for each line of Reduce output.
						_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
						if err != nil {
							log.Printf("error writting to a reduce output file: %v\n", file)
							log.Fatal(err)
						}

						i = j
					}

				}
			}

			// report back to master

			doneMessage := WorkerMessage{
				Task:   "reduce",
				TaskID: masterMssg.TaskID,
			}
			isSuccessful := call("Master.ReportDone", &doneMessage, &masterMssg)
			if isSuccessful {
				//rename tempfile to the correct name
				os.Rename(ofile.Name(), "mr-out-"+strconv.Itoa(reduceTaskID))
			} else {
				log.Println("Error reporting a task result")
				os.Remove(ofile.Name())
			}

			ofile.Close()

		case "idle":
			time.Sleep(1 * time.Second)

		case "done":
			log.Printf("No more map or reduce tasks, worker shutting done...")
			os.Exit(0)
		}

	}

}

// for sorting intermediate key values by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// deserialize a file that contains key value pairs and populate a slice.
// return the slice as sorted by key
//
func deserialize(file string, intermediate []KeyValue) []KeyValue {
	f, err := os.Open(file)
	defer f.Close()

	if err != nil {
		log.Fatalf("cannot open %v", file)
	}

	fileScanner := bufio.NewScanner(f)
	for fileScanner.Scan() {

		kv := strings.Split(fileScanner.Text(), " ")
		if len(kv) == 2 {
			//log.Printf("kv entry:%v %v\n", kv[0], kv[1])
			intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
		} else {
			// e.g. empty newlines at the end of the file?
			log.Printf("kv entry with more or less tokens:\n %v\n", kv)
		}
	}

	//sort kvs from this file
	sort.Sort(ByKey(intermediate))
	return intermediate
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
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
