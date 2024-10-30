package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for reply := CallTaskRequest(); reply.Task.TaskType != WORKER_SHUTDOWN_TASK; reply = CallTaskRequest() {
		time.Sleep(time.Second)
		// awesome debugging tool
		// b, _ := json.MarshalIndent(reply, "", "  ")
		// fmt.Println(string(b))
		if reply.Task.TaskType == MAP_TASK {
			filesProduced := []string{}
			if processMapTask(reply.Task, mapf, &filesProduced) == 0 {
				CallReportTaskDone(reply.Task, filesProduced)
			} else {
				return
			}
		} else if reply.Task.TaskType == REDUCE_TASK {
			fileProduced := ""
			if processReduceTask(reply.Task, reducef, reply.IntermediateFiles, &fileProduced) == 0 {
				CallReportTaskDone(reply.Task, []string{fileProduced})
			} else {
				return
			}
		} else if reply.Task.TaskType == WAIT_TASK {
			continue
		} else if reply.Task.TaskType == WORKER_SHUTDOWN_TASK {
			break
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func processMapTask(task Task, mapf func(string, string) []KeyValue, filesProduced *[]string) int {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
		return 1
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
		return 1
	}
	file.Close()
	kva := mapf(task.FileName, string(content))

	fileNameToKeyValueList := make(map[string][]KeyValue)

	for _, kv := range kva {
		mapTask := task.TaskId
		reduceTask := ihash(kv.Key) % task.NReduce
		intermediateFileName := fmt.Sprintf("mr-%v-%v", mapTask, reduceTask)
		fileNameToKeyValueList[intermediateFileName] = append(fileNameToKeyValueList[intermediateFileName], kv)
	}
	for fileName, kvList := range fileNameToKeyValueList {
		sort.Sort(ByKey(kvList))
		file, err = os.Create(fileName)
		if err != nil {
			log.Fatalf("Failed to create file: %v", err)
			return 1
		}
		*filesProduced = append(*filesProduced, fileName)

		defer file.Close()
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ") // For pretty-printing (optional)
		if err := encoder.Encode(kvList); err != nil {
			log.Fatalf("Failed to write JSON to file: %v", err)
			return 1
		}
	}
	return 0
}

func processReduceTask(task Task, reducef func(string, []string) string, intermediateFiles []string, fileProduced *string) int {
	outfileName := fmt.Sprintf("mr-out-%v", task.ReduceNumber)
	ofile, err := os.Create(outfileName)
	if err != nil {
		return 1
	}
	*fileProduced = outfileName
	defer ofile.Close()
	fullList := []KeyValue{}
	for _, file := range intermediateFiles {
		file, err := os.Open(file)
		if err != nil {
			return 1
		}
		decoder := json.NewDecoder(file)
		intermediateList := []KeyValue{}
		err = decoder.Decode(&intermediateList)
		if err != nil {
			fmt.Println("Error decoding JSON:", err)
			return 1
		}
		fullList = append(fullList, intermediateList...)
		file.Close()
	}
	sort.Sort(ByKey(fullList))
	i := 0
	for i < len(fullList) {
		j := i + 1
		for j < len(fullList) && fullList[j].Key == fullList[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, fullList[k].Value)
		}
		output := reducef(fullList[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", fullList[i].Key, output)
		i = j
	}

	return 0
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallTaskRequest() TaskReply {
	args := TaskArgs{}

	reply := TaskReply{}

	call("Coordinator.RequestTask", &args, &reply)

	return reply
}
func CallReportTaskDone(task Task, filesProduced []string) TaskReply {
	args := TaskArgs{}
	args.Task = task
	args.FilesProduced = filesProduced
	reply := TaskReply{}
	call("Coordinator.ReportTaskDone", &args, &reply)
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
