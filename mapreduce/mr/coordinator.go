package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// task status
type TaskStatus int

const (
	PENDING TaskStatus = iota
	IN_PROGRESS
	DONE
)

// task types
type TaskType int

const (
	MAP_TASK TaskType = iota
	REDUCE_TASK
	WAIT_TASK
	WORKER_SHUTDOWN_TASK
)

type Task struct {
	TaskId       int
	TaskType     TaskType
	FileName     string
	ReduceNumber int
	TaskStatus   TaskStatus
	NReduce      int
}

// Your definitions here.
type Coordinator struct {
	numReduceTasks int
	fileNames      []string
	taskList       []Task
	lock           sync.Mutex
	requestCount   int
}

func checkInProgress10SecondsLater(c *Coordinator, idx int) {
	time.Sleep(10 * time.Second)
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.taskList[idx].TaskStatus == IN_PROGRESS {
		c.taskList[idx].TaskStatus = PENDING
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	// make sure all map tasks are finished before handing off reduce tasks
	c.lock.Lock()
	defer c.lock.Unlock()

	// defer func() {
	// 	b, _ := json.MarshalIndent(reply, "", " \t")
	// 	fmt.Println("REQUESTED TASK:", string(b))
	// 	j := []string{}
	// 	for _, task := range c.taskList {
	// 		j = append(j, fmt.Sprintf("id: %v status: %v", task.TaskId, task.TaskStatus))
	// 	}
	// 	fmt.Println("task list:", j)
	// }()
	c.requestCount += 1
	if c.allTasksDone() {
		shutdownTask := Task{}
		shutdownTask.TaskType = WORKER_SHUTDOWN_TASK
		reply.Task = shutdownTask
		return nil
	}

	if !c.allMapTasksDone() {
		found := false
		for idx, task := range c.taskList {
			if task.TaskType == MAP_TASK && task.TaskStatus == PENDING {
				c.taskList[idx].TaskStatus = IN_PROGRESS
				reply.Task = task
				found = true
				go checkInProgress10SecondsLater(c, idx)
				break
			}
		}
		if !found {
			reply.Task.TaskType = WAIT_TASK
		}
	} else {
		found := false
		for idx, task := range c.taskList {
			if task.TaskType == REDUCE_TASK && task.TaskStatus == PENDING {
				found = true
				c.taskList[idx].TaskStatus = IN_PROGRESS
				reply.Task = task
				reply.IntermediateFiles = []string{}
				for _, fileName := range c.fileNames {
					tokens := strings.Split(fileName, "-")
					if len(tokens) != 3 {
						continue
					}
					val, err := strconv.Atoi(tokens[2])
					if err != nil {
						continue
					}
					if len(tokens) == 3 && val == task.ReduceNumber {
						reply.IntermediateFiles = append(reply.IntermediateFiles, fileName)
					}
				}
				go checkInProgress10SecondsLater(c, idx)
				break
			}
		}
		if !found {
			reply.Task.TaskType = WAIT_TASK
		}
	}
	return nil
}

func (c *Coordinator) ReportTaskDone(args *TaskArgs, reply *TaskReply) error {

	c.lock.Lock()
	defer c.lock.Unlock()
	// defer func() {
	// 	b, _ := json.MarshalIndent(args, "", " \t")
	// 	fmt.Println("REPORTED TASK DONE:", string(b))
	// 	j := []string{}
	// 	for _, task := range c.taskList {
	// 		j = append(j, fmt.Sprintf("id: %v status: %v", task.TaskId, task.TaskStatus))
	// 	}
	// 	fmt.Println("task list:", j)
	// }()
	c.requestCount += 1
	if args.Task.TaskType == WORKER_SHUTDOWN_TASK {
		return nil
	}
	c.fileNames = append(c.fileNames, args.FilesProduced...)
	for idx, task := range c.taskList {
		if task.TaskId == args.Task.TaskId {
			c.taskList[idx].TaskStatus = DONE
			break
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) allTasksDone() bool {
	for _, task := range c.taskList {
		if task.TaskStatus != DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) allMapTasksDone() bool {
	for _, task := range c.taskList {
		if task.TaskType == MAP_TASK && task.TaskStatus != DONE {
			return false
		}
	}
	return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	if !c.allTasksDone() {
		c.lock.Unlock()
		return false
	}
	curr := c.requestCount
	c.lock.Unlock()
	time.Sleep(time.Second)
	c.lock.Lock()
	afterTwoSeconds := c.requestCount
	c.lock.Unlock()
	return curr == afterTwoSeconds
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.numReduceTasks = nReduce
	c.fileNames = files
	c.taskList = []Task{}
	idCounter := 0
	for _, file := range files {
		task := Task{}
		task.TaskId = idCounter
		task.TaskType = MAP_TASK
		task.TaskStatus = PENDING
		task.ReduceNumber = -1
		task.FileName = file
		task.NReduce = nReduce
		c.taskList = append(c.taskList, task)
		// we need nReduce reduce tasks per map task
		idCounter += 1

	}

	for i := 0; i < nReduce; i += 1 {
		reduceTask := Task{}
		reduceTask.TaskId = idCounter
		reduceTask.TaskType = REDUCE_TASK
		reduceTask.TaskStatus = PENDING
		reduceTask.NReduce = nReduce
		reduceTask.ReduceNumber = i
		c.taskList = append(c.taskList, reduceTask)
		idCounter += 1
	}

	c.server()
	return &c
}
