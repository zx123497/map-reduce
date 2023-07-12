package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	// map & reduce tasks
	mapTasks    []TaskData
	reduceTasks []TaskData

	// map / reduce
	phase         string
	phaseFinished bool
}

type TaskData struct {
	taskID     int
	isStarted  bool
	isFinished bool
	// map or reduce
	taskType string
	fileName string
}

func (c *Coordinator) TaskHandler(args *TaskArgs, reply *TaskReply) error {
	// make sure only one taskHandler update data
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == "request" {
		// worker request a task
		if c.phase == "map" {
			if !c.phaseFinished {
				for index, task := range c.mapTasks {
					if !task.isStarted {
						c.mapTasks[index].isStarted = true
						reply.Task = c.mapTasks[index]

						// start timer if 10 sec not finished, task.isStarted = false
						go c.Timer(task.taskType, index, 10)
						return nil
					}
				}
				// tell the worker to wait for other workers to finish
				reply.Task.taskType = "wait"
				return nil
			}
		} else if c.phase == "reduce" {

		}
	} else if args.Type == "finish" {
		// worker finish a task

	}
}

func (c *Coordinator) Timer(taskType string, index int, setTime int) {
	time.Sleep(time.Second * time.Duration(setTime))
	c.mu.Lock()
	defer c.mu.Unlock()

	if taskType == "map" {
		if !c.mapTasks[index].isFinished {
			c.mapTasks[index].isStarted = false
		}
	} else if taskType == "reduce" {
		if !c.reduceTasks[index].isFinished {
			c.reduceTasks[index].isStarted = false
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var mapTasks []TaskData
	for index, file := range files {
		newMapTask := TaskData{taskID: index, taskType: "map", fileName: file}
		mapTasks = append(mapTasks, newMapTask)
	}

	c := Coordinator{mapTasks: mapTasks, phase: "map"}

	// Your code here.

	c.server()
	return &c
}
