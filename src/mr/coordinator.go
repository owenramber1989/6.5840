package mr

import (
	"6.5840/logger"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type WorkerInfo struct {
	TaskType   int // 0 for map, 1 for reduce
	WorkerNo   int
	Status     int // 0, 1 and 2 for running, completed and failed
	FileName   string
	ReduceZone int
}
type Coordinator struct {
	// Your definitions here.
	Stage       int          // 0 map 1 reduce, 2 meas finished all tasks
	WorkersInfo []WorkerInfo // -1 means not done yet, 1 means the opposite, 2 means it's regarded as dead.
	SplitFiles  []string
	NumMaps     int
	NumReduces  int
	Reduced     []int // 0 means this zone not done yet, 1 means already reduced, 2 means failed.
	NReduces    int   // how many reduce zones
	WorkerNo    int   // monotically increasing
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

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

func (c *Coordinator) Timer(WorkerNo int) {
	time.Sleep(time.Second * 10)
	if c.WorkersInfo[WorkerNo].Status == 1 {
		return
	}
	logger.Debug(logger.DInfo, "Worker #%d was regarded as dead", WorkerNo)
	c.WorkersInfo[WorkerNo].Status = 2
	c.HandleFailures(WorkerNo)
}

func (c *Coordinator) HandleFailures(WorkerNo int) {
	if c.WorkersInfo[WorkerNo].TaskType == 0 {
		c.SplitFiles = append(c.SplitFiles, c.WorkersInfo[WorkerNo].FileName)
		re := regexp.MustCompile(`^mr-(\d+)-(\d+)$`)
		files, err := filepath.Glob("*")
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		for _, filename := range files {
			matches := re.FindStringSubmatch(filename)
			if len(matches) == 3 {
				// 第一个捕获组是x，第二个捕获组是y
				x, _ := strconv.Atoi(matches[1])
				if x == WorkerNo {
					err := os.Remove(filename)
					if err != nil {
						return
					}
				}
			}
		}
	} else {
		c.Reduced[c.WorkersInfo[WorkerNo].ReduceZone] = 2
	}
}
func (c *Coordinator) RequestTask(args *RequestArgs, reply *ReplyArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Finished == 1 {
		c.WorkersInfo[args.WorkerNo].Status = 1
		c.FinishTask(args.WorkerNo)
	}
	if len(c.SplitFiles) == 0 && c.Stage == 0 {
		// logger.Debug(logger.DWarn, "Refuse to assign tasks, waiting for the rest of the map tasks")
		reply.Wait = true
		return nil
	}
	if c.Stage == 2 {
		// logger.Debug(logger.DInfo, "We finished all the tasks")
		reply.Done = true
		return nil
	}

	c.AssignTask(reply)
	return nil
}

func (c *Coordinator) AssignTask(reply *ReplyArgs) {

	if c.Stage == 0 {
		reply.TaskType = 0
		reply.NReduce = c.NReduces
		reply.FileName = c.SplitFiles[0]
		c.SplitFiles = c.SplitFiles[1:]
		c.WorkersInfo[c.WorkerNo].TaskType = 0
		c.WorkersInfo[c.WorkerNo].FileName = reply.FileName
	} else {
		reply.TaskType = 1
		find := false
		for i, rdzone := range c.Reduced {
			if rdzone == 0 || rdzone == 2 {
				reply.ReduceZone = i
				// logger.Debug(logger.DInfo, "The coordinator assigned worker the #%d reduce zone", reply.ReduceZone)
				c.Reduced[i] = 1
				find = true
				break
			}
		}
		if !find {
			reply.Done = true
			return
		}
		c.WorkersInfo[c.WorkerNo].TaskType = 1
		c.WorkersInfo[c.WorkerNo].ReduceZone = reply.ReduceZone
	}
	reply.Done = false
	reply.Wait = false
	reply.WorkerNo = c.WorkerNo
	// logger.Debug(logger.DInfo, "Assigned a new task, the worker no is %d", reply.WorkerNo)
	c.WorkerNo++
	c.WorkersInfo[reply.WorkerNo].WorkerNo = reply.WorkerNo
	c.WorkersInfo[reply.WorkerNo].Status = 0
	go c.Timer(reply.WorkerNo)
}
func (c *Coordinator) FinishTask(workno int) {
	if c.WorkersInfo[workno].TaskType == 0 {
		c.NumMaps -= 1
		// logger.Debug(logger.DInfo, "%d map tasks remained.", c.NumMaps)
	} else {
		c.NumReduces -= 1
		// logger.Debug(logger.DInfo, "%d reduce tasks remained.", c.NumReduces)
	}
	c.WorkersInfo[workno].Status = 1
	if c.NumMaps <= 0 {
		c.Stage = 1
		// logger.Debug(logger.DInfo, "Map tasks completed, moved into reduce phase")
	}
	if c.NumReduces <= 0 {
		c.Stage = 2
		// logger.Debug(logger.DInfo, "Reduce tasks completed, end of system")
	}
	return
}

func (c *Coordinator) Done() bool {
	if c.Stage > 1 {
		return true
	}
	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Stage = 0
	c.SplitFiles = files
	c.NumMaps = len(files)
	c.NumReduces = nReduce
	c.Reduced = make([]int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.Reduced[i] = 0
	}
	c.WorkersInfo = make([]WorkerInfo, 100)
	c.NReduces = nReduce
	c.WorkerNo = 1
	logger.Debug(logger.DInfo, "The coordinator has %d splits and %d reduce zones", c.NumMaps, c.NReduces)
	c.server()
	return &c
}
