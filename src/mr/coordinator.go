package mr

import (
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
	if args.Finished == 0 {
		if c.Stage == 0 {
			reply.TaskType = 0
			reply.NReduce = c.NReduces
			reply.FileName = c.SplitFiles[0]
			c.SplitFiles = c.SplitFiles[1:]
			c.WorkersInfo[c.WorkerNo].TaskType = 0
			c.WorkersInfo[c.WorkerNo].FileName = reply.FileName
		} else {
			reply.TaskType = 1
			for _, rdzone := range c.Reduced {
				if rdzone == 0 || rdzone == 2 {
					reply.ReduceZone = rdzone
				}
			}
			c.WorkersInfo[c.WorkerNo].TaskType = 1
			c.WorkersInfo[c.WorkerNo].ReduceZone = reply.ReduceZone
		}
		reply.WorkerNo = c.WorkerNo
		c.WorkerNo++
		c.WorkersInfo[reply.WorkerNo].WorkerNo = reply.WorkerNo
		c.WorkersInfo[reply.WorkerNo].Status = -1
		go c.Timer(reply.WorkerNo)
	} else {
		c.WorkersInfo[args.WorkerNo].Status = 1
		c.FinishTask(args.WorkerNo)
	}
	return nil
}

func (c *Coordinator) FinishTask(workno int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.WorkersInfo[workno].TaskType == 0 {
		c.NumMaps -= 1
	} else {
		c.NumReduces -= 1
	}
	if c.NumMaps == 0 {
		c.Stage = 1
	}
	if c.NumReduces == 0 {
		c.Stage = 2
	}
	return
}

func (c *Coordinator) Done() bool {
	ret := c.Stage > 1
	return ret
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
	for i := 0; i < nReduce; i++ {
		c.Reduced[i] = 0
	}
	c.NReduces = nReduce
	c.WorkerNo = 0
	c.server()
	return &c
}
