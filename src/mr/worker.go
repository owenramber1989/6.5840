package mr

import (
	"6.5840/logger"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose to reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	ok, workerno := AskForATask(true, 1, mapf, reducef)
	for ok {
		logger.Debug(logger.DInfo, "Worker %d completed a task, asking for a new one", workerno)
		ok, workerno = AskForATask(false, workerno, mapf, reducef)
	}
	return
}
func AskForATask(init bool, workerno int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (bool, int) {
	args := RequestArgs{}
	if init {
		args.Finished = 0
	} else {
		args.Finished = 1
	}
	args.WorkerNo = workerno
	reply := ReplyArgs{}
	reply.WorkerNo = 0
	reply.TaskType = 0
	reply.FileName = ""
	reply.NReduce = 0
	reply.ReduceZone = 0
	reply.Wait = false
	reply.Done = false
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		if reply.Done == true {
			return false, -1
		}
		if reply.Wait == true {
			time.Sleep(time.Second)
			return true, workerno
		}
		// logger.Debug(logger.DInfo, "The reply.TaskType is %d", reply.TaskType)
		if reply.TaskType == 0 {
			DoMap(reply.FileName, reply.WorkerNo, reply.NReduce, mapf)
		}
		if reply.TaskType == 1 {
			DoReduce(reducef, reply.WorkerNo, reply.ReduceZone)
		}
		return true, reply.WorkerNo
	} else {
		// logger.Debug(logger.DError, "Worker %d failed to request for a task", args.WorkerNo)
		return false, reply.WorkerNo
	}
}
func DoMap(filename string, workerno int, nreduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		return
	}
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	for _, kv := range kva {
		r := ihash(kv.Key) % nreduce
		filename := fmt.Sprintf("mr-%d-%d", workerno, r)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		if err != nil {
			fmt.Print(err)
		}
		err = file.Close()
		if err != nil {
			return
		}
	}
}

func DoReduce(reducef func(string, []string) string, workerno int, reducezone int) {
	re := regexp.MustCompile(`^mr-(\d+)-(\d+)$`)
	files, err := filepath.Glob("*")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	var intermediate []KeyValue
	for _, filename := range files {
		// 使用正则表达式匹配文件名
		matches := re.FindStringSubmatch(filename)
		if len(matches) == 3 {
			// 第一个捕获组是x，第二个捕获组是y
			_, errX := strconv.Atoi(matches[1])
			y, errY := strconv.Atoi(matches[2])
			if errX == nil && errY == nil && y == reducezone {
				file, _ := os.OpenFile(filename, os.O_RDONLY, 0755)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
		}
	}
	sort.Sort(ByKey(intermediate))
	// logger.Debug(logger.DInfo, "The assigned reduce zone is #%d zone", reducezone)
	oname := "mr-out-" + strconv.Itoa(reducezone)
	// tname := "TempReduce" + strconv.Itoa(reducezone)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	err = ofile.Close()
	if err != nil {
		return
	}
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
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(c)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
