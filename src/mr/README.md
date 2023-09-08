# Designs of Map Phase, Reduce Phase and Data Structures
## Coordinator
The Coordinator is responsible for handle the requests for tasks from different worker. To avoid conflicts, the 
coordinator has to maintain some global infomation listed below.
- Stage: The current phase. Whether it's the map phase(0) or reduce phase(1) or DONE(2).
- WorkersInfo: An array `[]WorkerInfo` that contains attributes as followed:
    - TaskType: Whether the worker currently handle map task or reduce task
    - WorkerNo: The unique identifier for each Worker. So when a worker finished work successfully, the coordinator 
    knows which split or which reduce zone is well handled. ***The WorkerNo is monotically increasing.***
    - Status: Running, completed or failed.
    - FileName: If the worker currently is working on the map task, which split is it working on? (used after the worker crashed)
    - ReduceZone: Same as above, after a crash, the coordinator knows which part needs redo.
- SplitFiles: All the files that needs to be mapped. 
- NumMaps: The total map tasks remained. Initially equals with the number of the split files.
- NumReduces: As it is.
- Reduced : `[]int`, 0 means assigned while not done yet, 1 means done, 2 means needs redo. 
- nReduces: So the coordinator can inform the map worker the total zones of reduce.
- WorkerNo: Used to assign worker number for any task. Monotically increasing.

---
In both phases, each worker will ask for a task from coordinator when the last task is finished. Or when the worker
is initialized without being assigned a task.

## Map phase
The worker should call the rpc with RequestArgs. This should include one thing. Whether it's finished or not. If
it hasn't finished yet, that means it's asking for a new task. In the opposite, the coordinator should mark the 
task previously assigned to the worker has been completed. And assign the worker a new task.

while for the ReplyArgs, it's kinda complexing...

## ReplyArgs
- WorkerNo: The coordinator should inform workers of their unique ID.
- TaskType: As it is.
- FileName: For map task, the coordinator should tell the worker which split file it should handle with.
- NReduces: Offered to map task.
- ReduceZone: For reduce task, the coordinator should tell the worker which zone it should handle with.

# Events need to be handled with
## Crash
Well I take a simple strategy, when the coordinator assigned a new task. It creates a new go routine, calling the
timer method. The timer will sleep for ten seconds. And then it goes through the WorkerInfo in coordinator, to check
the responding worker's status. If it's not done yet, mark it as DEAD.

And we should assign its original task to others. For a failed map task, we can append the filename in the WorkerInfo
to the SplitFiles maintained in coordinator. For a failed reduce task, set the `Reduced[WorkerNo]` to 2.



