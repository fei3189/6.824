package mapreduce
import "container/list"
import "fmt"
import "time"

type WorkerInfo struct {
  address string
  state int
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce)MapWorker(address string, mapId int) {
  var reply DoJobReply
  args := &DoJobArgs{}
  args.File = mr.file
  args.Operation = "Map"
  args.JobNumber = mapId
  args.NumOtherPhase = mr.nReduce
  call(address, "Worker.DoJob", args, &reply)
  if reply.OK {
      if mr.todoMap.Len() == 0 {
      } else {
      }
      mr.finished += 1
  } else {
      mr.todoMap.PushBack(mapId)
  }
}


func (mr *MapReduce) ReduceWorker(address string, reduceId int) {
  var reply DoJobReply
  args := &DoJobArgs{}
  args.Operation = "Reduce"
  args.JobNumber = reduceId
  args.NumOtherPhase = mr.nMap
  args.File = mr.file
  call(address, "Worker.DoJob", args, &reply)
  if reply.OK {
    mr.finished += 1
  } else {
    mr.todoReduce.PushBack(reduceId)
  }
}


func (mr *MapReduce) RunMaster() *list.List {
 //  Your code here
  for true {
      for e := mr.workerList.Front(); e != nil; e = e.Next() {
          if mr.finished == mr.nMap {
            break
          }
          if mr.todoMap.Len() == 0 {
            continue
          }
          if e.Value.(WorkerInfo).state == 0 {
              f := mr.todoMap.Front().Value.(int)
              go mr.MapWorker(e.Value.(WorkerInfo).address, f)
              mr.todoMap.Remove(mr.todoMap.Front())
          }
      }
      time.Sleep(time.Second)
      if mr.finished == mr.nMap {
          break
      }
  }

  mr.finished = 0
  for true {
        fmt.Println("&&& %d", mr.workerList.Len())
      for e := mr.workerList.Front(); e != nil; e = e.Next() {
          if mr.finished == mr.nReduce {
            break
          }
          if mr.todoReduce.Len() == 0 {
            continue
          }
          if e.Value.(WorkerInfo).state == 0 {
              f := mr.todoReduce.Front().Value.(int)
              go mr.ReduceWorker(e.Value.(WorkerInfo).address, f)
              mr.todoReduce.Remove(mr.todoReduce.Front())
          }
      }
      time.Sleep(time.Second)
      if mr.finished == mr.nReduce {
          break
      }
  }
  return mr.KillWorkers()
}
