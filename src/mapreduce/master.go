package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
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

func (mr *MapReduce) RunMaster() *list.List {

  // Your code here
  // Master to worker: register -> dojob -> kill

  MapProcess := func(worker string, jobNumber int) bool {
    args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: jobNumber, NumOtherPhase: mr.nReduce}
    var reply DoJobReply
    ok := call(worker, "Worker.DoJob", args, &reply)
    if ok == false {
      fmt.Println("DoWork: RPC %s dojob error\n", worker)
    }
    return ok
  }

  ReduceProcess := func(worker string, jobNumber int) bool {
    args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: jobNumber, NumOtherPhase: mr.nMap}
    var reply DoJobReply
    ok := call(worker, "Worker.DoJob", args, &reply)
    if ok == false {
      fmt.Println("DoWork: RPC %s dojob error\n", worker)
    }
    return ok
  }

  mapDoneChannel := make(chan int, mr.nMap)
  for i := 0; i < mr.nMap; i++ {
    go func(id int) {
      var worker string
      var res bool
      for {
        res = false
        select {
        case worker = <-mr.registerChannel:
          res = MapProcess(worker, id)
        case worker = <-mr.idleChannel:
          res = MapProcess(worker, id)
        }
        if (res) {
          // The sequence of following statements can not be changed, since the
          // idleChannel is a non-buffer channel, if not one takew the stuff
          // from that channel, the `mapDoneChannel <- id` statement cannot be
          // executed. Then this program can not get to the reduce phase.
          mapDoneChannel <- id
          mr.idleChannel <- worker
          return
        }
      }
    }(i)
  }

  for i := 0; i < mr.nMap; i++ {
    <-mapDoneChannel
  }
  fmt.Println("Map phase done.")

  reduceDoneChannel := make(chan int, mr.nReduce)
  for i := 0; i < mr.nReduce; i++ {
    go func(id int) {
      var worker string
      var res bool
      for {
        res = false
        select {
        case worker = <-mr.registerChannel:
          res = ReduceProcess(worker, id)
        case worker = <-mr.idleChannel:
          res = ReduceProcess(worker, id)
        }
        if (res) {
          reduceDoneChannel <- id
          mr.idleChannel <- worker
          return
        }
      }
    }(i)
  }
  for i := 0; i < mr.nReduce; i++ {
    <-reduceDoneChannel
  }
  fmt.Println("Reduce phase done.")

  return mr.KillWorkers()
}
