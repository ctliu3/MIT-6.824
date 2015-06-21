package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
//import "errors"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}

  // Your declarations here.

  // key -> value
  db map[string]string
  //filter map[string]string
  current viewservice.View
  mu sync.Mutex
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.

  pb.mu.Lock()

  if pb.dead {
    reply.Err = ErrWrongServer
    return fmt.Errorf("Server died.")
  }

  // Use the lated primary.
  if pb.me != pb.vs.Primary() {
    reply.Err = ErrWrongServer
    return fmt.Errorf("I'm not a primary.")
  }

  if value, ok := pb.db[args.UUID]; ok {
    reply.PreviousValue = value
    reply.Err = OK
    pb.mu.Unlock()
    return nil
  }

  if args.DoHash {
    reply.PreviousValue = pb.db[args.Key]
    hcode := hash(reply.PreviousValue + args.Value)
    args.Value = strconv.Itoa(int(hcode))
  }

  // Primary then Backup or reverse?
  kvs := map[string]string {
    args.Key: args.Value,
    args.UUID: reply.PreviousValue,
  }

  for key, value := range kvs {
    pb.db[key] = value
  }

  view, _ := pb.vs.Get()
  backup := view.Backup
  //backup := pb.current.Backup

  if backup != "" {
    err := pb.syncKvs(backup, kvs)
    if err != nil {
      reply.Err = ErrWrongServer
      pb.mu.Unlock()
      return fmt.Errorf("Sync to backup error.")
    }
  }

  reply.Err = OK
  pb.mu.Unlock()

  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.

  pb.mu.Lock()

  if pb.vs.Primary() != pb.me {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return fmt.Errorf("Not connect to primary server.")
  }

  reply.Value = pb.db[args.Key]
  reply.Err = OK

  pb.mu.Unlock()

  return nil
}

func (pb *PBServer) syncKvs(server string, kvs map[string]string) error {
  if server == "" {
    return nil
  }

  args := &ForwardArgs{Kvs: kvs}
  var reply ForwardReply

  ok := call(server, "PBServer.ForwardDB", args, &reply)
  if ok == false {
    return fmt.Errorf("Error when synchronizing backup %s\n", server)
  }
  return nil
}

func (pb *PBServer) ForwardDB(args *ForwardArgs, reply *ForwardReply) error {

  pb.mu.Lock()

  view, _ := pb.vs.Get()
  if view.Backup != pb.me {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return fmt.Errorf("Not backup.")
  }

  for key, value := range args.Kvs {
    pb.db[key] = value
  }
  reply.Err = OK

  pb.mu.Unlock()

  return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.

  view, _ := pb.vs.Ping(pb.current.Viewnum)

  sync := false
  if view.Primary == pb.me {
    if view.Backup != "" && view.Backup != pb.current.Backup {
      sync = true
    }
  }

  pb.mu.Lock()

  pb.current = view
  if sync {
    pb.syncKvs(view.Backup, pb.db)
  }

  pb.mu.Unlock()
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.current = viewservice.View{Viewnum: 0}
  pb.db = make(map[string]string)
  //pb.filter = make(map[string]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
