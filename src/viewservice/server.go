
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  lastPingTime map[string]time.Time
  ack map[string]uint
  current View
  next View
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock()

  from := args.Me
  vs.lastPingTime[from] = time.Now()

  if vs.ack[from] < args.Viewnum {
    vs.ack[from] = args.Viewnum
  }

  if vs.current.Primary == from && args.Viewnum == 0 {
    vs.next.Primary = vs.current.Backup
    vs.next.Backup = ""
    vs.next.Viewnum = vs.current.Viewnum + 1
  }

  if vs.current.Primary == "" {
    vs.next.Primary = from
    vs.next.Viewnum = vs.current.Viewnum + 1
  } else if vs.current.Primary != from && vs.current.Backup == "" {
    vs.next.Primary = vs.current.Primary
    vs.next.Backup = from
    vs.next.Viewnum = vs.current.Viewnum + 1
  }

  if vs.ack[vs.current.Primary] == vs.current.Viewnum {
    vs.current = vs.next
  }

  reply.View = vs.current

  vs.mu.Unlock()

  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.

  reply.View = vs.current

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.

  deadInterval := DeadPings * PingInterval
  var flag bool = false

  if vs.current.Primary != "" {
    pInterval := time.Now().Sub(vs.lastPingTime[vs.current.Primary])
    if pInterval > deadInterval {
      vs.next.Primary = ""
      flag = true
    }
  }
  if vs.current.Backup != "" {
    bInterval := time.Now().Sub(vs.lastPingTime[vs.current.Backup])
    if bInterval > deadInterval {
      vs.next.Backup = ""
      flag = true
    }
  }

  if vs.next.Primary == "" && vs.next.Backup != "" {
    vs.next.Primary = vs.next.Backup
    vs.next.Backup = ""
    flag = true
  }

  if flag {
    vs.next.Viewnum = vs.current.Viewnum + 1
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.current = View{Primary: "", Backup: "",  Viewnum: 0}
  vs.next = View{Primary: "", Backup: "",  Viewnum: 0}
  vs.lastPingTime = make(map[string]time.Time)
  vs.ack = make(map[string]uint)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
