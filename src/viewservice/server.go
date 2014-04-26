
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
  current View
  next View

  hbTime map[string]time.Time
  viewNum map[string]uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here: 2013210880
  vs.mu.Lock()
  vs.hbTime[args.Me] = time.Now()
  if vs.viewNum[args.Me] < args.Viewnum {
    vs.viewNum[args.Me] = args.Viewnum
  }
  if args.Me == vs.current.Primary {
    if args.Viewnum == 0 {
      vs.next.Primary = vs.current.Backup
      vs.next.Backup = args.Me
      vs.next.Viewnum = vs.current.Viewnum + 1
    }
  } else if args.Me == vs.current.Backup {
    // Currently do nothing
  } else {
    if vs.current.Primary == "" {
      vs.next.Primary = args.Me
      vs.next.Viewnum = vs.current.Viewnum + 1
    } else if vs.current.Backup == "" {
      vs.next.Backup = args.Me
      vs.next.Viewnum = vs.current.Viewnum + 1
    }
  }

  if vs.viewNum[vs.current.Primary] == vs.current.Viewnum {
    vs.current = vs.next
  }
  reply.View = vs.current;
  vs.mu.Unlock()
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here: 2013210880
  vs.mu.Lock()
  reply.View = vs.current;
  vs.mu.Unlock()
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  
  // Your code here: 2013210880
  vs.mu.Lock()
  ti := time.Now()
  dur1 := ti.Sub(vs.hbTime[vs.current.Primary])
  dur2 := ti.Sub(vs.hbTime[vs.current.Backup])
  threshold := DeadPings * PingInterval

  if dur1 > threshold {
    vs.next.Primary = ""
  }
  if dur2 > threshold {
    vs.next.Backup = ""
  }
  if vs.next.Primary == "" {
    vs.next.Primary = vs.next.Backup
    vs.next.Backup = ""
  }
  if vs.next.Primary != vs.current.Primary || vs.next.Backup != vs.current.Backup {
    vs.next.Viewnum = vs.current.Viewnum + 1
  }
  vs.mu.Unlock()
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

  vs.current = View{}
  vs.next = View{}
  vs.hbTime = make(map[string]time.Time)
  vs.viewNum = make(map[string]uint)

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
