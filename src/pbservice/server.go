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

import "strconv"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}
type CopyArgs struct {
  KV map[string]string
  Serials map[int64]string
}
type CopyReply struct {
  Err Err
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
  view viewservice.View
  kv map[string]string
  serials map[int64]string

  mu  sync.Mutex
}

func (pb *PBServer) Forward(args *PutArgs) int {
  var reply PutReply
  if pb.view.Backup != "" {
    args.From = FromServer
    ok := call(pb.view.Backup, "PBServer.Put", args, &reply)
    if !ok {
      return -1
    }
    if reply.Err != OK {
      return -1
    }
  }
  return 0
}

func (pb *PBServer) ForwardComplete(args *CopyArgs, reply *CopyReply) error {
  pb.kv = args.KV
  pb.serials = args.Serials
  reply.Err = OK
  fmt.Printf("*****%s copy database\n", pb.me)
  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  if pb.me != pb.view.Primary && args.From == FromClient {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return nil
  }
  if pb.me == pb.view.Primary && args.From == FromServer {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return nil
  }
  if _, ok := pb.serials[args.Serial]; ok {
    reply.Err = OK
    reply.Serial = args.Serial
    reply.PreviousValue = pb.serials[args.Serial]
    pb.mu.Unlock()
    return nil
  }
  // Your code here.
  if args.DoHash {
    if pb.me == pb.view.Primary && pb.Forward(args) != 0 {
      reply.Err = ErrNonConsistent
    pb.mu.Unlock()
      return nil
    }
    val := pb.kv[args.Key]
    next := val + args.Value
    pb.kv[args.Key] = strconv.Itoa(int(hash(next)))
    pb.serials[args.Serial] = val
    reply.PreviousValue = val
    reply.Err = OK
    reply.Serial = args.Serial
  } else {
    if pb.me == pb.view.Primary && pb.Forward(args) != 0 {
      reply.Err = ErrNonConsistent
    pb.mu.Unlock()
      return nil
    }
    pb.kv[args.Key] = args.Value
    reply.Err = OK
    reply.Serial = args.Serial
    pb.serials[args.Serial] = ""
  }
    pb.mu.Unlock()
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  if pb.me != pb.view.Primary {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return nil
  }
  if _, ok := pb.serials[args.Serial]; ok {
    reply.Err = OK
    reply.Value = pb.serials[args.Serial]
    pb.mu.Unlock()
    return nil
  }
  reply.Value = pb.kv[args.Key]
  reply.Err = OK
  pb.serials[args.Serial] = reply.Value

    pb.mu.Unlock()
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  pb.mu.Lock()
  // Your code here
  v := pb.view
  pb.view, _ = pb.vs.Ping(pb.view.Viewnum)
  if v.Backup != pb.view.Backup && pb.view.Backup != "" && pb.me == pb.view.Primary {
    args := &CopyArgs{}
    reply := CopyReply{}
    args.KV = pb.kv
    args.Serials = pb.serials
    fmt.Printf("######%s copy database\n", pb.me)
    call(pb.view.Backup, "PBServer.ForwardComplete", args, &reply)
  }
    pb.mu.Unlock()
//    DPrintf("tick! %s %d\n", pb.me, pb.view.Viewnum);
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
  pb.kv = make(map[string]string)
  pb.serials = make(map[int64]string)
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
