package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Operation string // "GET PUT PUTHASH or NOOP"
  Key string
  Value string
  Serial int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kv map[string]string
  serials map[int64]string
  min int
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  tryTimes := 5
  seq := -1
  for tryTimes > 0 {
    tmp := kv.px.Max() + 1
    seq = seq + 1
    if seq < tmp {
      seq = tmp
    }
    op := makeGetOp(args)
    kv.px.Start(seq, op)
    agreedOp := kv.waitPaxos(seq)
    if agreedOp.Operation == "ERROR" {
      reply.Err = "network down"
      kv.mu.Unlock()
      return nil
    } else if op == agreedOp {
      res, ok := kv.runLog(seq)
      if ok != "OK" {
        reply.Err = "network error1234"
      } else {
        reply.Value = res
        reply.Err = "OK"
      }
      kv.mu.Unlock()
      return nil
    }
    tryTimes--
  }
  reply.Err = "network"
  kv.mu.Unlock()
  return nil
}

func makePutOp(args *PutArgs) (op Op) {
  op.Operation = "PUT"
  if args.DoHash {
    op.Operation = "PUTHASH"
  }
  op.Key = args.Key
  op.Value = args.Value
  op.Serial = args.Serial
  return op
}

func makeGetOp(args *GetArgs) (op Op) {
  op.Operation = "GET"
  op.Key = args.Key
  op.Serial = args.Serial
  return op
}

func (kv *KVPaxos) waitPaxos(seq int) (op Op) {
  period := 10 * time.Millisecond
  for {
    decided, value := kv.px.Status(seq)
    if decided {
      return value.(Op)
    }
    time.Sleep(period)
    if period < time.Second {
      period *= 2
    } else {
      op.Operation = "ERROR"
      return op
    }
  }
}

func (kv *KVPaxos) runOp(op Op) (ret string) {
  ret = ""
  v, ok := kv.serials[op.Serial]
  if ok {
    return v
  }
  switch op.Operation {
    case "PUT":
      kv.kv[op.Key] = op.Value
      kv.serials[op.Serial] = ""
    case "GET":
      ret, ok = kv.kv[op.Key]
      if !ok {
        ret = ""
      }
    case "PUTHASH":
      ret, ok = kv.kv[op.Key]
      if !ok {
        ret = ""
      }
      kv.serials[op.Serial] = ret
      next := ret + op.Value
      kv.kv[op.Key] = strconv.Itoa(int(hash(next)))
    default:
      ret = ""
  }
  return ret
}


func (kv *KVPaxos) runLog(max int) (ret string, err string) {
  for ; kv.min <= max; kv.min++ {
    decided, value := kv.px.Status(kv.min)
    if decided {
      ret = kv.runOp(value.(Op))
    } else {
      op := Op{"NOOP", "", "", 0}
      kv.px.Start(kv.min, op)
      op = kv.waitPaxos(kv.min)
      if op.Operation == "ERROR" {
        return ret, "network error"
      } else {
        ret = kv.runOp(op)
      }
    }
  }
  kv.px.Done(kv.min - 1)
  return ret, "OK"
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  // Your code here.
  tryTimes := 5
  seq := -1
  for tryTimes > 0 {
    tmp := kv.px.Max() + 1
    seq = seq + 1
    if seq < tmp {
      seq = tmp
    }
    op := makePutOp(args)
    kv.px.Start(seq, op)
    agreedOp := kv.waitPaxos(seq)
    if agreedOp.Operation == "ERROR" {
      reply.Err = "network error"
      kv.mu.Unlock()
      return nil
    } else if op == agreedOp {
      res, ok := kv.runLog(seq)
      if ok != "OK" {
        reply.Err = "network error"
      } else {
        reply.PreviousValue = res
        reply.Err = "OK"
      }
      kv.mu.Unlock()
      return nil
    }
    tryTimes--
  }
  reply.Err = "network error"
  kv.mu.Unlock()
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.kv = make(map[string]string)
  kv.serials = make(map[int64]string)
  kv.min = 0


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

