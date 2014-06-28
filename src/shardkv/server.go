package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "errors"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}

type KVRequest struct {
  Shard int
  maxView int
}

type KVReply struct {
  KVMap map[string]string
}

type ReconfArgs struct {
  Shard int
  KVMap map[string]string
}

type ReconfReply struct {
}

type MoveArgs struct {
  ShardKV map[string]string
  Shard int
}


type Op struct {
  // Your definitions here.
  Operation string   // PUT PUTHASH GET RECONF NOOP
  Args interface{}
  Serial int64
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  kv map[string]string
  shards map[string]int
  serials map[int64]string
  config shardmaster.Config
  min int
  maxView int
  muKV sync.Mutex
}

func (kv *ShardKV) makeOp(oper string, serial int64, args interface{}) (op Op) {
  op = Op{}
  op.Operation = oper
  op.Args = args
  op.Serial = serial
  return op
}

func (kv *ShardKV) waitPaxos(seq int) (op Op) {
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

func (kv *ShardKV) PutShard(args *ReconfArgs, reply *ReconfReply) error {
  kv.muKV.Lock()
  defer kv.muKV.Unlock()
  for key, value := range(args.KVMap) {
    kv.kv[key] = value
    kv.shards[key] = args.Shard
  }
  return nil
}

func (kv *ShardKV) GetShard(args *KVRequest, reply *KVReply) error {
//  kv.mu.Lock()
//  fmt.Println("Getshard start")
//  kv.runLog(kv.px.Max())
  if kv.maxView >= args.maxView {
    shard := make(map[string]string)
    kv.muKV.Lock()
    for key, value := range(kv.kv) {
      if kv.shards[key] == args.Shard {
        shard[key] = value
      }
    }
    kv.muKV.Unlock()
    reply.KVMap = shard
//  kv.mu.Unlock()
    return nil
  } else {
    return errors.New("Please visit me later")
  }
}

func (kv *ShardKV) runOp(op Op) interface{} {
  v, ok := kv.serials[op.Serial]
  if ok {
    return v
  }
  switch op.Operation {
    case "PUT":
      kv.muKV.Lock()
      args := op.Args.(PutArgs)
      kv.kv[args.Key] = args.Value
      kv.serials[op.Serial] = ""
      kv.shards[args.Key] = args.Shard
      kv.muKV.Unlock()
      fmt.Println("PUT", kv.gid, "[" + args.Key + ":", args.Value)
      return ""
    case "GET":
      args := op.Args.(GetArgs)
      v, ok = kv.kv[args.Key]
      fmt.Println("GET", kv.gid, "[" + args.Key + ":", v)
      return v
    case "PUTHASH":
      kv.muKV.Lock()
      args := op.Args.(PutArgs)
      v, ok = kv.kv[args.Key]
      kv.serials[op.Serial] = v
      next := v + args.Value
      kv.kv[args.Key] = strconv.Itoa(int(hash(next)))
      kv.shards[args.Key] = args.Shard
      kv.muKV.Unlock()
      fmt.Println("HASH", kv.gid, "[" + args.Key + ":", int(hash(next)), v)
      return v
    case "RECONF":
      newConfig := op.Args.(shardmaster.Config)
//      prevConfig := kv.sm.Query(newConfig.Num - 1)
//    fmt.Println("@@", newConfig.Num, prevConfig.Num, kv.config.Num)
//      if prevConfig.Num > kv.config.Num {
//        kv.config = prevConfig
//    }
/*      for i := 0; i < len(kv.config.Shards); i++ {
        if kv.config.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid && newConfig.Shards[i] != 0 {
          args := ReconfArgs{i, make(map[string]string)}
          reply := ReconfReply{}
          servers := newConfig.Groups[newConfig.Shards[i]]
          for key, value := range(kv.kv) {
            if kv.shards[key] == i {
              args.KVMap[key] = value
            }
          }
          for j := 0; j < len(servers); j++ {
            ok = call(servers[j], "ShardKV.PutShard", &args, &reply)
            if ok {
//              break
            } else {
            }
          }
        }
      }
*/
      fmt.Println("@ GID", kv.gid, kv.config.Shards)
      kv.maxView = newConfig.Num
      succeed := false
      for !succeed {
      for i := 0; i < len(kv.config.Shards); i++ {
        succeed = true
        if kv.config.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid && kv.config.Shards[i] != 0 {
          succeed = false
          args := KVRequest{i, kv.maxView}
          reply := KVReply{}
          servers := kv.config.Groups[kv.config.Shards[i]]
          for j := 0; j < len(servers); j++ {
            ok = call(servers[j], "ShardKV.GetShard", &args, &reply)
            if ok {
              for key, value := range(reply.KVMap) {
                kv.kv[key] = value
                kv.shards[key] = i
              }
              fmt.Println("MOVE", i, kv.maxView, kv.gid, kv.config.Shards[i], reply.KVMap)
              succeed = true
              break
            } else {
            }
          }
        }
      }
      }
      kv.serials[op.Serial] = ""
      kv.config = newConfig
      fmt.Println("# GID", kv.gid, kv.config.Shards)
      return ""
    case "NOOP":
      return ""
    default:
      return ""
  }
}


func (kv *ShardKV) runLog(max int) (ret interface{}, err error) {
  err = nil
  ret = ""
  for ; kv.min <= max; kv.min++ {
    decided, value := kv.px.Status(kv.min)
    if value.(Op).Operation == "PUT" && kv.config.Shards[value.(Op).Args.(PutArgs).Shard] != kv.gid  || value.(Op).Operation == "GET" && kv.config.Shards[value.(Op).Args.(GetArgs).Shard] != kv.gid || value.(Op).Operation == "PUTHASH" && kv.config.Shards[value.(Op).Args.(PutArgs).Shard] != kv.gid {
        ret = "error"
        err = errors.New("Network error")
        break
    }
    if decided {
      ret = kv.runOp(value.(Op))
    } else {
      op := kv.makeOp("NOOP", 0, "")
      kv.px.Start(kv.min, op)
      op = kv.waitPaxos(kv.min)
      if op.Operation == "ERROR" {
        ret = "error"
        err = errors.New("Network error")
        break
      } else {
        ret = kv.runOp(op)
      }
    }
  }
  kv.px.Done(kv.min - 1)
  return ret, err
}

func (kv *ShardKV) process(op Op) (interface{}, error) {
//  kv.mu.Lock()
  tryTimes := 5
  seq := -1
  for tryTimes > 0 {
    tmp := kv.px.Max() + 1
    seq = seq + 1
    if seq < tmp {
      seq = tmp
    }
    kv.px.Start(seq, op)
    agreedOp := kv.waitPaxos(seq)
    if agreedOp.Operation == "ERROR" {
//      kv.mu.Unlock()
      return "error", errors.New("Network error")
    } else if op.Serial == agreedOp.Serial {
      res, ok := kv.runLog(seq)
//      kv.mu.Unlock()
      if ok != nil {
        return "error", errors.New("Network error")
      } else {
        return res, nil
      }
    } else {
    }
    tryTimes--
  }
//  kv.mu.Unlock()
  return "error", errors.New("Network error")
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
//  fmt.Println("GET", kv.config.Shards[args.Shard], kv.gid)
  kv.mu.Lock()
//  fmt.Println("Get start")
  defer func() {
      kv.mu.Unlock()
//      fmt.Println("Get finish")
  }()
  if kv.config.Shards[args.Shard] != kv.gid {
    reply.Err = ErrWrongGroup
    return nil
  }
  op := kv.makeOp("GET", args.Serial, *args)
  ret, err := kv.process(op)
  if err == nil {
    reply.Err = "OK"
    reply.Value = ret.(string)
  } else {
    reply.Err = "Wrong"
  }
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
//  fmt.Println("Put start")
  defer func() {
      kv.mu.Unlock()
//      fmt.Println("Put finish")
  }()
  if kv.config.Shards[args.Shard] != kv.gid {
    reply.Err = ErrWrongGroup
    return nil
  }
  op := kv.makeOp("PUT", args.Serial, *args)
  if args.DoHash {
    op = kv.makeOp("PUTHASH", args.Serial, *args)
  }
  ret, err := kv.process(op)
  if err == nil {
    reply.Err = "OK"
    reply.PreviousValue = ret.(string)
  } else {
    reply.Err = "Wrong"
  }
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
//  fmt.Println("tick start")
  defer func() {
      kv.mu.Unlock()
//      fmt.Println("tick finish")
  }()
  newConfig := kv.sm.Query(-1)
  if newConfig.Num > kv.config.Num {
    for true {
      op := kv.makeOp("RECONF", int64(newConfig.Num), newConfig)
      _, err := kv.process(op)
      if err == nil {
        break
      }
    }
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(shardmaster.Config{})
  gob.Register(PutArgs{})
  gob.Register(PutReply{})
  gob.Register(GetArgs{})
  gob.Register(GetReply{})
  gob.Register(ReconfArgs{})
  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.kv = make(map[string]string)
  kv.shards = make(map[string]int)
  kv.serials = make(map[int64]string)
  kv.config = kv.sm.Query(-1)
  kv.min = 0
  kv.maxView = 0

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
