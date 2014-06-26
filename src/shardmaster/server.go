package shardmaster

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
import "errors"
import "strconv"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  min int
  serial int
}

type Op struct {
  // Your data here.
  Operation string // "JOIN LEAVE MOVE QUERY or NOOP"
  Args interface{}
  Serial string
}

func (sm *ShardMaster) makeOp(oper string, args interface{}) (op Op) {
  op = Op{}
  op.Operation = oper
  op.Args = args
  op.Serial = strconv.Itoa(sm.me) + "&" + strconv.Itoa(sm.serial)
  sm.serial++
  return op
}

func (sm *ShardMaster) waitPaxos(seq int) (op Op) {
  period := 10 * time.Millisecond
  for {
    decided, value := sm.px.Status(seq)
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

func (sm *ShardMaster) getLastConfig() Config {
  current := sm.configs[len(sm.configs) - 1]
  config := Config{}
  config.Num = current.Num
  config.Shards = current.Shards
  config.Groups = make(map[int64][]string)
  for key, value := range(current.Groups) {
    servers := value[:]
    config.Groups[key] = servers
  }
  return config
}

func (sm *ShardMaster) allToOne(config Config, gid int64) Config {
  current := make(map[int64]int)
  for key, _ := range(config.Groups) {
    current[key] = 0
  }
  current[gid] = 0
  for i := 0; i < len(config.Shards); i++ {
    if config.Shards[i] > 0 {
      current[config.Shards[i]]++
    }
  }
  avg := NShards / (len(config.Groups) + 1)
  bigCounter := NShards % (len(config.Groups) + 1)
  smallCounter := len(config.Groups) + 1 - bigCounter
  for key, value := range(current) {
    if value >= avg + 1 {
      if bigCounter > 0 {
        current[key] = value - (avg + 1)
        bigCounter--
      } else {
        current[key] = value - avg
        smallCounter--
      }
    } else {
      if smallCounter > 0 {
        current[key] = value - avg
        smallCounter--
      } else {
        current[key] = value - (avg + 1)
        bigCounter--
      }
    }
  }

  for i := 0; i < len(config.Shards); i++ {
    if current[config.Shards[i]] > 0 {
      current[config.Shards[i]]--
      config.Shards[i] = 0
    }
  }

  pos := 0
  for key, value := range(current) {
    for value < 0 {
      for config.Shards[pos] != 0 {
        pos++
      }
      config.Shards[pos] = key
      pos++
      value++
    }
  }

  return config
}

func (sm *ShardMaster) oneToAll(config Config, gid int64) Config {
  current := make(map[int64]int)
  for key, _ := range(config.Groups) {
    current[key] = 0
  }
  for i := 0; i < len(config.Shards); i++ {
    if config.Shards[i] == gid {
      config.Shards[i] = 0
    }
  }
  if len(config.Groups) == 0 {
    return config
  }

  avg := NShards / (len(config.Groups))
  bigCounter := NShards % (len(config.Groups))
  smallCounter := len(config.Groups) - bigCounter
  for i := 0; i < len(config.Shards); i++ {
    if config.Shards[i] > 0 {
      current[config.Shards[i]]++
    }
  }
  for key, value := range(current) {
    if value >= avg + 1 {
      if bigCounter > 0 {
        current[key] = value - (avg + 1)
        bigCounter--
      } else {
        current[key] = value - avg
        smallCounter--
      }
    } else {
      if smallCounter > 0 {
        current[key] = value - avg
        smallCounter--
      } else {
        current[key] = value - (avg + 1)
        bigCounter--
      }
    }
  }

  for i := 0; i < len(config.Shards); i++ {
    if current[config.Shards[i]] > 0 {
      current[config.Shards[i]]--
      config.Shards[i] = 0
    }
  }

  pos := 0
  for key, value := range(current) {
    for value < 0 {
      for config.Shards[pos] != 0 {
        pos++
      }
      config.Shards[pos] = key
      pos++
      value++
    }
  }
  return config
}
/*
  if len(config.Groups) == 0 {
    for i := 0; i < len(config.Shards); i++ {
      config.Shards[i] = 0
    }
    return config
  }
  preAvg := NShards / (len(config.Groups) + 1)
  for i := 0; i < len(config.Shards); i++ {
    if config.Shards[i] != gid {
      current[config.Shards[i]]++
    }
  }
  
  if len(current) == 0 {
    return config
  }
  pos := 0
  for key, value := range(current) {
    if value == preAvg {
      for pos < len(config.Shards) {
        if config.Shards[pos] == gid {
          config.Shards[pos] = key
          pos++
          break
        }
        pos++
      }
    }
  }
  for pos < len(config.Shards) {
    for key, _ := range(current) {
      for pos < len(config.Shards) {
        if config.Shards[pos] == gid {
          config.Shards[pos] = key
          pos++
          break
        }
        pos++
      }
    }
  }
  return config
}
*/

func (sm *ShardMaster) runOp(op Op) interface{} {
  switch op.Operation {
    case "JOIN":
      config := sm.getLastConfig()
      args := op.Args.(JoinArgs)
      servers := args.Servers[:]
      config.Num++
      _, ok := config.Groups[args.GID]
      if !ok {
        config = sm.allToOne(config, args.GID)
        config.Groups[args.GID] = servers
      } else {
        for i := 0; i < len(servers); i++ {
          pos := 0
          for ;pos < len(config.Groups[args.GID]); pos++ {
            if config.Groups[args.GID][pos] == servers[i] {
              break
            }
          }
          if pos == len(config.Groups[args.GID]) {
            config.Groups[args.GID] = append(config.Groups[args.GID], servers[i])
          }
        }
      }
      sm.configs = append(sm.configs, config)
      return ""
    case "LEAVE":
      config := sm.getLastConfig()
      args := op.Args.(LeaveArgs)
      config.Num++
      delete(config.Groups, args.GID)
      config = sm.oneToAll(config, args.GID)
      sm.configs = append(sm.configs, config)
      return ""
    case "MOVE":
      config := sm.getLastConfig()
      args := op.Args.(MoveArgs)
      config.Num++
      config.Shards[args.Shard] = args.GID
      sm.configs = append(sm.configs, config)
      return ""
    case "QUERY":
      args := op.Args.(QueryArgs)
      num := args.Num
      if num < 0 || num >= len(sm.configs) {
        num = len(sm.configs) - 1
      }
      return sm.configs[num]
    case "NOOP":
      return ""
    default:
      return ""
  }
}


func (sm *ShardMaster) runLog(max int) (ret interface{}, err error) {
  err = nil
  ret = ""
  for ; sm.min <= max; sm.min++ {
    decided, value := sm.px.Status(sm.min)
    if decided {
      ret = sm.runOp(value.(Op))
    } else {
      op := sm.makeOp("NOOP", "")
      sm.px.Start(sm.min, op)
      op = sm.waitPaxos(sm.min)
      if op.Operation == "ERROR" {
        ret = "error"
        err = errors.New("Network error")
        break
      } else {
        ret = sm.runOp(op)
      }
    }
  }
  sm.px.Done(sm.min - 1)
  return ret, err
}

func (sm *ShardMaster) process(op Op) (interface{}, error) {
  sm.mu.Lock()
  tryTimes := 5
  seq := -1
  for tryTimes > 0 {
    tmp := sm.px.Max() + 1
    seq = seq + 1
    if seq < tmp {
      seq = tmp
    }
    sm.px.Start(seq, op)
    agreedOp := sm.waitPaxos(seq)
    if agreedOp.Operation == "ERROR" {
      sm.mu.Unlock()
      return "error", errors.New("Network error")
    } else if op.Serial == agreedOp.Serial {
      res, ok := sm.runLog(seq)
      sm.mu.Unlock()
      if ok != nil {
        return "error", errors.New("Network error")
      } else {
        return res, nil
      }
    } else {
    }
    tryTimes--
  }
  sm.mu.Unlock()
  return "error", errors.New("Network error")
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  op := sm.makeOp("JOIN", *args)
  _, err := sm.process(op)
  return err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  op := sm.makeOp("LEAVE", *args)
  _, err := sm.process(op)
  return err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  op := sm.makeOp("MOVE", *args)
  _, err := sm.process(op)
  return err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  op := sm.makeOp("QUERY", *args)
  ret, err := sm.process(op)
  if err == nil {
    reply.Config = ret.(Config)
  }
  return err
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(QueryArgs{})
  gob.Register(JoinArgs{})
  gob.Register(LeaveArgs{})
  gob.Register(MoveArgs{})
  gob.Register(QueryReply{})
  gob.Register(JoinReply{})
  gob.Register(LeaveReply{})
  gob.Register(MoveReply{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  sm.min = 0
  sm.serial = 0
  
  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
