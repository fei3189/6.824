package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

type State struct {
  n_p int
  n_a int
  n_v interface{}
  done bool
}

type PrepareArgs struct {
  Seq int
  Number int
}

type PrepareReply struct {
  HighestNumber int
  HighestValue interface{}
  Max int
  OK bool
}

type AcceptArgs struct {
  Seq int
  Number int
  Value interface{}
}

type DecideArgs struct {
  Seq int
  Value interface{}
}

type Reply struct {
  OK bool
}

type MinArgs struct {
  Seq int
}

type AskMinArgs struct {
}

type AskMinReply struct {
  Seq int
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  instances map[int]*State
  min int
  minLocal int
  max int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) SyncMin(args *MinArgs, rep *Reply) error {
  if px.min < args.Seq {
    px.min = args.Seq
    if px.min > px.minLocal {
      px.minLocal = px.min
    }
    px.DeleteExpired(px.min)
  }
  return nil
}

func (px *Paxos) AskMin(args *AskMinArgs, rep *AskMinReply) error {
  rep.Seq = px.minLocal
  return nil
}

func (px *Paxos) Prepare(args *PrepareArgs, rep *PrepareReply) error {
  px.mu.Lock()
  if args.Seq > px.max {
    px.max = args.Seq
  }
  state, ok := px.instances[args.Seq]

  if ok {
    if args.Number > state.n_p {
      state.n_p = args.Number
      rep.OK = true
    } else {
      rep.OK = false
    }
  } else {
    state = &State{args.Number, -1, 0, false}
    px.instances[args.Seq] = state
    rep.OK = true
  }
  rep.HighestNumber = state.n_a
  rep.HighestValue = state.n_v
  rep.Max = state.n_p
  px.mu.Unlock()
//  fmt.Println(px.me, " prepare ", rep.OK, " ", args.Number)
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, rep *Reply) error {
  px.mu.Lock()
  if args.Seq > px.max {
    px.max = args.Seq
  }
  state, ok := px.instances[args.Seq]
  if ok {
    if args.Number >= state.n_p {
/*      if state.done && state.n_v != args.Value {
        for true {
          fmt.Println("diff value")
        }
      }  */
      state.n_a = args.Number
      state.n_v = args.Value
      rep.OK = true
    } else {
      rep.OK = false
    }
  } else {
    rep.OK = false
  }
  px.mu.Unlock()
  return nil
}

func (px *Paxos) Decide(args *DecideArgs, rep *Reply) error {
  px.mu.Lock()
  if args.Seq > px.max {
    px.max = args.Seq
  }
  state, ok := px.instances[args.Seq]
  if !ok {
    px.mu.Unlock()
    return nil
  }
  if state.n_v == args.Value {
    state.done = true
    rep.OK = true
  } else {
    rep.OK = false
  }
/*  if state.done {
    rep.OK = true
  } else if state.n_v == args.Value {
    state.done = true
    rep.OK = true
  } else {
    state.done = true
    state.n_v = args.Value
    rep.OK = true
  } */
  px.mu.Unlock()
//  fmt.Println(px.me, " decide ", state.done)
  return nil
}

func (px *Paxos) PreparePhase(seq int, number int) (majority []int, count int, highest_n int, max_s int, highest_v interface{}) {
      majority = make([]int, len(px.peers))
      highest_n = -1
      max_s = -1
      count = 0
      for i := 0; i < len(px.peers); i++ {
        args := &PrepareArgs{seq, number}
        rep := &PrepareReply{}
        if i == px.me {
          px.Prepare(args, rep)
          if rep.OK {
            count++
            majority[i] = 1
            if rep.HighestNumber > highest_n {
              highest_n = rep.HighestNumber
              highest_v = rep.HighestValue
            }
          }
          if rep.Max > max_s {
            max_s = rep.Max
          }
        } else {
          ok := call(px.peers[i], "Paxos.Prepare", args, rep)
          if ok && rep.OK {
            count++
            majority[i] = 1
            if rep.HighestNumber > highest_n {
              highest_n = rep.HighestNumber
              highest_v = rep.HighestValue
            }
          }
          if rep.Max > max_s {
            max_s = rep.Max
          }
        }
      }
      return majority, count, highest_n, max_s, highest_v
}

func (px *Paxos) AcceptPhase(seq int, number int, majority []int, value interface{}) (count int) {
    count = 0
    for i := 0; i < len(px.peers); i++ {
      if majority[i] == 0 {
        continue
      }
      args := &AcceptArgs{seq, number, value}
      rep := &Reply{}
      if i == px.me {
        px.Accept(args, rep)
        if rep.OK {
          count++
        } else {
          majority[i] = 0
        }
      } else {
        ok := call(px.peers[i], "Paxos.Accept", args, rep)
        if ok && rep.OK {
          count++
        } else {
          majority[i] = 0
        }
      }
    }
    return count
}

func (px *Paxos) SendDecide(seq int, majority []int, value interface{}) (count int) {
    for i := 0; i < len(px.peers); i++ {
      if majority[i] == 0 {
        continue
      }
      args := &DecideArgs{seq, value}
      rep := &Reply{}
      if i == px.me {
  /*      if px.me == 0 {
          fmt.Println("##", seq)
        }*/
        px.Decide(args, rep)
        if rep.OK {
          count++
        }
      } else {
        call(px.peers[i], "Paxos.Decide", args, rep);
        if rep.OK {
          count++
        }
      }
    }
    return count
}

/* Propose an instance of paxos */
func (px *Paxos) Propose(seq int, v interface{}) {
  number, step := px.me + 1, len(px.peers)
  for seq >= px.min {
    number = number + step
    majority, count, highest_n, max_s, highest_v := px.PreparePhase(seq, number)
/*    if px.me == 0 && seq == 1 {
      fmt.Println(count, " ", majority[0], highest_n, number, px.max)
      for key, value := range(px.instances) {
        fmt.Print(key, " ", value.done, " ")
      } 
      fmt.Println()
    } */
    if count * 2 < len(px.peers) {
      bigger := (max_s / len(px.peers) + 100) * len(px.peers) + step
      if bigger > number {
        number = bigger
      }
      time.Sleep(100 * time.Millisecond)
      continue
    }
    if highest_n < 0 {
      highest_v = v
    }
    count = px.AcceptPhase(seq, number, majority, highest_v)
    if count * 2 < len(px.peers) {
      time.Sleep(100 * time.Millisecond)
      continue
    }
    count = px.SendDecide(seq, majority, highest_v)
    if count == len(px.peers) {
      break
    }
  }
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  px.mu.Lock()
  if seq < px.min {
    px.mu.Unlock()
    return
  }
  if seq > px.max {
    px.max = seq
  }
  px.mu.Unlock()
  go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  if seq >= px.minLocal {
    px.minLocal = seq + 1

    //Gathering min value
    cmin := px.minLocal
    a_args := &AskMinArgs{}
    a_rep := &AskMinReply{}
    for i := 0; i < len(px.peers); i++ {
      if i != px.me {
        ok := call(px.peers[i], "Paxos.AskMin", a_args, a_rep)
        if ok && a_rep.Seq < cmin {
          cmin = a_rep.Seq
        } else if !ok {
          cmin = px.min
          break
        }
      }
    }
    if cmin > px.min {
      px.min = cmin
      px.DeleteExpired(cmin)
    }
    //Sync min value
    args := &MinArgs{cmin}
    rep := &Reply{}
    for i := 0; i < len(px.peers); i++ {
      if i != px.me {
        call(px.peers[i], "Paxos.SyncMin", args, rep)
      }
    }
  }
}

func (px *Paxos) DeleteExpired(seq int) {
  px.mu.Lock()
  for key, _ := range(px.instances) {
    if key < seq {
      delete(px.instances, key)
    }
  }
  px.mu.Unlock()
}
//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return px.max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  return px.min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  done := false
  var value interface{} = nil
  if seq < px.min {
    done = false
  }
  state, ok := px.instances[seq]
  if ok && state.done {
    done = true
    value = state.n_v
  }
  px.mu.Unlock()
  return done, value
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.instances = make(map[int]*State)
  px.min = 0
  px.minLocal = 0
  px.max = -1
  // Your initialization code here.

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
