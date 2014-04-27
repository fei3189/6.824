package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "time"

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"



type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
  kvs string
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here
  return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (ck *Clerk) Update() {
  v,_ := ck.vs.Get()
  ck.kvs = v.Primary
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

  ok := true
  if ck.kvs == "" {
    ok = false
  }

  serial := nrand()
  var reply GetReply
  for true {
    if !ok {
      ck.Update()
//      fmt.Printf("Change server %s\n", ck.kvs)
    }
    args := &GetArgs{}
    args.Key = key
    args.Serial = serial
    ok = call(ck.kvs, "PBServer.Get", args, &reply)
    if !ok {
//      fmt.Printf("wokao\n")
      time.Sleep(viewservice.PingInterval)
    } else {
      if reply.Err == OK  {
        break
      } else {
        ok = false
        time.Sleep(viewservice.PingInterval)
      }
    }
  }
  return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  ok := true
  if ck.kvs == "" {
    ok = false
  }
  serial := nrand()
  var reply PutReply
  for true {
    if !ok {
      ck.Update()
//      fmt.Printf("Change server %s\n", ck.kvs)
    }

    args := &PutArgs{}
    args.Key = key
    args.Value = value
    args.DoHash = dohash
    args.From = FromClient
    args.Serial = serial
  
    ok = call(ck.kvs, "PBServer.Put", args, &reply)
//  fmt.Printf("XXX %s %d\n", reply.PreviousValue, reply.Serial)
//      fmt.Printf("wokao1\n")
    if !ok {
      time.Sleep(viewservice.PingInterval)
    } else {
      if reply.Err == OK && reply.Serial == serial {
        break
      } else {
        ok = false
        time.Sleep(viewservice.PingInterval)
      }
    }
  }
  return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
