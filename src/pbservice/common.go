package pbservice

import "hash/fnv"
import "crypto/rand"
import "math/big"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
  ErrNonConsistent = "ErrNonConsistent"
  FromServer = 1
  FromClient = 2
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  Serial int64
  From int
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
  Serial int64
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Serial int64
}

type GetReply struct {
  Err Err
  Value string
  Serial int64
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}

// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

