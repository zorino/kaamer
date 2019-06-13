package kvstore

import (
	// "fmt"
	// "bytes"
	// "regexp"
	"github.com/dgraph-io/badger"
	// "encoding/hex"
)

// Hash store for values combination used in other stores
type P_ struct {
	*KVStore
}

func P_New(opts badger.Options, flushSize int, nbOfThreads int) *P_ {
	var p P_
	p.KVStore = new(KVStore)
	NewKVStore(p.KVStore, opts, flushSize, nbOfThreads)
	return &p
}
