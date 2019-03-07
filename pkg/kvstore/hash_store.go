package kvstore

import (
	// "fmt"
	// "bytes"
	// "regexp"
	"github.com/dgraph-io/badger"
	// "encoding/hex"
)

// Hash store for values combination used in other stores
type H_ struct {
	*KVStore
}

func H_New(opts badger.Options, flushSize int, nbOfThreads int) *H_ {
	var h H_
	h.KVStore = new(KVStore)
	NewKVStore(h.KVStore, opts, flushSize, nbOfThreads)
	return &h
}

func (h *H_) CreateValues(ids [][]byte, unique bool) ([]byte, []byte) {
	return CreateHashValue(ids, unique)
}
