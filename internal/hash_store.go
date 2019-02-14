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

func H_New(opts badger.Options, flushSize int) *H_ {
	var h H_
	h.KVStore = new(KVStore)
	NewKVStore(h.KVStore, opts, flushSize)
	return &h
}

func (h *H_) CreateValues(ids [][]byte, unique bool) ([]byte, []byte) {
	return CreateHashValue(ids, unique)
}
