package kvstore

import (
	// "fmt"
	"bytes"
	// "regexp"
	"github.com/dgraph-io/badger"
	// "encoding/hex"
)

// Protein Function Entries (HAMAP or manually defined in uniprot/swissprot)
type N_ struct {
	*KVStore
}

func N_New(opts badger.Options, flushSize int, nbOfThreads int) *N_ {
	var n N_
	n.KVStore = new(KVStore)
	NewKVStore(n.KVStore, opts, flushSize, nbOfThreads)
	return &n
}

func (n *N_) CreateValues(entry string, oldKey []byte, nn_ *H_, threadId int) ([]byte, bool) {

	// DNA polymerase IV (Pol IV) (EC 2.7.7.7) (Translesion synthesis polymerase IV) (TSL polymerase IV)

	var new = false

	if entry == "" && oldKey == nil {
		return nn_.NilVal, true
	} else if (entry == "" && oldKey != nil) {
		return nn_.NilVal, false
	}

	protName := entry

	if oldKey == nil {
		new = true
	}

	finalKeyValue := [][]byte{[]byte(protName)}
	finalKey, _ := CreateHashValue(finalKeyValue, false)

	n.AddValue(finalKey, []byte(protName), threadId)

	ids := [][]byte{finalKey}
	combinedKey, _ := nn_.CreateValues(ids, true)

	var newCombinedKey = nn_.NilVal
	var newCombinedVal = nn_.NilVal

	if ! bytes.Equal(combinedKey, oldKey) {
		new = true
		newCombinedKey, newCombinedVal = nn_.CreateValues(ids, true)
		nn_.AddValue(newCombinedKey, newCombinedVal, threadId)
	} else {
		new = false
	}

	return combinedKey, new
}
