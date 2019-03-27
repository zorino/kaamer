package kvstore

import (
	"bytes"
	"github.com/dgraph-io/badger"
)

// Protein Pathways Entries (HAMAP or manually defined in uniprot/swissprot)
type O_ struct {
	*KVStore
}

func O_New(opts badger.Options, flushSize int, nbOfThreads int) *O_ {
	var o O_
	o.KVStore = new(KVStore)
	NewKVStore(o.KVStore, opts, flushSize, nbOfThreads)
	return &o
}

func (o *O_) CreateValues(entry string, oldKey []byte, oo_ *H_, threadId int) ([]byte, bool) {

	// cellular organisms, Bacteria, Proteobacteria, Gammaproteobacteria, Enterobacterales, Enterobacteriaceae, Escherichia, Escherichia coli, Escherichia coli (strain K12)

	var new = false

	if entry == "" && oldKey == nil {
		return oo_.NilVal, true
	} else if entry == "" && oldKey != nil {
		return oo_.NilVal, false
	}

	protOrganism := entry[20:]

	if oldKey == nil {
		new = true
	}

	finalKeyValue := [][]byte{[]byte(protOrganism)}
	finalKey, _ := CreateHashValue(finalKeyValue, false)

	o.AddValue(finalKey, []byte(protOrganism), threadId)

	ids := [][]byte{finalKey}
	combinedKey, _ := oo_.CreateValues(ids, true)

	var newCombinedKey = oo_.NilVal
	var newCombinedVal = oo_.NilVal

	if !bytes.Equal(combinedKey, oldKey) {
		new = true
		newCombinedKey, newCombinedVal = oo_.CreateValues(ids, true)
		oo_.AddValue(newCombinedKey, newCombinedVal, threadId)
	} else {
		new = false
	}

	return combinedKey, new
}
