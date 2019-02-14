package kvstore

import (
	// "fmt"
	"bytes"
	"github.com/dgraph-io/badger"
	// "encoding/hex"
)

// Protein Pathways Entries (HAMAP or manually defined in uniprot/swissprot)
type O_ struct {
	*KVStore
}

func O_New(opts badger.Options, flushSize int) *O_ {
	var o O_
	o.KVStore = new(KVStore)
	NewKVStore(o.KVStore, opts, flushSize)
	return &o
}

func (o *O_) CreateValues(entry string, oldKey []byte, oo_ *H_) ([]byte, bool) {

	// cellular organisms, Bacteria, Proteobacteria, Gammaproteobacteria, Enterobacterales, Enterobacteriaceae, Escherichia, Escherichia coli, Escherichia coli (strain K12)

	var new = false

	if entry == "" && oldKey == nil {
		return oo_.NilVal, true
	} else if (entry == "" && oldKey != nil) {
		return oo_.NilVal, false
	}

	protOrganism := entry[20:]

	// fmt.Println("Protein organism:"+protOrganism+"##")

	if oldKey == nil {
		new = true
	}

	finalKeyValue := [][]byte{[]byte(protOrganism)}
	finalKey, _ := CreateHashValue(finalKeyValue, false)

	o.Mu.Lock()
	o.AddValue(finalKey, []byte(protOrganism))
	o.Mu.Unlock()

	ids := [][]byte{finalKey}
	combinedKey, _ := oo_.CreateValues(ids, true)

	var newCombinedKey = oo_.NilVal
	var newCombinedVal = oo_.NilVal

	if ! bytes.Equal(combinedKey, oldKey) {
		new = true
		oo_.Mu.Lock()
		if ! bytes.Equal(oldKey, oo_.NilVal) {
			oldVal, ok := oo_.GetValue(oldKey)
			if (ok) {
				for i:=0; (i+1)<len(oldVal); i+=20 {
					ids = append(ids, oldVal[i:i+20])
				}
			}
		}
		newCombinedKey, newCombinedVal = oo_.CreateValues(ids, true)
		combinedKey = newCombinedKey
		oo_.AddValue(newCombinedKey, newCombinedVal)
		oo_.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
