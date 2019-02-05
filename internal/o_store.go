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

func (o *O_) CreateValues(entry string, oldKey []byte) ([]byte, bool) {

	// cellular organisms, Bacteria, Proteobacteria, Gammaproteobacteria, Enterobacterales, Enterobacteriaceae, Escherichia, Escherichia coli, Escherichia coli (strain K12)

	var new = false

	if entry == "" && oldKey == nil {
		return o.NilVal, true
	} else if (entry == "" && oldKey != nil) {
		return o.NilVal, false
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
	combinedKey, _ := CreateHashValue(ids, true)

	var newCombinedKey = o.NilVal
	var newCombinedVal = o.NilVal

	if ! bytes.Equal(combinedKey, oldKey) {
		new = true
		o.Mu.Lock()
		if ! bytes.Equal(oldKey, o.NilVal) {
			oldVal, ok := o.GetValue(oldKey)
			if (ok) {
				for i:=0; (i+1)<len(oldVal); i+=20 {
					ids = append(ids, oldVal[i:i+20])
				}
			}
		}
		newCombinedKey, newCombinedVal = CreateHashValue(ids, true)
		combinedKey = newCombinedKey
		o.AddValue(newCombinedKey, newCombinedVal)
		o.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
