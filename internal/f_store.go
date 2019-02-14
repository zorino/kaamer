package kvstore

import (
	// "fmt"
	"bytes"
	"regexp"
	"github.com/dgraph-io/badger"
	// "encoding/hex"
)

// Protein Function Entries (HAMAP or manually defined in uniprot/swissprot)
type F_ struct {
	*KVStore
}

func F_New(opts badger.Options, flushSize int) *F_ {
	var f F_
	f.KVStore = new(KVStore)
	NewKVStore(f.KVStore, opts, flushSize)
	return &f
}

func (f *F_) CreateValues(entry string, oldKey []byte, ff_ *H_) ([]byte, bool) {

	// FUNCTION: Catalyzes the Claisen rearrangement of chorismate to prephenate. Probably involved in the aromatic amino acid biosynthesis. {ECO:0000269|PubMed:15737998, ECO:0000269|PubMed:18727669, ECO:0000269|PubMed:19556970}.

	var new = false

	if entry == "" && oldKey == nil {
		return ff_.NilVal, true
	} else if (entry == "" && oldKey != nil) {
		return ff_.NilVal, false
	}

	reg := regexp.MustCompile(` \{.*\}\.`)

	protFunction := reg.ReplaceAllString(entry, "${1}")

	protFunction =  protFunction[10:]

	// fmt.Println("Protein function:" + protFunction+"##")

	if oldKey == nil {
		new = true
	}

	finalKeyValue := [][]byte{[]byte(protFunction)}
	finalKey, _ := CreateHashValue(finalKeyValue, false)

	f.Mu.Lock()
	f.AddValue(finalKey, []byte(protFunction))
	f.Mu.Unlock()

	ids := [][]byte{finalKey}
	combinedKey, _ := ff_.CreateValues(ids, true)

	var newCombinedKey = ff_.NilVal
	var newCombinedVal = ff_.NilVal

	if ! bytes.Equal(combinedKey, oldKey) {
		new = true
		ff_.Mu.Lock()
		if ! bytes.Equal(oldKey, ff_.NilVal) {
			oldVal, ok := ff_.GetValue(oldKey)
			if (ok) {
				for i:=0; (i+1)<len(oldVal); i+=20 {
					ids = append(ids, oldVal[i:i+20])
				}
			}
		}
		newCombinedKey, newCombinedVal = ff_.CreateValues(ids, true)
		combinedKey = newCombinedKey
		ff_.AddValue(newCombinedKey, newCombinedVal)
		ff_.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
