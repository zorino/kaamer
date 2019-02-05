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

func (f *F_) CreateValues(entry string, oldKey []byte) ([]byte, bool) {

	// FUNCTION: Catalyzes the Claisen rearrangement of chorismate to prephenate. Probably involved in the aromatic amino acid biosynthesis. {ECO:0000269|PubMed:15737998, ECO:0000269|PubMed:18727669, ECO:0000269|PubMed:19556970}.

	var new = false

	if entry == "" && oldKey == nil {
		return f.NilVal, true
	} else if (entry == "" && oldKey != nil) {
		return f.NilVal, false
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
	combinedKey, _ := CreateHashValue(ids, true)

	var newCombinedKey = f.NilVal
	var newCombinedVal = f.NilVal

	if ! bytes.Equal(combinedKey, oldKey) {
		new = true
		f.Mu.Lock()
		if ! bytes.Equal(oldKey, f.NilVal) {
			oldVal, ok := f.GetValue(oldKey)
			if (ok) {
				for i:=0; (i+1)<len(oldVal); i+=20 {
					ids = append(ids, oldVal[i:i+20])
				}
			}
		}
		newCombinedKey, newCombinedVal = CreateHashValue(ids, true)
		combinedKey = newCombinedKey
		f.AddValue(newCombinedKey, newCombinedVal)
		f.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
