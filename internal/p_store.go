package kvstore

import (
	"bytes"
	"regexp"
	"github.com/dgraph-io/badger"
)

// Protein Pathways Entries (HAMAP or manually defined in uniprot/swissprot)
type P_ struct {
	*KVStore
}

func P_New(opts badger.Options, flushSize int) *P_ {
	var p P_
	p.KVStore = new(KVStore)
	NewKVStore(p.KVStore, opts, flushSize)
	return &p
}

func (p *P_) CreateValues(entry string, oldKey []byte, pp_ *H_) ([]byte, bool) {

	// PATHWAY: Metabolic intermediate biosynthesis; prephenate biosynthesis; prephenate from chorismate: step 1/1.
	// PATHWAY: Porphyrin-containing compound metabolism; heme O biosynthesis; heme O from protoheme: step 1/1. {ECO:0000255|HAMAP-Rule:MF_00154}.

	var new = false

	if entry == "" && oldKey == nil {
		return pp_.NilVal, true
	} else if (entry == "" && oldKey != nil) {
		return pp_.NilVal, false
	}

	reg := regexp.MustCompile(` \{.*\}\.`)
	protPathway := reg.ReplaceAllString(entry, "${1}")

	regStep := regexp.MustCompile(`: step [0-9]/[0-9]`)
	protPathway = regStep.ReplaceAllString(protPathway, "${1}")

	protPathway =  protPathway[9:]

	// fmt.Println("Protein pathway:" + protPathway+"##")

	if oldKey == nil {
		new = true
	}

	finalKeyValue := [][]byte{[]byte(protPathway)}
	finalKey, _ := CreateHashValue(finalKeyValue, false)

	p.Mu.Lock()
	p.AddValue(finalKey, []byte(protPathway))
	p.Mu.Unlock()

	ids := [][]byte{finalKey}
	combinedKey, _ := pp_.CreateValues(ids, true)

	var newCombinedKey = pp_.NilVal
	var newCombinedVal = pp_.NilVal

	if ! bytes.Equal(combinedKey, oldKey) {
		new = true
		pp_.Mu.Lock()
		if ! bytes.Equal(oldKey, pp_.NilVal) {
			oldVal, ok := pp_.GetValue(oldKey)
			if (ok) {
				for i:=0; (i+1)<len(oldVal); i+=20 {
					ids = append(ids, oldVal[i:i+20])
				}
			}
		}
		newCombinedKey, newCombinedVal = pp_.CreateValues(ids, true)
		combinedKey = newCombinedKey
		pp_.AddValue(newCombinedKey, newCombinedVal)
		pp_.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
