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

func (p *P_) CreateValues(entry string, oldKey []byte) ([]byte, bool) {

	// PATHWAY: Metabolic intermediate biosynthesis; prephenate biosynthesis; prephenate from chorismate: step 1/1.
	// PATHWAY: Porphyrin-containing compound metabolism; heme O biosynthesis; heme O from protoheme: step 1/1. {ECO:0000255|HAMAP-Rule:MF_00154}.

	var new = false

	if entry == "" && oldKey == nil {
		return p.NilVal, true
	} else if (entry == "" && oldKey != nil) {
		return p.NilVal, false
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
	combinedKey, _ := CreateHashValue(ids, true)

	var newCombinedKey = p.NilVal
	var newCombinedVal = p.NilVal

	if ! bytes.Equal(combinedKey, oldKey) {
		new = true
		p.Mu.Lock()
		if ! bytes.Equal(oldKey, p.NilVal) {
			oldVal, ok := p.GetValue(oldKey)
			if (ok) {
				for i:=0; (i+1)<len(oldVal); i+=20 {
					ids = append(ids, oldVal[i:i+20])
				}
			}
		}
		newCombinedKey, newCombinedVal = CreateHashValue(ids, true)
		combinedKey = newCombinedKey
		p.AddValue(newCombinedKey, newCombinedVal)
		p.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
