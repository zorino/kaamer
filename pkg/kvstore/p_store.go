package kvstore

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"regexp"
)

// Protein Pathways Entries (HAMAP or manually defined in uniprot/swissprot)
type P_ struct {
	*KVStore
}

func P_New(opts badger.Options, flushSize int, nbOfThreads int) *P_ {
	var p P_
	p.KVStore = new(KVStore)
	NewKVStore(p.KVStore, opts, flushSize, nbOfThreads)
	return &p
}

func (p *P_) CreateValues(entry string, oldKey []byte, pp_ *H_, threadId int) ([]byte, bool) {

	// PATHWAY: Metabolic intermediate biosynthesis; prephenate biosynthesis; prephenate from chorismate: step 1/1.
	// PATHWAY: Porphyrin-containing compound metabolism; heme O biosynthesis; heme O from protoheme: step 1/1. {ECO:0000255|HAMAP-Rule:MF_00154}.

	var new = false

	if entry == "" && oldKey == nil {
		return pp_.NilVal, true
	} else if entry == "" && oldKey != nil {
		return pp_.NilVal, false
	}

	reg := regexp.MustCompile(` \{.*\}\.`)
	protPathway := reg.ReplaceAllString(entry, "${1}")

	regStep := regexp.MustCompile(`: step [0-9]/[0-9]`)
	protPathway = regStep.ReplaceAllString(protPathway, "${1}")

	protPathway = protPathway[9:]

	if oldKey == nil {
		new = true
	}

	finalKeyValue := [][]byte{[]byte(protPathway)}
	finalKey, _ := CreateHashValue(finalKeyValue, false)

	p.AddValue(finalKey, []byte(protPathway), threadId)

	ids := [][]byte{finalKey}
	combinedKey, _ := pp_.CreateValues(ids, true)

	var newCombinedKey = pp_.NilVal
	var newCombinedVal = pp_.NilVal

	if !bytes.Equal(combinedKey, oldKey) {
		new = true
		newCombinedKey, newCombinedVal = pp_.CreateValues(ids, true)
		pp_.AddValue(newCombinedKey, newCombinedVal, threadId)
	} else {
		new = false
	}

	return combinedKey, new
}
