package kvstore

import (
	"bytes"
	"strings"
	"regexp"
	"github.com/dgraph-io/badger"
	// "encoding/hex"
)

// Gene Ontology Entries
type G_ struct {
	*KVStore
}

func G_New(opts badger.Options, flushSize int, nbOfThreads int) *G_ {
	var g G_
	g.KVStore = new(KVStore)
	NewKVStore(g.KVStore, opts, flushSize, nbOfThreads)
	return &g
}

func (g *G_) CreateValues(entry string, oldKey []byte, gg_ *H_, threadId int) ([]byte, bool) {

	// aldehyde dehydrogenase [NAD(P)+] activity [GO:0004030]; putrescine catabolic process [GO:0009447]

	if oldKey == nil && entry == "" {
		return gg_.NilVal, true
	}

	goArray := strings.Split(entry, "; ")

	reg := regexp.MustCompile(` \[GO:.*\]`)

	var ids [][]byte

	for _, _go := range goArray {

		goId := reg.FindString(_go)

		if goId == "" {
			continue
		}

		// real id prefix = "."
		// goId = "." + goId[5:len(goId)-1]
		goId = goId[5:len(goId)-1]

		goKey, _ := CreateHashValue([][]byte{[]byte(goId)}, false)
		ids = append(ids, goKey)

		g.AddValue(goKey, []byte(_go), threadId)

	}

	var combinedKey = gg_.NilVal
	var newCombinedKey = gg_.NilVal
	var newCombinedVal = gg_.NilVal

	var new = false


	if len(ids) == 0 {
		combinedKey = gg_.NilVal
	} else {

		combinedKey, _ = gg_.CreateValues(ids, true)

		if ! bytes.Equal(combinedKey, oldKey) {
			new = true
			newCombinedKey, newCombinedVal = gg_.CreateValues(ids, true)
			gg_.AddValue(newCombinedKey, newCombinedVal, threadId)
		} else {
			new = false
		}

	}

	return combinedKey, new

}
