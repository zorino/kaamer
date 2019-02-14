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

func G_New(opts badger.Options, flushSize int) *G_ {
	var g G_
	g.KVStore = new(KVStore)
	NewKVStore(g.KVStore, opts, flushSize)
	return &g
}

func (g *G_) CreateValues(entry string, oldKey []byte, gg_ *H_) ([]byte, bool) {

	// aldehyde dehydrogenase [NAD(P)+] activity [GO:0004030]; putrescine catabolic process [GO:0009447]

	if oldKey == nil && entry == "" {
		return gg_.NilVal, true
	}

	goArray := strings.Split(entry, "; ")

	reg := regexp.MustCompile(` \[GO:.*\]`)

	var goIds [][]byte

	for _, _go := range goArray {

		// goName := reg.ReplaceAllString(_go, "${1}")

		goId := reg.FindString(_go)

		if goId == "" {
			continue
		}

		// real id prefix = "."
		goId = "." + goId[5:len(goId)-1]

		goKey, _ := CreateHashValue([][]byte{[]byte(goId)}, false)
		goIds = append(goIds, goKey)

		g.Mu.Lock()
		g.AddValue(goKey, []byte(_go))
		g.Mu.Unlock()

	}

	var combinedKey = gg_.NilVal
	var newCombinedKey = gg_.NilVal
	var newCombinedVal = gg_.NilVal

	var new = false


	if len(goIds) == 0 {
		combinedKey = gg_.NilVal
	} else {

		combinedKey, _ = gg_.CreateValues(goIds, true)

		if ! bytes.Equal(combinedKey, oldKey) {
			new = true
			gg_.Mu.Lock()
			if ! bytes.Equal(oldKey, gg_.NilVal) {
				oldVal, ok := gg_.GetValue(oldKey)
				if (ok) {
					// fmt.Println("Old Val exists : " + oldVal)
					for i:=0; (i+1)<len(oldVal); i+=20 {
						goIds = append(goIds, oldVal[i:i+20])
					}
				}
			}

			newCombinedKey, newCombinedVal = gg_.CreateValues(goIds, true)
			combinedKey = newCombinedKey
			gg_.AddValue(newCombinedKey, newCombinedVal)
			gg_.Mu.Unlock()

		} else {
			new = false
		}

	}

	return combinedKey, new

}
