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

func (g *G_) CreateValues(entry string, oldKey []byte) ([]byte, bool) {

	// aldehyde dehydrogenase [NAD(P)+] activity [GO:0004030]; putrescine catabolic process [GO:0009447]

	if oldKey == nil && entry == "" {
		return g.NilVal, true
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

	var combinedKey = g.NilVal
	var newCombinedKey = g.NilVal
	var newCombinedVal = g.NilVal

	var new = false


	if len(goIds) == 0 {
		combinedKey = g.NilVal
	} else {

		combinedKey, _ = CreateHashValue(goIds, true)

		if ! bytes.Equal(combinedKey, oldKey) {
			new = true
			g.Mu.Lock()
			if ! bytes.Equal(oldKey, g.NilVal) {
				oldVal, ok := g.GetValue(oldKey)
				if (ok) {
					// fmt.Println("Old Val exists : " + oldVal)
					for i:=0; (i+1)<len(oldVal); i+=20 {
						goIds = append(goIds, oldVal[i:i+20])
					}
				}
			}

			newCombinedKey, newCombinedVal = CreateHashValue(goIds, true)
			combinedKey = newCombinedKey
			g.AddValue(newCombinedKey, newCombinedVal)
			g.Mu.Unlock()

		} else {
			new = false
		}

	}

	return combinedKey, new

}
