package kvstore

import (
	"strings"
	"regexp"
	"github.com/dgraph-io/badger"
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

func (g *G_) CreateValues(entry string, oldKey string) (string, bool) {

	// aldehyde dehydrogenase [NAD(P)+] activity [GO:0004030]; putrescine catabolic process [GO:0009447]

	if oldKey == "" && entry == "" {
		return "_nil", true
	}

	goArray := strings.Split(entry, "; ")

	reg := regexp.MustCompile(` \[GO:.*\]`)

	var goIds []string

	for _, _go := range goArray {

		goName := reg.ReplaceAllString(_go, "${1}")

		goId := reg.FindString(_go)

		if goId == "" {
			continue
		}

		// real id prefix = "."
		goId = "." + goId[5:len(goId)-1]

		goIds = append(goIds, goId)

		g.Mu.Lock()
		g.AddValue(goId, goName)
		g.Mu.Unlock()
	}

	var combinedKey = ""
	var combinedVal = ""
	var new = false


	if len(goIds) == 0 {
		combinedKey = "_nil"
	} else {

		combinedKey, combinedVal = CreateHashValue(goIds, true)

		if combinedKey != oldKey {
			new = true
			g.Mu.Lock()
			if oldKey != "_nil" {
				oldVal, ok := g.GetValue(oldKey)
				if (ok) {
					// fmt.Println("Old Val exists : " + oldVal)
					goIds = append(goIds, strings.Split(oldVal, ",")...)
				}
			}
			g.AddValue(combinedKey, combinedVal)
			g.Mu.Unlock()
		} else {
			new = false
		}

	}

	return combinedKey, new

}
