package kvstore

import (
	"strings"
	"regexp"
)

// Gene Ontology Entries
type G_ struct {
	*KVStore
}

func G_New(dbPath string) *G_ {
	var g G_
	g.KVStore = new(KVStore)
	NewKVStore(g.KVStore, dbPath+"/g_", 1000000)
	return &g
}

func (g *G_) CreateValues(key string, oldKey string) string {

	// aldehyde dehydrogenase [NAD(P)+] activity [GO:0004030]; putrescine catabolic process [GO:0009447]
	goArray := strings.Split(key, "; ")

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
		g.Add(goId, goName)
		g.Mu.Unlock()
	}

	var combinedKey = ""
	var combinedVal = ""

	if len(goIds) == 0 {
		combinedKey = "_nil"
	} else {
		if oldKey != "_nil" {
			g.Mu.Lock()
			oldVal, ok := g.GetValue(oldKey)
			if (ok) {
				// fmt.Println("Old Val exists : " + oldVal)
				goIds = append(goIds, strings.Split(oldVal, ",")...)
			}
		} else {
			g.Mu.Lock()
		}

		combinedKey, combinedVal = CreateHashValue(goIds)
		if oldKey != combinedKey {
			g.Add(combinedKey, combinedVal)
		}
		g.Mu.Unlock()
	}

	return combinedKey
}
