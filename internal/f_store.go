package kvstore

import (
	"strings"
	"regexp"
	"github.com/dgraph-io/badger"
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

func (f *F_) CreateValues(entry string, oldKey string) (string, bool) {

	// FUNCTION: Catalyzes the Claisen rearrangement of chorismate to prephenate. Probably involved in the aromatic amino acid biosynthesis. {ECO:0000269|PubMed:15737998, ECO:0000269|PubMed:18727669, ECO:0000269|PubMed:19556970}.

	var new = false

	if entry == "" && oldKey == "" {
		return "_nil", true
	} else if (entry == "" && oldKey != "") {
		return "_nil", false
	}


	reg := regexp.MustCompile(` \{.*\}\.`)

	protFunction := reg.ReplaceAllString(entry, "${1}")

	protFunction =  protFunction[10:]

	// fmt.Println("Protein function:" + protFunction+"##")

	if oldKey == "" {
		new = true
	}

	ids := []string{protFunction}

	combinedKey, _ := CreateHashValue(ids, true)

	if combinedKey != oldKey {
		new = true
		f.Mu.Lock()
		if oldKey != "_nil" {
			oldVal, ok := f.GetValue(oldKey)
			if (ok) {
				// fmt.Println("Old Val exists : " + oldVal)
				ids = append(ids, strings.Split(oldVal, ",")...)
			}
		}
		combinedKey, _ := CreateHashValue(ids, true)
		f.AddValue(combinedKey, entry)
		f.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
