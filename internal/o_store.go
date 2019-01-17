package kvstore

import (
	"strings"
)

// Protein Pathways Entries (HAMAP or manually defined in uniprot/swissprot)
type O_ struct {
	*KVStore
}

func O_New(dbPath string) *O_ {
	var o O_
	o.KVStore = new(KVStore)
	NewKVStore(o.KVStore, dbPath+"/o_store", 1000000)
	return &o
}

func (o *O_) CreateValues(entry string, oldKey string) (string, bool) {

	// cellular organisms, Bacteria, Proteobacteria, Gammaproteobacteria, Enterobacterales, Enterobacteriaceae, Escherichia, Escherichia coli, Escherichia coli (strain K12)

	var new = false

	if entry == "" {
		return "_nil", true
	}

	protOrganism := entry[20:]

	// fmt.Println("Protein organism:"+protOrganism+"##")

	if oldKey == "" {
		new = true
	}

	ids := []string{protOrganism}

	combinedKey, _ := CreateHashValue(ids)

	if combinedKey != oldKey {
		new = true
		o.Mu.Lock()
		if oldKey != "_nil" {
			oldVal, ok := o.GetValue(oldKey)
			if (ok) {
				// fmt.Println("Old Val exists : " + oldVal)
				ids = append(ids, strings.Split(oldVal, ",")...)
			}
		}
		combinedKey, _ := CreateHashValue(ids)
		o.AddValue(combinedKey, entry)
		o.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
