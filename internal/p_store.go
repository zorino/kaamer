package kvstore

import (
	"strings"
	"regexp"
)

// Protein Pathways Entries (HAMAP or manually defined in uniprot/swissprot)
type P_ struct {
	*KVStore
}

func P_New(dbPath string) *P_ {
	var p P_
	p.KVStore = new(KVStore)
	NewKVStore(p.KVStore, dbPath+"/p_store", 1000)
	return &p
}

func (p *P_) CreateValues(entry string, oldKey string) (string, bool) {

	// PATHWAY: Metabolic intermediate biosynthesis; prephenate biosynthesis; prephenate from chorismate: step 1/1.
	// PATHWAY: Porphyrin-containing compound metabolism; heme O biosynthesis; heme O from protoheme: step 1/1. {ECO:0000255|HAMAP-Rule:MF_00154}.

	var new = false

	if entry == "" && oldKey == "" {
		return "_nil", true
	} else if (entry == "" && oldKey != "") {
		return "_nil", false
	}


	reg := regexp.MustCompile(` \{.*\}\.`)
	protPathway := reg.ReplaceAllString(entry, "${1}")

	regStep := regexp.MustCompile(`: step [0-9]/[0-9]`)
	protPathway = regStep.ReplaceAllString(protPathway, "${1}")

	protPathway =  protPathway[9:]

	// fmt.Println("Protein pathway:" + protPathway+"##")

	if oldKey == "" {
		new = true
	}

	ids := []string{protPathway}

	combinedKey, _ := CreateHashValue(ids, true)

	if combinedKey != oldKey {
		new = true
		p.Mu.Lock()
		if oldKey != "_nil" {
			oldVal, ok := p.GetValue(oldKey)
			if (ok) {
				// fmt.Println("Old Val exists : " + oldVal)
				ids = append(ids, strings.Split(oldVal, ",")...)
			}
		}
		combinedKey, _ := CreateHashValue(ids, true)
		p.AddValue(combinedKey, entry)
		p.Mu.Unlock()
	} else {
		new = false
	}

	return combinedKey, new
}
