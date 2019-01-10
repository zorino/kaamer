package kvstore

import (
	// "strings"
	"regexp"
	"fmt"
)

// Protein Function Entries (HAMAP or manually defined in uniprot/swissprot)
type F_ struct {
	*KVStore
}

func F_New(dbPath string) *F_ {
	var f F_
	f.KVStore = new(KVStore)
	NewKVStore(f.KVStore, dbPath+"/f_store", 1000000)
	return &f
}

func (f *F_) CreateValues(key string, oldKey string) string {

	// FUNCTION: Catalyzes the Claisen rearrangement of chorismate to prephenate. Probably involved in the aromatic amino acid biosynthesis. {ECO:0000269|PubMed:15737998, ECO:0000269|PubMed:18727669, ECO:0000269|PubMed:19556970}.

	reg := regexp.MustCompile(` \{.*\}\.`)


	protFunction := reg.ReplaceAllString(key, "${1}")
	protFunction =  protFunction[10:]

	fmt.Println("Protein function:" + protFunction+"##")

	// goArray := strings.Split(key, "; ")

	// // reg := regexp.MustCompile(` \[GO:.*\]`)

	// var goIds []string

	// for _, _go := range goArray {

	// 	goName := reg.ReplaceAllString(_go, "${1}")

	// 	goId := reg.FindString(_go)

	// 	if goId == "" {
	// 		continue
	// 	}

	// 	// real id prefix = "."
	// 	goId = "." + goId[5:len(goId)-1]

	// 	goIds = append(goIds, goId)

	// 	f.Mu.Lock()
	// 	f.Add(goId, goName)
	// 	f.Mu.Unlock()
	// }

	// var combinedKey = ""
	// var combinedVal = ""

	// if len(goIds) == 0 {
	// 	combinedKey = "_nil"
	// } else {
	// 	if oldKey != "_nil" {
	// 		f.Mu.Lock()
	// 		oldVal, ok := f.GetValue(oldKey)
	// 		if (ok) {
	// 			// fmt.Println("Old Val exists : " + oldVal)
	// 			goIds = append(goIds, strings.Split(oldVal, ",")...)
	// 		}
	// 	} else {
	// 		f.Mu.Lock()
	// 	}

	// 	combinedKey, combinedVal = CreateHashValue(goIds)
	// 	if oldKey != combinedKey {
	// 		f.Add(combinedKey, combinedVal)
	// 	}
	// 	f.Mu.Unlock()
	// }

	return ""
}
