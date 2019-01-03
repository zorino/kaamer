package db_struct

import (
	// "encoding/json"
	"fmt"
	"github.com/dgraph-io/badger"
	"sync"
	"strings"
	"regexp"
	"sort"
	"crypto/sha1"
	"encoding/hex"
)

// Gene Ontology Entries
type G_ struct {
	FlushSize       int
	NumberOfEntries int
	Entries         map[string]string
	Mu              sync.Mutex
}

func G_New() *G_ {
	var g G_
	g.NumberOfEntries = 0
	g.FlushSize = 100000
	g.Entries = make(map[string]string, g.FlushSize)
	return &g
}

func (g *G_) Flush(db *badger.DB) {
	wb := db.NewWriteBatch()
	defer wb.Cancel()
	for k, v := range g.Entries {
		err := wb.Set([]byte(k), []byte(v), 0) // Will create txns as needed.
		if err != nil {
			fmt.Println("BUG: Error batch insert")
			fmt.Println(err)
		}
	}

	fmt.Println("BATCH INSERT")
	wb.Flush()
	db.RunValueLogGC(0.7)

	g.Entries = make(map[string]string, g.FlushSize)
	g.NumberOfEntries = 0
}

func (g *G_) Add(key string, add_val string, db *badger.DB) {

	if _, ok := g.Entries[key]; ok {
		// fmt.Println("Key exist in struct adding to it")
	} else {
		// fmt.Println("New Key")
		g.Entries[key] = add_val
		g.NumberOfEntries++
	}

	if g.NumberOfEntries == g.FlushSize {
		g.Flush(db)
	}
}

func (g *G_) GetValue(key string, db *badger.DB) (string, bool) {

	if val, ok := g.Entries[key]; ok {
		return val, true
	}

	var valCopy []byte

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == nil {
			item.Value(func(val []byte) error {
				// Accessing val here is valid.
				// fmt.Printf("The answer is: %s\n", val)
				valCopy = append([]byte{}, val...)
				return nil
			})
		}
		return err
	})

	if err == nil {
		return string(valCopy), true
	}

	return "", false

}

func (g *G_) CreateValues(key string, oldKey string, db *badger.DB) string {

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
		// fmt.Println(goId)

		goId = "g_"+goId[5:len(goId)-2]

		goIds = append(goIds, goId)

		g.Mu.Lock()
		g.Add(goId, goName, db)
		g.Mu.Unlock()

		// fmt.Println(goId +"  "+goName)

	}

	var combinedKey = ""
	var combinedVal = ""
	if len(goIds) == 1 && goIds[0] == "" {
		combinedKey = "gg_nil"
	} else {
		if oldKey != "gg_nil" {
			g.Mu.Lock()
			oldVal, ok := g.GetValue(oldKey, db)
			if (ok) {
				fmt.Println("Old Val exists : " + oldVal)
				goIds = append(goIds, strings.Split(oldVal, ",")...)
			}
		} else {
			g.Mu.Lock()
		}

		combinedKey, combinedVal = CreateHashValue(goIds)
		g.Add(combinedKey, combinedVal, db)
		g.Mu.Unlock()
	}

	return combinedKey
}

func RemoveDuplicatesFromSlice(s []string) []string {

	m := make(map[string]bool)
	for _, item := range s {
		if _, ok := m[item]; ok {
			// duplicate item
			// fmt.Println(item, "is a duplicate")
		} else {
			m[item] = true
		}
	}

	var result []string
	for item, _ := range m {
		result = append(result, item)
	}

	return result

}

func CreateHashValue(goIds []string) (string,string) {

	goIds = RemoveDuplicatesFromSlice(goIds)
	sort.Strings(goIds)

	var goIdsString = strings.Join(goIds, ",")

	h := sha1.New()
	h.Write([]byte(goIdsString))
	bs := h.Sum(nil)
	hashKey := hex.EncodeToString(bs)
	hashKey = "gg_" + hashKey[len(hashKey)-11:len(hashKey)]

	return hashKey, goIdsString

}
