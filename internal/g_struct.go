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
	"log"
	"time"
)

// Gene Ontology Entries
type G_ struct {
	DB              *badger.DB
	WGgc            *sync.WaitGroup
	FlushSize       int
	NumberOfEntries int
	Entries         map[string]string
	Mu              sync.Mutex
}

func G_New(dbPath string) *G_ {

	var g G_
	g.NumberOfEntries = 0
	g.FlushSize = 1000000
	g.Entries = make(map[string]string, g.FlushSize)

	// Open All the DBStructs Badger databases
	opts := badger.DefaultOptions
	opts.Dir = dbPath+"/g_"
	opts.ValueDir = dbPath+"/g_"

	err := error(nil)
	g.DB, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	g.WGgc = new(sync.WaitGroup)
	g.WGgc.Add(1)
	go func() {
		// Garbage collection every 5 minutes
		var stopGC = false
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			for ! stopGC {
				err := g.DB.RunValueLogGC(0.5)
				if err != nil {
					stopGC = true
				}
			}
		}
	}()

	return &g
}

func (g *G_) Close() {
	g.WGgc.Done()
	g.Flush()
	g.DB.RunValueLogGC(0.1)
	g.DB.Close()
}

func (g *G_) Flush() {
	wb := g.DB.NewWriteBatch()
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

	g.Entries = make(map[string]string, g.FlushSize)
	g.NumberOfEntries = 0
}

func (g *G_) Add(key string, add_val string) {

	if _, ok := g.Entries[key]; ok {
		// fmt.Println("Key exist in struct adding to it")
	} else {
		// fmt.Println("New Key")
		g.Entries[key] = add_val
		g.NumberOfEntries++
	}

	if g.NumberOfEntries == g.FlushSize {
		g.Flush()
	}
}

func (g *G_) GetValue(key string) (string, bool) {

	if val, ok := g.Entries[key]; ok {
		return val, true
	}

	var valCopy []byte

	err := g.DB.View(func(txn *badger.Txn) error {
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

	// combined key prefix = "_"
	hashKey = "_" + hashKey[len(hashKey)-11:len(hashKey)]

	return hashKey, goIdsString

}
