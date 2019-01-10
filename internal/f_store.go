package kvstore

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"sync"
	"strings"
	"regexp"
	"log"
	"time"
)

// Gene Ontology Entries
type F_ struct {
	DB              *badger.DB
	WGgc            *sync.WaitGroup
	FlushSize       int
	NumberOfEntries int
	Entries         map[string]string
	Mu              sync.Mutex
}

func F_New(dbPath string) *F_ {

	var f F_

	f.NumberOfEntries = 0
	f.FlushSize = 1000000
	f.Entries = make(map[string]string, f.FlushSize)

	// Open All the DBStructs Badger databases
	opts := badger.DefaultOptions
	opts.Dir = dbPath+"/g_"
	opts.ValueDir = dbPath+"/g_"

	err := error(nil)
	f.DB, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	f.WGgc = new(sync.WaitGroup)
	f.WGgc.Add(1)
	go func() {
		// Garbage collection every 5 minutes
		var stopGC = false
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			for ! stopGC {
				err := f.DB.RunValueLogGC(0.5)
				if err != nil {
					stopGC = true
				}
			}
		}
	}()

	return &f
}

func (f *F_) Close() {
	f.WGgc.Done()
	f.Flush()
	f.DB.RunValueLogGC(0.1)
	f.DB.Close()
}

func (f *F_) Flush() {
	wb := f.DB.NewWriteBatch()
	defer wb.Cancel()
	for k, v := range f.Entries {
		err := wb.Set([]byte(k), []byte(v), 0) // Will create txns as needed.
		if err != nil {
			fmt.Println("BUG: Error batch insert")
			fmt.Println(err)
		}
	}

	fmt.Println("BATCH INSERT")
	wb.Flush()

	f.Entries = make(map[string]string, f.FlushSize)
	f.NumberOfEntries = 0
}

func (f *F_) Add(key string, add_val string) {

	if _, ok := f.Entries[key]; ok {
		// fmt.Println("Key exist in struct adding to it")
	} else {
		// fmt.Println("New Key")
		f.Entries[key] = add_val
		f.NumberOfEntries++
	}

	if f.NumberOfEntries == f.FlushSize {
		f.Flush()
	}
}

func (f *F_) GetValue(key string) (string, bool) {

	if val, ok := f.Entries[key]; ok {
		return val, true
	}

	var valCopy []byte

	err := f.DB.View(func(txn *badger.Txn) error {
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

func (f *F_) CreateValues(key string, oldKey string) string {

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

		f.Mu.Lock()
		f.Add(goId, goName)
		f.Mu.Unlock()
	}

	var combinedKey = ""
	var combinedVal = ""

	if len(goIds) == 0 {
		combinedKey = "_nil"
	} else {
		if oldKey != "_nil" {
			f.Mu.Lock()
			oldVal, ok := f.GetValue(oldKey)
			if (ok) {
				// fmt.Println("Old Val exists : " + oldVal)
				goIds = append(goIds, strings.Split(oldVal, ",")...)
			}
		} else {
			f.Mu.Lock()
		}

		combinedKey, combinedVal = CreateHashValue(goIds)
		if oldKey != combinedKey {
			f.Add(combinedKey, combinedVal)
		}
		f.Mu.Unlock()
	}

	return combinedKey
}
