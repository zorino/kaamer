package db_struct

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"sync"
	"log"
	"time"
)

// Kmer Entries
type K_ struct {
	DB              *badger.DB
	WGgc            *sync.WaitGroup
	FlushSize       int
	NumberOfEntries int
	Entries         map[string]string
	Mu              sync.Mutex
}

func K_New(dbPath string) *K_ {

	var k K_
	k.NumberOfEntries = 0
	k.FlushSize = 1000000
	k.Entries = make(map[string]string, k.FlushSize)

	// Open All the DBStructs Badger databases
	opts := badger.DefaultOptions

	opts.Dir = dbPath+"/k_"
	opts.ValueDir = dbPath+"/k_"

	var err = error(nil)

	k.DB, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	k.WGgc = new(sync.WaitGroup)
	k.WGgc.Add(1)
	go func() {
		// Garbage collection every 5 minutes
		var stopGC = false
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			for ! stopGC {
				err := k.DB.RunValueLogGC(0.5)
				if err != nil {
					stopGC = true
				}
			}
		}
	}()

	return &k

}

func (k *K_) Close() {
	k.WGgc.Done()
	k.Flush()
	k.DB.RunValueLogGC(0.1)
	k.DB.Close()
}


func (k *K_) Flush() {
	wb := k.DB.NewWriteBatch()
	defer wb.Cancel()
	for k, v := range k.Entries {
		err := wb.Set([]byte(k), []byte(v), 0) // Will create txns as needed.
		if err != nil {
			fmt.Println("BUG: Error batch insert")
			fmt.Println(err)
		}
	}

	fmt.Println("BATCH INSERT")
	wb.Flush()
	k.Entries = make(map[string]string, k.FlushSize)
	k.NumberOfEntries = 0
}

func (k *K_) Add(key string, newVal string) {

	if _, ok := k.Entries[key]; ok {
		// fmt.Println("Key exist in struct adding to it")
		// k.Entries[key] = val + ";" + newVal
		k.Entries[key] = newVal
	} else {
		// fmt.Println("New Key")
		k.Entries[key] = newVal
		k.NumberOfEntries++
	}

	if k.NumberOfEntries == k.FlushSize {
		k.Flush()
	}
}
