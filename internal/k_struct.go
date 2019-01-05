package db_struct

import (
	// "encoding/json"
	"fmt"
	"github.com/dgraph-io/badger"
	"sync"
)

// Kmer Entries
type K_ struct {
	FlushSize       int
	NumberOfEntries int
	Entries         map[string]string
	Mu              sync.Mutex
}

func K_New() *K_ {
	var k K_
	k.NumberOfEntries = 0
	k.FlushSize = 1000000
	k.Entries = make(map[string]string, k.FlushSize)
	return &k
}

func (k *K_) Flush(db *badger.DB) {
	wb := db.NewWriteBatch()
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

func (k *K_) Add(key string, newVal string, db *badger.DB) {

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
		k.Flush(db)
	}
}
