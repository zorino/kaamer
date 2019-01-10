package kvstore

import (
	"sort"
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"fmt"
	"github.com/dgraph-io/badger"
	"sync"
	"log"
	"time"
)


// Gene Ontology Entries
type KVStore struct {
	DB              *badger.DB
	WGgc            *sync.WaitGroup
	FlushSize       int
	NumberOfEntries int
	Entries         map[string]string
	Mu              sync.Mutex
}

func NewKVStore(kv *KVStore, dbPath string, flushSize int) {

	kv.NumberOfEntries = 0
	kv.FlushSize = flushSize
	kv.Entries = make(map[string]string, kv.FlushSize)

	// Open All the DBStructs Badger databases
	opts := badger.DefaultOptions
	opts.Dir = dbPath
	opts.ValueDir = dbPath

	err := error(nil)
	kv.DB, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	kv.WGgc = new(sync.WaitGroup)
	kv.WGgc.Add(1)
	go func() {
		// Garbage collection every 5 minutes
		var stopGC = false
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			for ! stopGC {
				err := kv.DB.RunValueLogGC(0.5)
				if err != nil {
					stopGC = true
				}
			}
		}
	}()

}

func (kv *KVStore) Close() {
	kv.WGgc.Done()
	kv.Flush()
	kv.DB.RunValueLogGC(0.1)
	kv.DB.Close()
}

func (kv *KVStore) Flush() {
	wb := kv.DB.NewWriteBatch()
	defer wb.Cancel()
	for k, v := range kv.Entries {
		err := wb.Set([]byte(k), []byte(v), 0) // Will create txns as needed.
		if err != nil {
			fmt.Println("BUG: Error batch insert")
			fmt.Println(err)
		}
	}

	fmt.Println("BATCH INSERT")
	wb.Flush()
	kv.Entries = make(map[string]string, kv.FlushSize)
	kv.NumberOfEntries = 0
}

func (kv *KVStore) HasValue(key string) bool {
	_, hasValue := kv.Entries[key]
	return hasValue
}

func (kv *KVStore) AddValue(key string, newVal string) {

	if kv.HasValue(key) {
		// Key exist in struct adding new value to it
		kv.Entries[key] = newVal
	} else {
		// New Key into cache
		kv.Entries[key] = newVal
		kv.NumberOfEntries++
	}

	if kv.NumberOfEntries == kv.FlushSize {
		kv.Flush()
	}
}

func (kv *KVStore) GetValue(key string) (string, bool) {

	if val, ok := kv.Entries[key]; ok {
		return val, true
	}

	var valCopy []byte

	err := kv.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == nil {
			item.Value(func(val []byte) error {
				// Copying new value
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


// Utility functions
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

func CreateHashValue(ids []string) (string,string) {

	ids = RemoveDuplicatesFromSlice(ids)
	sort.Strings(ids)

	var idsString = strings.Join(ids, ",")

	h := sha1.New()
	h.Write([]byte(idsString))
	bs := h.Sum(nil)
	hashKey := hex.EncodeToString(bs)

	// combined key prefix = "_"
	hashKey = "_" + hashKey[len(hashKey)-11:len(hashKey)]

	return hashKey, idsString

}
