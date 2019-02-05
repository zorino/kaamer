package kvstore

import (
	"bytes"
	"sort"
	"crypto/sha1"
	"fmt"
	"github.com/dgraph-io/badger"
	// "sort"
	"sync"
	"log"
	"time"
	// "encoding/hex"
)

// Key Value Store
type KVStore struct {
	DB              *badger.DB
	WGgc            *sync.WaitGroup
	FlushSize       int
	NumberOfEntries int
	Entries         map[string][]byte
	NilVal          []byte
	Mu              sync.Mutex
}


func NewKVStore(kv *KVStore, options badger.Options, flushSize int) {

	kv.NumberOfEntries = 0
	kv.FlushSize = flushSize

	kv.Entries = make(map[string][]byte)
	kv.NilVal = []byte{'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'}

	err := error(nil)
	kv.DB, err = badger.Open(options)
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
		errTx := wb.Set([]byte(k), v, 0) // Will create txns as needed.
		if errTx != nil {
			fmt.Println(errTx.Error())
			log.Fatal("BUG: Error batch insert")
		}
	}

	fmt.Println("BATCH INSERT")
	wb.Flush()

	kv.Entries = make(map[string][]byte)
	kv.NumberOfEntries = 0

}

func (kv *KVStore) HasKey(key []byte) (bool, []byte) {

	if val, ok := kv.Entries[string(key)]; ok {
		return ok, val
	}

	return false, []byte{}

}

func (kv *KVStore) AddValue(key []byte, newVal []byte) {

	if hasKey, _ := kv.HasKey(key); hasKey {
		kv.Entries[string(key)] = newVal
	} else {
		kv.NumberOfEntries++
		kv.Entries[string(key)] = newVal
	}

	if len(kv.Entries) == kv.FlushSize {
		kv.Flush()
	}

}

func (kv *KVStore) GetValue(key []byte) ([]byte, bool) {

	if hasKey, val := kv.HasKey(key); hasKey {
		return val, true
	}

	var valCopy []byte

	err := kv.DB.View(func(txn *badger.Txn) error {
		item, errTx := txn.Get(key)
		if errTx == nil {
			item.Value(func(val []byte) error {
				// Copying new value
				valCopy = append([]byte{}, val...)
				return nil
			})
		}
		return errTx
	})

	if err == nil {
		return valCopy, true
	}

	return nil, false
}


// Utility functions
func RemoveDuplicatesFromSlice(s [][]byte) [][]byte {

	var sortedBytesArray [][]byte

	for _, e := range s {

		i := sort.Search(len(sortedBytesArray), func(i int) bool {
			return bytes.Compare(e, sortedBytesArray[i]) >= 0
		})

		if i < len(sortedBytesArray) && bytes.Equal(e, sortedBytesArray[i]) {
			// element in array already
		} else {
			sortedBytesArray = append(sortedBytesArray, []byte{})
			copy(sortedBytesArray[i+1:], sortedBytesArray[i:])
			sortedBytesArray[i] = e
		}
	}

	return sortedBytesArray

}

func CreateHashValue(ids [][]byte, unique bool) ([]byte, []byte) {

	if unique {
		ids = RemoveDuplicatesFromSlice(ids)
	}

	joinedIds := bytes.Join(ids, nil)

	h := sha1.New()
	h.Write(joinedIds)
	bs := h.Sum(nil)

	return bs, joinedIds

}
