package kvstore

import (
	"bytes"
	"sort"
	"crypto/sha1"
	// "fmt"
	"github.com/dgraph-io/badger"
	// "sort"
	"sync"
	"log"
)

type TxBatch struct {
	NbOfTx         int
	TxBufferSize   int
	Entries        *sync.Map
}


// Key Value Store
type KVStore struct {
	DB              *badger.DB
	TxBatches       []*TxBatch
	NilVal          []byte
	Mu              sync.Mutex
}


func NewKVStore(kv *KVStore, options badger.Options, flushSize int, nbOfThreads int) {

	kv.NilVal = []byte{'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'}

	err := error(nil)
	kv.DB, err = badger.Open(options)
	if err != nil {
		log.Fatal(err)
	}

	kv.TxBatches = make([]*TxBatch, nbOfThreads)

	for i :=0; i<nbOfThreads; i++ {
		kv.TxBatches[i] = &TxBatch{NbOfTx: 0, TxBufferSize: flushSize, Entries: new(sync.Map) }
	}


}

func (kv *KVStore) Close() {
	kv.Flush()
	kv.DB.Close()
}

func (kv *KVStore) Flush() {
	for i, _ := range kv.TxBatches {
		kv.CreateBatch(i)
	}
}

func (kv *KVStore) HasKey(key []byte) (bool, []byte) {

	for _, batch := range kv.TxBatches {
		if val, ok := batch.Entries.Load(string(key)); ok {
			if _val, okType := val.([]byte); okType {
				return ok, []byte(_val)
			}
		}
	}

	return false, []byte{}

}

func (kv *KVStore) AddValue(key []byte, newVal []byte, threadId int) {

	bufferFull := (kv.TxBatches[threadId].NbOfTx == kv.TxBatches[threadId].TxBufferSize)

	if bufferFull {
		kv.CreateBatch(threadId)
	}

	oldVal, hasKey := kv.TxBatches[threadId].Entries.LoadOrStore(string(key), newVal)
	if hasKey {
		oV, okOld := oldVal.([]byte)
		if okOld && ! bytes.Equal([]byte(oV), newVal) {
			kv.CreateBatch(threadId)
			kv.TxBatches[threadId].Entries.LoadOrStore(string(key), newVal)
		}
	}

	kv.TxBatches[threadId].NbOfTx += 1

}

func (kv *KVStore) CreateBatch(threadId int) {

	if kv.TxBatches[threadId].NbOfTx == 0 {
		return
	}

	WB := kv.DB.NewWriteBatch()

	kv.TxBatches[threadId].Entries.Range(func(k, v interface{}) bool {
		key, okKey := k.(string)
		value, okValue := v.([]byte)
		if okKey && okValue {
			WB.Set([]byte(key), value, 0) // Will create txns as needed.
		}
		return true
	})

	WB.Flush()

	kv.TxBatches[threadId].NbOfTx = 0
	kv.TxBatches[threadId].Entries = new(sync.Map)

}


func (kv *KVStore) GetValue(key []byte) ([]byte, bool) {

	if hasKey, val := kv.HasKey(key); hasKey {
		return val, true
	}

	val, err := kv.GetValueFromBadger(key)
	if err == nil && val != nil {
		return val, true
	}

	return nil, false

}

func (kv *KVStore) GetValueFromBadger(key []byte) ([]byte, error) {

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
		return valCopy, err
	}

	return nil, err

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
