package kvstore

import (
	"bytes"
	"sort"
	"crypto/sha1"
	"github.com/dgraph-io/badger"
	"sync"
	"log"
)

type TxBatch struct {
	NbOfTx         int
	TxBufferSize   int
	Entries        *sync.Map
	Mu             sync.Mutex
}


// Key Value Store
type KVStore struct {
	DB                  *badger.DB
	TxBatches           []*TxBatch
	TxBatchWithDiscard  *TxBatch
	NilVal              []byte
	Mu                  sync.Mutex
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

	kv.TxBatchWithDiscard = &TxBatch{NbOfTx: 0, TxBufferSize: flushSize, Entries: new(sync.Map) }

}

func (kv *KVStore) Close() {
	kv.Flush()
	kv.DB.Close()
}

func (kv *KVStore) Flush() {

	for i, _ := range kv.TxBatches {
		kv.CreateBatch(i)
	}
	kv.CreateBatchWithDiscardVersions()
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

	// Check if key is already in batch buffer - insert the new one otherwise
	oldVal, hasKey := kv.TxBatches[threadId].Entries.LoadOrStore(string(key), newVal)
	if hasKey {
		// key is already listed, check the old value
		oV, okOld := oldVal.([]byte)
		if okOld && ! bytes.Equal(newVal, oV) {
			kv.CreateBatch(threadId)
			kv.TxBatches[threadId].Entries.LoadOrStore(string(key), newVal)
			kv.TxBatches[threadId].NbOfTx += 1
		}
	} else {
		kv.TxBatches[threadId].NbOfTx += 1
	}

}

func (kv *KVStore) CreateBatch(threadId int) error {

	if kv.TxBatches[threadId].NbOfTx == 0 {
		// OK, nothing to flush.
		return nil
	}

	WB := kv.DB.NewWriteBatch()

	kv.TxBatches[threadId].Entries.Range(func(k, v interface{}) bool {
		key, okKey := k.(string)
		value, okValue := v.([]byte)
		if okKey && okValue {
			WB.Set([]byte(key), value, 0) // Will create txns as needed.
		} else {
			log.Fatal("Couldn't insert key val")
		}
		return true
	})

	WB.Flush()
	WB.Cancel()

	kv.TxBatches[threadId].NbOfTx = 0
	kv.TxBatches[threadId].Entries = new(sync.Map)

	return nil

}


func (kv *KVStore) AddValueWithDiscardVersions(key []byte, newVal []byte){

	kv.TxBatchWithDiscard.Mu.Lock()

	bufferFull := (kv.TxBatchWithDiscard.NbOfTx == kv.TxBatchWithDiscard.TxBufferSize)

	if bufferFull {
		kv.CreateBatchWithDiscardVersions()
	}

	// Check if key is already in batch buffer - insert the new one otherwise
	oldVal, hasKey := kv.TxBatchWithDiscard.Entries.LoadOrStore(string(key), newVal)
	if hasKey {
		// key is already listed, check the old value
		oV, okOld := oldVal.([]byte)
		if okOld && ! bytes.Equal([]byte(oV), newVal) {
			kv.CreateBatchWithDiscardVersions()
			kv.TxBatchWithDiscard.Entries.LoadOrStore(string(key), newVal)
			kv.TxBatchWithDiscard.NbOfTx += 1
		}
	} else {
		kv.TxBatchWithDiscard.NbOfTx += 1
	}

	kv.TxBatchWithDiscard.Mu.Unlock()

}


func (kv *KVStore) CreateBatchWithDiscardVersions() error {

	if kv.TxBatchWithDiscard.NbOfTx == 0 {
		// OK, nothing to flush.
		return nil
	}

	txn := kv.DB.NewTransaction(true)

	kv.TxBatchWithDiscard.Entries.Range(func(k, v interface{}) bool {
		key, okKey := k.(string)
		value, okValue := v.([]byte)
		if okKey && okValue {

			if err := txn.SetWithDiscard([]byte(key), value, 0); err != nil {
				_ = txn.Commit()
				txn = kv.DB.NewTransaction(true)
				_ = txn.SetWithDiscard([]byte(key), value, 0)
			}

		}
		return true
	})
	_ = txn.Commit()


	kv.TxBatchWithDiscard.Entries = new(sync.Map)
	kv.TxBatchWithDiscard.NbOfTx = 0

	return nil

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
	} else {
		log.Fatal(err.Error())
	}

	return nil, err

}

func (kv *KVStore) MergeCombinationKeys(combKeys [][]byte, threadId int) ([]byte) {

	kv.Mu.Lock()
	ids := [][]byte{}

	findKey := false
	for _, combKey := range combKeys {
		if bytes.Equal(combKey, kv.NilVal) {
			continue
		}
		oldValues, err := kv.GetValueFromBadger(combKey)
		if err != nil {
			log.Fatal(err.Error())
		}
		if err == nil && oldValues != nil {
			findKey = true
			for i:=0; (i+1)<len(oldValues); i+=20 {
				ids = append(ids, oldValues[i:i+20])
			}
		}
	}

	if !findKey {
		return nil
	}

	if (len(ids) < 1) {
		return nil
	}

	newKey, newVal := CreateHashValue(ids, true)
	kv.AddValue(newKey, newVal, threadId)

	kv.Mu.Unlock()

	return newKey

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
