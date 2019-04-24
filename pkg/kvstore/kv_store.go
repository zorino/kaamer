package kvstore

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger"
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	G_STORE = 0
	F_STORE = 1
	P_STORE = 2
	O_STORE = 3
	N_STORE = 4
)

type KV struct {
	Key    []byte
	Val    []byte
	Unique bool
}

type TxBatch struct {
	NbOfTx       int
	TxBufferSize int
	Entries      *sync.Map
	Mu           sync.Mutex
}

// Key Value Store
type KVStore struct {
	DB *badger.DB

	TxBatchChannel     []*TxBatch
	TxBatchChannelWG   *sync.WaitGroup
	TxBatchChannelJobs chan KV

	BatchCounter uint64
	NbOfThreads  int
	FlushSize    int
	NilVal       []byte
	Mu           sync.Mutex
}

func NewKVStore(kv *KVStore, options badger.Options, flushSize int, nbOfThreads int) {

	kv.NilVal = []byte{'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'}

	err := error(nil)
	kv.DB, err = badger.Open(options)
	if err != nil {
		log.Fatal(err)
	}

	kv.BatchCounter = 0
	kv.NbOfThreads = nbOfThreads
	kv.FlushSize = flushSize

}

// OpenInsertChannell
// Open a channel to concurrently do batch insert in the store
func (kv *KVStore) OpenInsertChannel() {

	// Tx batch channel
	kv.TxBatchChannel = make([]*TxBatch, kv.NbOfThreads)
	kv.TxBatchChannelWG = new(sync.WaitGroup)
	kv.TxBatchChannelJobs = make(chan KV, 10)

	for i := 0; i < kv.NbOfThreads; i++ {
		kv.TxBatchChannel[i] = &TxBatch{NbOfTx: 0, TxBufferSize: kv.FlushSize, Entries: new(sync.Map)}
		kv.TxBatchChannelWG.Add(1)
		go kv.AddValueChanWorker(kv.TxBatchChannel[i])
	}

}

func (kv *KVStore) CloseInsertChannel() {
	close(kv.TxBatchChannelJobs)
	kv.TxBatchChannelWG.Wait()
}

func (kv *KVStore) AddValueToChannel(key []byte, newVal []byte, unique bool) {
	newKV := KV{Key: key, Val: newVal, Unique: unique}
	kv.TxBatchChannelJobs <- newKV
}

func (kv *KVStore) AddValueChanWorker(batch *TxBatch) {

	defer kv.TxBatchChannelWG.Done()

	for i := range kv.TxBatchChannelJobs {

		// fmt.Printf("Received KV from chan %x %x\n", i.Key, i.Val)

		bufferFull := (batch.NbOfTx == batch.TxBufferSize)

		if bufferFull {
			kv.FlushBatchChan(batch)
		}

		// Check if key is already in batch buffer - insert the new one otherwise
		oldVal, hasKey := batch.Entries.LoadOrStore(string(i.Key), i)
		if hasKey {
			// key is already listed, check the old value
			oV, okOld := oldVal.(KV)
			if okOld && !bytes.Equal([]byte(oV.Val), i.Val) {
				kv.FlushBatchChan(batch)
				batch.Entries.LoadOrStore(string(i.Key), i)
				batch.NbOfTx += 1
			}
		} else {
			batch.NbOfTx += 1
		}

	}

}

func (kv *KVStore) FlushBatchChan(batch *TxBatch) {

	if batch.NbOfTx == 0 {
		// OK, nothing to flush.
		return
	}

	// fmt.Printf("Flushing Batch Chan with %d Txs\n", batch.NbOfTx)

	txn := kv.DB.NewTransaction(true)
	batch.Entries.Range(func(k, v interface{}) bool {
		key, okKey := k.(string)
		item, okValue := v.(KV)
		if okKey && okValue {

			if item.Unique {
				if err := txn.SetWithDiscard([]byte(key), item.Val, 0); err != nil {
					fmt.Println(err)
					_ = txn.Commit()
					txn = kv.DB.NewTransaction(true)
					_ = txn.SetWithDiscard([]byte(key), item.Val, 0)
				}
			} else {
				if err := txn.Set([]byte(key), item.Val); err != nil {
					fmt.Println(err)
					_ = txn.Commit()
					txn = kv.DB.NewTransaction(true)
					_ = txn.Set([]byte(key), item.Val)
				}
			}

		}
		return true
	})
	_ = txn.Commit()

	batch.Entries = new(sync.Map)
	batch.NbOfTx = 0

	// Do Value GC pseudo every 500 flushes
	atomic.AddUint64(&kv.BatchCounter, 1)
	if atomic.LoadUint64(&kv.BatchCounter) >= 499 {
		atomic.StoreUint64(&kv.BatchCounter, 0)
		kv.GarbageCollect(1, 0.5)
	}

}

func (kv *KVStore) Close() {
	kv.Flush()
	// kv.DB.Flatten(kv.NbOfThreads)
	// kv.GarbageCollect(10000, 0.1)
	kv.DB.Close()
}

func (kv *KVStore) Flush() {

	for _, c := range kv.TxBatchChannel {
		kv.FlushBatchChan(c)
	}

	kv.GarbageCollect(1000, 0.1)

}

// func (kv *KVStore) HasKey(key []byte) (bool, []byte) {

//	for _, batch := range kv.TxBatches {
//		if val, ok := batch.Entries.Load(string(key)); ok {
//			if _val, okType := val.([]byte); okType {
//				return ok, []byte(_val)
//			}
//		}
//	}

//	return false, []byte{}

// }

func (kv *KVStore) GarbageCollect(count int, ratio float64) {

	fmt.Println("# Garbage collect...")
	for x := 0; x < 10; x++ {
		for i := 0; i < count; i++ {
			err := kv.DB.RunValueLogGC(ratio)
			if err != nil {
				// stop iteration since we hit a GC error
				i = count
			}
		}
	}

}

func (kv *KVStore) GetValue(key []byte) ([]byte, bool) {

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

func (kv *KVStore) GetValues(key []byte) ([][]byte, error) {

	var values [][]byte

	var iteratorOptions badger.IteratorOptions
	iteratorOptions.PrefetchValues = true
	iteratorOptions.PrefetchSize = 20
	iteratorOptions.AllVersions = true

	err := kv.DB.View(func(txn *badger.Txn) error {

		it := txn.NewIterator(iteratorOptions)
		defer it.Close()
		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
			item := it.Item()
			val, _ := item.ValueCopy(nil)
			values = append(values, val)
			// fmt.Printf("key=%s, value=%s\n", key, val)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return RemoveDuplicatesFromSlice(values), err

}

func (kv *KVStore) MergeCombinationKeys(combKeys [][]byte, threadId int) []byte {

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
			for i := 0; (i + 1) < len(oldValues); i += 20 {
				ids = append(ids, oldValues[i:i+20])
			}
		}
	}

	if !findKey {
		return nil
	}

	if len(ids) < 1 {
		return nil
	}

	newKey, newVal := CreateHashValue(ids, true)
	kv.AddValueToChannel(newKey, newVal, false)

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

func SplitHashValue(hashValue []byte) ([][]byte, error) {

	if len(hashValue)%20 != 0 {
		return nil, errors.New("Wrong hash size")
	}

	values := [][]byte{}
	for i := 0; i < len(hashValue)/20; i++ {
		values = append(values, hashValue[(i)*20:(i+1)*20])
	}

	return values, nil

}
