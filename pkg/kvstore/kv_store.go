package kvstore

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/dgraph-io/badger"
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

	TxBatches          []*TxBatch

	TxBatchChannel     []*TxBatch
	TxBatchChannelWG   *sync.WaitGroup
	TxBatchChannelJobs chan KV

	NbOfThreads int
	FlushSize   int
	NilVal      []byte
	Mu          sync.Mutex
}

func NewKVStore(kv *KVStore, options badger.Options, flushSize int, nbOfThreads int) {

	kv.NilVal = []byte{'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'}

	err := error(nil)
	kv.DB, err = badger.Open(options)
	if err != nil {
		log.Fatal(err)
	}

	kv.TxBatches = make([]*TxBatch, nbOfThreads)

	for i := 0; i < nbOfThreads; i++ {
		kv.TxBatches[i] = &TxBatch{NbOfTx: 0, TxBufferSize: flushSize, Entries: new(sync.Map)}
	}

	kv.NbOfThreads = nbOfThreads
	kv.FlushSize = flushSize

}

// OpenInsertChannell
// Open a channel to concurrently do batch insert in the store
func (kv *KVStore) OpenInsertChannel() {

	// Tx batch channel
	kv.TxBatchChannel = make([]*TxBatch, kv.NbOfThreads)
	kv.TxBatchChannelWG = new(sync.WaitGroup)
	kv.TxBatchChannelJobs = make(chan KV)

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
			oV, okOld := oldVal.([]byte)
			if okOld && !bytes.Equal([]byte(oV), i.Val) {
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
					_ = txn.Commit()
					txn = kv.DB.NewTransaction(true)
					_ = txn.SetWithDiscard([]byte(key), item.Val, 0)
				}
			} else {
				if err := txn.Set([]byte(key), item.Val); err != nil {
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

}

func (kv *KVStore) Close() {
	kv.Flush()
	kv.DB.Close()
}

func (kv *KVStore) Flush() {

	for i, _ := range kv.TxBatches {
		kv.FlushTxBatch(i)
	}

	for _, c := range kv.TxBatchChannel {
		kv.FlushBatchChan(c)
	}

	kv.GarbageCollect(1000, 0.1)

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
		kv.FlushTxBatch(threadId)
	}

	// Check if key is already in batch buffer - insert the new one otherwise
	oldVal, hasKey := kv.TxBatches[threadId].Entries.LoadOrStore(string(key), newVal)
	if hasKey {
		// key is already listed, check the old value
		oV, okOld := oldVal.([]byte)
		if okOld && !bytes.Equal(newVal, oV) {
			kv.FlushTxBatch(threadId)
			kv.TxBatches[threadId].Entries.LoadOrStore(string(key), newVal)
			kv.TxBatches[threadId].NbOfTx += 1
		}
	} else {
		kv.TxBatches[threadId].NbOfTx += 1
	}

}

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

func (kv *KVStore) FlushTxBatch(threadId int) error {

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
