package kvstore

import (
	"bytes"
	"crypto/sha1"
	"errors"
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
	Entries      map[string]KV
	Mu           sync.Mutex
}

// Key Value Store
type KVStore struct {
	DB *badger.DB

	TxBatchChannel     []TxBatch
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
	kv.TxBatchChannel = make([]TxBatch, kv.NbOfThreads)
	kv.TxBatchChannelWG = new(sync.WaitGroup)
	kv.TxBatchChannelJobs = make(chan KV)

	for i := 0; i < kv.NbOfThreads; i++ {
		kv.TxBatchChannelWG.Add(1)
		go kv.AddValueChanWorker()
	}

}

func (kv *KVStore) CloseInsertChannel() {
	close(kv.TxBatchChannelJobs)
	kv.TxBatchChannelWG.Wait()
}

func (kv *KVStore) AddValueToChannel(key []byte, newVal []byte, unique bool) {
	kv.TxBatchChannelJobs <- KV{Key: key, Val: newVal, Unique: unique}
}

func (kv *KVStore) AddValueChanWorker() {

	nbOfTxs := 0
	keySeen := make(map[string]bool)
	wb := kv.DB.NewWriteBatch()

	for i := range kv.TxBatchChannelJobs {

		bufferFull := (nbOfTxs == kv.FlushSize)
		if _, ok := keySeen[string(i.Key)]; ok || bufferFull {
			wb.Flush()
			wb = kv.DB.NewWriteBatch()
			keySeen = make(map[string]bool)
			nbOfTxs = 0
		}
		nbOfTxs++
		keySeen[string(i.Key)] = true
		err := wb.Set(i.Key, i.Val) // Will create txns as needed.
		if err != nil {
			log.Fatal(err.Error())
		}

	}

	wb.Flush()
	kv.TxBatchChannelWG.Done()

}

func (kv *KVStore) Close() {
	kv.Flush()
	kv.DB.Close()
}

func (kv *KVStore) Flush() {
	// kv.DB.Flatten(kv.NbOfThreads)
	kv.GarbageCollect(1000000, 0.1)
}

func (kv *KVStore) GarbageCollect(count int, ratio float64) {

	fmt.Println("# Garbage collect...")
	numberOfGC := count
	for i := 0; i < count; i++ {
		numberOfGC = i + 1
		err := kv.DB.RunValueLogGC(ratio)
		if err != nil {
			fmt.Printf("DEBUG ValueLog GC failed with : %s \n", err.Error())
			// stop iteration since we hit a GC error
			i = count
			numberOfGC--
		}
	}
	fmt.Printf("# Garbage collected %d times\n", numberOfGC)

}

func (kv *KVStore) GetValue(key []byte) ([]byte, bool) {

	val, err := kv.GetValueFromBadger(key)
	if err == nil && val != nil {
		return val, true
	}

	return nil, false

}

func (kv *KVStore) UpdateValue(key []byte, val []byte) {

	txn := kv.DB.NewTransaction(true)
	txn.Delete(key)
	txn.Commit()
	txn = kv.DB.NewTransaction(true)
	txn.Set(key, val)
	txn.Commit()

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
	iteratorOptions.PrefetchSize = 100
	iteratorOptions.AllVersions = true

	err := kv.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(iteratorOptions)
		defer it.Close()
		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
			item := it.Item()
			var valCopy []byte
			err2 := item.Value(func(val []byte) error {
				valCopy = append([]byte{}, val...)
				return nil
			})
			if err2 != nil {
				log.Fatal(err2.Error())
				return err2
			}
			// val, _ := item.ValueCopy(nil)
			values = append(values, valCopy)
		}
		// fmt.Printf("%x - %d iteration\n", key, iterator)
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

// func CreateProteinHashId(entryId string) []byte {
//	h := fnv.New64a()
//	h.Write([]byte(s))
//	hashInt := h.Sum64()
//	if hashInt > 9223372036854775807 {
//		hashInt = (hashInt - 9223372036854775807)
//	}

//	bs := make([]byte, 8)
//	binary.BigEndian.PutUint64(bs, hashInt)

//	return bs
// }
