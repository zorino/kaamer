package mergedb

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	copy "github.com/zorino/metaprot/internal/helper/copy"
	"github.com/zorino/metaprot/pkg/kvstore"
)

type DBMerger struct {
	kvStores1 *kvstore.KVStores
	kvStores2 *kvstore.KVStores
	KVToMerge sync.Map
}

func NewMergedb(dbsPath string, outPath string, maxSize bool, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode, indexOnly bool) {

	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(512)

	nbOfThreads := runtime.NumCPU()

	if nbOfThreads < 1 {
		nbOfThreads = 1
	}

	if indexOnly {
		newKmerStore := CreateNewKmerStore(outPath, nbOfThreads)
		kvStores1 := kvstore.KVStoresNew(outPath, nbOfThreads, tableLoadingMode, valueLoadingMode, maxSize, true)
		IndexStore(kvStores1, newKmerStore, nbOfThreads)
		newKmerStore.GarbageCollect(10000000, 0.05)
		kvStores1.KmerStore.GarbageCollect(10000000, 0.05)
		kvStores1.KCombStore.GarbageCollect(10000000, 0.05)
		kvStores1.ProteinStore.GarbageCollect(10000000, 0.05)
		newKmerStore.Close()
		kvStores1.Close()
		return
	}

	pattern := dbsPath + "/*"
	allDBs, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Printf("# Syncing kv store 1 as the base store for the merge..\n")
	os.Mkdir(outPath, 0700)
	copy.Dir(allDBs[0], outPath)
	allDBs = allDBs[1:]

	kvStores1 := kvstore.KVStoresNew(outPath, nbOfThreads, tableLoadingMode, valueLoadingMode, maxSize, true)

	for _, db := range allDBs {

		if db != "" {

			fmt.Printf("# Merging database %s into %s...\n", db, outPath)

			kvStores2 := kvstore.KVStoresNew(db, nbOfThreads, tableLoadingMode, valueLoadingMode, maxSize, true)

			wg := new(sync.WaitGroup)
			wg.Add(2)
			go MergeStores(kvStores1.KmerStore.KVStore, kvStores2.KmerStore.KVStore, nbOfThreads, wg)
			go MergeStores(kvStores1.ProteinStore.KVStore, kvStores2.ProteinStore.KVStore, nbOfThreads, wg)
			wg.Wait()

			wg.Add(2)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				kvStores1.KmerStore.GarbageCollect(10000000, 0.05)
			}(wg)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				kvStores1.ProteinStore.GarbageCollect(10000000, 0.05)
			}(wg)
			wg.Wait()

			kvStores2.Close()

		}

	}

	// IndexStore(kvStores1, nbOfThreads)

	kvStores1.KmerStore.DB.Flatten(12)
	kvStores1.ProteinStore.DB.Flatten(12)

	// Final garbage collect before closing
	kvStores1.KmerStore.GarbageCollect(10000000, 0.05)
	kvStores1.ProteinStore.GarbageCollect(10000000, 0.05)
	kvStores1.Close()

}

func IndexStore(kvStores1 *kvstore.KVStores, newKmerStore *kvstore.KVStore, nbOfThreads int) {
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {

		again:
			err := kvStores1.KmerStore.DB.RunValueLogGC(0.1)
			if err == nil {
				goto again
			}

		again2:
			err = kvStores1.KCombStore.DB.RunValueLogGC(0.1)
			if err == nil {
				goto again2
			}
		}

	}()
	CreateCombinationValues(kvStores1.KmerStore.KVStore, kvStores1.KCombStore, newKmerStore, nbOfThreads)
}

func MergeStores(kvStore1 *kvstore.KVStore, kvStore2 *kvstore.KVStore, nbOfThreads int, wg *sync.WaitGroup) {

	defer wg.Done()
	// Stream keys
	stream := kvStore2.DB.NewStream()

	kvStore1.OpenInsertChannel()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = nbOfThreads            // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.

	// stream.KeyToList = nil
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		valCopy := []byte{}
		keyCopy := []byte{}

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if item.DiscardEarlierVersions() {
				break
			}
			if !bytes.Equal(key, item.Key()) {
				break
			}

			valCopy, err := item.ValueCopy(valCopy)
			if err != nil {
				log.Fatal(err.Error())
			}

			keyCopy = item.KeyCopy(keyCopy)

			kvStore1.AddValueToChannel(keyCopy, valCopy, false)

		}

		return nil, nil

	}

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = nil

	// // Run the stream
	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.
	kvStore1.CloseInsertChannel()
	kvStore1.Flush()

}

func CreateCombinationValues(kmerStore *kvstore.KVStore, kCombStore *kvstore.KC_, newKmerStore *kvstore.KVStore, nbOfThreads int) {

	fmt.Println("# Creating key combination store")
	// Stream keys
	stream := kmerStore.DB.NewStream()

	kCombStore.KVStore.OpenInsertChannel()
	newKmerStore.OpenInsertChannel()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = nbOfThreads            // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.

	// stream.KeyToList = nil
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		keys := [][]byte{}
		valCopy := []byte{}
		keyCopy := []byte{}
		// list := &pb.KVList{}

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if item.DiscardEarlierVersions() {
				break
			}
			if !bytes.Equal(key, item.Key()) {
				break
			}

			valCopy, err := item.ValueCopy(valCopy)
			if err != nil {
				log.Fatal(err.Error())
			}

			keys = append(keys, valCopy)

			keyCopy = item.KeyCopy(keyCopy)

		}

		combKey, combVal := kCombStore.CreateKCKeyValue(keys)
		kCombStore.AddValueToChannel(combKey, combVal, true)
		newKmerStore.AddValueToChannel(keyCopy, combKey, true)

		return nil, nil

		// list.Kv = append(list.Kv, &pb.KV{Key: keyCopy, Value: combKey})
		// return list, nil

	}

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = nil

	// stream.Send = func(list *pb.KVList) error {

	// 	var err error

	// 	// Delete keys
	// 	wb := kmerStore.DB.NewWriteBatch()
	// 	for _, kv := range list.Kv {
	// 		err = wb.Delete(kv.Key)
	// 		if err != nil {
	// 			fmt.Printf("# Write batch in combination key creation (delete) error : %s\n", err.Error())
	// 			wb.Flush()
	// 			wb.Cancel()
	// 			wb = kmerStore.DB.NewWriteBatch()
	// 		}
	// 		// kmerStore.UpdateValue(kv.Key, kv.Value)
	// 	}
	// 	wb.Flush()
	// 	wb.Cancel()

	// 	// Insert new key / value
	// 	wb = kmerStore.DB.NewWriteBatch()
	// 	for _, kv := range list.Kv {
	// 		err = wb.Set(kv.Key, kv.Value)
	// 		if err != nil {
	// 			fmt.Printf("# Write batch in combination key creation (create) error : %s\n", err.Error())
	// 			wb.Flush()
	// 			wb.Cancel()
	// 			wb = kmerStore.DB.NewWriteBatch()
	// 		}
	// 	}
	// 	wb.Flush()
	// 	wb.Cancel()

	// 	return nil
	// }

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.
	kCombStore.KVStore.CloseInsertChannel()
	kCombStore.KVStore.Flush()
	newKmerStore.CloseInsertChannel()
	newKmerStore.Flush()

}

func CreateNewKmerStore(dbPath string, nbOfThreads int) *kvstore.KVStore {

	// kmer_store options
	k_opts := badger.DefaultOptions
	k_opts.Dir = dbPath + "/kmer_store.new"
	k_opts.ValueDir = dbPath + "/kmer_store.new"
	k_opts.TableLoadingMode = options.MemoryMap
	k_opts.ValueLogLoadingMode = options.MemoryMap
	k_opts.SyncWrites = true
	k_opts.NumVersionsToKeep = 1
	k_opts.MaxTableSize = kvstore.MaxTableSize
	k_opts.ValueLogFileSize = kvstore.MaxValueLogFileSize
	k_opts.ValueLogMaxEntries = kvstore.MaxValueLogEntries
	k_opts.NumCompactors = 8

	newKmerStore := kvstore.K_New(k_opts, 1000, nbOfThreads)

	return newKmerStore.KVStore

}
