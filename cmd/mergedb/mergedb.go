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

func NewMergedb(dbsPath string, outPath string, maxSize bool, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode) {

	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(512)

	pattern := dbsPath + "/*"
	allDBs, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Printf("# Syncing kv store 1 as the base store for the merge..\n")
	os.Mkdir(outPath, 0700)
	copy.Dir(allDBs[0], outPath)
	allDBs = allDBs[1:]

	nbOfThreads := runtime.NumCPU()

	if nbOfThreads < 1 {
		nbOfThreads = 1
	}

	kvStores1 := kvstore.KVStoresNew(outPath, nbOfThreads, tableLoadingMode, valueLoadingMode, maxSize)

	for _, db := range allDBs {

		if db != "" {

			fmt.Printf("# Merging database %s into %s...\n", db, outPath)

			kvStores2 := kvstore.KVStoresNew(db, nbOfThreads, tableLoadingMode, valueLoadingMode, maxSize)

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

	go func() {
		ticker := time.NewTicker(20 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
		again:
			err := kvStores1.KmerStore.DB.RunValueLogGC(0.3)
			if err == nil {
				goto again
			}
		}
		CreateCombinationValues(kvStores1.KmerStore.KVStore, kvStores1.KCombStore, nbOfThreads)
	}()

	// Final garbage collect before closing
	kvStores1.KmerStore.GarbageCollect(10000000, 0.05)
	kvStores1.KCombStore.GarbageCollect(10000000, 0.05)
	kvStores1.ProteinStore.GarbageCollect(10000000, 0.05)
	kvStores1.Close()

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

func CreateCombinationValues(kmerStore *kvstore.KVStore, kCombStore *kvstore.KC_, nbOfThreads int) {

	fmt.Println("# Creating key combination store")
	// Stream keys
	stream := kmerStore.DB.NewStream()

	kCombStore.KVStore.OpenInsertChannel()

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
		list := &pb.KVList{}

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
		list.Kv = append(list.Kv, &pb.KV{Key: keyCopy, Value: combKey})

		return list, nil

	}

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.
	// stream.Send = nil
	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.Kv {
			kmerStore.UpdateValue(kv.Key, kv.Value)
		}
		return nil
	}

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.
	kCombStore.KVStore.CloseInsertChannel()
	kCombStore.KVStore.Flush()

}
