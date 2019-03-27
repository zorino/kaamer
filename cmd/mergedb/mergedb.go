package mergedb

import (
	copy "github.com/zorino/metaprot/internal/helper"
	"github.com/zorino/metaprot/pkg/kvstore"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"context"
	"log"
	"path/filepath"
	"runtime"
	"sync"
	"bytes"
	"fmt"
)

type DBMerger struct {
	kvStores1 *kvstore.KVStores
	kvStores2 *kvstore.KVStores
	KVToMerge sync.Map
}

func NewMergedb(dbsPath string, outPath string) {

	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(128)

	pattern := dbsPath + "/*"
	allDBs, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err.Error())
	}

	copy.Dir(allDBs[0], outPath)
	allDBs = allDBs[1:]

	nbOfThreads := runtime.NumCPU()

	if nbOfThreads < 1 {
		nbOfThreads = 1
	}

	kvStores1 := kvstore.KVStoresNew(outPath, nbOfThreads)

	for _, db := range allDBs {

		if db != "" {

			fmt.Printf("# Merging database %s into %s...\n", db, outPath)

			kvStores2 := kvstore.KVStoresNew(db, nbOfThreads)

			wg := new(sync.WaitGroup)
			wg.Add(12)

			go MergeStores(kvStores1.K_batch.KVStore, kvStores2.K_batch.KVStore, nbOfThreads, wg)
			go MergeStores(kvStores1.KK_batch.KVStore, kvStores2.KK_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.G_batch.KVStore, kvStores2.G_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.GG_batch.KVStore, kvStores2.GG_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.F_batch.KVStore, kvStores2.F_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.FF_batch.KVStore, kvStores2.FF_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.P_batch.KVStore, kvStores2.P_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.PP_batch.KVStore, kvStores2.PP_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.O_batch.KVStore, kvStores2.O_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.OO_batch.KVStore, kvStores2.OO_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.N_batch.KVStore, kvStores2.N_batch.KVStore, 2, wg)
			go MergeStores(kvStores1.NN_batch.KVStore, kvStores2.NN_batch.KVStore, 2, wg)

			wg.Wait()

			kvStores2.Close()

		}

	}

	kvStores1.Flush()

	kvStores1.MergeKmerValues(nbOfThreads)

	kvStores1.Close()

}

func MergeStores(kvStore1 *kvstore.KVStore, kvStore2 *kvstore.KVStore, nbOfThreads int, wg *sync.WaitGroup) {

	defer wg.Done()
	// Stream keys
	stream := kvStore2.DB.NewStream()

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

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if item.DiscardEarlierVersions() {
				break
			}
			if ! bytes.Equal(key, item.Key()) {
				break
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				log.Fatal(err.Error())
			}

			key := item.KeyCopy(nil)

			kvStore1.AddValueWithLock(key, val)
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

}
