/*
Copyright 2019 The kaamer Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/golang/protobuf/proto"
	copy "github.com/zorino/kaamer/internal/helper/copy"
	"github.com/zorino/kaamer/pkg/kvstore"
)

type DBMerger struct {
	kvStores1 *kvstore.KVStores
	kvStores2 *kvstore.KVStores
	KVToMerge sync.Map
}

func NewMergedb(dbsPath string, outPath string, maxSize bool) {

	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(512)

	nbOfThreads := runtime.NumCPU()

	if nbOfThreads < 1 {
		nbOfThreads = 1
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

	kvStores1 := kvstore.KVStoresNew(outPath, nbOfThreads, maxSize, false, false)

	dbStats := &kvstore.KStats{}
	dbStatsByte, ok := kvStores1.ProteinStore.GetValue([]byte("db_stats"))
	if !ok {
		fmt.Println("Couldn't find db stats")
		os.Exit(1)
	}
	proto.Unmarshal(dbStatsByte, dbStats)

	// Merge all DB into the first DB
	for _, db := range allDBs {

		if db != "" {

			fmt.Printf("# Merging database %s into %s...\n", db, outPath)

			kvStores2 := kvstore.KVStoresNew(db, 1, maxSize, false, false)

			_dbStats := &kvstore.KStats{}
			_dbStatsByte, ok := kvStores2.ProteinStore.GetValue([]byte("db_stats"))
			if !ok {
				fmt.Println("Couldn't find db stats")
				os.Exit(1)
			}
			proto.Unmarshal(_dbStatsByte, _dbStats)
			dbStats.NumberOfProteins += _dbStats.NumberOfProteins
			dbStats.NumberOfAA += _dbStats.NumberOfAA
			dbStats.NumberOfKmers += _dbStats.NumberOfKmers

			wg := new(sync.WaitGroup)
			wg.Add(2)
			go MergeStores(kvStores1.KmerStore.KVStore, kvStores2.KmerStore.KVStore, nbOfThreads, wg)
			go MergeStores(kvStores1.ProteinStore.KVStore, kvStores2.ProteinStore.KVStore, 2, wg)
			wg.Wait()

			wg.Add(2)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				kvStores1.KmerStore.GarbageCollect(100, 0.5)
			}(wg)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				kvStores1.ProteinStore.GarbageCollect(100, 0.5)
			}(wg)
			wg.Wait()

			kvStores2.Close()

		}

	}

	data, err := proto.Marshal(dbStats)
	if err != nil {
		log.Fatal(err.Error())
	}
	kvStores1.ProteinStore.OpenInsertChannel()
	kvStores1.ProteinStore.AddValueToChannel([]byte("db_stats"), data, true)
	kvStores1.ProteinStore.CloseInsertChannel()
	kvStores1.ProteinStore.Flush()

	kvStores1.KmerStore.DB.Flatten(4)
	kvStores1.ProteinStore.DB.Flatten(4)

	// Final garbage collect before closing
	kvStores1.KmerStore.GarbageCollect(100, 0.5)
	kvStores1.ProteinStore.GarbageCollect(100, 0.5)
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
	kvStore1.DB.Sync()
	kvStore1.Flush()

}
