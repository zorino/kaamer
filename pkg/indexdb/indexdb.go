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

package indexdb

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	"github.com/zorino/kaamer/pkg/kvstore"
)

func NewIndexDB(dbPath string, maxSize bool, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode) {

	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(512)

	nbOfThreads := runtime.NumCPU()

	if nbOfThreads < 1 {
		nbOfThreads = 1
	}

	newKmerStore := CreateNewKmerStore(dbPath, nbOfThreads)
	kvStores1 := kvstore.KVStoresNew(dbPath, nbOfThreads, tableLoadingMode, valueLoadingMode, maxSize, true, false)
	IndexStore(kvStores1, newKmerStore, nbOfThreads)
	newKmerStore.GarbageCollect(10000, 0.5)
	kvStores1.KmerStore.GarbageCollect(10000, 0.5)
	kvStores1.KCombStore.GarbageCollect(10000, 0.5)
	kvStores1.ProteinStore.GarbageCollect(10000, 0.5)
	newKmerStore.Close()
	kvStores1.Close()

	fmt.Println("Replacing kmer_store directory with the new indexed one")
	os.RemoveAll(dbPath + "/kmer_store")
	os.Rename(dbPath+"/kmer_store.new", dbPath+"/kmer_store")

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

	}

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = nil

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
