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

package restoredb

import (
	"log"
	"os"
	"runtime"

	"github.com/dgraph-io/badger/v3"
	"github.com/zorino/kaamer/pkg/kvstore"
)

func RestoreDB(backupPath string, output string, maxSize bool) {
	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(128)

	nbOfThreads := runtime.NumCPU()

	if nbOfThreads < 1 {
		nbOfThreads = 1
	}

	if _, err := os.Stat(output); os.IsNotExist(err) {
		os.Mkdir(output, 0700)
	}

	// kvStores1 := kvstore.KVStoresNew(dbPath, nbOfThreads, options.MemoryMap, options.FileIO)

	Restore(backupPath+"/kmer_store.bdg", output+"/kmer_store", maxSize)
	Restore(backupPath+"/protein_store.bdg", output+"/protein_store", maxSize)

	// kvStores1.Close()

}

func Restore(backupFile string, storeDir string, maxSize bool) {

	opts := badger.DefaultOptions(storeDir)
	opts.Dir = storeDir
	opts.ValueDir = storeDir
	if maxSize {
		opts.ValueLogFileSize = kvstore.MaxValueLogFileSize
		opts.ValueLogMaxEntries = kvstore.MaxValueLogEntries
	}

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err.Error())
	}

	backupFileReader, err := os.Open(backupFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = db.Load(backupFileReader, 100)
	if err != nil {
		log.Fatal(err.Error())
	}

	db.Flatten(8)

	// Run GC until err != nil
again:
	err = db.RunValueLogGC(0.1)
	if err == nil {
		goto again
	}

	db.Close()

}
