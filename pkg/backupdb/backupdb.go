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

package backupdb

import (
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/dgraph-io/badger/v3"
	"github.com/zorino/kaamer/pkg/kvstore"
)

func Backupdb(dbPath string, output string) {

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

	kvStores1 := kvstore.KVStoresNew(dbPath, nbOfThreads, true, false, true)

	Backup(kvStores1.KmerStore.DB, output+"/kmer_store.bdg")
	Backup(kvStores1.ProteinStore.DB, output+"/protein_store.bdg")

	kvStores1.Close()

}

func Backup(db *badger.DB, bckFile string) {

	f, err := os.Create(bckFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Printf("# Backup %s\n", bckFile)
	db.Backup(f, 0)

	f.Close()
}
