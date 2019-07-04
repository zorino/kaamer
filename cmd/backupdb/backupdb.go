package backupdb

import (
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/zorino/metaprot/pkg/kvstore"
)

func Backupdb(dbPath string, output string, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode) {

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

	kvStores1 := kvstore.KVStoresNew(dbPath, nbOfThreads, tableLoadingMode, valueLoadingMode, true, false)

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
