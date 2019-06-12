package restoredb

import (
	"log"
	"os"
	"runtime"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/zorino/metaprot/pkg/kvstore"
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

	opts := badger.LSMOnlyOptions
	opts.Dir = storeDir
	opts.ValueDir = storeDir
	opts.TableLoadingMode = options.MemoryMap
	opts.ValueLogLoadingMode = options.FileIO
	if maxSize {
		opts.MaxTableSize = kvstore.MaxTableSize
		opts.ValueLogFileSize = kvstore.MaxTableSize
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

}
