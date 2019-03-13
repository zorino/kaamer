package backupdb

import (
	"github.com/zorino/metaprot/pkg/kvstore"
	"github.com/dgraph-io/badger"
	// "github.com/dgraph-io/badger/options"
	"runtime"
	"fmt"
	"os"
	"log"
	// "bufio"
	// "math"
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

	kvStores1 := kvstore.KVStoresNew(dbPath, nbOfThreads)

	Backup(kvStores1.K_batch.DB, output+"/k_store.bck")
	Backup(kvStores1.KK_batch.DB, output+"/kk_store.bck")
	Backup(kvStores1.G_batch.DB, output+"/g_store.bck")
	Backup(kvStores1.GG_batch.DB, output+"/gg_store.bck")
	Backup(kvStores1.F_batch.DB, output+"/f_store.bck")
	Backup(kvStores1.FF_batch.DB, output+"/ff_store.bck")
	Backup(kvStores1.P_batch.DB, output+"/p_store.bck")
	Backup(kvStores1.PP_batch.DB, output+"/pp_store.bck")
	Backup(kvStores1.O_batch.DB, output+"/o_store.bck")
	Backup(kvStores1.OO_batch.DB, output+"/oo_store.bck")
	Backup(kvStores1.N_batch.DB, output+"/nn_store.bck")
	Backup(kvStores1.NN_batch.DB, output+"/nn_store.bck")

	kvStores1.Close()

}


func Backup(db *badger.DB, bckFile string) {

	f, err := os.Create(bckFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("Backup ...")
	db.Backup(f, 0)

	f.Close()
}
