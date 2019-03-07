package mergedb

import (
	"github.com/zorino/metaprot/internal"
	// "github.com/dgraph-io/badger"
	// "github.com/dgraph-io/badger/options"
	bpb "github.com/dgraph-io/badger/pb"
	"runtime"
	// "fmt"
	"context"
	"sync"
	"log"
)

type DBMerger struct {
	kvStores1    *kvstore.KVStores
	kvStores2    *kvstore.KVStores
	KVToMerge    sync.Map
}


func NewMergedb(dbPath_1 string, dbPath_2 string) {

	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(128)

	nbOfThreads := runtime.NumCPU()

	if nbOfThreads < 1 {
		nbOfThreads = 1
	}

	kvStores1 := kvstore.KVStoresNew(dbPath_1, nbOfThreads)
	kvStores2 := kvstore.KVStoresNew(dbPath_2, nbOfThreads)

	MergeStores(kvStores1.K_batch.KVStore, kvStores2.K_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.KK_batch.KVStore, kvStores2.KK_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.G_batch.KVStore, kvStores2.G_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.GG_batch.KVStore, kvStores2.GG_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.F_batch.KVStore, kvStores2.F_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.FF_batch.KVStore, kvStores2.FF_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.P_batch.KVStore, kvStores2.P_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.PP_batch.KVStore, kvStores2.PP_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.O_batch.KVStore, kvStores2.O_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.OO_batch.KVStore, kvStores2.OO_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.N_batch.KVStore, kvStores2.N_batch.KVStore, nbOfThreads)
	MergeStores(kvStores1.NN_batch.KVStore, kvStores2.NN_batch.KVStore, nbOfThreads)

	kvStores1.Close()
	kvStores2.Close()

	kvStores1 = kvstore.KVStoresNew(dbPath_1, nbOfThreads)
	kvStores1.MergeKmerValues(nbOfThreads)
	kvStores1.Close()

}


func MergeStores (kvStore1 *kvstore.KVStore, kvStore2 *kvstore.KVStore, nbOfThreads int) {

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
	stream.KeyToList = nil

	// -- End of optional settings.


	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = func(list *bpb.KVList) error {
		for _, kv := range list.GetKv() {
			kvStore1.AddValue(kv.GetKey(), kv.GetValue(), 0)
		}

		return nil
	}

	// // Run the stream
	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.

}
