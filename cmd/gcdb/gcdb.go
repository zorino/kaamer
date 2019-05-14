package gcdb

import (
	"github.com/dgraph-io/badger/options"
	"github.com/zorino/metaprot/pkg/kvstore"
	"runtime"
	"sync"
)

func NewGC(dbPath string, iteration int, ratio float64) {

	runtime.GOMAXPROCS(128)
	kvStores := kvstore.KVStoresNew(dbPath, runtime.NumCPU(), options.MemoryMap, options.MemoryMap)

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		kvStores.KmerStore.GarbageCollect(iteration, ratio)
	}(wg)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		kvStores.ProteinStore.GarbageCollect(iteration, ratio)
	}(wg)
	wg.Wait()

	kvStores.Close()

}
