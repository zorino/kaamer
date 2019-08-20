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

package gcdb

import (
	"runtime"
	"sync"

	"github.com/dgraph-io/badger/options"
	"github.com/zorino/kaamer/pkg/kvstore"
)

func NewGC(dbPath string, iteration int, ratio float64, maxSize bool, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode) {

	runtime.GOMAXPROCS(128)
	kvStores := kvstore.KVStoresNew(dbPath, runtime.NumCPU(), tableLoadingMode, valueLoadingMode, maxSize, true, false)

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
