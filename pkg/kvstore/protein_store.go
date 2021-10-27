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

package kvstore

import (
	"github.com/dgraph-io/badger/v3"
)

// Hash store for values combination used in other stores
type P_ struct {
	*KVStore
}

func P_New(opts badger.Options, flushSize int, nbOfThreads int) *P_ {
	var p P_
	p.KVStore = new(KVStore)
	NewKVStore(p.KVStore, opts, flushSize, nbOfThreads)
	return &p
}
