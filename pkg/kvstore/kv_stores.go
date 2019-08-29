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
	"math"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

// # Stores :
// kmer_store : kmer (uint32) -> prot_id
// prot_store : prot_id -> Protein (protobuff)

type KVsToMerge struct {
	Key    []byte
	Values [][]byte
}

type KVStores struct {
	KmerStore    *K_
	KCombStore   *KC_
	ProteinStore *P_
}

const (
	MaxTableSize        = 768 << 20
	MaxValueLogFileSize = 2048 << 20
	MaxValueLogEntries  = 100000000
)

func KVStoresNew(dbPath string, nbOfThreads int, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode, maxSize bool, syncWrite bool, readOnly bool) *KVStores {

	var kvStores KVStores

	// kmer_store options
	k_opts := badger.DefaultOptions
	k_opts.Logger = nil
	k_opts.Dir = dbPath + "/kmer_store"
	k_opts.ValueDir = dbPath + "/kmer_store"
	k_opts.TableLoadingMode = tableLoadingMode
	k_opts.ValueLogLoadingMode = valueLoadingMode
	k_opts.SyncWrites = syncWrite
	k_opts.NumVersionsToKeep = math.MaxUint32
	if maxSize {
		k_opts.MaxTableSize = MaxTableSize
		k_opts.ValueLogFileSize = MaxValueLogFileSize
		k_opts.ValueLogMaxEntries = MaxValueLogEntries
	}
	if readOnly {
		k_opts.ReadOnly = true
	}
	k_opts.NumCompactors = 8

	// kcomb_store options
	kc_opts := badger.DefaultOptions
	kc_opts.Logger = nil
	kc_opts.Dir = dbPath + "/kcomb_store"
	kc_opts.ValueDir = dbPath + "/kcomb_store"
	kc_opts.TableLoadingMode = tableLoadingMode
	kc_opts.ValueLogLoadingMode = valueLoadingMode
	kc_opts.SyncWrites = syncWrite
	kc_opts.NumVersionsToKeep = 1
	if maxSize {
		kc_opts.MaxTableSize = MaxTableSize
		kc_opts.ValueLogFileSize = MaxValueLogFileSize
		kc_opts.ValueLogMaxEntries = MaxValueLogEntries
	}
	if readOnly {
		kc_opts.ReadOnly = true
	}

	// protein_store options
	p_opts := badger.DefaultOptions
	p_opts.Logger = nil
	p_opts.Dir = dbPath + "/protein_store"
	p_opts.ValueDir = dbPath + "/protein_store"
	p_opts.TableLoadingMode = tableLoadingMode
	p_opts.ValueLogLoadingMode = valueLoadingMode
	p_opts.SyncWrites = syncWrite
	p_opts.NumVersionsToKeep = 1
	if maxSize {
		p_opts.MaxTableSize = MaxTableSize
		p_opts.ValueLogFileSize = MaxValueLogFileSize
		p_opts.ValueLogMaxEntries = MaxValueLogEntries
	}
	if readOnly {
		p_opts.ReadOnly = true
	}

	// Open all store
	kvStores.KmerStore = K_New(k_opts, 1000, nbOfThreads)
	kvStores.KCombStore = KC_New(kc_opts, 1000, nbOfThreads)
	kvStores.ProteinStore = P_New(p_opts, 1000, nbOfThreads)

	return &kvStores

}

func (kvStores *KVStores) OpenInsertChannel() {
	kvStores.KmerStore.OpenInsertChannel()
	kvStores.KCombStore.OpenInsertChannel()
	kvStores.ProteinStore.OpenInsertChannel()
}

func (kvStores *KVStores) CloseInsertChannel() {
	kvStores.KmerStore.CloseInsertChannel()
	kvStores.KCombStore.CloseInsertChannel()
	kvStores.ProteinStore.CloseInsertChannel()
}

func (kvStores *KVStores) Flush() {
	// Last DB flushes
	kvStores.KmerStore.Flush()
	kvStores.KCombStore.Flush()
	kvStores.ProteinStore.Flush()
}

func (kvStores *KVStores) Close() {
	// Last DB flushes
	kvStores.KmerStore.Close()
	kvStores.KCombStore.Close()
	kvStores.ProteinStore.Close()
}
