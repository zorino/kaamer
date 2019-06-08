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
	ProteinStore *H_
}

const (
	MaxTableSize        = 768 << 20
	MaxValueLogFileSize = 2048 << 20
	MaxValueLogEntries  = 100000000
)

func KVStoresNew(dbPath string, nbOfThreads int, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode, maxSize bool) *KVStores {

	var kvStores KVStores

	k_opts := badger.DefaultOptions
	k_opts.Dir = dbPath + "/kmer_store"
	k_opts.ValueDir = dbPath + "/kmer_store"
	k_opts.TableLoadingMode = tableLoadingMode
	k_opts.ValueLogLoadingMode = valueLoadingMode
	k_opts.SyncWrites = false
	k_opts.NumVersionsToKeep = math.MaxUint32
	if maxSize {
		k_opts.MaxTableSize = MaxTableSize
		k_opts.ValueLogFileSize = MaxValueLogFileSize
		k_opts.ValueLogMaxEntries = MaxValueLogEntries
	}
	k_opts.NumCompactors = 8

	p_opts := badger.DefaultOptions
	p_opts.Dir = dbPath + "/protein_store"
	p_opts.ValueDir = dbPath + "/protein_store"
	p_opts.TableLoadingMode = tableLoadingMode
	p_opts.ValueLogLoadingMode = valueLoadingMode
	p_opts.SyncWrites = false
	p_opts.NumVersionsToKeep = 1
	if maxSize {
		p_opts.MaxTableSize = MaxTableSize
		p_opts.ValueLogFileSize = MaxValueLogFileSize
		p_opts.ValueLogMaxEntries = MaxValueLogEntries
	}

	// Open all store
	kvStores.KmerStore = K_New(k_opts, 1000, nbOfThreads)
	kvStores.ProteinStore = H_New(p_opts, 1000, nbOfThreads)

	return &kvStores

}

func (kvStores *KVStores) OpenInsertChannel() {
	kvStores.KmerStore.OpenInsertChannel()
	kvStores.ProteinStore.OpenInsertChannel()
}

func (kvStores *KVStores) CloseInsertChannel() {
	kvStores.KmerStore.CloseInsertChannel()
	kvStores.ProteinStore.CloseInsertChannel()
}

func (kvStores *KVStores) Flush() {
	// Last DB flushes
	kvStores.KmerStore.Flush()
	kvStores.ProteinStore.Flush()
}

func (kvStores *KVStores) Close() {
	// Last DB flushes
	kvStores.KmerStore.Close()
	kvStores.ProteinStore.Close()
}
