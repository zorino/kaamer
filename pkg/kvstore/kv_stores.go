package kvstore

import (
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"math"
)

// # Stores :
// kmer_store : kmer (uint32) -> prot_id
// prot_store : prot_id -> Protein (protobuff)


type KVsToMerge struct {
	Key    []byte
	Values [][]byte
}

type KVStores struct {
	KmerStore  *K_
	ProteinStore *H_
}

func KVStoresNew(dbPath string, nbOfThreads int) *KVStores {

	var kvStores KVStores

	k_opts := badger.DefaultOptions
	k_opts.Dir = dbPath + "/kmer_store"
	k_opts.ValueDir = dbPath + "/kmer_store"
	k_opts.ValueLogLoadingMode = options.MemoryMap
	k_opts.TableLoadingMode = options.MemoryMap
	k_opts.SyncWrites = false
	k_opts.NumVersionsToKeep = math.MaxUint32
	k_opts.MaxTableSize = 768 << 20
	k_opts.ValueLogMaxEntries = 100000000
	k_opts.NumCompactors = 8

	p_opts := badger.DefaultOptions
	p_opts.Dir = dbPath + "/protein_store"
	p_opts.ValueDir = dbPath + "/protein_store"
	p_opts.ValueLogLoadingMode = options.MemoryMap
	p_opts.TableLoadingMode = options.MemoryMap
	p_opts.SyncWrites = false
	p_opts.NumVersionsToKeep = 1
	p_opts.MaxTableSize = 768 << 20
	p_opts.ValueLogMaxEntries = 100000000

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
