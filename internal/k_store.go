package kvstore

import ()

// Kmer Entries
type K_ struct {
	*KVStore
}

func K_New(dbPath string) *K_ {
	var k K_
	k.KVStore = new(KVStore)
	NewKVStore(k.KVStore, dbPath+"/k_store", 1000000)
	return &k
}
