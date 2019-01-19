package kvstore

import ()

// Kmer Entries
type K_ struct {
	*KVStore
}

func K_New(dbPath string) *K_ {
	var k K_
	k.KVStore = new(KVStore)
	NewKVStore(k.KVStore, dbPath+"/k_store", 1000)
	return &k
}


// Binary Amino Acid
// AMINO_ACID_BINARY_TABLE = {
// 	'A': [0, 0, 0, 0, 0],
// 	'C': [0, 0, 0, 0, 1],
// 	'D': [0, 0, 0, 1, 0],
// 	'E': [0, 0, 0, 1, 1],
// 	'F': [0, 0, 1, 0, 0],
// 	'G': [0, 0, 1, 0, 1],
// 	'H': [0, 0, 1, 1, 0],
// 	'I': [0, 0, 1, 1, 1],
// 	'K': [0, 1, 0, 0, 0],
// 	'L': [0, 1, 0, 0, 1],
// 	'M': [0, 1, 0, 1, 0],
// 	'N': [0, 1, 0, 1, 1],
// 	'P': [0, 1, 1, 0, 0],
// 	'Q': [0, 1, 1, 0, 1],
// 	'R': [0, 1, 1, 1, 1],
// 	'S': [1, 0, 0, 0, 0],
// 	'T': [1, 0, 0, 0, 1],
// 	'V': [1, 0, 0, 1, 0],
// 	'W': [1, 0, 0, 1, 1],
// 	'Y': [1, 0, 1, 0, 0]
// }
