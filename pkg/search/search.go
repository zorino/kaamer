package search

import (
	"github.com/zorino/metaprot/pkg/kvstore"
)

const (
	NUCLEOTIDE_STRING = 0
	NUCLEOTIDE_FILE   = 1
	PROTEIN_STRING    = 2
	PROTEIN_FILE      = 3
)


func Search(sequence string, sequenceType int, kvStores *kvstore.KVStores) {

	switch sequenceType {
	case NUCLEOTIDE_STRING:
		fmt.Println("Nucleotide type")
	case PROTEIN_STRING:
		fmt.Println("Protein type")
	}

}

func ProteinSearch(sequence string, kvStores *kvstore.KVStores) {





}
