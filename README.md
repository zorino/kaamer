# kAAmer

kAAmer is a GO package that provides the tools to produce and query a kmerized protein database.

It speeds up protein and translated nucleotide searches by a magnitude while lacking the
sensitivity of alignment search when it comes to find distant homology.

It is based on LSM-tree key-value (KV) stores ([badger](https://github.com/dgraph-io/badger)) which
provides efficient write workloads to build / index a kaamer database.

Badger is also optimized for SSDs and database files are memory mapped for best performance.


## kAAmer database

The database is built upon three KV stores (kmer_store, kcomb_store, protein_store).

* kmer_store : kmers &rarr; kcombination_ids
* kcomb_store : kcombination_ids &rarr; [prot_id_1, prot_id_2, prot_id_x]
* protein_store : prot_id &rarr; protein_informations


Kmers are amino acids 7-mers.

Size 7 has been chosen to fit the kmer keys onto 32 bits in the KV store.

Currently the making of a database takes EMBL files as input (such as UniprotKB entries) and will
incorporate information on protein such as its taxonomic profile, associated pathways and gene
ontology annotation. 


## Installation

``` shell
go get -u github.com/zorino/kaamer/...
```

