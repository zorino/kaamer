# kAAmer

kAAmer is a GO package that provides the tools to produce and query a kmerized protein database.

It speeds up protein and translated nucleotide searches by a magnitude while lacking the sensitivity of alignment search when it comes to find distant homology.

It is based on LSM-tree key-value (KV) stores which provides efficient write workloads to build / index a kaamer database.

The kAAmer KV engine [badger](https://github.com/dgraph-io/badger) is also optimized for SSD and database files are memory mapped for fast kmer lookup.

See the [Getting Started](./installation.md) section to start using kaamer.
