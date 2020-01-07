# kAAmer

kAAmer is a GO package that provides the tools to produce and query a kmerized protein database.

It can speed up protein and translated nucleotide searches by a magnitude in alignment-free mode and
it also supports an alignment mode with a runtime comparable to the most efficient and accurate aligners.

kAAmer is based on LSM-tree key-value (KV) stores which provides efficient write workloads to build / index a kAAmer database.

The kAAmer KV engine [badger](https://github.com/dgraph-io/badger) is also optimized for SSD and database files are memory mapped for fast kmer lookup.

See the [Getting Started](installation.md?id=getting-started) section to start using kAAmer.
