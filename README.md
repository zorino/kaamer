# kAAmer

![kAAmer logo](web/src/images/kaamer.svg)

kAAmer is a GO package that provides the tools to produce and query a kmerized protein database.

It provides fast protein and translated nucleotide searches over a protein database while lacking the sensitivity of alignment when it comes to find distant homology.

It is based on LSM-tree key value (KV) stores ([badger](https://github.com/dgraph-io/badger)) which
provides very efficient write and lookup workloads with modern hardware, especially solid state drive (SSD).

## Documentation

https://zorino.github.io/kaamer/

