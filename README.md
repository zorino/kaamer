# Metaprot


Metaprot is a kmerized bacterial protein database along a GO package to produce, manage and use it.

There are two functionality for Metaprot :

1. Single protein identification
2. Functionnal annotation of shotgun metagenomes


## Database

The database is built from the UniprotKB bacterial proteins.
There is a "light" version which only contains the proteins which have at least
an annotation in one of these database : EnzymeCommission, GeneOntology, BioCyc,
KEGG or HAMAP. The full version contains all the proteins even the one with
unknown functions. 

Each amino-acid kmer is of length 7 - therefore 7-mers.
The values are combination keys which represent a unique combination of the
features associated with the kmer.

## Installation

``` shell
go get -u github.com/zorino/metaprot
```


## Usage

``` shell
$ metaprot

metaprot

    // Analyses




    // Database
   

          -makedb       make the protein database

                           -i    input tsv file or dir // if doesn't exist metaprot will download from Uniprot (>60 GB)
                           -d    badger database dir path

          -mergedb      merge two metaprot badger databases

                           -d1   badger db 1
                           -d2   badger db 2

```
