# kAAmer Database

## Structure

A kAAmer database is composed of three KV stores (kmer_store, kcomb_store, protein_store).

* kmer_store : kmers &rarr; kcombination_id
* kcomb_store : kcombination_id &rarr; [prot_id_1, prot_id_2, prot_id_x]
* protein_store : prot_id &rarr; protein_annotation_object

The database folder include three subfolders for the corresponding KV stores.

Badger, the backend LSM tree engine, uses two kind of raw files that you will find in each KV store's folder :
* .sst (String Sorted Tables) files - keys of the LSM tree
* .vlog (value-log) files - values and WAL (write ahead logs of the transactions)


## Build a kAAmer database

 The following sections show how to build a kaamer database.
 
 We will use the UniprotKB viruses distribution as an example.

 
### 1. Raw input

Currently to build a database you will need EMBL files as input (such as UniprotKB entries).
kAAmer will incorporate information on proteins such as taxonomic and functional annotations if
they exists (GO, EC, HAMAP, Biocyc, KEGG) see
https://github.com/zorino/kaamer/blob/master/pkg/kvstore/protein.pb.go.

You can also download prebuilt UniprotKB (SwissProt/TrEMBL) taxonomic EMBL file with the -dl_uniprot
option. \
All taxon proteins will be downloaded and gzipped into the output file. 

```shell
kaamer-db -dl_uniprot -tax viruses -o uniprotkb-viruses.embl.gz
```

### 2. Make the database

The -make option will build two KV store :
* kmer_store (temporary) - contains all the unindexed kmer / proteins association
* protein_store - contains the actual protein annotations


```shell
kaamer-db -make -i uniprotkb-viruses.embl.gz -d kaamerdb-viruses
```

> Note that we can split and parallelize the make using an -offset and a -length for the number of proteins to be
> processed from one input file. The split databases can later be merged with -merge. (-noindex is needed when splitting jobs)

> On systems with a limited number of simultaneous opened files (ulimit -n) use the -maxsize option.

> By default keys and values from the stores are memory mapped. One can change it to fileio
> (-tablemode, -valuemode) in order to decrease the memory usage. However the gain is not substantial.

> No index (-noindex) prevent database indexing.

### 3. Index the database

> If makedb hasn't built the index (-noindex)

The -index option will create the kcomb_store which holds the unique keys for protein combination. \
Its purpose is to reuse hashed keys for all the kmers that share the same set of proteins.
It will also replace the kmer_store with a new one that uses the hashed keys as value.

```shell
kaamer-db -index -d kaamerdb-viruses
```

> Once again you can use the -maxsize and -tablemode -valuemode options


### 4. Start the server

Once you have a working database you can start a server on that database which will listen for queries.

```shell
kaamer-db -server -d kaamerdb-viruses
```

> See the [client section](/client?id=kaamer-cli) to see how to query the database.


## kaamer-db CLI

Execute kaamer-db to see all the options.


```shell

> kaamer-db

 kaamer-db

  // Server

  -server           start a kaamer server
    (input)
      -d            database directory
      -p            port (default: 8321)
      -t            number of threads to use (default all)
      -tmp          tmp folder for query import (default /tmp)

  // Database

  -make             make the protein database
    (input)
      -i            input raw EMBL file
      -d            badger database directory (output)
      -offset       start processing raw uniprot file at protein number x
      -length       process x number of proteins (-1 == infinity)
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)
      -noindex      will NOT index the database - need to be done afterward with -index
      
  -index            index the database for kmer samples association (kcomb_store)
    (input)
      -d            database directory
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)

  -dl_uniprot       download uniprot in EMBL format for a specific taxon
    (input)
      -o            output file (default: uniprotkb.txt.gz)
    (flag)
      -tax          taxon is one of the following :
                    archaea,bacteria,fungi,human,invertebrates,mammals,
                    plants,rodents,vertebrates,viruses

  -merge            merge 2 unindexed databases made with makedb
    (input)
      -dbs          databases directory
      -o            output directory of merged database
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)

  -backup           backup database
    (input)
      -d            badger db directory
      -o            badger backup output directory
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage

  -restore          restore a backup database
    (input)
      -d            badger backup db directory
      -o            badger db output directory
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)

  -gc               run garbage collection on database
    (input)
      -d            database directory
      -it           number of GC iterations
      -ratio        number of ratio of the GC (between 0-1)
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)


```


