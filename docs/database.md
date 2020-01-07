# kAAmer Database

## Structure

A kAAmer database is composed of three KV stores (kmer_store, kcomb_store, protein_store).

* kmer_store : kmers &rarr; kcombination_id
* kcomb_store : kcombination_id &rarr; [prot_id_1, prot_id_2, prot_id_x]
* protein_store : prot_id &rarr; protein_annotation_object (serialized with protocol buffer)

The database folder include three subfolders for the corresponding KV stores.

Badger, the backend LSM tree engine, uses two kind of raw files that you will find in each KV store's folder :
* .sst (String Sorted Tables) files - keys of the LSM tree
* .vlog (value-log) files - values and WAL (write ahead logs of the transactions)


## Build a kAAmer database

 The following sections show how to build a kaamer database.
 
 We will use the RefSeq archaea distribution as a demonstration (# Demo #) in the following section.


### 1. Raw input

Currently to build a database you will need either one EMBL, GenBank, TSV or FASTA file as input,
which can be compressed with gzip.

KAAmer input parser has been tested against UniprotKB (SwissProt, TrEMBL) for the EMBL parser, RefSeq for
GenBank parser, and custom TSV and FASTA input.

* **TSV** format **required** at least 1 column named "EntryID" and 1 column named "Sequence". However, a "ProteinName" column is always recommended. All the other columns will be treated has features of the protein and included in the database.

* **FASTA** parser will take from the sequence header ">..." the first string before a space as the
  "EntryId" and the rest of the line has a "ProteinName" feature.

* **EMBL** and **Genbank** parsers include predetermined features which can be found in the var
section of the respective parsers here:
    * https://github.com/zorino/kaamer/blob/master/pkg/makedb/inputEMBL.go#L43
    * https://github.com/zorino/kaamer/blob/master/pkg/makedb/inputGBK.go#L42



#### 1.1 Download
kAAmer also offer an automatic download of UniprotKB (SwissProt/TrEMBL) or RefSeq releases by taxonomic level.
All taxon proteins will be downloaded and gzipped into the output file ready to be parsed by
`kaamer-db -make`.


```shell
# Demo #
kaamer-db -download -refseq archaea -o refseq-archaea.gbk.gz
```

### 2. Make the database

The -make option will build two KV store :
* kmer_store (temporary) - contains all the unindexed kmer / proteins association
* protein_store - contains the actual protein annotations


```shell
# Demo #
kaamer-db -make -f gbk -i refseq-archaea.gbk.gz -d kaamerdb-refseq-archaea
```

> Note that we can split and parallelize the make using an -offset and a -length for the number of proteins to be
> processed from one input file. The split databases can later be merged with -merge. (-noindex is needed when splitting jobs)

> On systems with a limited number of simultaneous opened files (ulimit -n) use the -maxsize option.

> By default keys and values from the stores are memory mapped. One can change it to fileio
> (-tablemode, -valuemode) in order to decrease the memory usage. However the gain is not substantial.

> No index (-noindex) prevent database indexing.

### 3. Large dataset options

You can split the database by using different input files or using -offset and -length options.

The split databases can then be merged and indexed into a final working database.

The -index option creates the kcomb_store which holds the unique keys for protein combination. \
Its purpose is to reuse hashed keys for all the kmers that share the same set of proteins.
It will also replace the kmer_store with a new one that uses the hashed keys as value.


```shell
## Build 2 split db
# mkdir kaamerdb-refseq-archaea.splits
# kaamer-db -make -offset 0 -length 1000000 -noindex  -f gbk -i refseq-archaea.gbk.gz -d kaamerdb-refseq-archaea.splits/kaamerdb-refseq-archaea.01
# kaamer-db -make -offset 1000000  -length 1000000 -noindex -f gbk -i refseq-archaea.gbk.gz -d kaamerdb-refseq-archaea.splits/kaamerdb-refseq-archaea.02

## Merge the split
# kaamer-db -merge -dbs kaamerdb-refseq-archaea.splits -o kaamerdb-refseq-archaea.merged

## Index the merged databaes
# kaamer-db -index -d kaamerdb-refseq-archaea.merged

```

#### // Download KEGG / BioCyc pathway annotation

Uniprot includes KEGG and Biocyc identifiers and we added the option to download the actual
pathway information associated with these IDs (fetch from the BioCyc and KEGG APIs).

> Need to be executed on an indexed database containing Kegg or / Biocyc ids has features :
> "KEGG_ID", "BioCyc_ID".

> The features are present in the downloadable UniprotKB releases.

```shell
# kaamer-db -download -kegg uniprot-kaamer-db
# kaamer-db -download -biocyc uniprot-kaamer-db
```


### 4. Start the server

Once you have a working database you can start a server on that database which will listen for queries.

```shell
# Demo #
kaamer-db -server -d kaamerdb-refseq-archaea
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

      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage

  // Database

  -make             make the protein database
    (input)
      -i            input file
      -f            input format (embl, tsv, fasta)
      -d            badger database directory (output)
      -t            number of threads to use (default all)
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
      -t            number of threads to use (default all)
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)

  -download         download databases (Uniprot, RefSeq, KeggPathways, BiocycPathways)
    (input)
      -o            output file (default: uniprotkb.txt.gz)
      -d            database directory (only with kegg and biocyc options)

      -uniprot      download raw embl file for one of the following taxon :
                    archaea,bacteria,fungi,human,invertebrates,mammals,
                    plants,rodents,vertebrates,viruses

      -refseq       download raw genbank file for one of the following taxon :
                    archaea, bacteria, fungi, invertebrate, mitochondrion, plant, plasmid,
                    plastid, protozoa, viral, vertebrate_mammalian, vertebrate_other

    (flag)
      -kegg         download kegg pathways protein association and merge into database
      -biocyc       download biocyc pathways protein association and merge into database

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
