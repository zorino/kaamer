# Getting started


## Installation

To run kaamer you will need at least GO >= 1.13 (https://golang.org/doc/install)

This command will download and install kaamer executables (kaamer and kaamer-db) into your `GOPATH`

```shell
export GO111MODULE=on
git clone https://github.com/zorino/kaamer.git
cd kaamer
go install ./...
```

You can also download precompiled binaries : https://github.com/zorino/kaamer/releases 
> However to deploy the Web UI and Web documentation you need to compile from source.

## Requirement

The following benchmarks give an idea of the computing ressource usage to build a database and run a server on it.

The protein datasets were taken randomly from UniprotKB and from the Kingdom Bacteria.

Number of threads are indicative for the runtime it takes.

The memory peaks are indicative of a lower bound in memory requirement, and one should
provide more (2+ GB) to prevent makedb failures.

The final database size doesn't account for the temporary stores size that is built during the
creation of a database.
We suggest at least 3-5x the final database in size of free available space on disk during the creation.


#### Build a database

| Number Of Proteins | Threads        | Memory Peak    | Runtime MakeDB | Final DB Size |
| -------------:     | -------------: | -------------: | ------:        | ------:       |
| 1,000              | 2              | 0.279 GB       | 0m7.208s       | 44 MB         |
| 10,000             | 4              | 2.543 GB       | 1m40.028s      | 233 MB        |
| 100,000            | 6              | 5.538 GB       | 19m1.312s      | 2.1 GB        |
| 1,000,000          | 8              | 15.558 GB      | 157m13.666s    | 8.7 GB        |


#### Serve a database

One should provide at least +2 GB of RAM from the Memory peak to serve a database.

The more RAM, threads allocated to the database, the better performance you will obtain from kAAmer.

RAM will take advantage of Linux page caching for recurrently used key value from the database and
CPU will help with query parallelisation.


| Number Of Proteins | Threads        | Memory Peak    | Runtime OpenDB |
| -------------:     | -------------: | -------------: | ------:        |
| 1,000              | 2              | 3.152 GB       | 0m0.0s         |
| 10,000             | 4              | 3.151 GB       | 0m1.0s         |
| 100,000            | 6              | 3.221 GB       | 0m2.0s         |
| 1,000,000          | 8              | 5.597 GB       | 0m15.0s        |


## How to use kAAmer :

#### &rarr; [Build a database](/database?id=kaamer-database)
#### &rarr; [Query a database](/client?id=kaamer-client-cli)
