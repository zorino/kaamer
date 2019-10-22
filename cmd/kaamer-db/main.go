/*
Copyright 2019 The kaamer Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/dgraph-io/badger/options"
	"github.com/zorino/kaamer/api"
	"github.com/zorino/kaamer/pkg/backupdb"
	"github.com/zorino/kaamer/pkg/downloaddb"
	"github.com/zorino/kaamer/pkg/gcdb"
	"github.com/zorino/kaamer/pkg/indexdb"
	"github.com/zorino/kaamer/pkg/makedb"
	"github.com/zorino/kaamer/pkg/mergedb"
	"github.com/zorino/kaamer/pkg/restoredb"
)

const (
	MaxInt uint32 = 1<<32 - 1
)

var (
	LoadingMode = map[string]options.FileLoadingMode{"memorymap": options.MemoryMap, "fileio": options.FileIO}
)

func main() {

	usage := `
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
      -i            input raw EMBL file
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

  -download         download databases (Uniprot, KeggPathways, BiocycPathways)
    (input)
      -o            output file (default: uniprotkb.txt.gz)
      -d            database directory (only with kegg and biocyc options)

      -uniprot      download raw embl files for one of the following taxon :
                    archaea,bacteria,fungi,human,invertebrates,mammals,
                    plants,rodents,vertebrates,viruses
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

`

	var serverOpt = flag.Bool("server", false, "program")
	var portNumber = flag.Int("p", 8321, "port argument")
	var nbThreads = flag.Int("t", runtime.NumCPU(), "number of threads")
	var tmpFolder = flag.String("tmp", "/tmp/", "tmp folder for query import")

	var makedbOpt = flag.Bool("make", false, "program")
	var inputPath = flag.String("i", "", "input file argument")
	var dbPath = flag.String("d", "", "db path argument")
	var makedbOffset = flag.Uint("offset", 0, "offset to process raw file")
	var makedbLenght = flag.Uint("length", uint(MaxInt), "process x number of files")
	var maxSize = flag.Bool("maxsize", false, "to maximize badger output file size")
	var tableMode = flag.String("tablemode", "memorymap", "table loading mode (fileio, memorymap)")
	var valueMode = flag.String("valuemode", "memorymap", "value loading mode (fileio, memorymap)")
	var noIndex = flag.Bool("noindex", false, "prevent the indexing of database")

	var indexOpt = flag.Bool("index", false, "program")

	var downloadOpt = flag.Bool("download", false, "download uniprotkb or kaamer db")
	var uniprotOpt = flag.String("uniprot", "", "uniprot taxon")
	var keggOpt = flag.Bool("kegg", false, "download kegg pathways")
	var biocycOpt = flag.Bool("biocyc", false, "download biocyc pathways")

	var mergedbOpt = flag.Bool("merge", false, "program")
	var dbsPath = flag.String("dbs", "", "db path argument")
	var outPath = flag.String("o", "", "db path argument")

	var gcOpt = flag.Bool("gc", false, "program")
	var gcIteration = flag.Int("it", 100, "number of GC iterations")
	var gcRatio = flag.Float64("ratio", 0.5, "ratio for GC")

	var backupdbOpt = flag.Bool("backup", false, "program")

	var restoreOpt = flag.Bool("restore", false, "program")

	/* CLI usage */
	flag.Usage = func() {
		fmt.Println(usage)
	}
	flag.Parse()

	/* Setting values from CLI */
	var tableLoadingMode options.FileLoadingMode
	var valueLoadingMode options.FileLoadingMode
	var ok = false

	if tableLoadingMode, ok = LoadingMode[*tableMode]; !ok {
		fmt.Println("TableMode unrecognized ! use fileio or memorymap!")
		os.Exit(1)
	}
	if valueLoadingMode, ok = LoadingMode[*valueMode]; !ok {
		fmt.Println("ValueMode unrecognized ! use fileio or memorymap!")
		os.Exit(1)
	}

	if _, err := os.Stat(*tmpFolder); os.IsNotExist(err) {
		fmt.Printf("Directory %s does not exist !\n", tmpFolder)
		os.Exit(1)
	}

	/* Main Option Groups*/
	if *serverOpt == true {
		if *dbPath == "" {
			fmt.Println("No db path !")
		} else {
			server.NewServer(*dbPath, *portNumber, tableLoadingMode, valueLoadingMode, *nbThreads, *tmpFolder)
		}
		os.Exit(0)
	}

	if *downloadOpt == true {
		// Uniprot Taxon
		if *uniprotOpt != "" {
			if !downloaddb.Uniprot_ftp_taxonomic_valid[*uniprotOpt] {
				fmt.Println("Invalid taxon !")
				os.Exit(1)
			} else {
				downloaddb.DownloadUniprot(*outPath, *uniprotOpt)
			}
		} else if *keggOpt != false {
			if *dbPath == "" {
				fmt.Println("No input db path !")
				os.Exit(1)
			} else {
				downloaddb.DownloadKEGG(*dbPath)
			}
		} else if *biocycOpt != false {
			if *dbPath == "" {
				fmt.Println("No input db path !")
				os.Exit(1)
			} else {
				downloaddb.DownloadBiocyc(*dbPath)
			}
		} else {
			fmt.Println("Need uniprot, kegg or biocyc option !")
			os.Exit(1)
		}

		os.Exit(0)
	}

	if *makedbOpt == true {

		if *dbPath == "" {
			fmt.Println("No output db path !")
			os.Exit(1)
		} else if *inputPath == "" {
			fmt.Println("No input file !")
			os.Exit(1)
		} else {
			makedb.NewMakedb(*dbPath, *inputPath, *nbThreads, *makedbOffset, *makedbLenght, *maxSize, tableLoadingMode, valueLoadingMode, *noIndex)
		}

		os.Exit(0)
	}

	if *indexOpt == true {

		if *dbPath == "" {
			fmt.Println("No db path !")
			os.Exit(1)
		} else {
			indexdb.NewIndexDB(*dbPath, *nbThreads, *maxSize, tableLoadingMode, valueLoadingMode)
		}

		os.Exit(0)
	}

	if *mergedbOpt == true {
		if *dbsPath == "" || *outPath == "" {
			fmt.Println("Need to have a valid databases path !")
		} else {
			mergedb.NewMergedb(*dbsPath, *outPath, *maxSize, tableLoadingMode, valueLoadingMode)
		}
		os.Exit(0)
	}

	if *gcOpt == true {
		if *dbPath == "" {
			fmt.Println("No db path !")
		} else {
			gcdb.NewGC(*dbPath, *gcIteration, *gcRatio, *maxSize, tableLoadingMode, valueLoadingMode)
		}
		os.Exit(0)
	}

	if *backupdbOpt == true {
		if *dbPath == "" {
			fmt.Println("Need to have a valid databases path !")
		} else if *outPath == "" {
			fmt.Println("Need to have a valid backup directory path !")
		} else {
			backupdb.Backupdb(*dbPath, *outPath, tableLoadingMode, valueLoadingMode)
		}
		os.Exit(0)
	}

	if *restoreOpt == true {
		if *dbPath == "" {
			fmt.Println("Need to have a valid backup databases path !")
		} else if *outPath == "" {
			fmt.Println("Need to have a valid restore directory path !")
		} else {
			restoredb.RestoreDB(*dbPath, *outPath, *maxSize)
		}
		os.Exit(0)
	}

	fmt.Println(usage)
	os.Exit(0)

}
