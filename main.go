package main

import (
	"flag"
	"fmt"
	"github.com/zorino/metaprot/cmd/makedb"
	// "github.com/zorino/metaprot/cmd/backupdb"
	// "github.com/zorino/metaprot/cmd/mergedb"
	"github.com/zorino/metaprot/api"
	"github.com/zorino/metaprot/cmd/search"
	"os"
)

var (
	MaxInt uint64 = 1<<64 - 1
)

func main() {

	usage := `
 metaprot

  // Analyses


	   -serve        start a metaprot server

		  (input)
			   -d    database directory

			   -p    port (default: 8321)

	   -search       search for a protein in metaprot db

		  (input)
			   -d    database directory

			   -s    sequence string
		  or
			   -f    sequences fasta file / read file (.fasta(.gz) or .fastq(.gz))


  // Database Management

	   -downloadb    download metaprot database

		   (flag)
			   -m    metaprot release database
			   -r    raw UniprotKB database (to use with -makedb)

		  (input)
			   -o    output directory of database

	   -makedb       make the protein database

		  (input)
			   -i    input tsv file (raw tsv file from -downloaddb -r)
			   -d    badger database directory

			   -offset    start processing raw uniprot file at protein number x
			   -length    process x number of proteins (-1 == infinity)

		   (flag)
			   -full      to make a full database (default is the light version)


	   -backupdb     backup database

		  (input)
			   -d    badger db directory
			   -o    badger backup output directory

`

	var serverOpt = flag.Bool("server", false, "program")
	var portNumber = flag.Int("p", 8321, "port argument")

	var searchOpt = flag.Bool("search", false, "program")
	var filePath = flag.String("f", "", "file path argument")
	var sequenceString = flag.String("s", "", "db path argument")

	var makedbOpt = flag.Bool("makedb", false, "program")
	var inputPath = flag.String("i", "", "db path argument")
	var dbPath = flag.String("d", "", "db path argument")
	var fullDb = flag.Bool("full", false, "to build full database")
	var makedbOffset = flag.Uint64("offset", 0, "offset to process raw file")
	var makedbLenght = flag.Uint64("length", MaxInt, "process x number of files")

	// var mergedbOpt = flag.Bool("mergedb", false, "program")
	// var dbsPath = flag.String("dbs", "", "db path argument")
	// var outPath = flag.String("o", "", "db path argument")

	// var analyseOpt = flag.Bool("analyse", false, "program")

	// var backupdbOpt = flag.Bool("backupdb", false, "program")

	flag.Parse()

	if *serverOpt == true {
		if *dbPath == "" {
			fmt.Println("No db path !")
		} else {
			server.NewServer(*dbPath, *portNumber)
		}
		os.Exit(0)
	}

	if *searchOpt == true {

		if *dbPath == "" {
			fmt.Println("No db path !")
		} else if *filePath == "" && *sequenceString == "" {
			fmt.Println("Need a sequence of file input !")
		} else {
			if *filePath != "" {
				search.NewSearch(*dbPath, *filePath, 0)
			} else if *sequenceString != "" {
				search.NewSearch(*dbPath, *sequenceString, 1)
			}

		}

		os.Exit(0)
	}

	if *makedbOpt == true {

		if *dbPath == "" {
			fmt.Println("No db path !")
		} else {
			makedb.NewMakedb(*dbPath, *inputPath, *fullDb, *makedbOffset, *makedbLenght)
		}

		os.Exit(0)
	}

	// if *mergedbOpt == true {
	//	if *dbsPath == "" || *outPath == "" {
	//		fmt.Println("Need to have a valid databases path !")
	//	} else {
	//		mergedb.NewMergedb(*dbsPath, *outPath)
	//	}
	//	os.Exit(0)
	// }

	// if *backupdbOpt == true {
	//	if *dbPath == "" {
	//		fmt.Println("Need to have a valid databases path !")
	//	} else if *outPath == "" {
	//		fmt.Println("Need to have a valid backup directory path !")
	//	} else {
	//		backupdb.Backupdb(*dbPath, *outPath)
	//	}
	//	os.Exit(0)
	// }

	// if *analyseOpt == true {
	//	fmt.Println(*dbPath)
	//	fmt.Println("analyse..")
	//	os.Exit(0)
	// }

	fmt.Println(usage)
	os.Exit(0)

}
