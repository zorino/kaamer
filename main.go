package main

import (
	"flag"
	"fmt"
	"github.com/zorino/metaprot/cmd/makedb"
	"github.com/zorino/metaprot/cmd/mergedb"
	"github.com/zorino/metaprot/cmd/backupdb"
	"os"
)

func main() {

	usage := `
 metaprot

  // Analyses

	   -server       start a metaprot server - database

		  (input)
			   -d    database directory



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

	   -mergedb      merge metaprot badger databases

		  (input)
			   -dbs  badger databases directory
			   -o    merged database output

	   -backupdb     backup database

		  (input)
			   -d    badger db directory
			   -o    badger backup output directory

`

	// var serverOpt = flag.Bool("server", false, "program")

	var makedbOpt = flag.Bool("makedb", false, "program")
	var inputPath = flag.String("i", "", "db path argument")
	var dbPath = flag.String("d", "", "db path argument")

	var mergedbOpt = flag.Bool("mergedb", false, "program")
	var dbsPath = flag.String("dbs", "", "db path argument")
	var outPath = flag.String("o", "", "db path argument")

	var analyseOpt = flag.Bool("analyse", false, "program")

	var backupdbOpt = flag.Bool("backupdb", false, "program")

	flag.Parse()

	if *makedbOpt == true {

		if *inputPath == "" {
			fmt.Println("No input file path !")
		} else if *dbPath == "" {
			fmt.Println("No db path !")
		} else {
			makedb.NewMakedb(*dbPath, *inputPath)
		}

		os.Exit(0)
	}

	if *mergedbOpt == true {
		if *dbsPath == "" || *outPath == "" {
			fmt.Println("Need to have a valid databases path !")
		} else {
			mergedb.NewMergedb(*dbsPath, *outPath)
		}
		os.Exit(0)
	}

	if *backupdbOpt == true {
		if *dbPath == "" {
			fmt.Println("Need to have a valid databases path !")
		} else if *outPath == "" {
			fmt.Println("Need to have a valid backup directory path !")
		} else {
			backupdb.Backupdb(*dbPath, *outPath)
		}
		os.Exit(0)
	}

	if *analyseOpt == true {
		fmt.Println(*dbPath)
		fmt.Println("analyse..")
		os.Exit(0)
	}

	fmt.Println(usage)
	os.Exit(0)

}
