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

	   -makedb       make the protein database

			   -i    input tsv file (raw tsv file from -downloaddb -r)
			   -d    badger database directory

	   -mergedb      merge two metaprot badger databases

			   -d1   badger db 1 directory
			   -d2   badger db 2 directory

	   -backupdb     backup database

			   -d    badger db directory
			   -o    badger backup output directory
`

	var makedbOpt = flag.Bool("makedb", false, "program")
	var inputPath = flag.String("i", "", "db path argument")
	var dbPath = flag.String("d", "", "db path argument")

	var mergedbOpt = flag.Bool("mergedb", false, "program")
	var dbPath_1 = flag.String("d1", "", "db path argument")
	var dbPath_2 = flag.String("d2", "", "db path argument")

	var analyseOpt = flag.Bool("analyse", false, "program")

	var backupdbOpt = flag.Bool("backupdb", false, "program")
	var backupOutput = flag.String("o", "", "db path argument")

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
		if *dbPath_1 == "" {
			fmt.Println("Need to have a valid databases path !")
		} else {
			mergedb.NewMergedb(*dbPath_1, *dbPath_2)
		}
		os.Exit(0)
	}

	if *backupdbOpt == true {
		if *dbPath == "" {
			fmt.Println("Need to have a valid databases path !")
		} else if *backupOutput == "" {
			fmt.Println("Need to have a valid backup directory path !")
		} else {
			backupdb.Backupdb(*dbPath, *backupOutput)
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
