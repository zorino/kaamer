package main

import (
	"flag"
	"fmt"
	"github.com/zorino/metaprot/cmd/makedb"
	"os"
)

func main() {

	usage := `
 metaprot

       -makedb       make the protein database

               -i    input protein tsv files path
               -d    badger database dir path
               -k    kmer size (default: 11)


`

	var makedbOpt = flag.Bool("makedb", false, "program")

	var analyseOpt = flag.Bool("analyse", false, "program")

	var dbPath = flag.String("d", "./badger", "db path argument")
	var inputPath = flag.String("i", "", "db path argument")
	var kmerSize = flag.Int("k", 11, "kmer size argument")

	flag.Parse()

	if *makedbOpt == true {
		// fmt.Println(*dbPath)
		// fmt.Println(*inputPath)
		// fmt.Println(*kmerSize)
		if *inputPath == "" {
			fmt.Println("No input file path !")
		}
		makedb.NewMakedb(*dbPath, *inputPath, *kmerSize)
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
