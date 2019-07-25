package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/options"
	"github.com/zorino/kaamer/api"
	"github.com/zorino/kaamer/pkg/backupdb"
	"github.com/zorino/kaamer/pkg/gcdb"
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
      -tmp          tmp folder for query inport (default /tmp)

  // Database

  -downloadb        download kaamer database
    (input)
      -o            output directory of database
    (flag)
      -m            kaamer release database
      -r            raw UniprotKB database (to use with -makedb)

  -makedb           make the protein database
    (input)
      -i            input tsv file (raw tsv file from -downloaddb -r)
      -d            badger database directory (output)
      -offset       start processing raw uniprot file at protein number x
      -length       process x number of proteins (-1 == infinity)
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -full         to make a full database (default is the light version)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)

  -mergedb          merge 2 databases made with makedb
    (input)
      -dbs          databases directory
      -o            output directory of merged database
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -indexOnly    only create the kCombination store
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)

  -gcdb             run garbage collection on database
    (input)
      -d            database directory
      -it           number of GC iterations
      -ratio        number of ratio of the GC (between 0-1)
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)

  -backupdb         backup database
    (input)
      -d            badger db directory
      -o            badger backup output directory
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage

  -restoredb        restore a backup database
    (input)
      -d            badger backup db directory
      -o            badger db output directory
      -tableMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
      -valueMode    (fileio, memorymap) default memorymap / fileio decreases memory usage
    (flag)
      -maxsize      will maximize the size of tables (.sst) and vlog (.log) files
                    (to limit the number of open files)


`

	var serverOpt = flag.Bool("server", false, "program")
	var portNumber = flag.Int("p", 8321, "port argument")
	var nbThreads = flag.Int("t", 0, "number of threads")
	var tmpFolder = flag.String("tmp", "/tmp/", "tmp folder for query import")

	var makedbOpt = flag.Bool("makedb", false, "program")
	var inputPath = flag.String("i", "", "db path argument")
	var dbPath = flag.String("d", "", "db path argument")
	var fullDb = flag.Bool("full", false, "to build full database")
	var makedbOffset = flag.Uint("offset", 0, "offset to process raw file")
	var makedbLenght = flag.Uint("length", uint(MaxInt), "process x number of files")
	var maxSize = flag.Bool("maxsize", false, "to maximize badger output file size")
	var tableMode = flag.String("tablemode", "memorymap", "table loading mode (fileio, memorymap)")
	var valueMode = flag.String("valuemode", "memorymap", "value loading mode (fileio, memorymap)")

	// var downloadOpt = flag.Bool("downloaddb", false, "download uniprotkb or kaamer db")
	// var rawDbOpt = flag.Bool("r", false, "for uniprotkb raw database")

	var mergedbOpt = flag.Bool("mergedb", false, "program")
	var dbsPath = flag.String("dbs", "", "db path argument")
	var outPath = flag.String("o", "", "db path argument")
	var indexOnly = flag.Bool("indexOnly", false, "indexing mode")

	var gcOpt = flag.Bool("gcdb", false, "program")
	var gcIteration = flag.Int("it", 100, "number of GC iterations")
	var gcRatio = flag.Float64("ratio", 0.5, "ratio for GC")

	var backupdbOpt = flag.Bool("backupdb", false, "program")

	var restoreOpt = flag.Bool("restoredb", false, "program")

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

	if *makedbOpt == true {

		if *dbPath == "" {
			fmt.Println("No db path !")
		} else {
			makedb.NewMakedb(*dbPath, *inputPath, *fullDb, *makedbOffset, *makedbLenght, *maxSize, tableLoadingMode, valueLoadingMode)
		}

		os.Exit(0)
	}

	if *mergedbOpt == true {
		if *dbsPath == "" || *outPath == "" {
			fmt.Println("Need to have a valid databases path !")
		} else {
			mergedb.NewMergedb(*dbsPath, *outPath, *maxSize, tableLoadingMode, valueLoadingMode, *indexOnly)
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
