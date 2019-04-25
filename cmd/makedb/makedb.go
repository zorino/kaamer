package makedb

import (
	"bufio"
	"compress/gzip"

	// "log"
	"fmt"
	"log"
	"regexp"

	"github.com/zorino/metaprot/pkg/kvstore"

	// "github.com/zorino/metaprot/cmd/downloaddb"
	"os"
	// "path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

const (
	KMER_SIZE = 7
)

var buildFullDB = false

func NewMakedb(dbPath string, inputPath string, isFullDb bool) {

	buildFullDB = isFullDb
	runtime.GOMAXPROCS(128)

	if inputPath == "" {
		Download(".")
		inputPath = "./uniprotkb.txt.gz"
	}

	os.Mkdir(dbPath, 0700)

	threadByWorker := runtime.NumCPU()

	if threadByWorker < 1 {
		threadByWorker = 1
	}

	fmt.Printf("# Making Database %s from %s\n", dbPath, inputPath)
	fmt.Printf("# Using %d CPU\n", threadByWorker)

	kvStores := kvstore.KVStoresNew(dbPath, threadByWorker)
	kvStores.OpenInsertChannel()
	run(inputPath, kvStores, threadByWorker)
	kvStores.CloseInsertChannel()
	kvStores.Close()

}

func run(fileName string, kvStores *kvstore.KVStores, nbThreads int) int {

	file, _ := os.Open(fileName)
	defer file.Close()

	jobs := make(chan string)
	results := make(chan int, 10)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBuffer(jobs, results, wg, kvStores)
	}

	// Go over a file line by line and queue up a ton of work
	go func() {
		gz, err := gzip.NewReader(file)
		defer gz.Close()
		if err != nil {
			log.Fatal(err)
		}
		scanner := bufio.NewScanner(gz)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		proteinEntry := ""
		line := ""
		for scanner.Scan() {
			line = scanner.Text()
			if line == "//" {
				jobs <- proteinEntry
				proteinEntry = ""
			} else {
				proteinEntry += line
				proteinEntry += "\n"
			}
		}
		close(jobs)
	}()

	// Collect all the results...
	// First, make sure we close the result channel when everything was processed
	go func() {
		wg.Wait()
		close(results)
	}()

	// Now, add up the results from the results channel until closed
	timeStart := time.Now()
	counts := 0
	wgGC := new(sync.WaitGroup)
	for v := range results {
		counts += v
		if counts%10000 == 0 {
			fmt.Printf("Processed %d proteins in %f minutes\n", counts, time.Since(timeStart).Minutes())
		}
		// Valuelog GC every 100K processed proteins
		if counts%100000 == 0 {
			wgGC.Wait()
			wgGC.Add(2)
			go func() {
				kvStores.KmerStore.GarbageCollect(1, 0.5)
				wgGC.Done()
			}()
			go func() {
				kvStores.ProteinStore.GarbageCollect(1, 0.5)
				wgGC.Done()
			}()
		}
	}
	wgGC.Wait()

	return counts

}

func readBuffer(jobs <-chan string, results chan<- int, wg *sync.WaitGroup, kvStores *kvstore.KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInput(j, kvStores)
		results <- 1
	}

}

func processProteinInput(textEntry string, kvStores *kvstore.KVStores) {

	protein := &kvstore.Protein{}
	reg := regexp.MustCompile(` \{.*\}\.`)
	var fields []string

	for _, l := range strings.Split(textEntry, "\n") {

		if len(l) < 2 {
			continue
		}
		switch l[0:2] {
		case "ID":
			protein.Entry = strings.Fields(l[5:])[0]
		case "DE":
			if strings.Contains(l[5:], "RecName") {
				protein.ProteinName = strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
			} else if strings.Contains(l[5:], "SubName") {
				if protein.ProteinName != "" {
					protein.ProteinName += ";;"
					protein.ProteinName += strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
				} else {
					protein.ProteinName = strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
				}
			} else if strings.Contains(l[5:], "EC=") {
				protein.EC = strings.TrimRight(reg.ReplaceAllString(l[17:], "${1}"), ";")
			}
		case "OS":
			protein.Organism = strings.TrimRight(l[5:], ".")
		case "OC":
			protein.Taxonomy += l[5:]
			if protein.Taxonomy != "" {
				protein.Taxonomy += " "
			}
			protein.Taxonomy += l[5:]
		case "DR":
			fields = strings.Fields(l[5:])
			switch fields[0] {
			case "KEGG;":
				protein.KEGG = append(protein.KEGG, strings.TrimRight(fields[1], ";"))
			case "GO;":
				protein.GO = append(protein.GO, strings.TrimRight(fields[1], ";"))
			case "BioCyc;":
				protein.BioCyc = append(protein.BioCyc, strings.TrimRight(fields[1], ";"))
			case "HAMAP;":
				protein.HAMAP = append(protein.HAMAP, strings.TrimRight(fields[1], ";"))
			}
		case "SQ":
			fields = strings.Fields(l[5:])
			len, _ := strconv.Atoi(fields[1])
			protein.Length = int32(len)

		case "  ":
			protein.Sequence += strings.ReplaceAll(l[5:], " ", "")
		}
	}

	missingFeature := !buildFullDB
	missingFeature = missingFeature && (protein.GetEC() == "")
	missingFeature = missingFeature && len(protein.GetGO()) < 1
	missingFeature = missingFeature && len(protein.GetBioCyc()) < 1
	missingFeature = missingFeature && len(protein.GetKEGG()) < 1
	missingFeature = missingFeature && len(protein.GetHAMAP()) < 1

	if missingFeature {
		return
	}

	data, err := proto.Marshal(protein)
	if err != nil {
		log.Fatal(err.Error())
	} else {
		kvStores.ProteinStore.AddValueToChannel([]byte(protein.Entry), data, false)
	}

	// newProt := &kvstore.Protein{}
	// err = proto.Unmarshal(data, newProt)
	// if err != nil {
	//	log.Fatal("unmarshaling error: ", err)
	// }

	// skip peptide shorter than kmerSize
	if protein.Length < KMER_SIZE {
		return
	}

	// sliding windows of kmerSize on Sequence
	for i := 0; i < int(protein.Length)-KMER_SIZE+1; i++ {
		kmerKey := kvStores.KmerStore.CreateBytesKey(protein.Sequence[i : i+KMER_SIZE])
		kvStores.KmerStore.AddValueToChannel(kmerKey, []byte(protein.Entry), false)
	}

}
