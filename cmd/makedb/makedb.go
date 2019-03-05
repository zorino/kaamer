package makedb

import (
	"bufio"
	"log"
	"fmt"
	"github.com/zorino/metaprot/internal"
	"github.com/zorino/metaprot/cmd/downloaddb"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Protein struct {
	Entry            string
	Status           string  // reviewed / unreviewed
	ProteinName      string  // n_store
	TaxonomicLineage string  // o_store
	GeneOntology     string  // g_store
	FunctionCC       string  // f_store
	Pathway          string  // p_store
	EC_Number        string
	Sequence         string  // k_store
}


func NewMakedb(dbPath string, inputPath string, kmerSize int) {

	// For SSD throughput (as done in badger/graphdb) see :
	// https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion
	runtime.GOMAXPROCS(128)

	fi, err := os.Stat(inputPath)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	files := []string{}

	switch mode := fi.Mode(); {
	case mode.IsDir():
		// do directory stuff
		files, _ = filepath.Glob(inputPath + "/*.tsv")
	case mode.IsRegular():
		// do file stuff
		files = append(files, inputPath)
	}

	if len(files) == 0 {
		download_db.Download(inputPath)
		os.Exit(0)
	}

	os.Mkdir(dbPath, 0700)

	threadByWorker := runtime.NumCPU()*2/len(files)
	if threadByWorker < 1 {
		threadByWorker = 1
	}

	wgDB := new(sync.WaitGroup)
	wgDB.Add(len(files))

	kvStores_holder := make([]*kvstore.KVStores, len(files))

	for i, file := range files {

		_dbPath := dbPath + fmt.Sprintf("/store_%d",i)
		os.Mkdir(_dbPath, 0700)

		fmt.Printf("# Making Database %s from %s\n", _dbPath, file)

		kvStores := kvstore.KVStoresNew(_dbPath, threadByWorker)
		kvStores_holder[i] = kvStores

		go func(file string, dbPath string, i int, threadByWorker int, kvStores *kvstore.KVStores) {

			defer wgDB.Done()
			run(file, kmerSize, kvStores, threadByWorker)

		}(file, dbPath, i, threadByWorker, kvStores)

	}

	wgDB.Wait()

	for i, _ := range files {
		kvStores_holder[i].Flush()
		kvStores_holder[i].MergeKmerValues(runtime.NumCPU())
		kvStores_holder[i].Close()
	}

	// i := 10000
	// err = kvStores_holder[0].KK_batch.DB.RunValueLogGC(0.0001)
	// for err != nil && i > 0 {
	//	err = kvStores_holder[0].KK_batch.DB.RunValueLogGC(0.0001)
	//	i--
	// }

	// for i, _ := range files {
	//	kvStores_holder[i].Close()
	// }

	// DEBUG print Kmer store
	// for i, _ := range files {
	//	_dbPath := dbPath + fmt.Sprintf("/store_%d", i)
	//	kvStores := kvstore.KVStoresNew(_dbPath, 1)
	//	PrintKStore(kvStores)
	//	kvStores.Close()
	// }

}

func run(fileName string, kmerSize int, kvStores *kvstore.KVStores, nbThreads int) int {

	file, _ := os.Open(fileName)

	jobs := make(chan string, 100)
	results := make(chan int, 100)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBuffer(jobs, results, wg, kmerSize, kvStores, w-1)
	}

	// Go over a file line by line and queue up a ton of work
	go func() {
		scanner := bufio.NewScanner(file)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			jobs <- scanner.Text()
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
	for v := range results {
		counts += v
		if counts % 10000 == 0 {
			fmt.Printf("Processed %d proteins in %f minutes\n", counts, time.Since(timeStart).Minutes())
		}
	}

	return counts

}

func readBuffer(jobs <-chan string, results chan<- int, wg *sync.WaitGroup, kmerSize int, kvStores *kvstore.KVStores, threadId int) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInput(j, kmerSize, kvStores, threadId)
		results <- 1
	}

}

func processProteinInput(line string, kmerSize int, kvStores *kvstore.KVStores, threadId int) {

	s := strings.Split(line, "\t")

	if len(s) < 9 {
		return
	}

	c := Protein{}
	c.Entry = s[0]
	c.Status = s[1]
	c.ProteinName = s[2]
	c.TaxonomicLineage = s[3]
	c.GeneOntology = s[4]
	c.FunctionCC = s[5]
	c.Pathway = s[6]
	c.EC_Number = s[7]
	c.Sequence = s[8]

	// skip peptide shorter than kmerSize
	if len(c.Sequence) < kmerSize {
		return
	}

	// sliding windows of kmerSize on Sequence
	for i := 0; i < len(c.Sequence)-kmerSize+1; i++ {

		key := kvStores.K_batch.CreateBytesKey(c.Sequence[i:i+kmerSize])

		var isNewValue = false

		newValues := [5][]byte{nil,nil,nil,nil,nil}

		// Gene Ontology
		if gVal, new := kvStores.G_batch.CreateValues(c.GeneOntology, newValues[0], kvStores.GG_batch, threadId); new {
			isNewValue = isNewValue || new
			newValues[0] = gVal
		}

		// Protein Function
		if fVal, new := kvStores.F_batch.CreateValues(c.FunctionCC, newValues[1], kvStores.FF_batch, threadId); new {
			isNewValue = isNewValue || new
			newValues[1] = fVal
		}

		// Protein Pathway
		if pVal, new := kvStores.P_batch.CreateValues(c.Pathway, newValues[2], kvStores.PP_batch, threadId); new {
			isNewValue = isNewValue || new
			newValues[2] = pVal
		}

		// Protein Organism
		if oVal, new := kvStores.O_batch.CreateValues(c.TaxonomicLineage, newValues[3], kvStores.OO_batch, threadId); new {
			isNewValue = isNewValue || new
			newValues[3] = oVal
		}

		// Protein Name
		if nVal, new := kvStores.N_batch.CreateValues(c.ProteinName, newValues[4], kvStores.NN_batch, threadId); new {
			isNewValue = isNewValue || new
			newValues[4] = nVal
		}

		if isNewValue {
			combinedKey, combinedVal := kvStores.KK_batch.CreateValues(newValues[:], false)
			kvStores.KK_batch.AddValue(combinedKey, combinedVal, threadId)
			kvStores.K_batch.AddValue(key, combinedKey, threadId)
		}

	}

	// fmt.Printf("%#v done\n", c.Entry)

}
