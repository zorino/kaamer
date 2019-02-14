package makedb

import (
	"bufio"
	"fmt"
	"github.com/zorino/metaprot/internal"
	"github.com/zorino/metaprot/cmd/downloaddb"
	"github.com/dgraph-io/badger"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"encoding/hex"
	"unicode/utf8"
)

// uniprotkb-bacteria (https://github.com/zorino/microbe-dbs)
type Protein struct {
	Entry            string
	Status           string  // reviewed ?= Swisprot
	ProteinNames     string
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

	// Glob all uniprot tsv files to be processed
	files, _ := filepath.Glob(inputPath + "/*.tsv")

	if len(files) == 0 {
		download_db.Download(inputPath)
		os.Exit(0)
	}

	os.Mkdir(dbPath, 0700)

	threadByWorker := runtime.NumCPU()/len(files)
	if threadByWorker < 1 {
		threadByWorker = 1
	}

	wgDB := new(sync.WaitGroup)
	wgDB.Add(len(files))

	for i, file := range files {

		go func(file string, dbPath string, i int, threadByWorker int) {
			defer wgDB.Done()
			fmt.Println(file)

			_dbPath := dbPath + fmt.Sprintf("/store_%d",i)

			os.Mkdir(_dbPath, 0700)

			kvStores := kvstore.KVStoresNew(_dbPath)

			run(file, kmerSize, kvStores, threadByWorker)

			kvStores.Close()
		}(file, dbPath, i, threadByWorker)

	}

	wgDB.Wait()

	// Testing
	kvStores := kvstore.KVStoresNew(dbPath+"/store_0")
	PrintKStore(kvStores)
	kvStores.Close()

}

func run(fileName string, kmerSize int, kvStores *kvstore.KVStores, nbThreads int) int {

	file, _ := os.Open(fileName)

	jobs := make(chan string)
	results := make(chan int)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBuffer(jobs, results, wg, kmerSize, kvStores)
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
	counts := 0
	for v := range results {
		counts += v
	}

	return counts

}

func readBuffer(jobs <-chan string, results chan<- int, wg *sync.WaitGroup, kmerSize int, kvStores *kvstore.KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInput(j, kmerSize, kvStores)
	}
	results <- 1

}

func processProteinInput(line string, kmerSize int, kvStores *kvstore.KVStores) {

	s := strings.Split(line, "\t")

	if len(s) < 9 {
		return
	}

	c := Protein{}
	c.Entry = s[0]
	c.Status = s[1]
	c.ProteinNames = s[2]
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
		var currentValue []byte

		newValues := [4][]byte{nil,nil,nil,nil}

		kvStores.K_batch.Mu.Lock()
		__val, ok := kvStores.K_batch.GetValue(key)

		// Old value found
		if ok {
			kvStores.KK_batch.Mu.Lock()
			_val, _ := kvStores.KK_batch.GetValue(__val)
			kvStores.KK_batch.Mu.Unlock()
			currentValue = _val
			for i, _ := range newValues {
				newValues[i] = currentValue[(i)*20:(i+1)*20]
			}
		} else {
			isNewValue = true
		}

		// Gene Ontology
		if gVal, new := kvStores.G_batch.CreateValues(c.GeneOntology, newValues[0], kvStores.GG_batch); new {
			isNewValue = isNewValue || new
			newValues[0] = gVal
		}

		// Protein Function
		if fVal, new := kvStores.F_batch.CreateValues(c.FunctionCC, newValues[1], kvStores.FF_batch); new {
			isNewValue = isNewValue || new
			newValues[1] = fVal
		}

		// Protein Pathway
		if pVal, new := kvStores.P_batch.CreateValues(c.Pathway, newValues[2], kvStores.PP_batch); new {
			isNewValue = isNewValue || new
			newValues[2] = pVal
		}

		// Protein Organism
		if oVal, new := kvStores.O_batch.CreateValues(c.TaxonomicLineage, newValues[3], kvStores.OO_batch); new {
			isNewValue = isNewValue || new
			newValues[3] = oVal
		}

		if isNewValue {
			kvStores.KK_batch.Mu.Lock()
			combinedKey, combinedVal := kvStores.KK_batch.CreateValues(newValues[:], false)
			kvStores.KK_batch.AddValue(combinedKey, combinedVal)
			kvStores.KK_batch.Mu.Unlock()
			kvStores.K_batch.AddValue(key, combinedKey)
		}

		kvStores.K_batch.Mu.Unlock()

	}

	fmt.Printf("%#v done\n", c.Entry)

}


func PrintKStore (kvStores *kvstore.KVStores) {

	kvStores.K_batch.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				val := hex.EncodeToString(v)
				if utf8.ValidString(string(v)) {
					val = string(v)
				}
				if len(val) < 41 {
					fmt.Printf("Kmer: key=%s, value=%s\n", kvStores.K_batch.DecodeKmer(k), val)
				} else {
					fmt.Printf("Hash: key=%s, value=%s\n", hex.EncodeToString(k), val)
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

}
