package makedb

import (
	"bufio"
	"fmt"
	"github.com/zorino/metaprot/internal"
	"github.com/zorino/metaprot/cmd/downloaddb"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"encoding/hex"
	"encoding/binary"
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

// # Stores :
//                k_   ->   peptide kmers => [g_key, f_key, p_key, o_key]
//                g_   ->   gene ontology
//                f_   ->   function (uniprot)
//                p_   ->   pathway
//                o_   ->   taxonomic lineage
//
//
//  Each store uses a combination pattern to reduce its size (flyweight design pattern)
//  '.' prefix are for real keys and '_' prefix for combination keys
//  Combination keys are SHA1SUM of the content
//  Example :
//              '.MSAVALPRVSG' => '_213a326b89b'
//              '_213a326b89b' => '[g_key,f_key,p_key,o_key]'
//

type KVStores struct {
	k_batch         *kvstore.K_
	g_batch         *kvstore.G_
	f_batch         *kvstore.F_
	p_batch         *kvstore.P_
	o_batch         *kvstore.O_
}

func (kvStores *KVStores) Init (dbPath string) {

	k_opts := badger.DefaultOptions
	k_opts.Dir = dbPath+"/k_store"
	k_opts.ValueDir = dbPath+"/k_store"
	k_opts.ValueLogLoadingMode = options.FileIO
	k_opts.TableLoadingMode = options.MemoryMap

	g_opts := badger.DefaultOptions
	g_opts.Dir = dbPath+"/g_store"
	g_opts.ValueDir = dbPath+"/g_store"
	g_opts.ValueLogLoadingMode = options.FileIO
	g_opts.TableLoadingMode = options.MemoryMap

	f_opts := badger.DefaultOptions
	f_opts.Dir = dbPath+"/f_store"
	f_opts.ValueDir = dbPath+"/f_store"
	f_opts.ValueLogLoadingMode = options.FileIO
	f_opts.TableLoadingMode = options.MemoryMap

	p_opts := badger.DefaultOptions
	p_opts.Dir = dbPath+"/p_store"
	p_opts.ValueDir = dbPath+"/p_store"
	p_opts.ValueLogLoadingMode = options.FileIO
	p_opts.TableLoadingMode = options.MemoryMap

	o_opts := badger.DefaultOptions
	o_opts.Dir = dbPath+"/o_store"
	o_opts.ValueDir = dbPath+"/o_store"
	o_opts.ValueLogLoadingMode = options.FileIO
	o_opts.TableLoadingMode = options.MemoryMap

	kvStores.k_batch = kvstore.K_New(k_opts, 1000)
	kvStores.g_batch = kvstore.G_New(g_opts, 1000)
	kvStores.f_batch = kvstore.F_New(f_opts, 1000)
	kvStores.p_batch = kvstore.P_New(p_opts, 1000)
	kvStores.o_batch = kvstore.O_New(o_opts, 1000)
}

func (kvStores *KVStores) Close () {
	// Last DB flushes
	kvStores.k_batch.Close()
	kvStores.g_batch.Close()
	kvStores.f_batch.Close()
	kvStores.p_batch.Close()
	kvStores.o_batch.Close()
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

			kvStores := new(KVStores)
			kvStores.Init(_dbPath)

			run(file, kmerSize, kvStores, threadByWorker)

			kvStores.Close()
		}(file, dbPath, i, threadByWorker)

	}

	wgDB.Wait()

	// // Testing
	// kvStores := new(KVStores)
	// kvStores.Init(dbPath+"/store_0")
	// PrintKStore(kvStores)
	// kvStores.Close()

}

func run(fileName string, kmerSize int, kvStores *KVStores, nbThreads int) int {

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

func readBuffer(jobs <-chan string, results chan<- int, wg *sync.WaitGroup, kmerSize int, kvStores *KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInput(j, kmerSize, kvStores)
	}
	results <- 1

}

func processProteinInput(line string, kmerSize int, kvStores *KVStores) {

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

		key := kvStores.k_batch.CreateBytesKey(c.Sequence[i:i+kmerSize])

		var isNewValue = false
		var currentValue []byte

		newValues := [4][]byte{nil,nil,nil,nil}
		kvStores.k_batch.Mu.Lock()
		__val, ok := kvStores.k_batch.GetValue(key)

		// Old value found
		if ok {
			_val, _ := kvStores.k_batch.GetValue(__val)
			currentValue = append([]byte{}, _val...)

			for i, _ := range newValues {
				newValues[i] = currentValue[(i)*20:(i+1)*20]
			}
		} else {
			isNewValue = true
		}

		// Gene Ontology
		if gVal, new := kvStores.g_batch.CreateValues(c.GeneOntology, newValues[0]); new {
			isNewValue = isNewValue || new
			newValues[0] = gVal
		}

		// Protein Function
		if fVal, new := kvStores.f_batch.CreateValues(c.FunctionCC, newValues[1]); new {
			isNewValue = isNewValue || new
			newValues[1] = fVal
		}

		// Protein Pathway
		if pVal, new := kvStores.p_batch.CreateValues(c.Pathway, newValues[2]); new {
			isNewValue = isNewValue || new
			newValues[2] = pVal
		}

		// Protein Organism
		if oVal, new := kvStores.o_batch.CreateValues(c.TaxonomicLineage, newValues[3]); new {
			isNewValue = isNewValue || new
			newValues[3] = oVal
		}

		if isNewValue {
			combinedKey, combinedVal := kvstore.CreateHashValue(newValues[:], false)
			kvStores.k_batch.AddValue(combinedKey, combinedVal)
			kvStores.k_batch.AddValue(key, combinedKey)
		}

		kvStores.k_batch.Mu.Unlock()

	}

	fmt.Printf("%#v done\n", c.Entry)

}


func PrintKStore (kvStores *KVStores) {

	kvStores.k_batch.DB.View(func(txn *badger.Txn) error {
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
					kmerInt := binary.BigEndian.Uint32(k)
					fmt.Printf("Kmer: key=%s, value=%s\n", kvStores.k_batch.DecodeKmer(kmerInt), val)
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
