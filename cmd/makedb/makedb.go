package makedb

import (
	"bytes"
	"bufio"
	"fmt"
	"github.com/zorino/metaprot/internal"
	"github.com/zorino/metaprot/cmd/downloaddb"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"encoding/hex"
	"unicode/utf8"
	"log"
	"context"
	"time"
)

// uniprotkb-bacteria (https://github.com/zorino/microbe-dbs)
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

	kvStores_holder := make([]*kvstore.KVStores, len(files))

	for i, file := range files {

		_dbPath := dbPath + fmt.Sprintf("/store_%d",i)
		os.Mkdir(_dbPath, 0700)

		kvStores := kvstore.KVStoresNew(_dbPath, threadByWorker)
		kvStores_holder[i] = kvStores

		go func(file string, dbPath string, i int, threadByWorker int, kvStores *kvstore.KVStores) {

			defer wgDB.Done()
			fmt.Println(file)
			run(file, kmerSize, kvStores, threadByWorker)

		}(file, dbPath, i, threadByWorker, kvStores)

	}

	wgDB.Wait()

	for i, _ := range files {
		kvStores_holder[i].Flush()
		MergeValues(kvStores_holder[i])
		kvStores_holder[i].Close()
	}


}

func run(fileName string, kmerSize int, kvStores *kvstore.KVStores, nbThreads int) int {

	file, _ := os.Open(fileName)

	jobs := make(chan string, 3)
	results := make(chan int, 3)
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
		if counts % 100000 == 0 {
			fmt.Printf("Processed %d proteins in %f seconds\n", counts, time.Since(timeStart).Minutes())
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



func MergeValues (kvStores *kvstore.KVStores) {


	// Stream keys
	stream := kvStores.K_batch.DB.NewStream()

	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		nbOfItem := 0

		kvList := new(pb.KVList)

		// kvList.Kv = new([]*pb.KV)

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if ! bytes.Equal(key, item.Key()) {
				break
			}

			kmer := kvStores.K_batch.DecodeKmer(item.KeyCopy(nil))
			val := []byte{}
			val, err := item.ValueCopy(val)
			if err != nil {
				log.Fatal(err.Error())
			}
			if item.DiscardEarlierVersions() {
				break
			}

			kvNew := new(pb.KV)
			kvNew.Key = []byte(kmer)
			kvNew.Value = val

			kvList.Kv = append(kvList.Kv, kvNew)
			nbOfItem += 1

			// _val, _ := kvStores.KK_batch.GetValue(val)
			// fmt.Printf("Kmer=%s\tvalue=%x\n", kmer, val)

		}

		if nbOfItem > 1 {

			// merge values

		}

		return nil, nil
	}

	// stream.KeyToList = nil

	// -- End of optional settings.


	// Send is called serially, while Stream.Orchestrate is running.

	stream.Send = func(list *pb.KVList) error {
		// for _, kv := range list.GetKv() {
		// 	// kv.GetKey()
		// 	kmer := kvStores.K_batch.DecodeKmer(kv.GetKey())
		// 	fmt.Printf("Kmer=%s\tvalue=%x\n", kmer, kv.GetValue())
		// }
		return nil
	}

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.

	// kvStores.Close()


}




func MergeKmerValues (kvStores *kvstore.KVStores, key []byte, values [][]byte) (value []byte) {

	// uniqueValues := kvstore.RemoveDuplicatesFromSlice(values)

	// for i, _ := range uniqueValues {
	// 	newValues[i] = currentValue[(i)*20:(i+1)*20]
	// }

	return []byte{}

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
