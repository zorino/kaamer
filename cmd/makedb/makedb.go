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

	// DEBUG print Kmer store
	// for i, _ := range files {
	// 	_dbPath := dbPath + fmt.Sprintf("/store_%d", i)
	// 	kvStores := kvstore.KVStoresNew(_dbPath, 1)
	// 	PrintKStore(kvStores)
	// 	kvStores.Close()
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



func MergeValues (kvStores *kvstore.KVStores) {


	fmt.Println("# Merging Key Values...")

	// Stream keys
	stream := kvStores.K_batch.DB.NewStream()

	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	// stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.
	stream.LogPrefix = ""

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		nbOfItem := 0

		kvList := new(pb.KVList)

		currentKey := []byte{}
		valueList := [][]byte{}

		// kvList.Kv = new([]*pb.KV)

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if ! bytes.Equal(key, item.Key()) {
				break
			}

			currentKey = item.KeyCopy(currentKey)

			val := []byte{}
			val, err := item.ValueCopy(val)
			if err != nil {
				log.Fatal(err.Error())
			}
			if item.DiscardEarlierVersions() {
				break
			}

			valueList = append(valueList, val)

			nbOfItem += 1

		}

		if nbOfItem > 1 {

			// merge values
			if combKey, _, isNew := MergeKmerValues(kvStores, currentKey, valueList); isNew {
				kvStores.K_batch.AddValueWithDiscardVersions(currentKey, combKey)
			} else {
				kvStores.K_batch.AddValueWithDiscardVersions(currentKey, combKey)
			}

		}

		return kvList, nil

	}

	// -- End of optional settings.


	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = func(list *pb.KVList) error {
		return nil
	}

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.

}




func MergeKmerValues (kvStores *kvstore.KVStores, key []byte, values [][]byte) ([]byte, []byte, bool) {

	newValueIds := [][]byte{}
	uniqueValues := kvstore.RemoveDuplicatesFromSlice(values)

	if (len(uniqueValues) < 2) {
		return key, nil, false
	}

	g_values := make(map[string]bool)
	f_values := make(map[string]bool)
	p_values := make(map[string]bool)
	o_values := make(map[string]bool)
	n_values := make(map[string]bool)

	for _, value := range uniqueValues {
		val, _ := kvStores.KK_batch.GetValueFromBadger(value)
		i := 0
		g_values[string(val[(i)*20:(i+1)*20])] = true
		i += 1
		f_values[string(val[(i)*20:(i+1)*20])] = true
		i += 1
		p_values[string(val[(i)*20:(i+1)*20])] = true
		i += 1
		o_values[string(val[(i)*20:(i+1)*20])] = true
		i += 1
		n_values[string(val[(i)*20:(i+1)*20])] = true
	}

	i := 0
	g_CombKeys := make([][]byte, len(g_values))
	for k, _ := range g_values {
		g_CombKeys[i] = []byte(k)
		i++
	}
	i = 0
	f_CombKeys := make([][]byte, len(f_values))
	for k, _ := range f_values {
		f_CombKeys[i] = []byte(k)
		i++
	}
	i = 0
	p_CombKeys := make([][]byte, len(p_values))
	for k, _ := range p_values {
		p_CombKeys[i] = []byte(k)
		i++
	}
	i = 0
	o_CombKeys := make([][]byte, len(o_values))
	for k, _ := range o_values {
		o_CombKeys[i] = []byte(k)
		i++
	}
	i = 0
	n_CombKeys := make([][]byte, len(n_values))
	for k, _ := range n_values {
		n_CombKeys[i] = []byte(k)
		i++
	}

	if len(g_CombKeys) > 1 {
		newValueIds = append(newValueIds, kvStores.GG_batch.MergeCombinationKeys(g_CombKeys, 0))
	} else {
		newValueIds = append(newValueIds, g_CombKeys[0])
	}
	if len(f_CombKeys) > 1 {
		newValueIds = append(newValueIds, kvStores.FF_batch.MergeCombinationKeys(f_CombKeys, 0))
	} else {
		newValueIds = append(newValueIds, f_CombKeys[0])
	}
	if len(p_CombKeys) > 1 {
		newValueIds = append(newValueIds, kvStores.PP_batch.MergeCombinationKeys(p_CombKeys, 0))
	} else {
		newValueIds = append(newValueIds, p_CombKeys[0])
	}
	if len(o_CombKeys) > 1 {
		newValueIds = append(newValueIds, kvStores.OO_batch.MergeCombinationKeys(o_CombKeys, 0))
	} else {
		newValueIds = append(newValueIds, o_CombKeys[0])
	}
	if len(n_CombKeys) > 1 {
		newValueIds = append(newValueIds, kvStores.NN_batch.MergeCombinationKeys(n_CombKeys, 0))
	} else {
		newValueIds = append(newValueIds, n_CombKeys[0])
	}

	newKey, newVal := kvstore.CreateHashValue(newValueIds, false)
	kvStores.KK_batch.AddValueWithDiscardVersions(newKey, newVal)

	return newKey, newVal, true

}




func PrintKStore (kvStores *kvstore.KVStores) {

	// Stream keys
	stream := kvStores.K_batch.DB.NewStream()

	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	// stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.
	stream.LogPrefix = ""

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if ! bytes.Equal(key, item.Key()) {
				break
			}

			val := []byte{}
			val, err := item.ValueCopy(val)
			if err != nil {
				log.Fatal(err.Error())
			}
			if item.DiscardEarlierVersions() {
				kmer := kvStores.K_batch.DecodeKmer(item.KeyCopy(nil))
				fmt.Printf("Kmer=%s\tvalue=%x\n", kmer, string(val))
				break
			} else {
				kmer := kvStores.K_batch.DecodeKmer(item.KeyCopy(nil))
				fmt.Printf("Kmer=%s\tvalue=%x\n", kmer, string(val))
			}

		}

		return nil, nil
	}


	stream.Send = func(list *pb.KVList) error {
		return nil
	}

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

}

func PrintHStore (kvStore *kvstore.H_) {

	// Stream keys
	stream := kvStore.DB.NewStream()

	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16                     // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	// stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.
	stream.LogPrefix = ""

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			if ! bytes.Equal(key, item.Key()) {
				break
			}

			val := []byte{}
			val, errVal := item.ValueCopy(val)
			if errVal != nil {
				log.Fatal(errVal.Error())
			}
			key := []byte{}
			key = item.KeyCopy(key)

			// fmt.Printf("Kmer=%s\tvalue=%x\n", kmer, val)
			fmt.Printf("Key=%x\tValue=%x\n", string(key), string(val))

		}


		return nil, nil
	}


	stream.Send = func(list *pb.KVList) error {
		return nil
	}

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

}
