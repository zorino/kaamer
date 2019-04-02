package kvstore

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	"log"
	"math"
	"sync"
)

// # Stores :
//                k_   ->   peptide kmers => [gg_key, ff_key, pp_key, oo_key]
//                kk_  ->   k_ combination store
//                g_   ->   gene ontology
//                gg_  ->   g_ combination store
//                f_   ->   function (uniprot)
//                ff_  ->   f_ combination store
//                p_   ->   pathway
//                pp_  ->   p_ combination store
//                o_   ->   taxonomic lineage
//                oo_  ->   o_ combination store
//                n_   ->   protein name
//                nn_  ->   n_ combination store
//
//
//  Each store uses a combination pattern to reduce its size (flyweight design pattern)
//  '.' prefix are for real keys and '_' prefix for combination keys
//  Combination keys are SHA1SUM of the content
//  Example :
//              '.MSAVALPRVSG' => '_213a326b89b'
//              '_213a326b89b' => '[g_key,f_key,p_key,o_key]'
//

type KVsToMerge struct {
	Key    []byte
	Values [][]byte
}

type KVStores struct {
	K_batch  *K_
	KK_batch *H_
	G_batch  *G_
	GG_batch *H_
	F_batch  *F_
	FF_batch *H_
	P_batch  *P_
	PP_batch *H_
	O_batch  *O_
	OO_batch *H_
	N_batch  *N_
	NN_batch *H_
}

func KVStoresNew(dbPath string, nbOfThreads int) *KVStores {

	var kvStores KVStores

	k_opts := badger.DefaultOptions
	k_opts.Dir = dbPath + "/k_store"
	k_opts.ValueDir = dbPath + "/k_store"
	k_opts.ValueLogLoadingMode = options.MemoryMap
	k_opts.TableLoadingMode = options.MemoryMap
	//k_opts.SyncWrites = false
	k_opts.NumVersionsToKeep = math.MaxUint32
	k_opts.MaxTableSize = 768 << 20
	k_opts.ValueLogMaxEntries = 100000000

	kk_opts := badger.DefaultOptions
	kk_opts.Dir = dbPath + "/kk_store"
	kk_opts.ValueDir = dbPath + "/kk_store"
	kk_opts.ValueLogLoadingMode = options.MemoryMap
	kk_opts.TableLoadingMode = options.MemoryMap
	//kk_opts.SyncWrites = false
	kk_opts.NumVersionsToKeep = 1
	kk_opts.MaxTableSize = 768 << 20
	kk_opts.ValueLogMaxEntries = 100000000

	g_opts := badger.DefaultOptions
	g_opts.Dir = dbPath + "/g_store"
	g_opts.ValueDir = dbPath + "/g_store"
	g_opts.ValueLogLoadingMode = options.MemoryMap
	g_opts.TableLoadingMode = options.MemoryMap
	//g_opts.SyncWrites = false
	g_opts.NumVersionsToKeep = 1
	g_opts.MaxTableSize = 768 << 20
	g_opts.ValueLogMaxEntries = 100000000

	gg_opts := badger.DefaultOptions
	gg_opts.Dir = dbPath + "/gg_store"
	gg_opts.ValueDir = dbPath + "/gg_store"
	gg_opts.ValueLogLoadingMode = options.MemoryMap
	gg_opts.TableLoadingMode = options.MemoryMap
	//gg_opts.SyncWrites = false
	gg_opts.NumVersionsToKeep = 1
	gg_opts.MaxTableSize = 768 << 20
	gg_opts.ValueLogMaxEntries = 100000000

	f_opts := badger.DefaultOptions
	f_opts.Dir = dbPath + "/f_store"
	f_opts.ValueDir = dbPath + "/f_store"
	f_opts.ValueLogLoadingMode = options.MemoryMap
	f_opts.TableLoadingMode = options.MemoryMap
	//f_opts.SyncWrites = false
	f_opts.NumVersionsToKeep = 1
	f_opts.MaxTableSize = 768 << 20
	f_opts.ValueLogMaxEntries = 100000000

	ff_opts := badger.DefaultOptions
	ff_opts.Dir = dbPath + "/ff_store"
	ff_opts.ValueDir = dbPath + "/ff_store"
	ff_opts.ValueLogLoadingMode = options.MemoryMap
	ff_opts.TableLoadingMode = options.MemoryMap
	//ff_opts.SyncWrites = false
	ff_opts.NumVersionsToKeep = 1
	ff_opts.MaxTableSize = 768 << 20
	ff_opts.ValueLogMaxEntries = 100000000

	p_opts := badger.DefaultOptions
	p_opts.Dir = dbPath + "/p_store"
	p_opts.ValueDir = dbPath + "/p_store"
	p_opts.ValueLogLoadingMode = options.MemoryMap
	p_opts.TableLoadingMode = options.MemoryMap
	//p_opts.SyncWrites = false
	p_opts.NumVersionsToKeep = 1
	p_opts.MaxTableSize = 768 << 20
	p_opts.ValueLogMaxEntries = 100000000

	pp_opts := badger.DefaultOptions
	pp_opts.Dir = dbPath + "/pp_store"
	pp_opts.ValueDir = dbPath + "/pp_store"
	pp_opts.ValueLogLoadingMode = options.MemoryMap
	pp_opts.TableLoadingMode = options.MemoryMap
	//pp_opts.SyncWrites = false
	pp_opts.NumVersionsToKeep = 1
	pp_opts.MaxTableSize = 768 << 20
	pp_opts.ValueLogMaxEntries = 100000000

	o_opts := badger.DefaultOptions
	o_opts.Dir = dbPath + "/o_store"
	o_opts.ValueDir = dbPath + "/o_store"
	o_opts.ValueLogLoadingMode = options.MemoryMap
	o_opts.TableLoadingMode = options.MemoryMap
	//o_opts.SyncWrites = false
	o_opts.NumVersionsToKeep = 1
	o_opts.MaxTableSize = 768 << 20
	o_opts.ValueLogMaxEntries = 100000000

	oo_opts := badger.DefaultOptions
	oo_opts.Dir = dbPath + "/oo_store"
	oo_opts.ValueDir = dbPath + "/oo_store"
	oo_opts.ValueLogLoadingMode = options.MemoryMap
	oo_opts.TableLoadingMode = options.MemoryMap
	//oo_opts.SyncWrites = false
	oo_opts.NumVersionsToKeep = 1
	oo_opts.MaxTableSize = 768 << 20
	oo_opts.ValueLogMaxEntries = 100000000

	n_opts := badger.DefaultOptions
	n_opts.Dir = dbPath + "/n_store"
	n_opts.ValueDir = dbPath + "/n_store"
	n_opts.ValueLogLoadingMode = options.MemoryMap
	n_opts.TableLoadingMode = options.MemoryMap
	//n_opts.SyncWrites = false
	n_opts.NumVersionsToKeep = 1
	n_opts.MaxTableSize = 768 << 20
	n_opts.ValueLogMaxEntries = 100000000

	nn_opts := badger.DefaultOptions
	nn_opts.Dir = dbPath + "/nn_store"
	nn_opts.ValueDir = dbPath + "/nn_store"
	nn_opts.ValueLogLoadingMode = options.MemoryMap
	nn_opts.TableLoadingMode = options.MemoryMap
	//nn_opts.SyncWrites = false
	nn_opts.NumVersionsToKeep = 1
	nn_opts.MaxTableSize = 768 << 20
	nn_opts.ValueLogMaxEntries = 100000000

	// Open all store
	kvStores.K_batch = K_New(k_opts, 1000, nbOfThreads)
	kvStores.KK_batch = H_New(kk_opts, 1000, nbOfThreads)
	kvStores.G_batch = G_New(g_opts, 1000, nbOfThreads)
	kvStores.GG_batch = H_New(gg_opts, 1000, nbOfThreads)
	kvStores.F_batch = F_New(f_opts, 1000, nbOfThreads)
	kvStores.FF_batch = H_New(ff_opts, 1000, nbOfThreads)
	kvStores.P_batch = P_New(p_opts, 1000, nbOfThreads)
	kvStores.PP_batch = H_New(pp_opts, 1000, nbOfThreads)
	kvStores.O_batch = O_New(o_opts, 1000, nbOfThreads)
	kvStores.OO_batch = H_New(oo_opts, 1000, nbOfThreads)
	kvStores.N_batch = N_New(n_opts, 1000, nbOfThreads)
	kvStores.NN_batch = H_New(nn_opts, 1000, nbOfThreads)

	return &kvStores

}

func (kvStores *KVStores) Flush() {
	// Last DB flushes
	kvStores.K_batch.Flush()
	kvStores.KK_batch.Flush()
	kvStores.G_batch.Flush()
	kvStores.GG_batch.Flush()
	kvStores.F_batch.Flush()
	kvStores.FF_batch.Flush()
	kvStores.P_batch.Flush()
	kvStores.PP_batch.Flush()
	kvStores.O_batch.Flush()
	kvStores.OO_batch.Flush()
	kvStores.N_batch.Flush()
	kvStores.NN_batch.Flush()
}

func (kvStores *KVStores) Close() {
	// Last DB flushes
	kvStores.K_batch.Close()
	kvStores.KK_batch.Close()
	kvStores.G_batch.Close()
	kvStores.GG_batch.Close()
	kvStores.F_batch.Close()
	kvStores.FF_batch.Close()
	kvStores.P_batch.Close()
	kvStores.PP_batch.Close()
	kvStores.O_batch.Close()
	kvStores.OO_batch.Close()
	kvStores.N_batch.Close()
	kvStores.NN_batch.Close()
}

func (kvStores *KVStores) MergeKmerValues(nbOfThreads int) {

	fmt.Println("# Merging kmers multiple value versions...")

	jobs := make(chan KVsToMerge)
	wg := new(sync.WaitGroup)

	halfOfThreads := nbOfThreads
	if halfOfThreads < 1 {
		halfOfThreads = 1
	}

	// thread pool
	for w := 1; w <= halfOfThreads; w++ {
		wg.Add(1)
		go kvStores.KmerValuesMerger(jobs, wg)
	}

	// Open K_batch insert channel for merged kmers / kcomb KVs
	kvStores.K_batch.OpenInsertChannel()
	kvStores.KK_batch.OpenInsertChannel()

	// Stream keys
	stream := kvStores.K_batch.DB.NewStream()

	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = halfOfThreads // Set number of goroutines to use for iteration.
	stream.Prefix = nil        // Leave nil for iteration over the whole DB.
	// stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.
	stream.LogPrefix = ""

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		nbOfItem := 0

		currentKey := []byte{}
		valueList := [][]byte{}

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if item.DiscardEarlierVersions() {
				break
			}
			if !bytes.Equal(key, item.Key()) {
				break
			}
			if len(currentKey) < 1 {
				currentKey = item.KeyCopy(nil)
			}

			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				log.Fatal(err.Error())
			}

			valueList = append(valueList, valCopy)

			nbOfItem += 1

		}

		if nbOfItem > 1 {
			kvsToMerge := KVsToMerge{Key: currentKey, Values: valueList}
			// fmt.Printf("Sending KVs to merge key %x\n", currentKey)
			jobs <- kvsToMerge
		}

		return nil, nil

	}

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = nil

	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.

	fmt.Println("Closing jobs")

	close(jobs)
	wg.Wait()

	// Close K_batch insert channel for merged kmers / kcomb KVs
	kvStores.K_batch.CloseInsertChannel()
	kvStores.KK_batch.CloseInsertChannel()

	kvStores.K_batch.Flush()
	kvStores.KK_batch.Flush()


}

func (kvStores *KVStores) KmerValuesMerger(jobs <-chan KVsToMerge, wg *sync.WaitGroup) {

	defer wg.Done()

	for kvs := range jobs {

		// fmt.Printf("Receive KVs to merge key %x\n", kvs.Key)
		uniqueValues := RemoveDuplicatesFromSlice(kvs.Values)

		if len(uniqueValues) < 2 {
			kvStores.K_batch.AddValueToChannel(kvs.Key, kvs.Values[0], true)
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

		newValueIds := [][]byte{}
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

		newKey, newVal := CreateHashValue(newValueIds, false)

		kvStores.KK_batch.AddValueToChannel(newKey, newVal, true)
		kvStores.K_batch.AddValueToChannel(kvs.Key, newKey, true)

	}

	// return newKey, newVal, true

}

func (kvStores *KVStores) PrintStores() {

	kvStores.PrintKmerStore()

}

func (kvStores *KVStores) PrintKmerStore() {

	// Stream keys
	stream := kvStores.K_batch.DB.NewStream()

	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16   // Set number of goroutines to use for iteration.
	stream.Prefix = nil // Leave nil for iteration over the whole DB.
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
			if !bytes.Equal(key, item.Key()) {
				break
			}

			val, err := item.ValueCopy(nil)
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

func PrintCombinationStore(kvStore *H_) {

	// Stream keys
	stream := kvStore.DB.NewStream()

	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 16   // Set number of goroutines to use for iteration.
	stream.Prefix = nil // Leave nil for iteration over the whole DB.
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
			if !bytes.Equal(key, item.Key()) {
				break
			}

			val, errVal := item.ValueCopy(nil)
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
