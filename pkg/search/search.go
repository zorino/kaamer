package search

import (
	"fmt"
	cnt "github.com/zorino/counters"
	"github.com/zorino/metaprot/pkg/kvstore"
	"sort"
	"sync"
)

const (
	NUCLEOTIDE_STRING = 0
	NUCLEOTIDE_FILE   = 1
	PROTEIN_STRING    = 2
	PROTEIN_FILE      = 3
	KMER_SIZE         = 7
)

type SearchResults struct {
	Counter *cnt.CounterBox
	Hits    HitList
}

type Hit struct {
	Key   string
	Value int64
}

type HitList []Hit

func (p HitList) Len() int           { return len(p) }
func (p HitList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p HitList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func syncMapLen(syncMap *sync.Map) int {
	length := 0
	syncMap.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}

func sortMapByValue(hitFrequencies *sync.Map) HitList {
	pl := make(HitList, syncMapLen(hitFrequencies))
	i := 0

	hitFrequencies.Range(func(k, v interface{}) bool {
		key, okKey := k.(string)
		item, okValue := v.(cnt.Counter)
		if okKey && okValue {
			pl[i] = Hit{key, item.Value()}
			i++
		}
		return true
	})

	sort.Sort(sort.Reverse(pl))
	return pl
}

func NewSearchResult(sequenceOrFile string, sequenceType int, kvStores *kvstore.KVStores, nbOfThreads int) *SearchResults {

	// sequence is either file path or the actual sequence (depends on sequenceType)

	searchRes := new(SearchResults)
	searchRes.Counter = cnt.NewCounterBox()

	switch sequenceType {
	case NUCLEOTIDE_STRING:
		fmt.Println("Nucleotide type")
	case PROTEIN_STRING:
		fmt.Println("Protein type")
		searchRes.ProteinSearch(sequenceOrFile, kvStores, nbOfThreads)
	}

	return searchRes

}

func (searchRes *SearchResults) ProteinSearch(sequence string, kvStores *kvstore.KVStores, nbOfThreads int) {

	fmt.Printf("SEQ: %s\n", sequence)
	if len(sequence) < 7 {
		fmt.Println("Protein Sequence shorter than kmer size: 7")
		return
	}

	keyChan := make(chan []byte, 10)
	wg := new(sync.WaitGroup)
	for i := 0; i < nbOfThreads; i++ {
		wg.Add(1)
		go searchRes.KmerSearch(keyChan, kvStores, wg)
	}

	for i := 0; i < len(sequence)-KMER_SIZE+1; i++ {
		key := kvStores.KmerStore.CreateBytesKey(sequence[i : i+KMER_SIZE])
		keyChan <- key
	}

	close(keyChan)
	wg.Wait()

	searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())
	// fmt.Printf("%v\n", searchRes.Hits)

}

func (searchRes *SearchResults) KmerSearch(keyChan <-chan []byte, kvStores *kvstore.KVStores, wg *sync.WaitGroup) {

	defer wg.Done()
	for key := range keyChan {
		if protIds, err := kvStores.KmerStore.GetValues(key); err == nil {
			for _, id := range protIds {
				searchRes.Counter.GetCounter(string(id)).Increment()
			}
		}
	}

}
