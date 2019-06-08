package search

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	cnt "github.com/zorino/counters"
	"github.com/zorino/metaprot/pkg/kvstore"
)

const (
	NUCLEOTIDE_STRING = 0
	NUCLEOTIDE_FILE   = 1
	PROTEIN_STRING    = 2
	PROTEIN_FILE      = 3
	READ_STRING       = 4
	READ_FILE         = 5
	KMER_SIZE         = 7
	DNA_QUERY         = "DNA Query"
	PROTEIN_QUERY     = "Protein Query"
)

type SearchResults struct {
	Counter      *cnt.CounterBox
	Hits         HitList
	PositionHits []map[string]bool
}

type KeyPos struct {
	Key []byte
	Pos int
}

type QueryResult struct {
	Query         Query
	SearchResults *SearchResults
}

type Query struct {
	Sequence   string
	Name       string
	SizeInKmer int
	Type       string
	Location   Location
	Contig     string
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

func NewSearchResult(sequenceOrFile string, sequenceType int, kvStores *kvstore.KVStores, nbOfThreads int) []QueryResult {

	// sequence is either file path or the actual sequence (depends on sequenceType)
	queryResults := []QueryResult{}

	switch sequenceType {
	case NUCLEOTIDE_STRING:
		fmt.Println("Nucleotide type")
		queryResults = NucleotideSearch(sequenceOrFile, kvStores, nbOfThreads)
	case PROTEIN_STRING:
		fmt.Println("Searching from Protein Sequence")
		queryResults = ProteinSearch(sequenceOrFile, kvStores, nbOfThreads)
	case PROTEIN_FILE:
		fmt.Println("Searching from Protein file")
		queryResults = ProteinSearch(sequenceOrFile, kvStores, nbOfThreads)
	}

	return queryResults

}

func NucleotideSearch(sequence string, kvStores *kvstore.KVStores, nbOfThreads int) []QueryResult {

	queryResults := []QueryResult{}

	sequences := GetQueries(sequence)

	for _, s := range sequences {

		orfs := GetORFs(s.Sequence)

		for _, o := range orfs {

			q := Query{
				Sequence:   o.Sequence,
				Name:       "",
				SizeInKmer: (len(o.Sequence)) - KMER_SIZE + 1,
				Location:   o.Location,
				Contig:     s.Name,
				Type:       DNA_QUERY,
			}

			searchRes := new(SearchResults)
			searchRes.Counter = cnt.NewCounterBox()
			searchRes.PositionHits = make([]map[string]bool, q.SizeInKmer)
			keyChan := make(chan KeyPos, 10)

			wg := new(sync.WaitGroup)
			for i := 0; i < nbOfThreads; i++ {
				wg.Add(1)
				go searchRes.KmerSearch(keyChan, kvStores, wg)
			}

			for i := 0; i < q.SizeInKmer; i++ {
				searchRes.PositionHits[i] = make(map[string]bool)
				key := kvStores.KmerStore.CreateBytesKey(q.Sequence[i : i+KMER_SIZE])
				keyChan <- KeyPos{Key: key, Pos: i}
			}

			close(keyChan)
			wg.Wait()

			searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())
			queryResults = append(queryResults, QueryResult{Query: q, SearchResults: searchRes})
		}

	}

	queryResults = ResolveORFs(queryResults)

	return queryResults
}

func ProteinSearch(sequence string, kvStores *kvstore.KVStores, nbOfThreads int) []QueryResult {

	queryResults := []QueryResult{}

	queries := GetQueries(sequence)

	for _, q := range queries {

		// fmt.Printf("SEQ: %s\n", q.Sequence)
		if q.SizeInKmer < 7 {
			fmt.Println("Protein Sequence shorter than kmer size: 7")
			return queryResults
		}

		searchRes := new(SearchResults)
		searchRes.Counter = cnt.NewCounterBox()
		searchRes.PositionHits = make([]map[string]bool, q.SizeInKmer)
		keyChan := make(chan KeyPos, 10)

		wg := new(sync.WaitGroup)
		for i := 0; i < nbOfThreads; i++ {
			wg.Add(1)
			go searchRes.KmerSearch(keyChan, kvStores, wg)
		}

		for i := 0; i < q.SizeInKmer; i++ {
			searchRes.PositionHits[i] = make(map[string]bool)
			key := kvStores.KmerStore.CreateBytesKey(q.Sequence[i : i+KMER_SIZE])
			keyChan <- KeyPos{Key: key, Pos: i}
		}

		close(keyChan)
		wg.Wait()

		searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())
		queryResults = append(queryResults, QueryResult{Query: q, SearchResults: searchRes})
	}

	return queryResults

}

func GetQueries(sequence string) []Query {

	queries := []Query{}

	fastaLines := strings.Split(sequence, "\n")
	loc := Location{
		StartPosition:     1,
		EndPosition:       0,
		PlusStrand:        true,
		StartsAlternative: []int{},
	}
	query := Query{
		Sequence:   "",
		Name:       "",
		SizeInKmer: 0,
		Location:   loc,
		Type:       PROTEIN_QUERY,
		Contig:     "",
	}

	for i, l := range fastaLines {
		if len(l) < 1 {
			continue
		}
		if l[0] == '>' {
			if query.Sequence != "" {
				if query.Name == "" {
					query.Name = fmt.Sprintf("seq_%d", i)
				}
				query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
				query.Location.EndPosition = len(query.Sequence)
				queries = append(queries, query)
				query = Query{Sequence: "", Name: "", SizeInKmer: 0}
			}
			query.Name = l[1:]
		} else {
			query.Sequence += strings.TrimSpace(l)
		}
	}
	if query.Sequence != "" {
		query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
		query.Location.EndPosition = len(query.Sequence)
		queries = append(queries, query)
	}

	return queries

}

func (searchRes *SearchResults) KmerSearch(keyChan <-chan KeyPos, kvStores *kvstore.KVStores, wg *sync.WaitGroup) {

	defer wg.Done()
	for keyPos := range keyChan {
		if protIds, err := kvStores.KmerStore.GetValues(keyPos.Key); err == nil {
			for _, id := range protIds {
				intId := binary.BigEndian.Uint32(id)
				searchRes.Counter.GetCounter(strconv.Itoa(int(intId))).Increment()
				searchRes.PositionHits[keyPos.Pos][strconv.Itoa(int(intId))] = true
			}
		}
	}

}
