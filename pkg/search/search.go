package search

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	cnt "github.com/zorino/counters"
	"github.com/zorino/kaamer/pkg/kvstore"
)

const (
	NUCLEOTIDE    = 0
	PROTEIN       = 1
	READS         = 2
	KMER_SIZE     = 7
	DNA_QUERY     = "DNA Query"
	PROTEIN_QUERY = "Protein Query"
)

var (
	searchOptions = SearchOptions{}
)

type SearchOptions struct {
	File             string
	InputType        string
	SequenceType     int
	OutFormat        string
	MaxResults       int
	ExtractPositions bool
}

type SearchResults struct {
	Counter      *cnt.CounterBox
	Hits         HitList
	PositionHits map[uint32][]bool
}

type KeyPos struct {
	Key   []byte
	Pos   int
	QSize int
}

type MatchPosition struct {
	HitId uint32
	QPos  int
	QSize int
}

type QueryWriter struct {
	Query
	http.ResponseWriter
}

type QueryResult struct {
	Query         Query
	SearchResults *SearchResults
	HitEntries    map[uint32]kvstore.Protein
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
	Key    uint32
	Kmatch int64
}

type HitList []Hit

func (p HitList) Len() int           { return len(p) }
func (p HitList) Less(i, j int) bool { return p[i].Kmatch < p[j].Kmatch }
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
			idUint32, err := strconv.Atoi(key)
			if err != nil {
				log.Fatal(err.Error())
			}
			pl[i] = Hit{uint32(idUint32), item.Value()}
			i++
		}
		return true
	})

	sort.Sort(sort.Reverse(pl))
	return pl
}

func NewSearchResult(newSearchOptions SearchOptions, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter) []QueryResult {

	// sequence is either file path or the actual sequence (depends on sequenceType)
	queryResults := []QueryResult{}

	searchOptions = newSearchOptions

	switch searchOptions.SequenceType {
	case READS:
		fmt.Println("Searching for Reads file")
		NucleotideSearch(searchOptions.File, kvStores, nbOfThreads, w, true)
	case NUCLEOTIDE:
		fmt.Println("Searching for Nucleotide file")
		NucleotideSearch(searchOptions.File, kvStores, nbOfThreads, w, false)
	case PROTEIN:
		fmt.Println("Searching from Protein file")
		ProteinSearch(searchOptions.File, kvStores, nbOfThreads, w)
	}

	if searchOptions.InputType != "path" {
		os.Remove(searchOptions.File)
	}

	return queryResults

}

func NucleotideSearch(file string, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter, fastq bool) {

	queryChan := make(chan Query)

	wgSearch := new(sync.WaitGroup)
	wgSearch.Add(2)

	go func() {
		defer wgSearch.Done()
		if fastq {
			GetQueriesFastq(file, queryChan)
		} else {
			GetQueriesFasta(file, queryChan, false)
		}
		close(queryChan)
	}()

	go func() {

		defer wgSearch.Done()

		queryResults := []QueryResult{}

		// Concurrent query results writer
		queryResultChan := make(chan QueryResult, 10)
		wgWriter := new(sync.WaitGroup)
		wgWriter.Add(1)
		go QueryResultResponseWriter(queryResultChan, w, wgWriter)

		for s := range queryChan {

			queryResults = []QueryResult{}
			orfs := GetORFs(s.Sequence)

			for _, o := range orfs {

				q := Query{
					Sequence:   o.Sequence,
					Name:       s.Name,
					SizeInKmer: (len(o.Sequence)) - KMER_SIZE + 1,
					Location:   o.Location,
					Contig:     s.Contig,
					Type:       DNA_QUERY,
				}

				if q.Sequence[len(q.Sequence)-1:] == "*" {
					q.SizeInKmer = q.SizeInKmer - 1
				}

				searchRes := new(SearchResults)
				searchRes.Counter = cnt.NewCounterBox()
				searchRes.PositionHits = make(map[uint32][]bool)
				keyChan := make(chan KeyPos, 10)

				matchPositionChan := make(chan MatchPosition, 10)
				wgMP := new(sync.WaitGroup)
				wgMP.Add(1)
				go searchRes.StoreMatchPositions(matchPositionChan, wgMP)

				wg := new(sync.WaitGroup)
				// Add 4 workers for KmerSearch / KCombSearch
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go searchRes.KmerSearch(keyChan, kvStores, wg, matchPositionChan)
				}

				for i := 0; i < q.SizeInKmer; i++ {
					key := kvStores.KmerStore.CreateBytesKey(q.Sequence[i : i+KMER_SIZE])
					keyChan <- KeyPos{Key: key, Pos: i, QSize: q.SizeInKmer}
				}

				close(keyChan)
				wg.Wait()

				close(matchPositionChan)
				wgMP.Wait()

				searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())
				queryResults = append(queryResults, QueryResult{Query: q, SearchResults: searchRes, HitEntries: map[uint32]kvstore.Protein{}})
			}

			queryResults = ResolveORFs(queryResults)

			for _, qR := range queryResults {
				qR.FilterResults(0.2)
				if qR.SearchResults.Hits.Len() > 0 {
					qR.FetchHitsInformation(kvStores)
					queryResultChan <- qR
				}
			}

		}

		close(queryResultChan)
		wgWriter.Wait()

	}()

	wgSearch.Wait()

}

func ProteinSearch(file string, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter) {

	queryChan := make(chan Query)

	wgSearch := new(sync.WaitGroup)
	wgSearch.Add(2)

	go func() {
		defer wgSearch.Done()
		GetQueriesFasta(file, queryChan, true)
		close(queryChan)
	}()

	go func() {

		defer wgSearch.Done()

		queryResult := QueryResult{}

		// Concurrent query results writer
		queryResultChan := make(chan QueryResult, 50)
		wgWriter := new(sync.WaitGroup)
		wgWriter.Add(1)
		go QueryResultResponseWriter(queryResultChan, w, wgWriter)

		for q := range queryChan {

			q.Type = PROTEIN_QUERY

			if q.SizeInKmer < 7 {
				return
			}

			searchRes := new(SearchResults)
			searchRes.Counter = cnt.NewCounterBox()
			searchRes.PositionHits = make(map[uint32][]bool)
			keyChan := make(chan KeyPos, 10)

			matchPositionChan := make(chan MatchPosition, 10)
			wgMP := new(sync.WaitGroup)
			wgMP.Add(1)
			go searchRes.StoreMatchPositions(matchPositionChan, wgMP)

			wg := new(sync.WaitGroup)
			// Add 4 workers for KmerSearch
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go searchRes.KmerSearch(keyChan, kvStores, wg, matchPositionChan)
			}

			for i := 0; i < q.SizeInKmer; i++ {
				key := kvStores.KmerStore.CreateBytesKey(q.Sequence[i : i+KMER_SIZE])
				keyChan <- KeyPos{Key: key, Pos: i, QSize: q.SizeInKmer}
			}

			close(keyChan)
			wg.Wait()

			close(matchPositionChan)
			wgMP.Wait()

			searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())

			queryResult = QueryResult{Query: q, SearchResults: searchRes, HitEntries: map[uint32]kvstore.Protein{}}
			queryResult.FilterResults(0.2)
			queryResult.FetchHitsInformation(kvStores)

			queryResultChan <- queryResult

		}

		close(queryResultChan)
		wgWriter.Wait()

	}()

	wgSearch.Wait()

}

func (queryResult *QueryResult) FilterResults(kmerMatchRatio float64) {

	var hitsToDelete []uint32
	var lastGoodHitPosition = len(queryResult.SearchResults.Hits) - 1

	for i, hit := range queryResult.SearchResults.Hits {
		if (float64(hit.Kmatch)/float64(queryResult.Query.SizeInKmer)) < kmerMatchRatio || hit.Kmatch < 10 {
			if lastGoodHitPosition == (len(queryResult.SearchResults.Hits) - 1) {
				lastGoodHitPosition = i - 1
			}
			hitsToDelete = append(hitsToDelete, hit.Key)
		}
	}

	if lastGoodHitPosition >= searchOptions.MaxResults {
		lastGoodHitPosition = searchOptions.MaxResults - 1
		for _, h := range queryResult.SearchResults.Hits[lastGoodHitPosition+1:] {
			hitsToDelete = append(hitsToDelete, h.Key)
		}
	}

	if lastGoodHitPosition < 0 {
		queryResult.SearchResults.Hits = []Hit{}
	} else {
		queryResult.SearchResults.Hits = queryResult.SearchResults.Hits[0 : lastGoodHitPosition+1]
	}

	for _, k := range hitsToDelete {
		delete(queryResult.SearchResults.PositionHits, k)
	}

}

func GetQueriesFasta(fileName string, queryChan chan<- Query, isProtein bool) {

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
		Type:       "",
		Contig:     "",
	}

	// queries := []Query{}
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}

	// check filetype
	buff := make([]byte, 32)
	_, err = file.Read(buff)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	file.Seek(0, 0)

	filetype := http.DetectContentType(buff)

	scanner := new(bufio.Scanner)

	if filetype == "application/x-gzip" {
		gz, err := gzip.NewReader(file)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		scanner = bufio.NewScanner(gz)
		defer gz.Close()
	} else if filetype == "text/plain; charset=utf-8" {
		scanner = bufio.NewScanner(file)
	} else {
		return
	}

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	l := ""
	queryName := ""

	for scanner.Scan() {
		l = scanner.Text()
		if len(l) < 1 {
			continue
		}
		if l[0] == '>' {
			if query.Sequence != "" {
				query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
				if query.Sequence[len(query.Sequence)-1:] == "*" {
					query.SizeInKmer--
				}
				query.Location.EndPosition = len(query.Sequence)
				queryChan <- query
				query = Query{Sequence: "", Name: "", SizeInKmer: 0, Contig: ""}
			}
			queryName = strings.TrimSuffix(l[1:], "\n")
			if isProtein {
				query.Name = queryName
				query.Contig = ""
			} else {
				query.Name = queryName
				query.Contig = queryName
			}
		} else {
			query.Sequence += strings.TrimSpace(l)
		}
	}

	if query.Sequence != "" {
		query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
		if query.Sequence[len(query.Sequence)-1:] == "*" {
			query.SizeInKmer--
		}
		query.Location.EndPosition = len(query.Sequence)
		queryChan <- query
	}

}

func GetQueriesFastq(fileName string, queryChan chan<- Query) {

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
		Type:       "",
		Contig:     "",
	}

	// queries := []Query{}
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}

	// check filetype
	buff := make([]byte, 512)
	_, err = file.Read(buff)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	filetype := http.DetectContentType(buff)

	scanner := new(bufio.Scanner)

	if filetype == "application/x-gzip" {
		// fmt.Println("Loaded gzip file")
		gz, err := gzip.NewReader(file)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		scanner = bufio.NewScanner(gz)
		// defer gz.Close()
	} else if filetype == "text/plain; charset=utf-8" {
		scanner = bufio.NewScanner(file)
	} else {
		return
	}

	isSequence := regexp.MustCompile(`^[ATGCNatgcn]+$`).MatchString
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	seqNb := 0
	l := ""

	for scanner.Scan() {
		l = scanner.Text()
		if len(l) < 1 {
			continue
		}
		if l[0] == '@' {
			seqNb += 1
			if query.Sequence != "" {
				query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
				query.Location.EndPosition = len(query.Sequence)
				queryChan <- query
				query = Query{Sequence: "", Name: "", SizeInKmer: 0}
			}
			query.Name = strings.TrimSuffix(l[1:], "\n")
		} else if isSequence(l) {
			query.Sequence = strings.TrimSuffix(l, "\n")
		}
	}

	if query.Sequence != "" {
		query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
		query.Location.EndPosition = len(query.Sequence)
		queryChan <- query
	}

}

func (searchRes *SearchResults) KmerSearch(keyChan <-chan KeyPos, kvStores *kvstore.KVStores, wg *sync.WaitGroup, matchPositionChan chan<- MatchPosition) {

	defer wg.Done()
	for keyPos := range keyChan {

		if kCombId, err := kvStores.KmerStore.GetValueFromBadger(keyPos.Key); err == nil {

			if len(kCombId) < 1 {
				continue
			}

			kCombVal, _ := kvStores.KCombStore.GetValueFromBadger(kCombId)
			kC := &kvstore.KComb{}
			proto.Unmarshal(kCombVal, kC)

			for _, id := range kC.ProteinKeys {
				searchRes.Counter.GetCounter(strconv.Itoa(int(id))).Increment()
				matchPositionChan <- MatchPosition{HitId: id, QPos: keyPos.Pos, QSize: keyPos.QSize}
			}

		}
	}

}

func (searchRes *SearchResults) StoreMatchPositions(matchPosition <-chan MatchPosition, wg *sync.WaitGroup) {

	defer wg.Done()
	for mp := range matchPosition {
		if _, ok := searchRes.PositionHits[mp.HitId]; !ok {
			searchRes.PositionHits[mp.HitId] = make([]bool, mp.QSize)
		}
		searchRes.PositionHits[mp.HitId][mp.QPos] = true
	}

}

func (queryResult *QueryResult) FetchHitsInformation(kvStores *kvstore.KVStores) {

	for _, h := range queryResult.SearchResults.Hits {
		proteinId := make([]byte, 4)
		binary.BigEndian.PutUint32(proteinId, h.Key)
		val, err := kvStores.ProteinStore.GetValueFromBadger(proteinId)
		if err != nil {
			return
		}
		prot := &kvstore.Protein{}
		proto.Unmarshal(val, prot)
		queryResult.HitEntries[h.Key] = *prot
	}

}

func QueryResultResponseWriter(queryResult <-chan QueryResult, w http.ResponseWriter, wg *sync.WaitGroup) {

	defer wg.Done()

	if searchOptions.OutFormat == "tsv" {

		// set http response header
		w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
		w.WriteHeader(200)

		w.Write([]byte("QueryName\tQueryKSize\tQStart\tQEnd\tKMatch\tHitId\n"))
		output := ""

		for qR := range queryResult {

			for _, h := range qR.SearchResults.Hits {
				output = ""
				output += qR.Query.Name
				output += "\t"
				output += strconv.Itoa(qR.Query.SizeInKmer)
				output += "\t"
				output += strconv.Itoa(qR.Query.Location.StartPosition)
				output += "\t"
				output += strconv.Itoa(qR.Query.Location.EndPosition)
				output += "\t"
				output += strconv.Itoa(int(h.Kmatch))
				output += "\t"
				output += qR.HitEntries[h.Key].Entry
				output += "\n"
				w.Write([]byte(output))
			}

		}

	}

	if searchOptions.OutFormat == "json" {

		// set http response header
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)

		// open results array
		w.Write([]byte("["))

		firstResult := true

		for qR := range queryResult {

			if !firstResult {
				w.Write([]byte(","))
			}
			data, err := json.Marshal(qR)
			if err != nil {
				fmt.Println(err.Error())
			}
			w.Write(data)

			firstResult = false

		}

		// open results array
		w.Write([]byte("]"))

	}

}
