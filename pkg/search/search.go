package search

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	"github.com/zorino/metaprot/pkg/kvstore"
)

const (
	NUCLEOTIDE    = 0
	PROTEIN       = 1
	READS         = 2
	KMER_SIZE     = 7
	DNA_QUERY     = "DNA Query"
	PROTEIN_QUERY = "Protein Query"
)

type SearchResults struct {
	Counter      *cnt.CounterBox
	Hits         HitList
	PositionHits []map[uint32]bool
}

type KeyPos struct {
	Key []byte
	Pos int
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
	Key   uint32
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

func NewSearchResult(file string, sequenceType int, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter) []QueryResult {

	// sequence is either file path or the actual sequence (depends on sequenceType)
	queryResults := []QueryResult{}

	// set http response header
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)

	// open results array
	w.Write([]byte("["))

	switch sequenceType {
	case READS:
		fmt.Println("Searching for Reads file")
		// sequence := ReadFileInMemory(file)
		NucleotideSearch(file, kvStores, nbOfThreads, w, true)
	case NUCLEOTIDE:
		fmt.Println("Searching for Nucleotide file")
		// sequence := ReadFileInMemory(file)
		NucleotideSearch(file, kvStores, nbOfThreads, w, false)
		// os.Remove(file)
	case PROTEIN:
		fmt.Println("Searching from Protein file")
		// sequence := ReadFileInMemory(file)
		ProteinSearch(file, kvStores, nbOfThreads, w)
		// os.Remove(file)
	}

	// close results array
	w.Write([]byte("]"))

	return queryResults

}

func ReadFileInMemory(filePath string) string {
	dat, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println(err.Error())
		return ""
	}
	// fmt.Println(string(dat))
	return string(dat)
}

func ReadSearch(file string, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter) {

	// queryResults := []QueryResult{}

	// sequences := GetQueriesFastq(sequence)

	// return queryResults

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

		resultNb := 0

		for s := range queryChan {

			queryResults := []QueryResult{}
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

				searchRes := new(SearchResults)
				searchRes.Counter = cnt.NewCounterBox()
				searchRes.PositionHits = make([]map[uint32]bool, q.SizeInKmer)
				keyChan := make(chan KeyPos, 10)

				wg := new(sync.WaitGroup)
				for i := 0; i < nbOfThreads; i++ {
					wg.Add(1)
					go searchRes.KmerSearch(keyChan, kvStores, wg)
				}

				for i := 0; i < q.SizeInKmer; i++ {
					searchRes.PositionHits[i] = make(map[uint32]bool)
					key := kvStores.KmerStore.CreateBytesKey(q.Sequence[i : i+KMER_SIZE])
					keyChan <- KeyPos{Key: key, Pos: i}
				}

				close(keyChan)
				wg.Wait()

				searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())
				queryResults = append(queryResults, QueryResult{Query: q, SearchResults: searchRes, HitEntries: map[uint32]kvstore.Protein{}})
			}

			queryResults = ResolveORFs(queryResults)

			for _, qR := range queryResults {
				qR.FetchHitsInformation(kvStores)
				if resultNb > 0 {
					w.Write([]byte(","))
				}
				data, err := json.Marshal(qR)
				if err != nil {
					fmt.Println(err.Error())
				}
				w.Write(data)
				resultNb += 1

			}

		}

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

		resultNb := 0

		for q := range queryChan {

			q.Type = PROTEIN_QUERY

			// fmt.Printf("SEQ: %s\n", q.Sequence)
			if q.SizeInKmer < 7 {
				// fmt.Println("Protein Sequence shorter than kmer size: 7")
				return
			}

			searchRes := new(SearchResults)
			searchRes.Counter = cnt.NewCounterBox()
			searchRes.PositionHits = make([]map[uint32]bool, q.SizeInKmer)
			keyChan := make(chan KeyPos, 10)

			wg := new(sync.WaitGroup)
			for i := 0; i < nbOfThreads; i++ {
				wg.Add(1)
				go searchRes.KmerSearch(keyChan, kvStores, wg)
			}

			for i := 0; i < q.SizeInKmer; i++ {
				searchRes.PositionHits[i] = make(map[uint32]bool)
				key := kvStores.KmerStore.CreateBytesKey(q.Sequence[i : i+KMER_SIZE])
				keyChan <- KeyPos{Key: key, Pos: i}
			}

			close(keyChan)
			wg.Wait()

			searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())
			// queryResults = append(queryResults, QueryResult{Query: q, SearchResults: searchRes})

			if resultNb > 0 {
				w.Write([]byte(","))
			}

			queryResult := QueryResult{Query: q, SearchResults: searchRes, HitEntries: map[uint32]kvstore.Protein{}}
			queryResult.FetchHitsInformation(kvStores)

			data, err := json.Marshal(queryResult)
			if err != nil {
				fmt.Println(err.Error())
			}
			w.Write(data)
			resultNb += 1

		}

	}()

	wgSearch.Wait()

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
	buff := make([]byte, 512)
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

	for scanner.Scan() {
		l = scanner.Text()
		if len(l) < 1 {
			continue
		}
		if l[0] == '>' {
			if query.Sequence != "" {
				query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
				query.Location.EndPosition = len(query.Sequence)
				queryChan <- query
				query = Query{Sequence: "", Name: "", SizeInKmer: 0, Contig: ""}
			}
			if isProtein {
				query.Name = strings.TrimSuffix(l[1:], "\n")
				query.Contig = ""
			} else {
				query.Name = ""
				query.Contig = strings.TrimSuffix(l[1:], "\n")
			}
		} else {
			query.Sequence += strings.TrimSpace(l)
		}
	}

	if query.Sequence != "" {
		query.SizeInKmer = len(query.Sequence) - KMER_SIZE + 1
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

func (searchRes *SearchResults) KmerSearch(keyChan <-chan KeyPos, kvStores *kvstore.KVStores, wg *sync.WaitGroup) {

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
				searchRes.PositionHits[keyPos.Pos][id] = true
			}

		}
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

	// queryResult.HitEntries

}
