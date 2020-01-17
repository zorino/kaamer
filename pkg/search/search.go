/*
Copyright 2019 The kaamer Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	"time"

	"github.com/golang/protobuf/proto"
	cnt "github.com/zorino/counters"
	"github.com/zorino/kaamer/pkg/align"
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
	dbStats     kvstore.KStats
	kMatchRatio = 0.05      // at least 5% of kmer hits (on query)
	minKMatch   = int64(10) // at least 10 kmer hits
)

type SearchOptions struct {
	File             string
	InputType        string
	SequenceType     int
	GeneticCode      int
	OutFormat        string
	MaxResults       int
	Align            bool
	ExtractPositions bool
	Annotations      bool
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
	Key       uint32
	Kmatch    int64
	Alignment *align.AlignmentResult
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
			pl[i] = Hit{uint32(idUint32), item.Value(), &align.AlignmentResult{}}
			i++
		}
		return true
	})

	sort.Sort(sort.Reverse(pl))
	return pl
}

func NewSearchResult(searchOptions SearchOptions, _dbStats kvstore.KStats, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter, r *http.Request) []QueryResult {

	// Query cancellation
	cancelQuery := false
	go func(cancelQuery *bool) {
	again:
		select {
		case <-time.After(3 * time.Second):
			goto again
		case <-r.Context().Done():
			*cancelQuery = true
		}
	}(&cancelQuery)

	queryResults := []QueryResult{}

	dbStats = _dbStats

	switch searchOptions.SequenceType {
	case READS:
		NucleotideSearch(searchOptions, kvStores, nbOfThreads, w, true, &cancelQuery)
	case NUCLEOTIDE:
		NucleotideSearch(searchOptions, kvStores, nbOfThreads, w, false, &cancelQuery)
	case PROTEIN:
		ProteinSearch(searchOptions, kvStores, nbOfThreads, w, &cancelQuery)
	}

	if searchOptions.InputType != "path" {
		os.Remove(searchOptions.File)
	}

	return queryResults

}

func (queryResult *QueryResult) FilterResults(searchOptions SearchOptions) {

	var hitsToDelete []uint32
	var lastGoodHitPosition = len(queryResult.SearchResults.Hits) - 1

	for i, hit := range queryResult.SearchResults.Hits {
		if (float64(hit.Kmatch)/float64(queryResult.Query.SizeInKmer)) < kMatchRatio || hit.Kmatch < minKMatch {
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

func GetQueriesFasta(fileName string, queryChan chan<- Query, isProtein bool, cancelQuery *bool) {

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

		if *cancelQuery {
			break
		}

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
				query.Sequence = strings.ToUpper(query.Sequence)
				queryChan <- query
				query = Query{Sequence: "", Name: "", SizeInKmer: 0, Contig: ""}
			}
			queryName = strings.TrimSuffix(l[1:], "\n")
			if isProtein {
				query.Name = queryName
				query.Contig = ""
				query.Location.StartPosition = 1
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

func GetQueriesFastq(fileName string, queryChan chan<- Query, cancelQuery *bool) {

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

		if *cancelQuery {
			break
		}

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

func (searchRes *SearchResults) KmerSearch(keyChan <-chan KeyPos, kvStores *kvstore.KVStores, wg *sync.WaitGroup, matchPositionChan chan<- MatchPosition, searchOptions SearchOptions) {

	extractPos := (searchOptions.ExtractPositions || (searchOptions.SequenceType == NUCLEOTIDE) || (searchOptions.SequenceType == READS))

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
				if extractPos {
					matchPositionChan <- MatchPosition{HitId: id, QPos: keyPos.Pos, QSize: keyPos.QSize}
				}
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
		if _, ok := queryResult.HitEntries[h.Key]; !ok {
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

}

func QueryResultHandler(queryResult <-chan QueryResult, queryWriter chan<- []byte, w http.ResponseWriter, wg *sync.WaitGroup, searchOptions SearchOptions) {

	defer wg.Done()

	// SetResponseFormatAndHeader(w)
	// firstResult := true
	output := ""

	for qR := range queryResult {

		// Launch Alignment
		if searchOptions.Align {
			for i, _ := range qR.SearchResults.Hits {
				alignment, err := align.Align(qR.Query.Sequence, qR.HitEntries[qR.SearchResults.Hits[i].Key].Sequence, dbStats, "blosum62", 11, 1)
				if err != nil {
					continue
				}
				qR.SearchResults.Hits[i].Alignment = &alignment
			}
		}

		// Write respopnse json
		if searchOptions.OutFormat == "json" {
			data, err := json.Marshal(qR)
			if err != nil {
				fmt.Println(err.Error())
			}
			queryWriter <- data
		}

		// Write respopnse tsv + no alignement
		if searchOptions.OutFormat == "tsv" && !searchOptions.Align {
			for _, h := range qR.SearchResults.Hits {
				posString := ""
				output = ""
				output += strings.Split(qR.Query.Name, " ")[0]
				output += "\t"
				output += qR.HitEntries[h.Key].EntryId
				output += "\t"
				output += fmt.Sprintf("%.2f", (float32(h.Kmatch) / float32(qR.Query.SizeInKmer) * float32(100.00)))
				output += "\t"
				output += strconv.Itoa(qR.Query.SizeInKmer)
				output += "\t"
				output += strconv.Itoa(int(h.Kmatch))
				output += "\t"
				if searchOptions.ExtractPositions {
					posString = FormatPositionsToString(qR.SearchResults.PositionHits[h.Key])
					output += fmt.Sprintf("%d", strings.Count(posString, ","))
				} else {
					output += "N/A"
				}
				output += "\t"
				output += strconv.Itoa(qR.Query.Location.StartPosition)
				output += "\t"
				output += strconv.Itoa(qR.Query.Location.EndPosition)
				output += "\t"
				output += "1" // subject always start at 1 in kmer
				output += "\t"

				// Only know subject lenght when adding annotation
				if searchOptions.Annotations {
					output += fmt.Sprintf("%d", qR.HitEntries[h.Key].Length)
				} else {
					output += "N/A"
				}
				if searchOptions.ExtractPositions {
					output += "\t"
					output += posString
				}

				if searchOptions.Annotations {
					for _, annotation := range dbStats.Features {
						output += "\t"
						output += qR.HitEntries[h.Key].Features[annotation]
					}
				}
				output += "\n"
				queryWriter <- []byte(output)
			}
		}

		if searchOptions.OutFormat == "tsv" && searchOptions.Align {
			for _, h := range qR.SearchResults.Hits {
				output = ""
				output += strings.Split(qR.Query.Name, " ")[0]
				output += "\t"
				output += qR.HitEntries[h.Key].EntryId
				output += "\t"
				output += fmt.Sprintf("%.2f", h.Alignment.Identity)
				output += "\t"
				output += fmt.Sprintf("%d", h.Alignment.Length)
				output += "\t"
				output += fmt.Sprintf("%d", h.Alignment.Mismatches)
				output += "\t"
				output += fmt.Sprintf("%d", h.Alignment.GapOpenings)
				output += "\t"
				if searchOptions.SequenceType != PROTEIN {
					output += strconv.Itoa(qR.Query.Location.StartPosition)
					output += "\t"
					output += strconv.Itoa(qR.Query.Location.EndPosition)
					output += "\t"
				} else {
					output += fmt.Sprintf("%d", h.Alignment.QueryStart)
					output += "\t"
					output += fmt.Sprintf("%d", h.Alignment.QueryEnd)
					output += "\t"
				}
				output += fmt.Sprintf("%d", h.Alignment.SubjectStart)
				output += "\t"
				output += fmt.Sprintf("%d", h.Alignment.SubjectEnd)
				output += "\t"
				output += fmt.Sprintf("%e", h.Alignment.EValue)
				output += "\t"
				output += fmt.Sprintf("%.2f", h.Alignment.BitScore)

				if searchOptions.ExtractPositions {
					output += "\t"
					output += FormatPositionsToString(qR.SearchResults.PositionHits[h.Key])
				}

				if searchOptions.Annotations {
					for _, annotation := range dbStats.Features {
						output += "\t"
						output += qR.HitEntries[h.Key].Features[annotation]
					}
				}
				output += "\n"
				queryWriter <- []byte(output)
			}

		}

	}

}

func QueryResultWriter(queryResultOutput <-chan []byte, w http.ResponseWriter, wg *sync.WaitGroup, searchOptions SearchOptions) {

	defer wg.Done()
	SetResponseFormatAndHeader(w, searchOptions)
	firstResult := true
	for output := range queryResultOutput {
		// Write respopnse json
		if searchOptions.OutFormat == "json" {
			if !firstResult {
				w.Write([]byte(","))
			}
			w.Write(output)
			firstResult = false
		} else if searchOptions.OutFormat == "tsv" {
			w.Write(output)
		}
	}
	if searchOptions.OutFormat == "json" {
		// open results array
		w.Write([]byte("]}"))
	}

}

func SetResponseFormatAndHeader(w http.ResponseWriter, searchOptions SearchOptions) {

	// Set output response header TSV
	if searchOptions.OutFormat == "tsv" {

		// set http response header
		w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
		w.WriteHeader(200)

		if !searchOptions.Align {
			w.Write([]byte("QueryId\tSubjectId\t%KMatchIdentity\tQueryKLength\tKMatch\tGapOpen\tQStart\tQEnd\tSStart\tSEnd"))
		} else {
			// TSV output for alignment
			w.Write([]byte("QueryId\tSubjectId\t%Identity\tAlnLength\tMismatches\tGapOpen\tQStart\tQEnd\tSStart\tSEnd\tEvalue\tBitscore"))
		}
		if searchOptions.ExtractPositions {
			w.Write([]byte("\tQueryPositions"))
		}
		if searchOptions.Annotations {
			for _, annotation := range dbStats.Features {
				w.Write([]byte("\t"))
				w.Write([]byte(annotation))
			}
		}
		w.Write([]byte("\n"))

	}

	// Set output response header json
	if searchOptions.OutFormat == "json" {

		// set http response header
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)

		// open results array
		w.Write([]byte("{\"dbProteinFeatures\":["))
		if searchOptions.Annotations {
			first := true
			for _, annotation := range dbStats.Features {
				if first {
					first = false
				} else {
					w.Write([]byte(","))
				}
				w.Write([]byte("\""))
				w.Write([]byte(annotation))
				w.Write([]byte("\""))
			}
		}
		w.Write([]byte("]"))
		w.Write([]byte(",\"results\":"))
		w.Write([]byte("["))

	}

}

func FormatPositionsToString(positions []bool) string {

	currentStart := 0
	inSequence := false

	positionsString := ""

	for pos, match := range positions {
		if match {
			if !inSequence {
				currentStart = pos + 1
				inSequence = true
			}
		} else {
			if inSequence {
				if pos+1 > currentStart {
					if positionsString != "" {
						positionsString += ","
					}
					positionsString += (strconv.Itoa(currentStart) + "-" + (strconv.Itoa(pos + 1)))
					inSequence = false
				} else {
					if positionsString != "" {
						positionsString += ","
					}
					positionsString += strconv.Itoa(currentStart)
					inSequence = false
				}
			}
		}
	}
	if inSequence {
		if positionsString != "" {
			positionsString += ","
		}
		positionsString += (strconv.Itoa(currentStart) + "-" + (strconv.Itoa(len(positions))))
	}

	return positionsString

}
