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
	"net/http"
	"sync"

	cnt "github.com/zorino/counters"
	"github.com/zorino/kaamer/pkg/kvstore"
)

func NucleotideSearch(searchOptions SearchOptions, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter, fastq bool, cancelQuery *bool) {

	file := searchOptions.File

	queryChan := make(chan Query)

	wgReader := new(sync.WaitGroup)
	wgReader.Add(1)

	go func() {
		defer wgReader.Done()
		if fastq {
			GetQueriesFastq(file, queryChan)
		} else {
			GetQueriesFasta(file, queryChan, false)
		}
		close(queryChan)
	}()

	// Single query results writer
	queryWriterChan := make(chan []byte, 10)
	wgResWriter := new(sync.WaitGroup)
	wgResWriter.Add(1)
	go QueryResultWriter(queryWriterChan, w, wgResWriter)

	// Concurrent query result handlers
	queryResultChan := make(chan QueryResult, 10)
	wgResHandler := new(sync.WaitGroup)
	for i := 0; i < nbOfThreads; i++ {
		wgResHandler.Add(1)
		go QueryResultHandler(queryResultChan, queryWriterChan, w, wgResHandler)
	}

	wgSearch := new(sync.WaitGroup)

	for i := 0; i < nbOfThreads; i++ {

		wgSearch.Add(1)

		go func() {

			defer wgSearch.Done()
			searchRes := new(SearchResults)
			keyChan := make(chan KeyPos, 10)

			for s := range queryChan {

				if *cancelQuery {
					return
				}

				// Concurrent query results writer
				queryResultStoreChan := make(chan QueryResult, 10)
				wgResultStore := new(sync.WaitGroup)
				wgResultStore.Add(1)
				go QueryResultStore(queryResultStoreChan, queryResultChan, w, wgResultStore, kvStores, cancelQuery)

				orfs := GetORFs(s.Sequence, searchOptions.GeneticCode)

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

					searchRes = new(SearchResults)
					searchRes.Counter = cnt.NewCounterBox()
					searchRes.PositionHits = make(map[uint32][]bool)
					keyChan = make(chan KeyPos, 10)

					matchPositionChan := make(chan MatchPosition, 10)
					wgMP := new(sync.WaitGroup)
					wgMP.Add(1)
					go searchRes.StoreMatchPositions(matchPositionChan, wgMP)

					wg := new(sync.WaitGroup)
					wg.Add(1)
					go searchRes.KmerSearch(keyChan, kvStores, wg, matchPositionChan)

					for i := 0; i < q.SizeInKmer; i++ {
						key := kvStores.KmerStore.CreateBytesKey(q.Sequence[i : i+KMER_SIZE])
						keyChan <- KeyPos{Key: key, Pos: i, QSize: q.SizeInKmer}
					}

					close(keyChan)
					wg.Wait()
					close(matchPositionChan)
					wgMP.Wait()

					searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())
					if len(searchRes.Hits) > 0 && searchRes.Hits[0].Kmatch >= minKMatch {
						queryResultStoreChan <- QueryResult{Query: q, SearchResults: searchRes, HitEntries: map[uint32]kvstore.Protein{}}
					}

				}

				close(queryResultStoreChan)
				wgResultStore.Wait()

			}

		}()

	}

	wgReader.Wait()

	wgSearch.Wait()
	close(queryResultChan)

	wgResHandler.Wait()

	close(queryWriterChan)
	wgResWriter.Wait()

}

func QueryResultStore(queryResultStoreChan <-chan QueryResult, queryResultChan chan<- QueryResult, w http.ResponseWriter, wg *sync.WaitGroup, kvStores *kvstore.KVStores, cancelQuery *bool) {

	defer wg.Done()
	queryResults := []QueryResult{}
	lastQueryResult := &QueryResult{}
	lastPos := 0
	currentPos := 0

	for qR := range queryResultStoreChan {

		if *cancelQuery {
			return
		}

		if lastQueryResult == nil {
			queryResults = append(queryResults, qR)
			lastQueryResult = &queryResults[len(queryResults)-1]
			return
		}

		if qR.Query.Location.PlusStrand {
			currentPos = qR.Query.Location.EndPosition
		} else {
			currentPos = qR.Query.Location.StartPosition
		}

		if lastQueryResult.Query.Location.PlusStrand {
			lastPos = lastQueryResult.Query.Location.EndPosition
		} else {
			lastPos = lastQueryResult.Query.Location.StartPosition
		}

		if currentPos > lastPos {
			queryResults = ResolveORFs(queryResults)
			for _, _qR := range queryResults {
				_qR.FilterResults()
				if _qR.SearchResults.Hits.Len() > 0 {
					_qR.FetchHitsInformation(kvStores)
					queryResultChan <- _qR
				}
			}
			queryResults = []QueryResult{}
		}

		queryResults = append(queryResults, qR)
		lastQueryResult = &queryResults[len(queryResults)-1]

	}

	queryResults = ResolveORFs(queryResults)
	for _, _qR := range queryResults {
		_qR.FilterResults()
		if _qR.SearchResults.Hits.Len() > 0 {
			_qR.FetchHitsInformation(kvStores)
			queryResultChan <- _qR
		}
	}

}
