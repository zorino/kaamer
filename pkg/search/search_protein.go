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

func ProteinSearch(searchOptions SearchOptions, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter, cancelQuery *bool) {

	file := searchOptions.File

	queryChan := make(chan Query, 5)

	wgReader := new(sync.WaitGroup)
	wgReader.Add(1)

	go func() {
		defer wgReader.Done()
		GetQueriesFasta(file, queryChan, true)
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

			if *cancelQuery {
				return
			}

			queryResult := QueryResult{}
			searchRes := new(SearchResults)
			keyChan := make(chan KeyPos, 20)

			for q := range queryChan {

				q.Type = PROTEIN_QUERY

				if q.SizeInKmer < 7 {
					return
				}

				searchRes = new(SearchResults)
				searchRes.Counter = cnt.NewCounterBox()
				searchRes.PositionHits = make(map[uint32][]bool)

				matchPositionChan := make(chan MatchPosition, 10)
				wgMP := new(sync.WaitGroup)
				if searchOptions.ExtractPositions {
					wgMP.Add(1)
					go searchRes.StoreMatchPositions(matchPositionChan, wgMP)
				}

				keyChan = make(chan KeyPos, 10)
				_wg := new(sync.WaitGroup)
				_wg.Add(1)
				go searchRes.KmerSearch(keyChan, kvStores, _wg, matchPositionChan)

				for k := 0; k < q.SizeInKmer; k++ {
					key := kvStores.KmerStore.CreateBytesKey(q.Sequence[k : k+KMER_SIZE])
					keyChan <- KeyPos{Key: key, Pos: k, QSize: q.SizeInKmer}
				}

				close(keyChan)
				_wg.Wait()
				close(matchPositionChan)
				wgMP.Wait()

				searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())

				queryResult = QueryResult{Query: q, SearchResults: searchRes, HitEntries: map[uint32]kvstore.Protein{}}
				queryResult.FilterResults()
				queryResult.FetchHitsInformation(kvStores)

				queryResultChan <- queryResult

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
