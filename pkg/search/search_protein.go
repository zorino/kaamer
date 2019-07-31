package search

import (
	"net/http"
	"sync"

	cnt "github.com/zorino/counters"
	"github.com/zorino/kaamer/pkg/kvstore"
)

func ProteinSearch(file string, kvStores *kvstore.KVStores, nbOfThreads int, w http.ResponseWriter) {

	queryChan := make(chan Query, 5)

	wgReader := new(sync.WaitGroup)
	wgReader.Add(1)

	go func() {
		defer wgReader.Done()
		GetQueriesFasta(file, queryChan, true)
		close(queryChan)
	}()

	// Concurrent query results writer
	queryResultChan := make(chan QueryResult, 5)
	wgWriter := new(sync.WaitGroup)
	wgWriter.Add(1)
	go QueryResultResponseWriter(queryResultChan, w, wgWriter)

	wgSearch := new(sync.WaitGroup)

	for i := 0; i < nbOfThreads; i++ {

		wgSearch.Add(1)

		go func() {

			defer wgSearch.Done()

			queryResult := QueryResult{}

			for q := range queryChan {

				q.Type = PROTEIN_QUERY

				if q.SizeInKmer < 7 {
					return
				}

				searchRes := new(SearchResults)
				searchRes.Counter = cnt.NewCounterBox()
				searchRes.PositionHits = make(map[uint32][]bool)

				var matchPositionChan chan MatchPosition
				var wgMP sync.WaitGroup
				if searchOptions.ExtractPositions {
					matchPositionChan = make(chan MatchPosition, 10)
					wgMP := new(sync.WaitGroup)
					wgMP.Add(1)
					go searchRes.StoreMatchPositions(matchPositionChan, wgMP)
				}

				keyChan := make(chan KeyPos, 20)
				_wg := new(sync.WaitGroup)
				_wg.Add(1)
				go searchRes.KmerSearch(keyChan, kvStores, _wg, matchPositionChan)

				for k := 0; k < q.SizeInKmer; k++ {
					key := kvStores.KmerStore.CreateBytesKey(q.Sequence[k : k+KMER_SIZE])
					keyChan <- KeyPos{Key: key, Pos: k, QSize: q.SizeInKmer}
				}

				close(keyChan)
				_wg.Wait()

				if searchOptions.ExtractPositions {
					close(matchPositionChan)
					wgMP.Wait()
				}

				searchRes.Hits = sortMapByValue(searchRes.Counter.GetCountersMap())

				queryResult = QueryResult{Query: q, SearchResults: searchRes, HitEntries: map[uint32]kvstore.Protein{}}
				queryResult.FilterResults(0.2)
				queryResult.FetchHitsInformation(kvStores)

				queryResultChan <- queryResult

			}

		}()

	}

	wgReader.Wait()

	wgSearch.Wait()
	close(queryResultChan)

	wgWriter.Wait()

}
