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

package makedb

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zorino/kaamer/pkg/kvstore"
)

type ProteinBufTSV struct {
	proteinId    uint
	proteinEntry kvstore.Protein
}

func runTSV(fileName string, kvStores *kvstore.KVStores, nbThreads int, offset uint, length uint) int {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	jobs := make(chan ProteinBufTSV)
	results := make(chan int32, 10)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBufferTSV(jobs, results, wg, kvStores)
	}

	features := []string{}

	// Go over a file line by line and queue up a ton of work
	go func() {
		proteinNb := uint(0)

		buff := make([]byte, 512)
		_, err = file.Read(buff)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		filetype := http.DetectContentType(buff)
		file.Seek(0, 0)

		var scanner *bufio.Scanner

		if filetype == "application/x-gzip" {
			reader, err := gzip.NewReader(file)
			if err != nil {
				log.Fatal(err)
			}
			scanner = bufio.NewScanner(reader)
		} else {
			reader := bufio.NewReader(file)
			scanner = bufio.NewScanner(reader)
		}

		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)

		line := ""
		firstLine := true
		_cols := []string{}

		for scanner.Scan() {
			line = scanner.Text()
			if firstLine {
				// this is the header
				features = strings.Fields(line)
				hasEntryId := false
				hasSequence := false
				for _, f := range features {
					if strings.ToLower(f) == "entryid" {
						hasEntryId = true
					} else if strings.ToLower(f) == "sequence" {
						hasSequence = true
					}
				}
				if !hasEntryId {
					log.Fatal("TSV file doesn't contain 'EntryID' header")
				}
				if !hasSequence {
					log.Fatal("TSV file doesn't contain 'Sequence' header")
				}

				firstLine = false
				continue
			}

			line = strings.TrimRight(line, "\n")
			_cols = strings.Split(line, "\t")
			protein := &kvstore.Protein{}
			protein.Features = map[string]string{}
			protein.Sequence = ""
			protein.EntryId = ""

			for i, f := range _cols {
				if strings.ToLower(features[i]) == "entryid" {
					protein.EntryId = f
				} else if strings.ToLower(features[i]) == "sequence" {
					protein.Sequence = f
					protein.Length = int32(len(f))
				} else {
					protein.Features[features[i]] = f
				}
			}

			// skip peptide shorter than kmerSize
			if protein.Length < KMER_SIZE || protein.Sequence == "" || protein.EntryId == "" {
				continue
			}
			jobs <- ProteinBufTSV{proteinId: proteinNb, proteinEntry: *protein}
			proteinNb += 1
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
	countProteins := uint64(0)
	countAA := uint64(0)
	countKmers := uint64(0)

	wgGC := new(sync.WaitGroup)
	for v := range results {
		countProteins += 1
		countAA += uint64(v)
		countKmers += uint64(v) - KMER_SIZE + 1
		if countProteins%10000 == 0 {
			fmt.Printf("Processed %d proteins in %f minutes\n", countProteins, time.Since(timeStart).Minutes())
		}
		// Valuelog GC every 100K processed proteins
		if countProteins%1000000 == 0 {
			wgGC.Wait()
			wgGC.Add(2)
			go func() {
				kvStores.KmerStore.GarbageCollect(10, 0.5)
				wgGC.Done()
			}()
			go func() {
				kvStores.ProteinStore.GarbageCollect(1, 0.5)
				wgGC.Done()
			}()
		}
	}
	wgGC.Wait()

	// Remove entryid and sequence from features
	finalFeatures := []string{}
	for _, f := range features {
		if strings.ToLower(f) != "entryid" && strings.ToLower(f) != "sequence" {
			finalFeatures = append(finalFeatures, f)
		}
	}

	// Add Stats to protein_store
	kstats := &kvstore.KStats{
		NumberOfProteins:  countProteins,
		NumberOfAA:        countAA,
		NumberOfKmers:     countKmers,
		NumberOfKCombSets: 0,

		Features: finalFeatures,
	}
	data, err := proto.Marshal(kstats)
	if err != nil {
		log.Fatal(err.Error())
	}
	kvStores.ProteinStore.AddValueToChannel([]byte("db_stats"), data, true)

	return 0

}

func readBufferTSV(jobs <-chan ProteinBufTSV, results chan<- int32, wg *sync.WaitGroup, kvStores *kvstore.KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInputTSV(j, results, kvStores)
	}

}

func processProteinInputTSV(proteinBuf ProteinBufTSV, results chan<- int32, kvStores *kvstore.KVStores) {

	results <- proteinBuf.proteinEntry.Length

	proteinId := make([]byte, 4)
	binary.BigEndian.PutUint32(proteinId, uint32(proteinBuf.proteinId))

	data, err := proto.Marshal(&proteinBuf.proteinEntry)
	if err != nil {
		log.Fatal(err.Error())
	} else {
		kvStores.ProteinStore.AddValueToChannel(proteinId, data, false)
	}

	// sliding windows of kmerSize on Sequence
	for i := 0; i < int(proteinBuf.proteinEntry.Length)-KMER_SIZE+1; i++ {
		kmerKey := kvStores.KmerStore.CreateBytesKey(proteinBuf.proteinEntry.Sequence[i : i+KMER_SIZE])
		kvStores.KmerStore.AddValueToChannel(kmerKey, proteinId, false)
	}

}
