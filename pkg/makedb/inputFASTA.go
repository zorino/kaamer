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

type ProteinBufFASTA struct {
	proteinId    uint
	proteinEntry string
}

var (
	FASTA_DEF_FTS = []string{"ProteinName"}
)

func runFASTA(fileName string, kvStores *kvstore.KVStores, nbThreads int, offset uint, length uint) int {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	jobs := make(chan ProteinBufFASTA)
	results := make(chan int32, 10)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBufferFASTA(jobs, results, wg, kvStores)
	}

	// Go over a file line by line and queue up a ton of work
	go func() {
		proteinNb := uint(0)
		lastProtein := offset + length

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

		proteinEntry := ""
		line := ""

		for scanner.Scan() {
			line = scanner.Text()
			if line[0:1] == ">" {
				proteinNb += 1
				if proteinNb >= lastProtein {
					if proteinEntry != "" {
						jobs <- ProteinBufFASTA{proteinId: proteinNb, proteinEntry: proteinEntry}
					}
					break
				}
				if proteinNb >= offset {
					if proteinEntry != "" {
						jobs <- ProteinBufFASTA{proteinId: proteinNb, proteinEntry: proteinEntry}
						proteinEntry = ""
					}
				}
			}

			if proteinNb >= offset {
				proteinEntry += line
				proteinEntry += "\n"
			}

		}
		if proteinNb >= offset {
			if proteinEntry != "" {
				jobs <- ProteinBufFASTA{proteinId: proteinNb, proteinEntry: proteinEntry}
			}
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

	// Add Stats to protein_store
	kstats := &kvstore.KStats{
		NumberOfProteins:  countProteins,
		NumberOfAA:        countAA,
		NumberOfKmers:     countKmers,
		NumberOfKCombSets: 0,

		Features: FASTA_DEF_FTS,
	}
	data, err := proto.Marshal(kstats)
	if err != nil {
		log.Fatal(err.Error())
	}
	kvStores.ProteinStore.AddValueToChannel([]byte("db_stats"), data, true)

	return 0

}

func readBufferFASTA(jobs <-chan ProteinBufFASTA, results chan<- int32, wg *sync.WaitGroup, kvStores *kvstore.KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInputFASTA(j, results, kvStores)
		// results <- 1
	}

}

func processProteinInputFASTA(proteinBuf ProteinBufFASTA, results chan<- int32, kvStores *kvstore.KVStores) {

	textEntry := proteinBuf.proteinEntry
	protein := &kvstore.Protein{}
	features := map[string]string{}

	for _, l := range strings.Split(textEntry, "\n") {

		if len(l) < 1 {
			continue
		}

		switch l[0:1] {
		case ">":
			line := strings.TrimSuffix(l, "\n")
			headerSplit := strings.Split(line, " ")
			protein.EntryId = headerSplit[0][1:]
			features["ProteinName"] = strings.Join(headerSplit[1:], " ")
		default:
			protein.Sequence += strings.ToUpper(strings.TrimSuffix(l[0:], "\n"))
		}

	}

	if strings.Contains(features["ProteinName"], ", partial") {
		return
	}

	protein.Length = int32(len(protein.Sequence))

	// skip peptide shorter than kmerSize
	if protein.Length < KMER_SIZE {
		return
	}

	protein.Features = features

	results <- protein.Length

	proteinId := make([]byte, 4)
	binary.BigEndian.PutUint32(proteinId, uint32(proteinBuf.proteinId))

	data, err := proto.Marshal(protein)
	if err != nil {
		log.Fatal(err.Error())
	} else {
		kvStores.ProteinStore.AddValueToChannel(proteinId, data, false)
	}

	// sliding windows of kmerSize on Sequence
	for i := 0; i < int(protein.Length)-KMER_SIZE+1; i++ {
		kmerKey := kvStores.KmerStore.CreateBytesKey(protein.Sequence[i : i+KMER_SIZE])
		kvStores.KmerStore.AddValueToChannel(kmerKey, proteinId, false)
	}

}
