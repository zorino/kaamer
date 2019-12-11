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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zorino/kaamer/pkg/kvstore"
)

type ProteinBufGBK struct {
	proteinId    uint
	proteinEntry string
}

var (
	GBK_DEF_FTS = []string{"ProteinName", "Organism", "FullTaxonomy"}
)

func runGBK(fileName string, kvStores *kvstore.KVStores, nbThreads int, offset uint, length uint) int {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	jobs := make(chan ProteinBufGBK)
	results := make(chan int32, 10)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBufferGBK(jobs, results, wg, kvStores)
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
			if line == "//" {
				proteinNb += 1
				if proteinNb >= lastProtein {
					jobs <- ProteinBufGBK{proteinId: proteinNb, proteinEntry: proteinEntry}
					break
				}
				if proteinNb >= offset {
					if proteinEntry != "" {
						jobs <- ProteinBufGBK{proteinId: proteinNb, proteinEntry: proteinEntry}
						proteinEntry = ""
					}
				}
			} else {
				if proteinNb >= offset {
					proteinEntry += line
					proteinEntry += "\n"
				}
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

		Features: GBK_DEF_FTS,
	}
	data, err := proto.Marshal(kstats)
	if err != nil {
		log.Fatal(err.Error())
	}
	kvStores.ProteinStore.AddValueToChannel([]byte("db_stats"), data, true)

	return 0

}

func readBufferGBK(jobs <-chan ProteinBufGBK, results chan<- int32, wg *sync.WaitGroup, kvStores *kvstore.KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInputGBK(j, results, kvStores)
		// results <- 1
	}

}

func processProteinInputGBK(proteinBuf ProteinBufGBK, results chan<- int32, kvStores *kvstore.KVStores) {

	textEntry := proteinBuf.proteinEntry
	protein := &kvstore.Protein{}
	features := map[string]string{}

	insideAnnotation := 0
	// 0 skip
	// 1 DEFINITION (protein product)
	// 2 VERSION
	// 3 ORGANISM
	// 4 FEATURES (CDS product / gene)
	// 5 ORIGIN (sequence)
	// 6 END of entry

	for _, l := range strings.Split(textEntry, "\n") {

		if len(l) < 2 {
			continue
		}

		switch strings.Trim(l[0:11], " ") {
		case "LOCUS":
			insideAnnotation = 0
		case "DEFINITION":
			insideAnnotation = 1
		case "ACCESSION":
			insideAnnotation = 0
		case "VERSION":
			insideAnnotation = 2
		case "KEYWORDS":
			insideAnnotation = 0
		case "SOURCE":
			insideAnnotation = 0
		case "ORGANISM":
			insideAnnotation = 3
		case "COMMENT":
			insideAnnotation = 0
		case "FEATURES":
			insideAnnotation = 4
		case "ORIGIN":
			insideAnnotation = 5
		case "//":
			insideAnnotation = 6
		}

		switch insideAnnotation {
		case 1:
			if features["ProteinName"] != "" {
				features["ProteinName"] += " "
			}
			features["ProteinName"] += l[12:]
		case 2:
			protein.EntryId = strings.Fields(l[12:])[0]
		case 3:
			if features["Organism"] == "" {
				features["Organism"] = l[12:]
			} else {
				if features["FullTaxonomy"] != "" {
					features["FullTaxonomy"] += " "
				}
				features["FullTaxonomy"] += l[12:]
			}
		case 4:
			// more annotation on the protein
		case 5:
			if l[10:] != "" {
				protein.Sequence += strings.ToUpper(strings.ReplaceAll(l[10:], " ", ""))
			}
		case 6:
			// end
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

	reg := regexp.MustCompile(` \[.*\]\.`)
	features["ProteinName"] = reg.ReplaceAllString(features["ProteinName"], "${1}")
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
