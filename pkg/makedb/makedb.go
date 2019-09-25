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
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/options"
	"github.com/golang/protobuf/proto"
	"github.com/zorino/kaamer/pkg/indexdb"
	"github.com/zorino/kaamer/pkg/kvstore"
)

const (
	KMER_SIZE = 7
)

type ProteinBuf struct {
	proteinId    uint
	proteinEntry string
}

func NewMakedb(dbPath string, inputPath string, offset uint, lenght uint, maxSize bool, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode, noIndex bool) {

	runtime.GOMAXPROCS(128)

	os.Mkdir(dbPath, 0700)

	threadByWorker := runtime.NumCPU()

	if threadByWorker < 1 {
		threadByWorker = 1
	}

	fmt.Printf("# Making Database %s from %s\n", dbPath, inputPath)
	fmt.Printf("# Using %d CPU\n", threadByWorker)

	kvStores := kvstore.KVStoresNew(dbPath, threadByWorker, tableLoadingMode, valueLoadingMode, maxSize, false, false)
	kvStores.OpenInsertChannel()
	run(inputPath, kvStores, threadByWorker, offset, lenght)
	kvStores.CloseInsertChannel()
	kvStores.Close()

	kvStores = kvstore.KVStoresNew(dbPath, threadByWorker, tableLoadingMode, valueLoadingMode, maxSize, false, false)

	fmt.Printf("# Flattening KmerStore...\n")
	kvStores.KmerStore.DB.Flatten(threadByWorker)
	fmt.Printf("# Flattening ProteinStore...\n")
	kvStores.ProteinStore.DB.Flatten(threadByWorker)

	fmt.Printf("# GC KmerStore...\n")
	kvStores.KmerStore.GarbageCollect(1000, 0.5)
	fmt.Printf("# GC ProteinStore...\n")
	kvStores.ProteinStore.GarbageCollect(1000, 0.5)

	kvStores.Close()

	if !noIndex {
		indexdb.NewIndexDB(dbPath, maxSize, tableLoadingMode, valueLoadingMode)
	}

}

func run(fileName string, kvStores *kvstore.KVStores, nbThreads int, offset uint, length uint) int {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	jobs := make(chan ProteinBuf)
	results := make(chan int32, 10)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBuffer(jobs, results, wg, kvStores)
	}

	// Go over a file line by line and queue up a ton of work
	go func() {
		proteinNb := uint(0)
		lastProtein := offset + length
		gz, err := gzip.NewReader(file)
		defer gz.Close()
		if err != nil {
			log.Fatal(err)
		}
		scanner := bufio.NewScanner(gz)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		proteinEntry := ""
		line := ""
		for scanner.Scan() {
			line = scanner.Text()
			if line == "//" {
				proteinNb += 1
				if proteinNb >= lastProtein {
					jobs <- ProteinBuf{proteinId: proteinNb, proteinEntry: proteinEntry}
					break
				}
				if proteinNb >= offset {
					if proteinEntry != "" {
						jobs <- ProteinBuf{proteinId: proteinNb, proteinEntry: proteinEntry}
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
	}
	data, err := proto.Marshal(kstats)
	if err != nil {
		log.Fatal(err.Error())
	}
	kvStores.ProteinStore.AddValueToChannel([]byte("db_stats"), data, true)

	return 0

}

func readBuffer(jobs <-chan ProteinBuf, results chan<- int32, wg *sync.WaitGroup, kvStores *kvstore.KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInput(j, results, kvStores)
		// results <- 1
	}

}

func processProteinInput(proteinBuf ProteinBuf, results chan<- int32, kvStores *kvstore.KVStores) {

	textEntry := proteinBuf.proteinEntry
	protein := &kvstore.Protein{}
	reg := regexp.MustCompile(` \{.*\}\.`)
	var fields []string

	for _, l := range strings.Split(textEntry, "\n") {

		if len(l) < 2 {
			continue
		}
		switch l[0:2] {
		case "ID":
			protein.Entry = strings.Fields(l[5:])[0]
		case "DE":
			if strings.Contains(l[5:], "RecName") {
				protein.ProteinName = strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
			} else if strings.Contains(l[5:], "SubName") {
				if protein.ProteinName != "" {
					protein.ProteinName += ";;"
					protein.ProteinName += strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
				} else {
					protein.ProteinName = strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
				}
			} else if strings.Contains(l[5:], "EC=") {
				protein.EC = strings.TrimRight(reg.ReplaceAllString(l[17:], "${1}"), ";")
			}
		case "OS":
			protein.Organism = strings.TrimRight(l[5:], ".")
		case "OC":
			protein.Taxonomy += l[5:]
			if protein.Taxonomy != "" {
				protein.Taxonomy += " "
			}
			protein.Taxonomy += l[5:]
		case "DR":
			fields = strings.Fields(l[5:])
			switch fields[0] {
			case "KEGG;":
				protein.KEGG = append(protein.KEGG, strings.TrimRight(fields[1], ";"))
			case "GO;":
				protein.GO = append(protein.GO, strings.TrimRight(fields[1], ";"))
			case "BioCyc;":
				protein.BioCyc = append(protein.BioCyc, strings.TrimRight(fields[1], ";"))
			case "HAMAP;":
				protein.HAMAP = append(protein.HAMAP, strings.TrimRight(fields[1], ";"))
			}
		case "SQ":
			fields = strings.Fields(l[5:])
			len, _ := strconv.Atoi(fields[1])
			protein.Length = int32(len)

		case "  ":
			protein.Sequence += strings.ReplaceAll(l[5:], " ", "")
		}
	}

	// skip peptide shorter than kmerSize
	if protein.Length < KMER_SIZE {
		return
	}

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
