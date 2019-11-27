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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zorino/kaamer/pkg/kvstore"
)

type ProteinBufEMBL struct {
	proteinId    uint
	proteinEntry string
}

var (
	EMBL_DEF_FTS = []string{"ProteinName", "GeneName", "EC", "GO", "KEGG_ID", "BioCyc_ID", "HAMAP", "Organism", "TaxId", "FullTaxonomy"}
)

func runEMBL(fileName string, kvStores *kvstore.KVStores, nbThreads int, offset uint, length uint) int {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	jobs := make(chan ProteinBufEMBL)
	results := make(chan int32, 10)
	wg := new(sync.WaitGroup)

	// thread pool
	for w := 1; w <= nbThreads; w++ {
		wg.Add(1)
		go readBufferEMBL(jobs, results, wg, kvStores)
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
					jobs <- ProteinBufEMBL{proteinId: proteinNb, proteinEntry: proteinEntry}
					break
				}
				if proteinNb >= offset {
					if proteinEntry != "" {
						jobs <- ProteinBufEMBL{proteinId: proteinNb, proteinEntry: proteinEntry}
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

		Features: EMBL_DEF_FTS,
	}
	data, err := proto.Marshal(kstats)
	if err != nil {
		log.Fatal(err.Error())
	}
	kvStores.ProteinStore.AddValueToChannel([]byte("db_stats"), data, true)

	return 0

}

func readBufferEMBL(jobs <-chan ProteinBufEMBL, results chan<- int32, wg *sync.WaitGroup, kvStores *kvstore.KVStores) {

	defer wg.Done()
	// line by line
	for j := range jobs {
		processProteinInputEMBL(j, results, kvStores)
		// results <- 1
	}

}

func processProteinInputEMBL(proteinBuf ProteinBufEMBL, results chan<- int32, kvStores *kvstore.KVStores) {

	textEntry := proteinBuf.proteinEntry
	protein := &kvstore.Protein{}
	features := map[string]string{}

	reg := regexp.MustCompile(` \{.*\}\.`)
	var fields []string

	for _, l := range strings.Split(textEntry, "\n") {

		if len(l) < 2 {
			continue
		}
		switch l[0:2] {
		case "ID":
			protein.EntryId = strings.Fields(l[5:])[0]
		case "GN":
			if features["GeneName"] == "" && strings.Contains(l, "Name=") {
				geneName := strings.Fields(l[5:])[0][5:]
				features["GeneName"] = strings.TrimRight(geneName, ";")
			}
		case "DE":
			if strings.Contains(l[5:], "RecName") {
				features["ProteinName"] = strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
			} else if strings.Contains(l[5:], "SubName") {
				if features["ProteinName"] != "" {
					features["ProteinName"] += ";;"
					features["ProteinName"] += strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
				} else {
					features["ProteinName"] = strings.TrimRight(reg.ReplaceAllString(l[19:], "${1}"), ";")
				}
			} else if strings.Contains(l[5:], "EC=") {
				features["EC"] = strings.TrimRight(reg.ReplaceAllString(l[17:], "${1}"), ";")
				// protein.EC = strings.TrimRight(reg.ReplaceAllString(l[17:], "${1}"), ";")
			} else if strings.Contains(l[5:], "Flags: Fragment;") {
				// skipping protein fragments
				return
			}
		case "OX":
			taxId := strings.Fields(l[5:])[0][12:]
			features["TaxId"] = strings.TrimRight(taxId, ";")
		case "OS":
			if _, ok := features["Organism"]; ok {
				features["Organism"] += " "
				features["Organism"] += strings.TrimRight(l[5:], ".")
			} else {
				features["Organism"] += strings.TrimRight(l[5:], ".")
			}
			// features["Organism"] = strings.TrimRight(l[5:], ".")
		case "OC":
			features["FullTaxonomy"] += l[5:]
			if features["FullTaxonomy"] != "" {
				features["FullTaxonomy"] += " "
			}
			features["FullTaxonomy"] += l[5:]
		case "DR":
			fields = strings.Fields(l[5:])
			switch fields[0] {
			case "KEGG;":
				if _, ok := features["KEGG_ID"]; ok {
					features["KEGG_ID"] += ";"
					features["KEGG_ID"] += strings.TrimRight(fields[1], ";")
				} else {
					features["KEGG_ID"] += strings.TrimRight(fields[1], ";")
				}
				// protein.KEGG = append(protein.KEGG, strings.TrimRight(fields[1], ";"))
			case "GO;":
				if _, ok := features["GO"]; ok {
					features["GO"] += ";"
					features["GO"] += strings.TrimRight(fields[1], ";")
				} else {
					features["GO"] += strings.TrimRight(fields[1], ";")
				}
				// protein.GO = append(protein.GO, strings.TrimRight(fields[1], ";"))
			case "BioCyc;":
				if _, ok := features["BioCyc_ID"]; ok {
					features["BioCyc_ID"] += ";"
					features["BioCyc_ID"] += strings.TrimRight(fields[1], ";")
				} else {
					features["BioCyc_ID"] += strings.TrimRight(fields[1], ";")
				}
				// protein.BioCyc = append(protein.BioCyc, strings.TrimRight(fields[1], ";"))
			case "HAMAP;":
				if _, ok := features["HAMAP"]; ok {
					features["HAMAP"] += ";"
					features["HAMAP"] += strings.TrimRight(fields[1], ";")
				} else {
					features["HAMAP"] += strings.TrimRight(fields[1], ";")
				}
				// protein.HAMAP = append(protein.HAMAP, strings.TrimRight(fields[1], ";"))
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
