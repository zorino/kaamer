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

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/zorino/kaamer/pkg/align"
	"github.com/zorino/kaamer/pkg/search"
	"github.com/zorino/kaamer/pkg/searchcli"
)

var (
	validQueryType    = map[string]int{"prot": search.PROTEIN, "nt": search.NUCLEOTIDE, "fastq": search.READS}
	validGeneticCode  = search.GCodes
	validOutputFormat = map[string]bool{"tsv": true, "json": true}
)

func main() {

	usage := `
 kaamer

  // Search

  -search           search for a query

    (input)

      -h            server host (default http://localhost:8321)

      -t            (prot, nt, fastq) query type

      -g            genetic code for nt/fastq type (default: 11 for bacteria)

      -i            input file (fasta or fastq)

      -m            max number of results (default 10)

      -o            output file (default stdout)

      -fmt          (tsv, json) output format (default tsv)

    (flag)

      -aln          do an alignment for query / database hit matches

      -ann          add hit annotations in tsv fmt (always true in json fmt)

      -pos          add query positions that hit


    // aln options

      -mink         minimum number of k-mer matches to report a hit (default: 10)
      -minr         minimum ratio of query k-mer matches to report a hit (default: 0.05)

      -mat          substitution matrix (default: BLOSUM62)
      -gop          gap open penalty (default: 11)
      -gex          gap extension penalty (default: 1)

`

	var searchOpt = flag.Bool("search", false, "program")

	var serverHost = flag.String("h", "http://localhost:8321", "server URL")
	var inputFile = flag.String("i", "", "input file")
	var queryTypeArg = flag.String("t", "", "query type")
	var geneticCode = flag.Int("g", 11, "genetic code")
	var maxResults = flag.Int("m", 10, "max number of results")
	var outputFile = flag.String("o", "stdout", "output file")
	var outputFormat = flag.String("fmt", "tsv", "output format")
	var addAlignment = flag.Bool("aln", false, "add alignment flag")
	var addAnnotation = flag.Bool("ann", false, "add annotation flag")
	var addPositions = flag.Bool("pos", false, "add position flag")

	var minKMatch = flag.Int64("mink", 10, "minimum number of k-mer matches to report a hit")
	var minKRatio = flag.Float64("minr", 0.05, "minimum ratio of query k-mer matches to report a hit")
	var subMatrix = flag.String("mat", "blosum62", "substitution matrix")
	var gapOpen = flag.Int("gop", 11, "gap open penalty")
	var gapExtend = flag.Int("gex", 1, "gap extension penalty")

	/* CLI usage */
	flag.Usage = func() {
		fmt.Println(usage)
	}
	flag.Parse()

	if *searchOpt == true {

		if *inputFile == "" {
			fmt.Println("No query intput file !")
			os.Exit(1)
		}

		var ok = false
		var queryType int

		if queryType, ok = validQueryType[*queryTypeArg]; !ok {
			fmt.Println("Invalid query type ! use prot, nt or reads !")
			os.Exit(1)
		}

		if _, ok = validGeneticCode[*geneticCode]; !ok {
			fmt.Println("Invalid genetic code !")
			os.Exit(1)
		}

		if _, ok = validOutputFormat[*outputFormat]; !ok {
			fmt.Println("Invalid output format ! use tsv or json !")
			os.Exit(1)
		}

		if !strings.Contains(*serverHost, "http://") && !strings.Contains(*serverHost, "https://") {
			fmt.Println("Server URL (-s) needs the http(s):// !")
			os.Exit(1)
		}

		options := searchcli.SearchRequestOptions{
			ServerHost: *serverHost,
			Sequence:   "",
			OutputFile: *outputFile,
		}

		hostDomaine := strings.Split(*serverHost, "/")[2]
		if strings.Contains(hostDomaine, "localhost") || strings.Contains(hostDomaine, "127.0.0.1") {
			// sequence is on the same host as the server
			options.InputType = "path"
			if (*inputFile)[0] != '/' {
				dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
				*inputFile = dir + "/" + *inputFile
			}
		} else {
			// remote server
			options.InputType = "file"
		}

		subMatrixKey := *subMatrix + "_" + strconv.Itoa(*gapOpen) + "_" + strconv.Itoa(*gapExtend)
		if _, ok = align.AllMatrixScores[subMatrixKey]; !ok {
			fmt.Println("Invalid Substitution matrix and gap penalty options !")
			os.Exit(1)
		}

		options.File = *inputFile
		options.SequenceType = queryType
		options.GeneticCode = *geneticCode
		options.OutFormat = *outputFormat
		options.MaxResults = *maxResults
		options.Align = *addAlignment
		options.ExtractPositions = *addPositions
		options.Annotations = *addAnnotation
		options.MinKMatch = *minKMatch
		options.MinKRatio = *minKRatio
		options.SubMatrix = *subMatrix
		options.GapOpen = *gapOpen
		options.GapExtend = *gapExtend

		searchcli.NewSearchRequest(options)

		os.Exit(0)

	}

	fmt.Println(usage)
	os.Exit(0)

}
