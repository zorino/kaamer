package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zorino/kaamer/pkg/search"
	"github.com/zorino/kaamer/pkg/searchcli"
)

var (
	validQueryType    = map[string]int{"prot": search.PROTEIN, "nt": search.NUCLEOTIDE, "fastq": search.READS}
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

      -i            input file (fasta or fastq)

      -m            max number of results (default 10)

      -o            output file (default stdout)

      -fmt          (tsv, json) output format (default tsv)

    (flag)

      -ann          add hit annotations in tsv fmt (always true in json fmt)

`

	var searchOpt = flag.Bool("search", false, "program")

	var serverHost = flag.String("h", "http://localhost:8321", "server URL")
	var inputFile = flag.String("i", "", "input file")
	var queryTypeArg = flag.String("t", "", "query type")
	var maxResults = flag.Int("m", 10, "max number of results")
	var outputFile = flag.String("o", "stdout", "output file")
	var outputFormat = flag.String("fmt", "tsv", "output format")
	var addAnnotation = flag.Bool("ann", false, "add annotation flag")

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
			dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
			*inputFile = dir + "/" + *inputFile
		} else {
			// remote server
			options.InputType = "file"
		}

		options.File = *inputFile
		options.SequenceType = queryType
		options.OutFormat = *outputFormat
		options.MaxResults = *maxResults
		options.ExtractPositions = false
		options.Annotations = *addAnnotation

		searchcli.NewSearchRequest(options)

		os.Exit(0)

	}

	fmt.Println(usage)
	os.Exit(0)

}
