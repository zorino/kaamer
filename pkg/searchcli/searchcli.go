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

package searchcli

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"

	"github.com/zorino/kaamer/pkg/search"
)

type SearchRequestOptions struct {
	ServerHost string
	Sequence   string
	OutputFile string
	search.SearchOptions
}

func NewSearchRequest(options SearchRequestOptions) {

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	bodyWriter.WriteField("type", options.InputType)
	bodyWriter.WriteField("gcode", strconv.Itoa(options.GeneticCode))
	bodyWriter.WriteField("output-format", options.OutFormat)
	bodyWriter.WriteField("max-results", strconv.Itoa(options.MaxResults))
	bodyWriter.WriteField("align", strconv.FormatBool(options.Align))
	bodyWriter.WriteField("annotations", strconv.FormatBool(options.Annotations))
	bodyWriter.WriteField("positions", strconv.FormatBool(options.ExtractPositions))

	host := options.ServerHost + "/api/search/"
	switch options.SequenceType {
	case search.PROTEIN:
		host += "protein"
	case search.NUCLEOTIDE:
		host += "nucleotide"
	case search.READS:
		host += "fastq"
	}

	if options.InputType == "file" {

		fileWriter, err := bodyWriter.CreateFormFile("file", options.File)

		dat, err := ioutil.ReadFile(options.File)

		if err != nil {
			log.Fatal(err.Error())
		}

		fileWriter.Write(dat)

	} else if options.InputType == "path" {
		bodyWriter.WriteField("file", options.File)
	}

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()

	resp, err := http.Post(host, contentType, bodyBuf)

	if err != nil || resp.StatusCode == 502 {
		fmt.Printf("No kaamer-db server running at %s\n", options.ServerHost)
		os.Exit(1)
	}

	defer resp.Body.Close()

	out := os.Stdout
	if options.OutputFile != "stdout" {
		out, err = os.Create(options.OutputFile)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	reader := bufio.NewReader(resp.Body)
	runeSep := byte('\n')
	if options.OutFormat == "json" {
		runeSep = byte(',')
	}
	for {
		chunk, err := reader.ReadBytes(runeSep)

		if err == io.EOF {
			fmt.Fprint(out, string(chunk))
			break
		} else {
			fmt.Fprint(out, string(chunk))
		}

	}

}
