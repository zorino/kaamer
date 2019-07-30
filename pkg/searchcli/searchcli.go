package searchcli

import (
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
	bodyWriter.WriteField("output-format", options.OutFormat)
	bodyWriter.WriteField("max-results", strconv.Itoa(options.MaxResults))
	bodyWriter.WriteField("annotations", strconv.FormatBool(options.Annotations))

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

	readerBuf := make([]byte, 4096)
	bytesRead := 0

	out := os.Stdout
	if options.OutputFile != "stdout" {
		out, err = os.Create(options.OutputFile)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	for {

		n, err := resp.Body.Read(readerBuf)
		bytesRead += n

		if err == io.EOF {
			fmt.Fprint(out, string(readerBuf[0:n]))
			break
		} else {
			fmt.Fprint(out, string(readerBuf))
		}

		if err != nil {
			log.Fatal("Error reading HTTP response: ", err.Error())
		}

	}

}
