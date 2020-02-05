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

package downloaddb

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
)

const (
	NCBI_refseq_ftp_host = "ftp.ncbi.nlm.nih.gov:21"
	NCBI_refseq_ftp_path = "/refseq/release/"
	NCBI_eutil_api_path  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
)

var (
	NCBI_refseq_valid = map[string]bool{
		"archaea":              true,
		"bacteria":             true,
		"fungi":                true,
		"invertebrate":         true,
		"mitochondrion":        true,
		"plant":                true,
		"plasmid":              true,
		"plastid":              true,
		"protozoa":             true,
		"viral":                true,
		"vertebrate_mammalian": true,
		"vertebrate_other":     true,
	}
)

func DownloadRefseq(outputFile string, taxon string) {

	if outputFile == "" {
		outputFile = "refseq-" + taxon + ".gpff.gz"
	}

	dstFile, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	refseq_path := NCBI_refseq_ftp_path + taxon

	entries := IOListFTP(NCBI_refseq_ftp_host, refseq_path)

	for _, e := range entries {
		if strings.Contains(e.Name, ".nonredundant_protein.") && strings.Contains(e.Name, ".gpff.gz") {
			fmt.Printf("# Downloading %s into %s..\n", e.Name, outputFile)
			IODownloadFTP(dstFile, NCBI_refseq_ftp_host, (refseq_path + "/" + e.Name))
		}
	}

	err = dstFile.Close()
	if err != nil {
		log.Fatal(err)
	}

}

func DownloadGenbankGenome(genomeId string) {

	// Search the corresponding ID in the API
	url := NCBI_eutil_api_path + "esearch.fcgi?db=nucleotide&term=" + genomeId
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	var eSearchResult struct {
		Id string `xml:"IdList>Id"`
	}
	if err := xml.Unmarshal(body, &eSearchResult); err != nil {
		log.Fatal(err)
	}

	// Download the genome
	genomeFileName := genomeId + ".gbk"
	dstFile, err := os.Create(genomeFileName)
	if err != nil {
		log.Fatal(err.Error())
	}

	url = NCBI_eutil_api_path + "efetch.fcgi?db=nucleotide&rettype=gb&id=" + eSearchResult.Id
	IODownloadHTTP(dstFile, url)
	dstFile.Close()

	ParseGenbank(genomeFileName)

}

func ParseGenbank(gbkFile string) {

	outputFile, err := os.Create(strings.Replace(gbkFile, ".gbk", ".tsv", -1))

	if err != nil {
		log.Fatal(err)
	}

	var scanner *bufio.Scanner

	file, err := os.Open(gbkFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	reader := bufio.NewReader(file)
	scanner = bufio.NewScanner(reader)

	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	type CDS struct {
		EntryId     string
		ProteinName string
		GeneName    string
		Sequence    string
	}

	var cds = CDS{}

	line := ""
	insideCDS := false
	insideProteinName := false
	insideTranslation := false

	attributeReg := regexp.MustCompile(`\s+\/.*="(.*)`)
	geneReg := regexp.MustCompile(`\s+/gene="(.*)"`)
	proteinIdReg := regexp.MustCompile(`\s+/protein_id="(.*)"`)
	productReg := regexp.MustCompile(`\s+/product="(.*)`)
	translationReg := regexp.MustCompile(`\s+/translation="(.*)`)

	fmt.Fprintf(outputFile, "EntryID\tGeneName\tProteinName\tSequence\n")

	for scanner.Scan() {

		line = scanner.Text()
		if len(line) < 21 {
			continue
		}

		if line[0:21] == "     CDS             " {
			insideCDS = true
			if cds.EntryId != "" {
				if string(cds.ProteinName[len(cds.ProteinName)-1]) == "\"" {
					cds.ProteinName = cds.ProteinName[:len(cds.ProteinName)-1]
				}
				if string(cds.Sequence[len(cds.Sequence)-1]) == "\"" {
					cds.Sequence = cds.Sequence[:len(cds.Sequence)-1]
				}
				fmt.Fprintf(outputFile, "%s\t%s\t%s\t%s\n", cds.EntryId, cds.GeneName, cds.ProteinName, cds.Sequence)
			}
			cds = CDS{}

		} else if line[0:21] != "                     " {
			insideCDS = false
		}

		if insideCDS {
			if len(attributeReg.FindStringSubmatch(line)) != 0 {
				insideProteinName = false
				insideTranslation = false
			}

			// Continue translation or protein name
			if insideTranslation {
				cds.Sequence += strings.Trim(line, " ")
			}
			if insideProteinName {
				cds.ProteinName += strings.Trim(line, " ")
			}

			// Detect qualifier
			if strings.Contains(line, "/gene=") {
				cds.GeneName = geneReg.FindStringSubmatch(line)[1]
			}
			if strings.Contains(line, "/product=") {
				cds.ProteinName = productReg.FindStringSubmatch(line)[1]
				insideProteinName = true
			}
			if strings.Contains(line, "/translation=") {
				cds.Sequence = translationReg.FindStringSubmatch(line)[1]
				insideTranslation = true
			}
			if strings.Contains(line, "/protein_id=") {
				cds.EntryId = proteinIdReg.FindStringSubmatch(line)[1]
			}

		}
	}

	outputFile.Close()

}
