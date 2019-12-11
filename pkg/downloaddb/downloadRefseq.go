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
	"fmt"
	"log"
	"os"
	"strings"
	// "path/filepath"
)

const (
	NCBI_refseq_ftp_host = "ftp.ncbi.nlm.nih.gov:21"
	NCBI_refseq_ftp_path = "/refseq/release/"
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
