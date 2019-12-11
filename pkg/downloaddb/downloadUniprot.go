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
	"path/filepath"
)

const (
	Uniprot_ftp_host           = "ftp.uniprot.org:21"
	Uniprot_ftp_taxonomic_path = "/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/"
)

var (
	Uniprot_ftp_taxonomic_valid = map[string]bool{
		"archaea":       true,
		"bacteria":      true,
		"fungi":         true,
		"human":         true,
		"invertebrates": true,
		"mammals":       true,
		"plants":        true,
		"rodents":       true,
		"vertebrates":   true,
		"viruses":       true,
	}
)

func DownloadUniprot(outputFile string, taxon string) {

	if outputFile == "" {
		outputFile = "uniprotkb-" + taxon + ".dat.gz"
	}

	outputPath := filepath.Dir(outputFile)

	dstFileLicense, err := os.Create(outputPath + "/LICENSE")
	if err != nil {
		log.Fatal(err.Error())
	}

	dstFile, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	license_path := Uniprot_ftp_taxonomic_path + "LICENSE"
	sprot_path := Uniprot_ftp_taxonomic_path + "uniprot_sprot_" + taxon + ".dat.gz"
	trembl_path := Uniprot_ftp_taxonomic_path + "uniprot_trembl_" + taxon + ".dat.gz"

	fmt.Println("# Downloading uniprotkb - LICENSE..")
	IODownloadFTP(dstFileLicense, Uniprot_ftp_host, license_path)
	fmt.Printf("# Downloading uniprotkb - swissprot (%s)..\n", taxon)
	IODownloadFTP(dstFile, Uniprot_ftp_host, sprot_path)
	fmt.Printf("# Downloading uniprotkb - trembl (%s)..\n", taxon)
	IODownloadFTP(dstFile, Uniprot_ftp_host, trembl_path)

	err = dstFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = dstFileLicense.Close()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("See LICENSE : %s\n", outputPath+"/LICENSE")

}
