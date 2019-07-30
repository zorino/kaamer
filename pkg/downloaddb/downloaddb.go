package downloaddb

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jlaffaye/ftp"
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

type TSVOutputWriter struct {
	File     *os.File
	Buffer   *bufio.Writer
	Iterator int
}

// WriteCounter counts the number of bytes written to it. It implements to the io.Writer
// interface and we can pass this into io.TeeReader() which will report progress on each
// write cycle.
type WriteCounter struct {
	Total uint64
}

func (wc *WriteCounter) Write(p []byte) (int, error) {
	n := len(p)
	wc.Total += uint64(n)
	wc.PrintProgress()
	return n, nil
}

func (wc WriteCounter) PrintProgress() {
	// Clear the line by using a character return to go back to the start and remove
	// the remaining characters by filling it with spaces
	fmt.Printf("\r%s", strings.Repeat(" ", 35))

	// Return again and print current status of download
	// We use the humanize package to print the bytes in a meaningful way (e.g. 10 MB)
	fmt.Printf("\r  Downloading... %s complete", humanize.Bytes(wc.Total))
}

func DownloadDB(outputFile string, taxon string) {

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
	IODownloadFile(dstFileLicense, license_path)
	fmt.Printf("# Downloading uniprotkb - swissprot (%s)..\n", taxon)
	IODownloadFile(dstFile, sprot_path)
	fmt.Printf("# Downloading uniprotkb - trembl (%s)..\n", taxon)
	IODownloadFile(dstFile, trembl_path)

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

func IODownloadFile(dstFile *os.File, path string) {

	c, err := ftp.Dial(Uniprot_ftp_host, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		fmt.Println("Error c.Dial")
		log.Fatal(err.Error())
	}

	err = c.Login("anonymous", "anonymous")
	if err != nil {
		fmt.Println("Error c.Login")
		log.Fatal(err.Error())
	}

	reader, err := c.Retr(path)
	if err != nil {
		fmt.Println("Error c.Retr")
		log.Fatal(err.Error())
	}

	_, err = io.Copy(dstFile, reader)

	if err := c.Quit(); err != nil {
		log.Fatal(err)
	}

}
