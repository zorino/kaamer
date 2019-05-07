package makedb

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	// "net/http"
	"os"
	"strings"

	"github.com/dustin/go-humanize"

	// gzip "github.com/klauspost/pgzip"
	"bufio"
	// "runtime"
	// "strconv"
	"time"

	"github.com/jlaffaye/ftp"
)

const (
	uniprot_ftp = "ftp.uniprot.org:21"
	sprot_path  = "/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_sprot_bacteria.dat.gz"
	trembl_path = "/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_trembl_bacteria.dat.gz"
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

func Download(inputPath string) {


	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		os.MkdirAll(inputPath, 0700)
	}

	dstFile, err := ioutil.TempFile(inputPath, "uniprotkb.txt.gz")
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("# Downloading uniprotkb - swissprot..")
	IODownloadFile(dstFile, sprot_path)
	fmt.Println("# Downloading uniprotkb - trembl..")
	IODownloadFile(dstFile, trembl_path)

	err = dstFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	os.Rename(dstFile.Name(), inputPath+"/uniprotkb.txt.gz")

}


func IODownloadFile(dstFile *os.File, path string) {

	c, err := ftp.DialWithOptions(uniprot_ftp, ftp.DialWithTimeout(5*time.Second))
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
