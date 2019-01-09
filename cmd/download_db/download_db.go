package download_db

import (
	"fmt"
	"os"
	"net/http"
	"log"
	"io"
	"github.com/dustin/go-humanize"
	"strings"
	gzip "github.com/klauspost/pgzip"
	"bufio"
	"runtime"
)

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

	filePath := inputPath+"/uniprotkb.tsv.gz"

	if _, err := os.Stat(filePath); ! os.IsNotExist(err) {
		// path/to/whatever does not exist
		os.MkdirAll(inputPath, 0700)
		PrepareFiles(filePath)
		return
	}

	fmt.Println("# Raw Database Files Not Found in inputPath")

	var url = "https://www.uniprot.org/uniprot/?query=taxonomy:2&format=tab&force=true&columns=id,entry%20name,protein%20names,genes,go,comment(FUNCTION),comment(PATHWAY),lineage-id(SUPERKINGDOM),lineage(SUPERKINGDOM),lineage-id(PHYLUM),lineage(PHYLUM),lineage-id(CLASS),lineage(CLASS),lineage-id(ORDER),lineage(ORDER),lineage-id(FAMILY),lineage(FAMILY),lineage-id(GENUS),lineage(GENUS),lineage-id(SPECIES),lineage(SPECIES),sequence&sort=score&compress=yes"

	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		// path/to/whatever does not exist
		os.MkdirAll(inputPath, 0700)
	}

	fmt.Println("# Downloading Database :")

	res, err := http.Head(url)
	if err != nil {
		panic(err)
	}

	nbResults := res.Header.Get("X-Total-Results")
	uniprotVersion := res.Header.Get("X-Uniprot-Release")

	fmt.Println("  UniprotKB (2:Bacteria) release " + uniprotVersion)
	fmt.Println("  Number of Proteins : " + nbResults)

	// Create the file
	out, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	// Create our progress reporter and pass it to be used alongside our writer
	counter := &WriteCounter{}
	_, err = io.Copy(out, io.TeeReader(resp.Body, counter))
	if err != nil {
		log.Fatal(err)
	}

	PrepareFiles(filePath)

	fmt.Println("\nPlease rerun makedb with -i " + inputPath)
	fmt.Println()

}

func PrepareFiles(filePath string) {

	fmt.Println("# Preparing Files..")

	runtime.GOMAXPROCS(runtime.NumCPU())

	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	r, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}

	scan := bufio.NewScanner(r)

	n := 0
	fileNum := 0

	baseFile := strings.Replace(filePath, ".tsv.gz", "-", -1)
	outFile := baseFile + fmt.Sprintf("%03d", fileNum) + ".tsv"
	f, err := os.Create(outFile)
	w := bufio.NewWriter(f)

	for scan.Scan() {
		line := scan.Text()
		if n < 1 {
			n++
			continue
		}

		w.WriteString(line+"\n")

		if (n%10000 == 0) {
			w.Flush()
		}

		if (n%10000000 == 0) {
			f.Close()
			fileNum += 1
			outFile := baseFile + fmt.Sprintf("%03d", fileNum) + ".tsv"
			f, _ := os.Create(outFile)
			w = bufio.NewWriter(f)
		}

		n++
	}

	f.Close()

	os.Remove(filePath)

}
