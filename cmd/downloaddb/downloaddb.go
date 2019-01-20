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
	"strconv"
	"sync"
)

type TSVOutputWriter struct {
	File          *os.File
	Buffer        *bufio.Writer
	Iterator      int
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

	filePath := inputPath+"/uniprotkb.tsv.gz"

	if _, err := os.Stat(filePath); ! os.IsNotExist(err) {
		// path/to/whatever does not exist
		os.MkdirAll(inputPath, 0700)
		PrepareFiles(filePath, 10)
		return
	}

	fmt.Println("# Raw Database Files Not Found in inputPath")

	var url = "https://www.uniprot.org/uniprot/?query=taxonomy:2&format=tab&force=true&columns=id,reviewed,protein%20names,lineage(all),go,comment(FUNCTION),comment(PATHWAY),ec,sequence&sort=score&compress=yes"

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

	numFiles, _ := strconv.Atoi(nbResults)
	numFiles = numFiles / 10000000

	PrepareFiles(filePath, numFiles)

	fmt.Println("\nPlease rerun makedb with -i " + inputPath)
	fmt.Println()

}

func PrepareFiles(filePath string, nbOfFiles int) {

	fmt.Println("# Preparing Files..")


	bufferArray := []*TSVOutputWriter{}
	baseFile := strings.Replace(filePath, ".tsv.gz", "-", -1)
	for i := 0; i < nbOfFiles; i++ {
		tsvWrite := new(TSVOutputWriter)
		outFile := baseFile + fmt.Sprintf("%03d", i) + ".tsv"
		f, _ := os.Create(outFile)
		tsvWrite.File = f
		tsvWrite.Buffer = bufio.NewWriter(f)
		tsvWrite.Iterator = 0
		bufferArray = append(bufferArray, tsvWrite)
	}

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
        buf := make([]byte, 0, 64*1024)
        scan.Buffer(buf, 1024*1024)

	n := 0

	for scan.Scan() {

		line := scan.Text()

		bufferIndex := n%nbOfFiles

		bufferArray[bufferIndex].Iterator += 1
		_, err := bufferArray[bufferIndex].Buffer.WriteString(line+"\n")

		if err != nil {
			log.Panic(err)
		}

		if (bufferArray[bufferIndex].Iterator%10000 == 0) {
			bufferArray[bufferIndex].Buffer.Flush()
		}

		n++
	}

	for i:=0; i< len(bufferArray); i++ {
		bufferArray[i].Buffer.Flush()
		bufferArray[i].File.Close()
	}

	fmt.Println("line read : %i", n)

	// os.Remove(filePath)

}
