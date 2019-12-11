package downloaddb

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jlaffaye/ftp"
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

func IODownloadFTP(dstFile *os.File, host string, path string) {

	c, err := ftp.Dial(host, ftp.DialWithTimeout(5*time.Second))
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

func IOListFTP(host string, path string) []*ftp.Entry {

	c, err := ftp.Dial(host, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		fmt.Println("Error c.Dial")
		log.Fatal(err.Error())
	}

	err = c.Login("anonymous", "anonymous")
	if err != nil {
		fmt.Println("Error c.Login")
		log.Fatal(err.Error())
	}

	listing, err := c.List(path)
	if err != nil {
		fmt.Println("Error c.Retr")
		log.Fatal(err.Error())
	}

	if err := c.Quit(); err != nil {
		log.Fatal(err)
	}

	return listing

}
