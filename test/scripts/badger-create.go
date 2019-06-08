package main

import (
	"github.com/dgraph-io/badger"
	"log"
	"fmt"
)


func main() {


	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	wb := db.NewWriteBatch()
	defer wb.Cancel()

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		err := wb.Set([]byte(key), []byte(val), 0) // Will create txns as needed.
		if err != nil {
			log.Fatal(err)
		}
	}

	wb.Flush()

	fmt.Println("Bulk upload done")

}
