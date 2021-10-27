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

package makedb

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/zorino/kaamer/pkg/indexdb"
	"github.com/zorino/kaamer/pkg/kvstore"
)

const (
	KMER_SIZE = 7 // 7 is currently the only supported kmer size
)

func NewMakedb(dbPath string, inputPath string, inputFmt string, threadByWorker int, offset uint, lenght uint, maxSize bool, noIndex bool) {

	runtime.GOMAXPROCS(128)

	os.Mkdir(dbPath, 0700)

	if threadByWorker < 1 {
		threadByWorker = 1
	}

	inputFmt = strings.ToLower(inputFmt)

	fmt.Printf("# Making Database %s from %s\n", dbPath, inputPath)
	fmt.Printf("# Using %d CPU\n", threadByWorker)

	kvStores := kvstore.KVStoresNew(dbPath, threadByWorker, maxSize, false, false)
	kvStores.OpenInsertChannel()

	switch inputFmt {
	case "embl":
		runEMBL(inputPath, kvStores, threadByWorker, offset, lenght)
	case "tsv":
		runTSV(inputPath, kvStores, threadByWorker, offset, lenght)
	case "gbk":
		runGBK(inputPath, kvStores, threadByWorker, offset, lenght)
	case "genbank":
		runGBK(inputPath, kvStores, threadByWorker, offset, lenght)
	case "fasta":
		runFASTA(inputPath, kvStores, threadByWorker, offset, lenght)
	default:
		fmt.Println("Input format unrecognized !")
		os.Exit(1)
	}
	kvStores.CloseInsertChannel()
	kvStores.Close()

	kvStores = kvstore.KVStoresNew(dbPath, threadByWorker, maxSize, false, false)

	fmt.Printf("# GC KmerStore...\n")
	kvStores.KmerStore.GarbageCollect(10, 0.5)
	fmt.Printf("# GC ProteinStore...\n")
	kvStores.ProteinStore.GarbageCollect(10, 0.5)

	kvStores.Close()

	if !noIndex {
		indexdb.NewIndexDB(dbPath, threadByWorker, maxSize)
	}

}
