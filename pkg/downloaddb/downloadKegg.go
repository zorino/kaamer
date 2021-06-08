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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/golang/protobuf/proto"
	"github.com/zorino/kaamer/pkg/kvstore"
)

var (
	KEGG_API = "http://rest.kegg.jp"
)

func DownloadKEGG(dbPath string) {

	// TODO add to CLI
	fmt.Println("## Notice ##")
	fmt.Println("KEGG API is provided for academic use by academic users belonging to academic institutions.")
	fmt.Println("See https://www.kegg.jp/kegg/rest/")
	fmt.Println("")
	fmt.Printf("Do you accept KEGG terms and conditions Y/n : ")

	reader := bufio.NewReader(os.Stdin)
	answer, _ := reader.ReadString('\n')
	answer = strings.Trim(answer, "\n")

	if strings.ToLower(answer) != "y" {
		fmt.Println("I am sorry you couldn't accept that license")
		os.Exit(0)
	}

	kvStores := kvstore.KVStoresNew(dbPath, 2, true, true, false)

	proteinStore := kvStores.ProteinStore

	stream := proteinStore.DB.NewStream()

	proteinStore.OpenInsertChannel()

	// -- Optional settings
	stream.NumGo = 1                      // Set number of goroutines to use for iteration.
	stream.Prefix = nil                   // Leave nil for iteration over the whole DB.
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.

	// ChooseKey is called concurrently for every key. If left nil, assumes true by default.
	stream.ChooseKey = nil

	// KeyToList is called concurrently for chosen keys. This can be used to convert
	// Badger data into custom key-values. If nil, uses stream.ToList, a default
	// implementation, which picks all valid key-values.

	// stream.KeyToList = nil
	stream.KeyToList = func(key []byte, it *badger.Iterator) (*pb.KVList, error) {

		valCopy := []byte{}
		keyCopy := []byte{}

		for ; it.Valid(); it.Next() {

			item := it.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			if item.DiscardEarlierVersions() {
				break
			}
			if !bytes.Equal(key, item.Key()) {
				break
			}

			valCopy, err := item.ValueCopy(valCopy)
			if err != nil {
				log.Fatal(err.Error())
			}

			keyCopy = item.KeyCopy(keyCopy)

			prot := &kvstore.Protein{}
			proto.Unmarshal(valCopy, prot)

			keggIds := []string{}
			if ids, ok := prot.Features["KEGG_ID"]; ok {
				keggIds = strings.Split(ids, ";")
			}

			if len(keggIds) > 0 {
				fmt.Printf("KEGG IDs for %s.. ", prot.GetEntryId())
				// prot.KEGG_Pathways = []string{}
				prot.Features["KEGG_Pathways"] = ""
				for _, keggId := range keggIds {
					pathways := GetKeggPathway(keggId)
					fmt.Printf("%d\n", len(pathways))
					if len(pathways) > 0 {
						prot.Features["KEGG_Pathways"] = strings.Join(pathways, ";")
						// prot.KEGG_Pathways = append(prot.KEGG_Pathways, pathways...)
						fmt.Println(strings.Join(pathways, ";"))
						newVal, err := proto.Marshal(prot)
						if err == nil {
							proteinStore.AddValueToChannel(keyCopy, newVal, false)
						}
					}
				}
			}

		}

		return nil, nil

	}

	// -- End of optional settings.

	// Send is called serially, while Stream.Orchestrate is running.
	stream.Send = nil

	// // Run the stream
	// Run the stream
	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err.Error)
	}

	// Done.
	proteinStore.CloseInsertChannel()
	proteinStore.Flush()
	kvStores.Close()

}

func GetKeggPathway(id string) []string {

	url := KEGG_API + "/get/" + id
	resp, err := http.Get(url)

	if err != nil {
		fmt.Println(err.Error())
		return []string{}
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	var pathways []string

	insidePathway := false
	splitRegEx := regexp.MustCompile(`\s+`)

	for _, l := range strings.Split(string(body), "\n") {
		if len(l) < 7 {
			continue
		}
		if l[0:7] == "PATHWAY" {
			insidePathway = true
			lSplit := splitRegEx.Split(l, 3)
			pathway := fmt.Sprintf("%s [%s]", lSplit[2], lSplit[1])
			pathways = append(pathways, pathway)
		} else if insidePathway {
			if l[0:7] != "       " {
				insidePathway = false
			} else {
				lSplit := splitRegEx.Split(l, 3)
				pathway := fmt.Sprintf("%s [%s]", lSplit[2], lSplit[1])
				pathways = append(pathways, pathway)
			}
		}
	}

	return pathways

}
