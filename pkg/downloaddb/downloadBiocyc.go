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
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/pb"
	"github.com/golang/protobuf/proto"
	"github.com/zorino/kaamer/pkg/kvstore"
	"golang.org/x/net/html/charset"
)

var (
	BIOCYC_API = "https://websvc.biocyc.org"
)

func DownloadBiocyc(dbPath string) {

	// TODO add to CLI
	fmt.Println("## Notice ##")
	fmt.Println("Biocyc Webservices are provided by SRI International with a limited use license.")
	fmt.Println("See https://bioinformatics.ai.sri.com/ptools/licensing/all-reg.shtml")
	fmt.Println("")
	fmt.Printf("Do you accept Biocyc terms and conditions Y/n : ")

	reader := bufio.NewReader(os.Stdin)
	answer, _ := reader.ReadString('\n')
	answer = strings.Trim(answer, "\n")

	if strings.ToLower(answer) != "y" {
		fmt.Println("I am sorry you couldn't accept that license")
		os.Exit(0)
	}

	kvStores := kvstore.KVStoresNew(dbPath, 2, options.MemoryMap, options.MemoryMap, true, true, false)

	proteinStore := kvStores.ProteinStore

	stream := proteinStore.DB.NewStream()

	proteinStore.OpenInsertChannel()

	// -- Optional settings
	stream.NumGo = 2                      // Set number of goroutines to use for iteration.
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

			biocycIds := prot.GetBioCyc()

			if len(biocycIds) > 0 {
				fmt.Printf("Biocyc IDs for %s.. ", prot.GetEntry())
				prot.Biocyc_Pathways = []string{}
				for _, biocycId := range biocycIds {
					geneId := strings.Replace(biocycId, "-MONOMER", "", 1)
					pathways := GetBiocycPathway(geneId)
					fmt.Printf("%d\n", len(pathways))
					if len(pathways) > 0 {
						prot.Biocyc_Pathways = append(prot.Biocyc_Pathways, pathways...)
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

type Pathway struct {
	XMLName xml.Name `xml:"Pathway"`
	ID      string   `xml:"ID,attr"`
	Name    string   `xml:"common-name"`
}

type XMLRoot struct {
	XMLName  xml.Name  `xml:"ptools-xml"`
	Version  xml.Attr  `xml:"ptools-version,attr"`
	Pathways []Pathway `xml:"Pathway"`
}

func GetBiocycPathway(id string) []string {

	url := BIOCYC_API + "/apixml?fn=pathways-of-gene&id=" + id
	resp, err := http.Get(url)

	if err != nil {
		fmt.Println(err.Error())
		return []string{}
	}

	defer resp.Body.Close()

	var pathways []string

	container := XMLRoot{}

	decoder := xml.NewDecoder(resp.Body)
	decoder.CharsetReader = charset.NewReaderLabel
	err = decoder.Decode(&container)
	if err != nil {
		// fmt.Println(err.Error())
		return []string{}
	}

	for _, p := range container.Pathways {
		pathway := fmt.Sprintf("%s [%s]", p.Name, p.ID)
		pathways = append(pathways, pathway)
	}

	return pathways

}
