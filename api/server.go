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

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/options"
	"github.com/go-chi/chi"
	"github.com/golang/protobuf/proto"
	"github.com/rs/xid"
	"github.com/zorino/kaamer/internal/helper/duration"
	"github.com/zorino/kaamer/pkg/kvstore"
	"github.com/zorino/kaamer/pkg/search"
)

/* global variables */
var kvStores *kvstore.KVStores
var dbStats *kvstore.KStats
var tmpFolder = "/tmp/"
var nbOfThreads = 0

func NewServer(dbPath string, portNumber int, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode, newNbThreads int, newTmpFolder string) {

	runtime.GOMAXPROCS(512)

	if _, err := os.Stat(newTmpFolder); !os.IsNotExist(err) {
		tmpFolder = newTmpFolder
	}

	if newNbThreads == 0 {
		nbOfThreads = runtime.NumCPU()
	} else {
		nbOfThreads = newNbThreads
	}

	/* Open database */
	fmt.Printf(" + Opening kAAmer Database.. ")
	startTime := time.Now()

	kvStores = kvstore.KVStoresNew(dbPath, 12, tableLoadingMode, valueLoadingMode, true, false, true)
	defer kvStores.Close()

	dbStatsByte, ok := kvStores.ProteinStore.GetValue([]byte("db_stats"))
	if !ok {
		fmt.Println("No database stats, aborting !")
		os.Exit(1)
	}
	dbStats = &kvstore.KStats{}
	proto.Unmarshal(dbStatsByte, dbStats)

	elapsed := time.Since(startTime)
	elapsed = elapsed.Round(time.Second)
	out := fmt.Sprintf("done [%s]\n", duration.FmtDuration(elapsed))
	fmt.Printf(out)

	r := chi.NewRouter()

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.RedirectHandler("/web/", 302).ServeHTTP(w, r)
	})

	/* Documentation */
	_, docDir, _, _ := runtime.Caller(1)
	docDir = filepath.Dir(docDir)
	docDir += "/../../docs"
	StaticPages(r, "/docs", http.Dir(docDir))

	/* Web Search */
	_, webDir, _, _ := runtime.Caller(1)
	webDir = filepath.Dir(webDir)
	webDir += "/../../web/public/"
	StaticPages(r, "/web", http.Dir(webDir))

	/* API */
	APIRoutes(r, "/api", kvStores)

	/* Set port */
	var port bytes.Buffer
	port.WriteString(":")
	port.WriteString(strconv.Itoa(portNumber))

	/* Start server */
	fmt.Printf(" + kAAmer server listening on port %d with %d CPU workers\n", portNumber, nbOfThreads)

	err := http.ListenAndServe(port.String(), r)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

}

func APIRoutes(r chi.Router, path string, kvStores *kvstore.KVStores) {

	// RESTy routes
	r.Route("/api", func(r chi.Router) {
		r.Post("/search/protein", searchProtein)
		r.Post("/search/fastq", searchFastq)
		r.Post("/search/nucleotide", searchNucleotide)
		r.Get("/dbinfo", func(w http.ResponseWriter, r *http.Request) {
			b, err := json.Marshal(dbStats)
			if err != nil {
				fmt.Println(err)
				return
			}
			w.Write([]byte(string(b)))
		})
	})

}

func searchFastq(w http.ResponseWriter, r *http.Request) {

	searchOptions := search.SearchOptions{
		File:             "",
		InputType:        r.FormValue("type"),
		GeneticCode:      11,
		SequenceType:     search.READS,
		OutFormat:        "tsv",
		MaxResults:       10,
		ExtractPositions: false,
		SubMatrix:        "blosum62",
		GapOpen:          11,
		GapExtend:        1,
	}

	err := parseSearchOptions(&searchOptions, w, r)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
	} else {
		search.NewSearchResult(searchOptions, *dbStats, kvStores, nbOfThreads, w, r)
	}

}

func searchNucleotide(w http.ResponseWriter, r *http.Request) {

	searchOptions := search.SearchOptions{
		File:             "",
		InputType:        r.FormValue("type"),
		GeneticCode:      11,
		SequenceType:     search.NUCLEOTIDE,
		OutFormat:        "tsv",
		MaxResults:       10,
		ExtractPositions: false,
		SubMatrix:        "blosum62",
		GapOpen:          11,
		GapExtend:        1,
	}

	err := parseSearchOptions(&searchOptions, w, r)

	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
	} else {
		search.NewSearchResult(searchOptions, *dbStats, kvStores, nbOfThreads, w, r)
	}

}

func searchProtein(w http.ResponseWriter, r *http.Request) {

	searchOptions := search.SearchOptions{
		File:             "",
		InputType:        r.FormValue("type"),
		GeneticCode:      11, // not needed
		SequenceType:     search.PROTEIN,
		OutFormat:        "tsv",
		MaxResults:       10,
		ExtractPositions: false,
		SubMatrix:        "blosum62",
		GapOpen:          11,
		GapExtend:        1,
	}

	err := parseSearchOptions(&searchOptions, w, r)

	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
	} else {
		search.NewSearchResult(searchOptions, *dbStats, kvStores, nbOfThreads, w, r)
	}

}

func parseSearchOptions(searchOpts *search.SearchOptions, w http.ResponseWriter, r *http.Request) error {

	// Input sequence format (string, file, path)
	switch r.FormValue("type") {
	case "string":
		file, err := stringUploadHandler(r, "fasta")
		if err != nil {
			// w.WriteHeader(400)
			// fmt.Fprintln(w, err.Error())
			return err
		} else {
			searchOpts.File = file
		}
	case "file":
		file, err := fileUploadHandler(r, "fasta")
		if err != nil {
			// w.WriteHeader(400)
			// fmt.Fprintln(w, err.Error())
			return err
		} else {
			searchOpts.File = file
		}
	case "path":
		if r.FormValue("file") != "" {
			if _, err := os.Stat(r.FormValue("file")); os.IsNotExist(err) {
				// w.WriteHeader(400)
				// w.Write([]byte("File does not exist!"))
				return err
			} else {
				searchOpts.File = r.FormValue("file")
			}
		}
	default:
		// w.WriteHeader(400)
		// w.Write([]byte("Need request type (string|file|path)"))
		return errors.New("Need request type (string|file|path)")
	}

	if r.FormValue("max-results") != "" {
		if maxRes, err := strconv.Atoi(r.FormValue("max-results")); err == nil {
			searchOpts.MaxResults = maxRes
		}
	}

	if r.FormValue("gcode") != "11" {
		if gCode, err := strconv.Atoi(r.FormValue("gcode")); err == nil {
			searchOpts.GeneticCode = gCode
		}
	}

	if strings.ToLower(r.FormValue("output-format")) == "json" {
		searchOpts.OutFormat = "json"
	}

	if strings.ToLower(r.FormValue("positions")) == "true" {
		searchOpts.ExtractPositions = true
	}

	if strings.ToLower(r.FormValue("annotations")) == "true" {
		searchOpts.Annotations = true
	}

	if strings.ToLower(r.FormValue("align")) == "true" {
		searchOpts.Align = true
	}

	if strings.ToLower(r.FormValue("sub-matrix")) != "blosum62" {
		searchOpts.SubMatrix = strings.ToLower(r.FormValue("sub-matrix"))
	}

	if r.FormValue("gap-open") != "11" {
		if gop, err := strconv.Atoi(r.FormValue("gap-open")); err == nil {
			searchOpts.GapOpen = gop
		}
	}

	if r.FormValue("gap-extend") != "1" {
		if gex, err := strconv.Atoi(r.FormValue("gap-extend")); err == nil {
			searchOpts.GapExtend = gex
		}
	}

	return nil

}

// FileServer conveniently sets up a http.FileServer handler to serve
// static files from a http.FileSystem.
func StaticPages(r chi.Router, path string, root http.FileSystem) {

	if strings.ContainsAny(path, "{}*") {
		panic("FileServer does not permit URL parameters.")
	}

	fs := http.StripPrefix(path, http.FileServer(root))

	if path != "/" && path[len(path)-1] != '/' {
		r.Get(path, http.RedirectHandler(path+"/", 301).ServeHTTP)
		path += "/"
	}
	path += "*"

	r.Get(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.ServeHTTP(w, r)
	}))

}

func fileUploadHandler(r *http.Request, format string) (string, error) {

	r.ParseMultipartForm(0)
	defer r.MultipartForm.RemoveAll()
	fi, _, err := r.FormFile("file")
	if err != nil {
		return "", err
	}
	defer fi.Close()

	// fmt.Printf("Received %v", info.Filename)
	guid := xid.New()
	file := tmpFolder + guid.String() + "." + format

	out, err := os.Create(file)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(out, fi)
	if err != nil {
		return "", err
	}

	return file, err
}

func stringUploadHandler(r *http.Request, format string) (string, error) {

	guid := xid.New()
	file := tmpFolder + guid.String() + "." + format

	out, err := os.Create(file)
	if err != nil {
		return "", err
	}

	_, err = io.Copy(out, strings.NewReader(r.FormValue("sequence")))
	if err != nil {
		return "", err
	}

	return file, err
}
