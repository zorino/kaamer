package server

import (
	"bytes"
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
	"github.com/rs/xid"
	"github.com/zorino/kaamer/internal/helper/duration"
	"github.com/zorino/kaamer/pkg/kvstore"
	"github.com/zorino/kaamer/pkg/search"
)

var kvStores *kvstore.KVStores
var tmpFolder string

func NewServer(dbPath string, portNumber int, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode, maxSize bool) {

	runtime.GOMAXPROCS(512)

	tmpFolder = "/tmp/"

	/* Open database */
	fmt.Printf(" + Opening kAAmer Database.. ")
	startTime := time.Now()

	kvStores = kvstore.KVStoresNew(dbPath, 12, tableLoadingMode, valueLoadingMode, maxSize, false, true)
	defer kvStores.Close()

	elapsed := time.Since(startTime)
	elapsed = elapsed.Round(time.Second)
	out := fmt.Sprintf("done [%s]\n", duration.FmtDuration(elapsed))
	fmt.Printf(out)

	r := chi.NewRouter()

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.RedirectHandler("/docs/", 302).ServeHTTP(w, r)
	})

	/* Documentation */
	_, workDir, _, _ := runtime.Caller(1)
	workDir = filepath.Dir(workDir)
	workDir += "/docs"
	DocRoutes(r, "/docs", http.Dir(workDir))

	/* API */
	APIRoutes(r, "/api", kvStores)

	/* Set port */
	var port bytes.Buffer
	port.WriteString(":")
	port.WriteString(strconv.Itoa(portNumber))

	/* Start server */
	fmt.Printf(" + kAAmer server listening on port %d\n", portNumber)
	http.ListenAndServe(port.String(), r)

}

func APIRoutes(r chi.Router, path string, kvStores *kvstore.KVStores) {

	// RESTy routes for "search" function
	r.Route("/api/search", func(r chi.Router) {
		r.Post("/protein", searchProtein)
		r.Post("/fastq", searchFastq)
		r.Post("/nucleotide", searchNucleotide)
	})

}

func searchFastq(w http.ResponseWriter, r *http.Request) {

	searchOptions := search.SearchOptions{
		File:             "",
		InputType:        r.FormValue("type"),
		SequenceType:     search.READS,
		OutFormat:        "tsv",
		MaxResults:       10,
		ExtractPositions: true,
	}

	err := parseSearchOptions(&searchOptions, w, r)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
	} else {
		search.NewSearchResult(searchOptions, kvStores, 2, w)
	}

}

func searchNucleotide(w http.ResponseWriter, r *http.Request) {

	searchOptions := search.SearchOptions{
		File:             "",
		InputType:        r.FormValue("type"),
		SequenceType:     search.NUCLEOTIDE,
		OutFormat:        "tsv",
		MaxResults:       10,
		ExtractPositions: true,
	}

	err := parseSearchOptions(&searchOptions, w, r)

	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
	} else {
		search.NewSearchResult(searchOptions, kvStores, 2, w)
	}

}

func searchProtein(w http.ResponseWriter, r *http.Request) {

	searchOptions := search.SearchOptions{
		File:             "",
		InputType:        r.FormValue("type"),
		SequenceType:     search.PROTEIN,
		OutFormat:        "tsv",
		MaxResults:       10,
		ExtractPositions: false,
	}

	err := parseSearchOptions(&searchOptions, w, r)

	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintln(w, err.Error())
	} else {
		search.NewSearchResult(searchOptions, kvStores, 2, w)
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

	if strings.ToLower(r.FormValue("output-format")) == "json" {
		searchOpts.OutFormat = "json"
	}

	if strings.ToLower(r.FormValue("extract-positions")) == "true" {
		searchOpts.ExtractPositions = true
	}

	return nil

}

// FileServer conveniently sets up a http.FileServer handler to serve
// static files from a http.FileSystem.
func DocRoutes(r chi.Router, path string, root http.FileSystem) {

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
