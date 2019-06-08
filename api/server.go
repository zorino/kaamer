package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/options"
	"github.com/go-chi/chi"
	"github.com/rs/xid"
	"github.com/zorino/metaprot/pkg/kvstore"
	"github.com/zorino/metaprot/pkg/search"
)

var kvStores *kvstore.KVStores
var tmpFolder string

func NewServer(dbPath string, portNumber int, tableLoadingMode options.FileLoadingMode, valueLoadingMode options.FileLoadingMode) {

	tmpFolder = "/tmp/"

	kvStores = kvstore.KVStoresNew(dbPath, 12, tableLoadingMode, valueLoadingMode, false)
	defer kvStores.Close()

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

	/* Start server */
	var port bytes.Buffer
	port.WriteString(":")
	port.WriteString(strconv.Itoa(portNumber))

	fmt.Printf("Metaprot server listening on port %d\n", portNumber)
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

	// chi.URLParam(r, "key")
	searchRes := search.NewSearchResult(r.FormValue("sequence"), search.PROTEIN_STRING, kvStores, 2)
	output, _ := json.Marshal(searchRes)
	w.Header().Set("Content-Type", "application/json")
	w.Write(output)

}

func searchNucleotide(w http.ResponseWriter, r *http.Request) {

	// chi.URLParam(r, "key")
	searchRes := search.NewSearchResult(r.FormValue("sequence"), search.NUCLEOTIDE_STRING, kvStores, 2)
	output, _ := json.Marshal(searchRes)
	w.Header().Set("Content-Type", "application/json")
	w.Write(output)

}

func searchProtein(w http.ResponseWriter, r *http.Request) {

	// chi.URLParam(r, "key")
	switch r.FormValue("type") {
	case "string":
		searchRes := search.NewSearchResult(r.FormValue("sequence"), search.PROTEIN_STRING, kvStores, 2)
		output, _ := json.Marshal(searchRes)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write(output)
	case "file":
		file, err := uploadHandler(w, r)
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintln(w, err.Error())
		} else {
			w.Write([]byte("Uploaded file to " + file))
			search.NewSearchResult(file, search.PROTEIN_FILE, kvStores, 2)
		}
	case "path":
		w.Write([]byte("Reading local file"))
		search.NewSearchResult(r.FormValue("file"), search.PROTEIN_FILE, kvStores, 2)
	default:
		w.WriteHeader(400)
		w.Write([]byte("Need request type ! string or file or path"))
	}

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

func uploadHandler(w http.ResponseWriter, r *http.Request) (string, error) {

	r.ParseMultipartForm(0)
	defer r.MultipartForm.RemoveAll()
	fi, info, err := r.FormFile("file")
	if err != nil {
		return "", err
	}
	defer fi.Close()

	fmt.Printf("Received %v", info.Filename)
	guid := xid.New()
	file := tmpFolder + guid.String() + ".fasta"

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
