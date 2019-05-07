package server

import (
	"github.com/zorino/metaprot/pkg/kvstore"
	"github.com/zorino/metaprot/pkg/search"

	// "golang.org/x/net/context"
	"bytes"
	"fmt"
	"net/http"

	"github.com/go-chi/chi"

	// "github.com/go-chi/chi/middleware"
	"strconv"
	"strings"

	// "os"
	"encoding/json"
	"path/filepath"
	"runtime"
)

var kvStores *kvstore.KVStores

func NewServer(dbPath string, portNumber int) {

	kvStores = kvstore.KVStoresNew(dbPath, 12)
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

	// RESTy routes for "articles" resource
	r.Route("/api/search", func(r chi.Router) {
		r.Post("/protein", searchProtein)
		r.Post("/fastq", searchFastq)
	})

	// searchRes := search.NewSearchResult(sequenceOrFile, sequenceType, kvStores, nbOfThreads)
	// r.Get(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	//	w.Write([]byte("API route"))
	// }))

}

func searchFastq(w http.ResponseWriter, r *http.Request) {

	// chi.URLParam(r, "key")
	searchRes := search.NewSearchResult(r.FormValue("sequence"), search.PROTEIN_STRING, kvStores, 2)
	output, _ := json.Marshal(searchRes)
	w.Header().Set("Content-Type", "application/json")
	w.Write(output)

}

func searchProtein(w http.ResponseWriter, r *http.Request) {

	// chi.URLParam(r, "key")
	switch  r.FormValue("type") {
	case "string":
		searchRes := search.NewSearchResult(r.FormValue("sequence"), search.PROTEIN_STRING, kvStores, 2)
		output, _ := json.Marshal(searchRes)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write(output)
	case "file":
		w.Write([]byte("Uploading file"))
	default:
		w.WriteHeader(400)
		w.Write([]byte("Need request type ! string or file"))
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
