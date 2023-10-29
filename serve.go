package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	directory := "build/dist"
	handler := &BrotliHandler{directory}
	http.Handle("/", handler)

	log.Printf("Serving %s on HTTP port 8000\n", directory)
	log.Fatal(http.ListenAndServe(":8000", nil))
}

type BrotliHandler struct {
	RootDir string
}

func (h *BrotliHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := os.ReadFile(h.RootDir + "/" + r.URL.Path)
	if errors.Is(err, os.ErrNotExist) {
		w.WriteHeader(http.StatusNotFound)
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		if strings.HasSuffix(r.URL.Path, ".br") {
			w.Header().Set("Content-Encoding", "br")
		}
		w.Write(body)
	}
}
