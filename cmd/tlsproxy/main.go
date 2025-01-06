package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/smarty/cproxy/v2"
)

type roothandler struct {
	proxy   http.Handler
	manager http.Handler
}

func (h *roothandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Method: %s, Host: %s, URL: %s, IP: %s\n", r.Method, r.Host, r.URL, r.RemoteAddr)

	if strings.HasPrefix(r.URL.Path, "/proxy/") {
		ProxyManager(w, r)
	} else {
		h.proxy.ServeHTTP(w, r)
	}

}

func main() {

	rp := cproxy.New(
		cproxy.Options.Logger(log.New(os.Stderr, "", log.LstdFlags)),
		cproxy.Options.Filter(NewFilter()),
		cproxy.Options.LogConnections(true),
	)

	rh := &roothandler{
		proxy: rp,
	}

	s := &http.Server{
		Addr:           ":8080",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		Handler:        rh,
		// TLSConfig:      m.TLSConfig(),
	}

	log.Println("Listening on:", s.Addr)
	_ = s.ListenAndServe()
}

type myFilter struct {
}

func NewFilter() cproxy.Filter {
	return &myFilter{}
}

func (this myFilter) IsAuthorized(_ http.ResponseWriter, request *http.Request) bool {

	log.Printf("Host: %s, ", request.URL)

	return true
}

func ProxyManager(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ProxyManager: %s\n", r.URL.Path)
}
