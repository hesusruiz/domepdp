package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"
)

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	rp := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "https"
			log.Printf("PROXY -> Method: %s Host: %s URL: %s IP: %s\n", r.Method, r.Host, r.URL, r.RemoteAddr)
			r.URL.Host = r.Host
			if r.Host == "dome-marketplace.eu" || r.Host == "dome-marketplace-prd.eu" {
				if strings.HasPrefix(r.URL.Path, "/catalog/") || strings.HasPrefix(r.URL.Path, "/party/") {
					r.URL.Host = "dome-marketplace.eu"
					fmt.Println(r.URL.Path)
				}
			}
		},
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		log.Printf("Method: %s Host: %s URL: %s IP: %s\n", r.Method, r.Host, r.URL, r.RemoteAddr)

		if strings.HasPrefix(r.URL.Path, "/proxy/") {
			ProxyManager(w, r)
		} else {
			rp.ServeHTTP(w, r)
		}
	})

	// m := &autocert.Manager{
	// 	Cache:      autocert.DirCache("certs"),
	// 	Prompt:     autocert.AcceptTOS,
	// 	HostPolicy: autocert.HostWhitelist("market.evidenceledger.eu"),
	// }

	s := &http.Server{
		Addr:           "localhost:8080",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		// TLSConfig:      m.TLSConfig(),
	}

	log.Println("Serving")
	log.Fatal(s.ListenAndServe())
}

func ProxyManager(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ProxyManager: %s\n", r.URL.Path)
}
