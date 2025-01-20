// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/rs/cors"
)

// TMFServerHandler is an HTTP server which implements access control for TMForum APIs
// with an embedded PDP (Policy Decision Point) where policy rules are evaluated and enforced.
//
// The server can act in two different modes:
//  1. As a combination of PIP+PDP, intercepting all requests to downstream TMF APIs,
//     evaluating the policies before the requests arrive to the actual implementation of the APIs.
//  2. As a 'pure' PDP, acting as an authorization server for some upstream PIP like NGINX. In this
//     mode, requests are intercepted by the PIP which asks the PDP for an authorization decision.
func TMFServerHandler(environment pdp.Environment, pdpAddress string, debug bool) (tmfConfig *pdp.Config, execute func() error, interrupt func(error), err error) {

	// Set the defaul configuration, depending on the environment
	tmfConfig = pdp.DefaultConfig(environment)
	tmf, err := pdp.New(tmfConfig)
	if err != nil {
		return nil, nil, nil, err
	}

	mux := http.NewServeMux()

	// Add the TMForum API routes
	addHttpRoutes(environment, mux, tmf, debug)

	// Enable CORS with permissive options.
	handler := cors.AllowAll().Handler(mux)

	// Set some middleware, for recovery of panics in the routes and for logging all requests
	handler = PanicHandler(handler)

	// An HTTP server with sane defaults
	s := &http.Server{
		Addr:           pdpAddress,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		Handler:        handler,
	}

	// Return the group run functions: one for starting the server and another for shutting it down
	return tmfConfig,
		func() error {

			// Start a cloning process immediately
			slog.Info("started cloning", "time", time.Now().String())
			slog.Info("Starting PDP and TMForum API server", "addr", pdpAddress)
			slog.Info("finished cloning", "time", time.Now().String())

			// Start a backgroud process to clone the database every 10 minutes
			go func() {
				tmf.CloneRemoteProductOfferings()
				c := time.Tick(10 * time.Minute)
				for next := range c {
					slog.Info("started cloning", "time", next.String())
					tmf.CloneRemoteProductOfferings()
					slog.Info("finished cloning", "time", time.Now().String())
				}
			}()

			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil

		}, func(error) {
			tmf.Close()
			slog.Info("Cancelling the HTTP server")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s.Shutdown(ctx)
		},
		nil

}

// PanicHandler is a simple http handler for recovering panics in downstream handlers
func PanicHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[:n]

				fmt.Printf("panic recovered: %v\n %s", err, buf)
				errorTMF(w, http.StatusInternalServerError, "unknown error", "unknown error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func DefaultSecureHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[:n]

				fmt.Printf("panic recovered: %v\n %s", err, buf)
				errorTMF(w, http.StatusInternalServerError, "unknown error", "unknown error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// The standard HTTP error response for TMF APIs
type ErrorTMF struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

// errorTMF sends back an HTTP error response using the TMForum standard format
func errorTMF(w http.ResponseWriter, statusCode int, code string, reason string) {
	errtmf := &ErrorTMF{
		Code:   code,
		Reason: reason,
	}

	h := w.Header()

	// Delete the Content-Length header, just in case the handler panicking already set it.
	h.Del("Content-Length")
	h.Set("X-Powered-By", "JRM Proxy")

	// There might be content type already set, but we reset it
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(errtmf)

}

// replyTMF sends an HTTP response in the TMForum format
func replyTMF(w http.ResponseWriter, data []byte) {

	h := w.Header()

	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set("X-Powered-By", "JRM Proxy")

	// There might be content type already set, but we reset it to
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.Write(data)

}
