// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfproxy

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/errl"
	"github.com/hesusruiz/domeproxy/internal/middleware"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/hesusruiz/domeproxy/tmfcache"
	"github.com/rs/cors"
)

// TMFServerHandler is an HTTP server which implements access control for TMForum APIs
// with an embedded PDP (Policy Decision Point) where policy rules are evaluated and enforced.
//
// The server can act in two different modes:
//  1. As a combination of PIP+PDP, intercepting all requests to downstream TMF APIs,
//     evaluating the policies before the requests arrive to the actual implementation of the APIs.
//  2. As a 'pure' PDP, acting as an authorization server for some upstream PIP like NGINX. In this
//     mode, requests are intercepted by the PIP which asks the PDP (this program) for an authorization decision.
func TMFServerHandler(
	cfg *config.Config,
) (execute func() error, interrupt func(error), err error) {

	// Set the default configuration, depending on the environment (production, development, ...)
	tmfDb, err := tmfcache.NewTMFCache(cfg, false)
	if err != nil {
		return nil, nil, errl.Error(err)
	}

	mux := http.NewServeMux()

	// Create an instance of the rules engine for the evaluation of the authorization policy rules
	rulesEngine, err := pdp.NewPDP(cfg, nil, nil)
	if err != nil {
		return nil, nil, errl.Error(err)
	}

	addAdminRoutes(cfg, mux, tmfDb, rulesEngine)

	// Add the TMForum API routes
	addHttpRoutesISBE(cfg, mux, tmfDb, rulesEngine)

	// Enable CORS with permissive options.
	handler := cors.AllowAll().Handler(mux)

	// Log all requests and replies
	handler = middleware.RequestLogger(slog.Default(), handler)

	// Recovery of panics in the routes
	handler = middleware.PanicHandler(handler)

	// An HTTP server with sensible defaults (no need to make them configurable)
	s := &http.Server{
		Addr:           cfg.PDPAddress,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		Handler:        handler,
	}

	// This function will start the server
	startServer := func() error {

		// Start a cloning process immediately
		slog.Info("Starting PDP and TMForum API server", "addr", cfg.PDPAddress)

		// Start a backgroud process to clone the database
		// We make an initial cloning and then repeat every ClonePeriod (10 minutes by default)
		go func() {

			start := time.Now()
			slog.Info("started cloning", "time", start.String())

			tmfDb.CloneRemoteProductOfferings()

			_, _, err = tmfDb.CloneRemoteResources([]string{"category", "productCatalog"})

			elapsed := time.Since(start)
			slog.Info("finished cloning", "elapsed (ms)", elapsed.Milliseconds())

			c := time.Tick(cfg.ClonePeriod)
			for next := range c {
				slog.Info("started cloning", "time", next.String())

				tmfDb.CloneRemoteProductOfferings()
				_, _, err = tmfDb.CloneRemoteResources([]string{"category", "productCatalog"})

				elapsed := time.Since(next)
				slog.Info("finished cloning", "elapsed (ms)", elapsed.Milliseconds())
			}

		}()

		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return errl.Error(err)
		}
		return nil

	}

	// And this will stop the server
	stopServer := func(error) {
		tmfDb.Close()
		slog.Info("Cancelling the HTTP server")
		// Give 10 seconds to the server to clean up orderly
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	}

	// Return the group run functions: one for starting the server and another for shutting it down
	return startServer, stopServer, nil

}
