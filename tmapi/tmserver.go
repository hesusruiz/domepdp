package tmapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/hesusruiz/domeproxy/constants"
	"github.com/hesusruiz/domeproxy/hlog"
	"github.com/hesusruiz/domeproxy/tmfsync"
)

func HttpServerHandler(environment constants.Environment, pdpAddress string, debug bool) (tmfConfig *tmfsync.Config, execute func() error, interrupt func(error), err error) {

	tmfConfig = tmfsync.DefaultConfig(environment)
	tmf, err := tmfsync.New(tmfConfig)
	if err != nil {
		return nil, nil, nil, err
	}

	mux := http.NewServeMux()

	// Add the TMForum API routes
	addHttpRoutes(environment, mux, tmf, debug)

	// Set some middleware, for recovery of panics in the routes and for logging all requests
	handler := hlog.Recovery(mux)
	// handler = hlog.New(slog.Default())(handler)

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

			slog.Info("Starting HTTP server", "addr", pdpAddress)

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
