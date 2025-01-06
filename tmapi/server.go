package tmapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/hesusruiz/domeproxy/hlog"
	"github.com/hesusruiz/domeproxy/tmfsync"
)

func HttpServerHandler(ctx context.Context, config *Config, tmf *tmfsync.TMFdb, w io.Writer, args []string) (execute func() error, interrupt func(error)) {

	mux := http.NewServeMux()

	// Add the TMForum API routes
	addHttpRoutes(mux, config, tmf)

	// Set some middleware, for recovery of panics in the routes and for logging all requests
	handler := hlog.Recovery(mux)
	handler = hlog.New(slog.Default())(handler)

	// An HTTP server with sane defaults
	s := &http.Server{
		Addr:           config.Listen,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		Handler:        handler,
	}

	// Return the group run functions: one for starting the server and another for shutting it down
	return func() error {

			slog.Info("Starting HTTP server", "addr", config.Listen)

			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				return err
			}
			return nil

		}, func(error) {
			slog.Info("Cancelling the HTTP server")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s.Shutdown(ctx)
			tmf.Close()
		}

}
