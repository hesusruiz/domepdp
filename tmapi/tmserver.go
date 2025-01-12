package tmapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/constants"
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
	handler := PanicHandler(mux)
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

			slog.Info("Starting PDP and TMForum API server", "addr", pdpAddress)

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

// The standard HTTP error response for TMF APIs
type ErrorTMF struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

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

func replyTMF(w http.ResponseWriter, data []byte) {

	h := w.Header()

	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set("X-Powered-By", "JRM Proxy")

	// There might be content type already set, but we reset it to
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.Write(data)

}
