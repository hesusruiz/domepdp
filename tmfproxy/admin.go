package tmfproxy

import (
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/middleware"
	"github.com/hesusruiz/domeproxy/internal/sqlogger"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/hesusruiz/domeproxy/tmfcache"
	"gitlab.com/greyxor/slogor"

	"github.com/gofiber/template/html/v2"
)

//go:embed views/*
var viewsfs embed.FS

func addAdminRoutes(
	config *config.Config,
	mux *http.ServeMux,
	tmf *tmfcache.TMFCache,
	rulesEngine *pdp.PDP,
) {

	config.Debug = true

	// Try to load first the embedded templates, and later the user-provided ones
	var engine *html.Engine

	// Use the embedded directory
	viewsDir, err := fs.Sub(viewsfs, "views")
	if err != nil {
		panic(err)
	}

	if config.Debug {
		engine = html.NewFileSystem(http.Dir("tmfproxy/views"), ".hbs")
		engine.Reload(true)
	} else {
		engine = html.NewFileSystem(http.FS(viewsDir), ".hbs")
		engine.Reload(true)
	}

	err = engine.Load()
	if err != nil {
		panic(err)
	}

	mux.Handle("/admin/assets/", http.StripPrefix("/admin/", http.FileServerFS(viewsDir)))

	mux.HandleFunc("GET /admin/{$}",
		func(w http.ResponseWriter, r *http.Request) {

			middleware.RenderHTML(engine, w, "index", map[string]any{
				"Title": "Hello, World!",
			}, "layouts/main")

		})

	mux.HandleFunc("GET /admin/page/policies",
		func(w http.ResponseWriter, r *http.Request) {

			filename := "auth_policies.star"

			entry, err := rulesEngine.GetFile(filename)
			if err != nil {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}
			if entry == nil {
				middleware.ErrorTMF(w, http.StatusNotFound, "file not found", filename)
				slog.Error("file not found", slog.String("filename", filename))
				return
			}

			middleware.RenderHTML(engine, w, "policies", map[string]any{
				"Title": "Hello, World!",
				"File":  string(entry.Content),
			}, "layouts/main")

		})

	mux.HandleFunc("POST /admin/page/policies",
		func(w http.ResponseWriter, r *http.Request) {

			filename := "auth_policies.star"

			// Get the contents of the body of the request
			incomingRequestBody, err := io.ReadAll(r.Body)
			if err != nil {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error reading body", err.Error())
				slog.Error("reading body", slogor.Err(err))
				return
			}

			err = rulesEngine.PutFile(filename, incomingRequestBody)
			if err != nil {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error writing file", err.Error())
				slog.Error("writing file", slogor.Err(err))
				return
			}

			middleware.ResponseSecurityHeaders(w)

		})

	mux.HandleFunc("GET /admin/page/logs",
		func(w http.ResponseWriter, r *http.Request) {

			start := middleware.LogHTTPRequest(slog.Default(), r)

			filename := "auth_policies.star"

			handler := slog.Default().Handler()

			sqlog, ok := handler.(sqlogger.SQLogHandlerInterface)
			if !ok {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error retrieving", "logger is not a SQLogger")
				slog.Error("logger is not a SQLogger")
				return
			}

			entries, err := sqlog.Retrieve(1000)
			if err != nil {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}
			if entries == nil {
				middleware.ErrorTMF(w, http.StatusNotFound, "file not found", filename)
				slog.Error("file not found", slog.String("filename", filename))
				return
			}

			status := middleware.RenderHTML(engine, w, "logs", map[string]any{
				"Title":      "Hello, World!",
				"LogEntries": entries,
			}, "layouts/main")

			middleware.LogHTTPReply(slog.Default(), r, start, status)

		})

	mux.HandleFunc("GET /admin/page/upstream",
		func(w http.ResponseWriter, r *http.Request) {

			upstreams := config.GetAllUpstreamHosts()

			b, err := json.MarshalIndent(upstreams, "", "  ")
			if err != nil {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error marshalling upstreams", err.Error())
				slog.Error("marshalling upstreams", slogor.Err(err))
				return
			}

			middleware.RenderHTML(engine, w, "upstream", map[string]any{
				"Title":     "Hello, World!",
				"Upstreams": string(b),
			}, "layouts/main")

		})

	mux.HandleFunc("POST /admin/page/upstream",
		func(w http.ResponseWriter, r *http.Request) {

			// Get the contents of the body of the request
			incomingRequestBody, err := io.ReadAll(r.Body)
			if err != nil {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error reading body", err.Error())
				slog.Error("reading body", slogor.Err(err))
				return
			}

			var upstreams map[string]string
			err = json.Unmarshal(incomingRequestBody, &upstreams)
			if err != nil {
				middleware.ErrorTMF(w, http.StatusInternalServerError, "error unmarshalling body", err.Error())
				slog.Error("unmarshalling body", slogor.Err(err))
				return
			}
			if upstreams == nil {
				middleware.ErrorTMF(w, http.StatusBadRequest, "invalid body", "upstreams cannot be nil")
				slog.Error("invalid body", slog.String("body", string(incomingRequestBody)))
				return
			}
			// Update the upstream hosts in the config
			config.InitUpstreamHosts(upstreams)

			middleware.ResponseSecurityHeaders(w)

		})

}
