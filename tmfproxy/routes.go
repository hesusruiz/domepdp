// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"

	"github.com/goccy/go-json"

	"slices"

	"github.com/hesusruiz/domeproxy/config"
	mdl "github.com/hesusruiz/domeproxy/internal/middleware"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/hesusruiz/domeproxy/tmfcache"
	slogformatter "github.com/samber/slog-formatter"
	"gitlab.com/greyxor/slogor"
)

var RoutePrefixes = []string{
	"/catalog/category",
	"/catalog/catalog",
	"/catalog/productOffering",
	"/catalog/productSpecification",
	"/catalog/productOfferingPrice",
	"/service/serviceSpecification",
	"/resource/resourceSpecification",
	"/party/organization",
	"/api/v1/entities",
}

func PrefixInRequest(uri string) bool {
	return slices.Contains(RoutePrefixes, uri)
}

type MyLogHandler struct {
}

func (h *MyLogHandler) Enabled(c context.Context, l slog.Level) bool {
	return true
}

func (h *MyLogHandler) Handle(c context.Context, r slog.Record) error {
	var b strings.Builder

	b.WriteString(r.Message)

	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(a.String())
		return true
	})

	fmt.Println("MYLOG", b.String())

	return nil
}

func (h *MyLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *MyLogHandler) WithGroup(name string) slog.Handler {
	return h
}

func addHttpRoutes(
	_ *config.Config,
	mux *http.ServeMux,
	tmf *tmfcache.TMFCache,
	rulesEngine *pdp.PDP,
) {

	logger := slog.New(
		slogformatter.NewFormatterHandler(
			slogformatter.HTTPRequestFormatter(false),
			slogformatter.HTTPResponseFormatter(false),
		)(
			slog.Default().Handler(),
		),
	)

	// ****************************************************
	// Set the route needed for acting as a pure PDP.
	// In this mode, an external PIP will call us.
	// This route can also be used by any application or service asking for authorization info
	// TODO: fix this, as it is not working properly
	mux.HandleFunc("GET /authorize/v1/policies/authz", pdp.HandleGETAuthorization(logger, tmf, rulesEngine))

	// This is for the Access Node requests, which are only for reads
	mux.HandleFunc("GET /api/v1/entities", func(w http.ResponseWriter, r *http.Request) {
		// TODO: set the processing for these requests
		mdl.ReplyTMF(w, []byte("{}"), nil)
	})

	// Generic LIST handler
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/v4/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

			tmfManagementSystem := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			// Retrieve the list of objects according to the parameters specified in the HTTP request
			logger.Info("GET LIST", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource)

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "GET")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "LIST")

			// tmfObjectList, err := retrieveList(tmf, tmfResource, r)
			tmfObjectList, err := pdp.AuthorizeLIST(logger, tmf, rulesEngine, r, tmfManagementSystem, tmfResource)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusForbidden, "error retrieving list", err.Error())
				logger.Error("retrieving", slogor.Err(err))
				return
			}

			// Create the output list with the map content fields, ready for marshalling
			var listMaps = []map[string]any{}
			for _, v := range tmfObjectList {
				listMaps = append(listMaps, v.ContentAsMap)
			}

			// We must send a randomly ordered list, to preserve fairness in the presentation of the offerings
			rand.Shuffle(len(listMaps), func(i, j int) {
				listMaps[i], listMaps[j] = listMaps[j], listMaps[i]
			})

			// Create the JSON representation of the list of objects
			out, err := json.Marshal(listMaps)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusInternalServerError, "error marshalling list", err.Error())
				logger.Error("error marshalling list", slogor.Err(err))
				return
			}

			additionalHeaders := map[string]string{
				"X-Total-Count": strconv.Itoa(len(listMaps)),
			}

			// Send the reply in the TMF format
			mdl.ReplyTMF(w, out, additionalHeaders)

		})

	// Generic GET handler
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/v4/{tmfResource}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			tmfManagementSystem := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")
			tmfID := r.PathValue("id")

			logger.Info("GET Object", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource, "tmfid", tmfID)

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "GET")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "READ")

			tmfObject, err := pdp.AuthorizeREAD(logger, tmf, rulesEngine, r, tmfManagementSystem, tmfResource, tmfID)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusForbidden, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}

			// Add the ETag header with the hash of the TMFObject
			additionalHeaders := map[string]string{
				"ETag": tmfObject.ETag(),
			}

			mdl.ReplyTMF(w, tmfObject.GetContentAsJSON(), additionalHeaders)

		})

	// Generic POST handler
	mux.HandleFunc("POST /tmf-api/{tmfAPI}/v4/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

			tmfManagementSystem := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			logger.Info("POST", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource)

			// Treat the HUB resource for notifications especially
			if tmfResource == "hub" {
				// Log the request
				logger.Error("creation of HUB for notifications")
				// TODO: handle the request
				return
			}

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "POST")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "CREATE")

			tmfObject, err := pdp.AuthorizeCREATE(logger, tmf, rulesEngine, r, tmfManagementSystem, tmfResource)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusForbidden, "error creating", err.Error())
				slog.Error("creating", slogor.Err(err))
				return
			}

			// Use Location HTTP header to specify the URI of a newly created resource (POST)
			location := "/tmf-api/" + tmfManagementSystem + "/v4/" + tmfResource + "/" + tmfObject.GetID()
			additionalHeaders := map[string]string{
				"Location": location,
			}

			// Send the reply in the TMF format
			mdl.ReplyTMF(w, tmfObject.GetContentAsJSON(), additionalHeaders)

		})

	// Generic PATCH handler
	mux.HandleFunc("PATCH /tmf-api/{tmfAPI}/v4/{tmfResource}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			tmfManagementSystem := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")
			tmfID := r.PathValue("id")

			logger.Info("PATCH", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource, "tmfid", tmfID)

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "PATCH")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "UPDATE")

			tmfObject, err := pdp.AuthorizeUPDATE(logger, tmf, rulesEngine, r, tmfManagementSystem, tmfResource, tmfID)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusForbidden, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}

			// TODO: use Location HTTP header to specify the URI of a newly created resource (POST)
			additionalHeaders := map[string]string{
				"Location": "location",
			}

			// Send the reply in the TMF format
			mdl.ReplyTMF(w, tmfObject.GetContentAsJSON(), additionalHeaders)

		})

	mux.HandleFunc("GET /adminapi/v1/file/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			filename := r.PathValue("id")
			logger.Info("Admin API GET File", "filename", filename, slog.Any("request", r))

			entry, err := rulesEngine.GetFile(filename)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}
			if entry == nil {
				mdl.ErrorTMF(w, http.StatusNotFound, "file not found", filename)
				slog.Error("file not found", slog.String("filename", filename))
				return
			}

			w.Write(entry.Content)

		})

	mux.HandleFunc("POST /adminapi/v1/file/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			filename := r.PathValue("id")
			logger.Info("Admin API PUT File", "filename", filename, slog.Any("request", r))

			incomingRequestBody, err := io.ReadAll(r.Body)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusInternalServerError, "error reading request body", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}

			err = rulesEngine.PutFile(filename, incomingRequestBody)
			if err != nil {
				mdl.ErrorTMF(w, http.StatusInternalServerError, "error storing file", err.Error())
				slog.Error("storing", slogor.Err(err))
				return
			}

			w.WriteHeader(http.StatusOK)

		})

}
