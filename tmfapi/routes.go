// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfapi

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"slices"

	"github.com/hesusruiz/domeproxy/pdp"
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
	environment pdp.Environment,
	mux *http.ServeMux,
	tmf *pdp.TMFdb,
	debug bool,
) {

	mylogger := &MyLogHandler{}

	logger := slog.New(
		slogformatter.NewFormatterHandler(
			slogformatter.HTTPRequestFormatter(false),
			slogformatter.HTTPResponseFormatter(false),
		)(
			mylogger,
		),
	)

	// logger := slog.New(
	// 	slogformatter.NewFormatterHandler(
	// 		slogformatter.HTTPRequestFormatter(false),
	// 		slogformatter.HTTPResponseFormatter(false),
	// 	)(
	// 		slog.Default().Handler(),
	// 	),
	// )

	// Create an instance of the rules engine for the evaluation of the authorization policy rules
	rulesEngine, err := pdp.NewPDP(environment, "auth_policies.star", debug, nil, nil)
	if err != nil {
		panic(err)
	}

	// ****************************************************
	// Set the route needed for acting as a pure PDP.
	// In this mode, an external PIP will call us.
	// This route can also be used by any application or service asking for authotization info
	mux.HandleFunc("GET /authorize/v1/policies/authz", pdp.HandleGETAuthorization(logger, tmf, rulesEngine))

	// This is for the Access Node requests, which are only for reads
	mux.HandleFunc("GET /api/v1/entities", func(w http.ResponseWriter, r *http.Request) {
		// TODO: set the processing for these requests
		replyTMF(w, []byte("{}"), nil)
	})

	// Generic LIST handler
	mux.HandleFunc("GET /{tmfAPI}/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfAPI := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			// Retrieve the list of objects according to the parameters specified in the HTTP request
			logger.Info("GET LIST", "api", tmfAPI, "type", tmfResource)

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "GET")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "LIST")

			// tmfObjectList, err := retrieveList(tmf, tmfResource, r)
			tmfObjectList, err := pdp.HandleLISTAuth(logger, tmf, rulesEngine, r)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error retrieving list", err.Error())
				logger.Error("retrieving", slogor.Err(err))
				return
			}

			// Create the output list with the map content fields, ready for marshalling
			var listMaps = []map[string]any{}
			for _, v := range tmfObjectList {
				listMaps = append(listMaps, v.ContentMap)
			}

			// We must send a randomly ordered list, to preserve fairness in the presentation of the offerings
			rand.Shuffle(len(listMaps), func(i, j int) {
				listMaps[i], listMaps[j] = listMaps[j], listMaps[i]
			})

			// Create the JSON representation of the list of objects
			out, err := json.Marshal(listMaps)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error marshalling list", err.Error())
				logger.Error("error marshalling list", slogor.Err(err))
				return
			}

			additionalHeaders := map[string]string{
				"X-Total-Count": strconv.Itoa(len(listMaps)),
			}

			replyTMF(w, out, additionalHeaders)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("GET LIST", "latency", slog.Duration("latency", latency))

		})

	// Generic GET handler
	mux.HandleFunc("GET /{tmfAPI}/{tmfResource}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfAPI := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			logger.Info("GET Object", "api", tmfAPI, "type", tmfResource, slog.Any("request", r))

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "GET")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "READ")

			tmfObject, err := pdp.HandleREADAuth(logger, tmf, rulesEngine, r)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}

			// Add the ETag header with the hash of the TMFObject
			additionalHeaders := map[string]string{
				"ETag": tmfObject.ETag(),
			}

			replyTMF(w, tmfObject.Content, additionalHeaders)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("GET Object", "latency", slog.Duration("latency", latency))

		})

	// Generic POST handler
	mux.HandleFunc("POST /{tmfAPI}/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfAPI := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			logger.Info("POST", "api", tmfAPI, "type", tmfResource, slog.Any("request", r))

			// Treat the HUB resource for notifications especially
			if tmfResource == "hub" {
				// Log the request
				logger.Info("creation of HUB for notifications")
				// TODO: handle the request
				return
			}

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "POST")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "CREATE")

			tmfObject, err := pdp.HandleCREATEAuth(logger, tmf, rulesEngine, r)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error creating", err.Error())
				slog.Error("creating", slogor.Err(err))
				return
			}

			location := "/" + tmfAPI + "/" + tmfResource + "/" + tmfObject.ID

			// TODO: use Location HTTP header to specify the URI of a newly created resource (POST)
			additionalHeaders := map[string]string{
				"Location": location,
			}

			replyTMF(w, tmfObject.Content, additionalHeaders)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("POST", "latency", slog.Duration("latency", latency))

		})

	// Generic PATCH handler
	mux.HandleFunc("PATCH /{tmfAPI}/{tmfResource}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfAPI := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			logger.Info("PATCH", "api", tmfAPI, "type", tmfResource, slog.Any("request", r))

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "PATCH")
			// This is a semantic alias of the operation being requested
			r.Header.Set("X-Original-Operation", "UPDATE")

			tmfObject, err := pdp.HandleUPDATEAuth(logger, tmf, rulesEngine, r)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}

			// TODO: use Location HTTP header to specify the URI of a newly created resource (POST)
			additionalHeaders := map[string]string{
				"Location": "location",
			}

			replyTMF(w, tmfObject.Content, additionalHeaders)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("PATCH", "latency", slog.Duration("latency", latency))

		})

}
