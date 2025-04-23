// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfapi

import (
	"log/slog"
	"math/rand/v2"
	"net/http"
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

func addHttpRoutes(
	environment pdp.Environment,
	mux *http.ServeMux,
	tmf *pdp.TMFdb,
	debug bool,
) {

	logger := slog.New(
		slogformatter.NewFormatterHandler(
			slogformatter.HTTPRequestFormatter(false),
			slogformatter.HTTPResponseFormatter(false),
		)(
			slog.Default().Handler(),
		),
	)

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
		replyTMF(w, []byte("{}"))
	})

	// Generic LIST handler
	mux.HandleFunc("GET /{tmfService}/{tmfType}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfService := r.PathValue("tmfService")
			tmfType := r.PathValue("tmfType")

			// Retrieve the list of objects according to the parameters specified in the HTTP request
			logger.Info("GET LIST", "service", tmfService, "type", tmfType)

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "GET")

			// tmfObjectList, err := retrieveList(tmf, tmfType, r)
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

			replyTMF(w, out)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("GET LIST", "latency", slog.Duration("latency", latency))

		})

	// Generic GET handler
	mux.HandleFunc("GET /{tmfService}/{tmfType}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfService := r.PathValue("tmfService")
			tmfType := r.PathValue("tmfType")

			logger.Info("GET Object", "service", tmfService, "type", tmfType, slog.Any("request", r))

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "GET")

			tmfObject, err := pdp.HandleREADAuth(logger, tmf, rulesEngine, r)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}

			replyTMF(w, tmfObject.Content)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("GET Object", "latency", slog.Duration("latency", latency))

		})

	// Generic POST handler
	mux.HandleFunc("POST /{tmfService}/{tmfType}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfService := r.PathValue("tmfService")
			tmfType := r.PathValue("tmfType")

			logger.Info("POST", "service", tmfService, "type", tmfType, slog.Any("request", r))

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "POST")

			tmfObject, err := pdp.HandleCREATEAuth(logger, tmf, rulesEngine, r)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error creating", err.Error())
				slog.Error("creating", slogor.Err(err))
				return
			}

			replyTMF(w, tmfObject.Content)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("POST", "latency", slog.Duration("latency", latency))

		})

	// Generic PATCH handler
	mux.HandleFunc("PATCH /{tmfService}/{tmfType}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			tmfService := r.PathValue("tmfService")
			tmfType := r.PathValue("tmfType")

			logger.Info("PATCH", "service", tmfService, "type", tmfType, slog.Any("request", r))

			// Set the proper fields in the request
			r.Header.Set("X-Original-URI", r.URL.RequestURI())
			r.Header.Set("X-Original-Method", "PATCH")

			tmfObject, err := pdp.HandleUPDATEAuth(logger, tmf, rulesEngine, r)
			if err != nil {
				errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
				slog.Error("retrieving", slogor.Err(err))
				return
			}

			replyTMF(w, tmfObject.Content)

			end := time.Now()
			latency := end.Sub(start)
			slog.Info("PATCH", "latency", slog.Duration("latency", latency))

		})

}

func addOldHttpRoutes(
	environment pdp.Environment,
	mux *http.ServeMux,
	tmf *pdp.TMFdb,
	debug bool,
) {

	logger := slog.New(
		slogformatter.NewFormatterHandler(
			slogformatter.HTTPRequestFormatter(false),
			slogformatter.HTTPResponseFormatter(false),
		)(
			slog.Default().Handler(),
		),
	)

	// Create an instance of the rules engine for the evaluation of the authorization policy rules
	rulesEngine, err := pdp.NewPDP(environment, "auth_policies.star", debug, nil, nil)
	if err != nil {
		panic(err)
	}

	// ****************************************************
	// Set the routes needed for acting as a PIP (reverse proxy)
	//
	// Many routes share the same handlers, thanks to the consistency of the TMF APIs and underlying data model.
	// We have one for all LIST requests (get several objects), and another for GET requests (get one object).

	// The LISTING handlers for Category, Catalog and ProductOffering
	mux.HandleFunc("GET /catalog/category", handleGETlist("category", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /catalog/productOffering", handleGETlist("productOffering", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /catalog/catalog", handleGETlist("catalog", logger, tmf, rulesEngine))

	// The GET one object handlers for all objects of interest
	mux.HandleFunc("GET /catalog/category/{id}", handleGET("category", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /catalog/productOffering/{id}", handleGET("productOffering", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /catalog/productSpecification/{id}", handleGET("productSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /catalog/productOfferingPrice/{id}", handleGET("productOfferingPrice", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /service/serviceSpecification/{id}", handleGET("serviceSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /resource/resourceSpecification/{id}", handleGET("resourceSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("GET /party/organization/{id}", handleGET("organization", logger, tmf, rulesEngine))

	// The POST handlers
	mux.HandleFunc("POST /catalog/category", handlePOST("category", logger, tmf, rulesEngine))
	mux.HandleFunc("POST /catalog/productOffering", handlePOST("productOffering", logger, tmf, rulesEngine))
	mux.HandleFunc("POST /catalog/productSpecification", handlePOST("productSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("POST /catalog/productOfferingPrice", handlePOST("productOfferingPrice", logger, tmf, rulesEngine))
	mux.HandleFunc("POST /service/serviceSpecification", handlePOST("serviceSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("POST /resource/resourceSpecification", handlePOST("resourceSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("POST /party/organization", handlePOST("organization", logger, tmf, rulesEngine))

	// The PATCH handlers
	mux.HandleFunc("PATCH /catalog/category/{id}", handlePATCH("category", logger, tmf, rulesEngine))
	mux.HandleFunc("PATCH /catalog/productOffering/{id}", handlePATCH("productOffering", logger, tmf, rulesEngine))
	mux.HandleFunc("PATCH /catalog/productSpecification/{id}", handlePATCH("productSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("PATCH /catalog/productOfferingPrice/{id}", handlePATCH("productOfferingPrice", logger, tmf, rulesEngine))
	mux.HandleFunc("PATCH /service/serviceSpecification/{id}", handlePATCH("serviceSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("PATCH /resource/resourceSpecification/{id}", handlePATCH("resourceSpecification", logger, tmf, rulesEngine))
	mux.HandleFunc("PATCH /party/organization/{id}", handlePATCH("organization", logger, tmf, rulesEngine))

	//
	// End of the routes needed for acting as a PIP
	// ****************************************************

	// ****************************************************
	// Set the route needed for acting as a pure PDP.
	// In this mode, an external PIP will call us.
	// This route can also be used by any application or service asking for authotization info
	mux.HandleFunc("GET /authorize/v1/policies/authz", pdp.HandleGETAuthorization(logger, tmf, rulesEngine))

}

// handleGETlist retrieves a list of objects, subject to filtering
func handleGETlist(tmfType string, logger *slog.Logger, tmf *pdp.TMFdb, rulesEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()

		// For the moment, a LIST request does not go through authorization, and is fully public
		_ = rulesEngine

		// Retrieve the list of objects according to the parameters specified in the HTTP request
		logger.Info("GET LIST", "type", tmfType)

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "GET")

		// tmfObjectList, err := retrieveList(tmf, tmfType, r)
		tmfObjectList, err := pdp.HandleLISTAuth(logger, tmf, rulesEngine, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
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

		replyTMF(w, out)

		end := time.Now()
		latency := end.Sub(start)
		slog.Info("GET LIST", "latency", slog.Duration("latency", latency))

	}
}

// handleGET retrieves a single TMF object, subject to authorization policy rules
func handleGET(tmfType string, logger *slog.Logger, tmf *pdp.TMFdb, rulesEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("GET", "type", tmfType, slog.Any("request", r))

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "GET")

		tmfObject, err := pdp.HandleREADAuth(logger, tmf, rulesEngine, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		replyTMF(w, tmfObject.Content)

	}
}

func handlePOST(tmfType string, logger *slog.Logger, tmf *pdp.TMFdb, rulesEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("POST", "type", tmfType, slog.Any("request", r))

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "POST")

		tmfObject, err := pdp.HandleCREATEAuth(logger, tmf, rulesEngine, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		replyTMF(w, tmfObject.Content)

	}
}

func handlePATCH(tmfType string, logger *slog.Logger, tmf *pdp.TMFdb, rulesEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("PATCH", "type", tmfType, slog.Any("request", r))

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "PATCH")

		tmfObject, err := pdp.HandleUPDATEAuth(logger, tmf, rulesEngine, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		replyTMF(w, tmfObject.Content)

	}
}
