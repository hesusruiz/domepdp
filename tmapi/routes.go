// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmapi

import (
	"log/slog"
	"net/http"

	"github.com/goccy/go-json"

	"github.com/hesusruiz/domeproxy/constants"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/hesusruiz/domeproxy/tmfsync"
	slogformatter "github.com/samber/slog-formatter"
	"gitlab.com/greyxor/slogor"
)

func addHttpRoutes(
	environment constants.Environment,
	mux *http.ServeMux,
	tmf *tmfsync.TMFdb,
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
	ruleEngine, err := pdp.NewPDP(environment, "auth_policies.star", debug, nil, nil)
	if err != nil {
		panic(err)
	}

	// Many routes share the same handlers, thanks to the consistency of the TMF APIs and underlying data model.
	// We have one for all LIST requests (get several objects), and another for GET requests (get one object).

	// The LISTING handlers for Category, Catalog and ProductOffering
	mux.HandleFunc("GET /catalog/category", handleGETlist("category", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOffering", handleGETlist("productOffering", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/catalog", handleGETlist("catalog", logger, tmf, ruleEngine))

	// The GET one object handlers for all objects of interest
	mux.HandleFunc("GET /catalog/category/{id}", handleGET("category", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOffering/{id}", handleGET("productOffering", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productSpecification/{id}", handleGET("productSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOfferingPrice/{id}", handleGET("productOfferingPrice", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /service/serviceSpecification/{id}", handleGET("serviceSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /resource/resourceSpecification/{id}", handleGET("resourceSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /party/organization/{id}", handleGET("organization", logger, tmf, ruleEngine))

	// The POST handlers
	mux.HandleFunc("POST /catalog/category", handlePOST("category", logger, tmf, ruleEngine))
	mux.HandleFunc("POST /catalog/productOffering", handlePOST("productOffering", logger, tmf, ruleEngine))
	mux.HandleFunc("POST /catalog/productSpecification", handlePOST("productSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("POST /catalog/productOfferingPrice", handlePOST("productOfferingPrice", logger, tmf, ruleEngine))
	mux.HandleFunc("POST /service/serviceSpecification", handlePOST("serviceSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("POST /resource/resourceSpecification", handlePOST("resourceSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("POST /party/organization", handlePOST("organization", logger, tmf, ruleEngine))

	// The PATCH handlers
	mux.HandleFunc("PATCH /catalog/category/{id}", handlePATCH("category", logger, tmf, ruleEngine))
	mux.HandleFunc("PATCH /catalog/productOffering/{id}", handlePATCH("productOffering", logger, tmf, ruleEngine))
	mux.HandleFunc("PATCH /catalog/productSpecification/{id}", handlePATCH("productSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("PATCH /catalog/productOfferingPrice/{id}", handlePATCH("productOfferingPrice", logger, tmf, ruleEngine))
	mux.HandleFunc("PATCH /service/serviceSpecification/{id}", handlePATCH("serviceSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("PATCH /resource/resourceSpecification/{id}", handlePATCH("resourceSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("PATCH /party/organization/{id}", handlePATCH("organization", logger, tmf, ruleEngine))

	// The AUTH handler for acting as a pure PDP
	mux.HandleFunc("GET /authorize/v1/policies/authz", pdp.HandleGETAuthorization(logger, tmf, ruleEngine))

}

// handleGETlist retrieves a list of objects, subject to filtering
func handleGETlist(tmfType string, logger *slog.Logger, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// For the moment, a LIST request does not go through authorization, and is fully public
		_ = ruleEngine

		logger.Info("GET LIST", "type", tmfType)
		tmfObjectList, err := retrieveList(tmf, tmfType, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
			logger.Error("retrieving", slogor.Err(err))
			return
		}

		var listMaps = []map[string]any{}
		for _, v := range tmfObjectList {
			listMaps = append(listMaps, v.ContentMap)
		}

		out, err := json.Marshal(listMaps)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error marshalling list", err.Error())
			logger.Error("error marshalling list", slogor.Err(err))
			return
		}

		replyTMF(w, out)

	}
}

// handleGET retrieves a single TMF object, subject to authorization policy rules
func handleGET(tmfType string, logger *slog.Logger, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("GET", "type", tmfType, slog.Any("request", r))

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "GET")

		tmfObject, err := pdp.HandleREADAuth(logger, tmf, ruleEngine, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		replyTMF(w, tmfObject.Content)

	}
}

func handlePOST(tmfType string, logger *slog.Logger, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("POST", "type", tmfType, slog.Any("request", r))

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "GET")

		tmfObject, err := pdp.HandleCREATEAuth(logger, tmf, ruleEngine, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		replyTMF(w, tmfObject.Content)

	}
}

func handlePATCH(tmfType string, logger *slog.Logger, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("PATCH", "type", tmfType, slog.Any("request", r))

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "PATCH")

		tmfObject, err := pdp.HandleUPDATEAuth(logger, tmf, ruleEngine, r)
		if err != nil {
			errorTMF(w, http.StatusInternalServerError, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		replyTMF(w, tmfObject.Content)

	}
}

func retrieveList(tmf *tmfsync.TMFdb, tmfType string, r *http.Request) ([]*tmfsync.TMFObject, error) {

	r.ParseForm()

	slog.Debug("retrieving", "type", tmfType)

	// Retrieve the product offerings
	objects, _, err := tmf.RetrieveLocalListTMFObject(nil, tmfType, r.Form)
	if err != nil {
		return nil, err
	}

	slog.Debug("object retrieved locally")

	return objects, nil

}
