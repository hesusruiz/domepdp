// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfproxy

import (
	"io"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/goccy/go-json"

	"slices"

	"github.com/hesusruiz/domepdp/config"
	mdl "github.com/hesusruiz/domepdp/internal/middleware"
	"github.com/hesusruiz/domepdp/pdp"
	"github.com/hesusruiz/domepdp/tmfcache"
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
	cc *config.Config,
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

	url, err := url.Parse(cc.TMFURLPrefix)
	if err != nil {
		log.Fatalf("Failed to parse target URL: %v", err)
	}

	rewriteFunc := func(r *httputil.ProxyRequest) {
		r.SetURL(url)
		r.Out.Host = r.In.Host
		r.SetXForwarded()
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: rewriteFunc,
	}

	// ****************************************************
	// Set the route needed for acting as a pure PDP.
	// In this mode, an external PIP will call us.
	// This route can also be used by any application or service asking for authorization info
	// TODO: fix this, as it is not working properly
	mux.HandleFunc("GET /authorize/v1/policies/authz", pdp.HandleGETAuthorization(logger, tmf, rulesEngine))

	// This is for the Access Node requests, which are only for reads
	mux.HandleFunc("GET /api/v1/entities", func(w http.ResponseWriter, r *http.Request) {
		// TODO: set the processing for these requests
		mdl.ReplyTMF(w, http.StatusOK, []byte("{}"), nil)
	})

	// This route is specific for serving the OpenAPI interactive interface
	mux.HandleFunc("GET /tmf-api/productCatalogManagement/v5/api-docs/{$}",
		func(w http.ResponseWriter, r *http.Request) {

			proxy.ServeHTTP(w, r)

		})

	// RETRIEVE a list of objects, according to the parameters specified in the HTTP request
	// This is a GET operation, which is the TMF standard for retrieving a list of objects
	// The response will contain the list of objects, if they exist, or an error if they do not
	// The request may contain query parameters to filter the list of objects
	// The response will also contain the X-Total-Count header, which indicates the total number of objects in the list
	// The response will also contain the ETag header, which is the hash of the list of objects
	listHandler := func(w http.ResponseWriter, r *http.Request) {

		tmfManagementSystem := r.PathValue("tmfAPI")
		tmfResource := r.PathValue("tmfResource")

		logger.Info("GET LIST", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource)

		// If the request does not correspond to a TMF resource, just proxy it
		if _, err := cc.UpstreamHostAndPathFromResource(tmfResource); err != nil {
			proxy.ServeHTTP(w, r)
			return
		}

		// Set the proper fields in the request
		r.Header.Set("X-Original-URI", r.URL.RequestURI())
		r.Header.Set("X-Original-Method", "GET")
		// This is a semantic alias of the operation being requested
		r.Header.Set("X-Original-Operation", "LIST")

		tmfObjectList, err := pdp.AuthorizeLIST(logger, tmf, rulesEngine, r, tmfManagementSystem, tmfResource)
		if err != nil {
			mdl.ErrorTMF(w, http.StatusForbidden, "error retrieving list", err.Error())
			logger.Error("retrieving", slogor.Err(err))
			return
		}

		// Create the output list with the map content fields, ready for marshalling
		var listMaps = []map[string]any{}
		for _, v := range tmfObjectList {
			listMaps = append(listMaps, v.GetContentAsMap())
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
		mdl.ReplyTMF(w, http.StatusOK, out, additionalHeaders)

	}

	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}", listHandler)
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}/{$}", listHandler)

	// RETRIEVE one object, according to the id specified in the URL
	// This is a GET operation, which is the TMF standard for retrieving an object
	// The response will contain the object, if it exists, or an error if it does not
	getHandler := func(w http.ResponseWriter, r *http.Request) {

		tmfManagementSystem := r.PathValue("tmfAPI")
		tmfResource := r.PathValue("tmfResource")
		tmfID := r.PathValue("id")

		logger.Info("GET Object", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource, "tmfid", tmfID)

		if tmfResource == "api-docs" {
			proxy.ServeHTTP(w, r)
			return
		}

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

		mdl.ReplyTMF(w, http.StatusOK, tmfObject.GetContentAsJSON(), additionalHeaders)

	}

	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}", getHandler)
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}/{$}", getHandler)

	// CREATE one object, according to the body of the request
	// This is a POST operation, which is the TMF standard for creating new objects
	// The request body must contain the object to be created, in the TMF format
	// The response will contain the created object
	// It creates a new 'id' and 'href' fileds if they are not present in the request body
	postHandler := func(w http.ResponseWriter, r *http.Request) {

		tmfManagementSystem := r.PathValue("tmfAPI")
		tmfResource := r.PathValue("tmfResource")

		logger.Info("POST", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource)

		// Treat the HUB resource for notifications especially
		if tmfResource == "hub" {
			mdl.ErrorTMF(w, http.StatusBadRequest, "not supported", "creating HUB for notifications is not supported")
			logger.Error("creating HUB for notifications is not supported")
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
		mdl.ReplyTMF(w, http.StatusCreated, tmfObject.GetContentAsJSON(), additionalHeaders)

	}

	mux.HandleFunc("POST /tmf-api/{tmfAPI}/{version}/{tmfResource}", postHandler)
	mux.HandleFunc("POST /tmf-api/{tmfAPI}/{version}/{tmfResource}/{$}", postHandler)

	// UPDATE one object, according to the body of the request
	// PATCH /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}
	// This is a PATCH operation, which is the TMF standard for updates
	// It is not a PUT operation, as it does not replace the whole object, but only some fields
	// The PUT operation is not supported in this proxy, as it is not part of the TMF standard
	// and it would require a different handling of the request body
	// The PATCH operation is used to update an existing object, according to the TMF standard
	// It is a partial update, which means that only some fields of the object are updated
	// The request body must contain the fields to be updated, in the TMF format
	patchHandler := func(w http.ResponseWriter, r *http.Request) {

		// PATCH /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}
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

		// TODO: use Location HTTP header
		additionalHeaders := map[string]string{
			"Location": "location",
		}

		// Send the reply in the TMF format
		mdl.ReplyTMF(w, http.StatusOK, tmfObject.GetContentAsJSON(), additionalHeaders)

	}

	mux.HandleFunc("PATCH /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}", patchHandler)
	mux.HandleFunc("PATCH /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}/{$}", patchHandler)

	// This is an administrative function to retrieve configuration data
	// It is not part of the TMF standard, but it is needed for the proper functioning of the proxy
	// It is used to retrieve files stored in the rules engine, such as configuration files or other data
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
