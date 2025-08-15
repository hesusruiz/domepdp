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

	proxyPass := func(w http.ResponseWriter, r *http.Request) {

		// tmfManagementSystem := r.PathValue("tmfAPI")
		// tmfResource := r.PathValue("tmfResource")

		logger.Info("Proxy Pass", mdl.RequestID(r), "url", r.URL.Path)

		url := cc.TMFURLPrefix + r.URL.Path

		// url := cc.TMFURLPrefix + "/tmf-api/" + tmfManagementSystem + "/v5/" + tmfResource

		res, err := http.Get(url)
		if err != nil {
			mdl.ErrorTMF(w, http.StatusInternalServerError, "error retrieving entrypoint", err.Error())
			logger.Error("retrieving", "url", url, slogor.Err(err))
			return
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode > 299 {
			mdl.ErrorTMF(w, http.StatusInternalServerError, "error retrieving entrypoint", res.Status)
			logger.Error("retrieving", "url", url, "satus", res.Status)
			return
		}
		if err != nil {
			mdl.ErrorTMF(w, http.StatusInternalServerError, "error retrieving entrypoint", err.Error())
			logger.Error("retrieving", "url", url, slogor.Err(err))
			return
		}

		contenType := res.Header.Get("Content-Type")
		w.Header().Set("Content-Type", contenType)

		w.Write(body)

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
		mdl.ReplyTMF(w, []byte("{}"), nil)
	})

	// Retrieve the list of objects according to the parameters specified in the HTTP request
	mux.HandleFunc("GET /tmf-api/productCatalogManagement/v5/api-docs/{$}",
		func(w http.ResponseWriter, r *http.Request) {

			proxyPass(w, r)

		})

	// Retrieve the list of objects according to the parameters specified in the HTTP request
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

			tmfManagementSystem := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			logger.Info("GET LIST", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource)

			if _, err := cc.GetHostAndPathFromResourcename(tmfResource); err != nil {
				proxyPass(w, r)
				return
			}

			// if tmfResource == "entrypoint" || tmfResource == "openapi" || tmfResource == "api-docs" {
			// 	proxyPass(w, r)
			// 	return
			// }

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
			mdl.ReplyTMF(w, out, additionalHeaders)

		})

	// Retrieve one object given its id
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

			tmfManagementSystem := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")
			tmfID := r.PathValue("id")

			logger.Info("GET Object", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource, "tmfid", tmfID)

			if tmfResource == "api-docs" {
				proxyPass(w, r)
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

			mdl.ReplyTMF(w, tmfObject.GetContentAsJSON(), additionalHeaders)

		})

	// Create one object, according to the body of the request
	mux.HandleFunc("POST /tmf-api/{tmfAPI}/{version}/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

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
			mdl.ReplyTMF(w, tmfObject.GetContentAsJSON(), additionalHeaders)

		})

	// Update one object
	mux.HandleFunc("PATCH /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}",
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

func addHttpRoutesISBE(
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
		mdl.ReplyTMF(w, []byte("{}"), nil)
	})

	// Retrieve the list of objects according to the parameters specified in the HTTP request
	mux.HandleFunc("GET /tmf-api/productCatalogManagement/v5/api-docs/{$}",
		func(w http.ResponseWriter, r *http.Request) {

			proxy.ServeHTTP(w, r)

		})

	// Retrieve the list of objects according to the parameters specified in the HTTP request
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

			tmfManagementSystem := r.PathValue("tmfAPI")
			tmfResource := r.PathValue("tmfResource")

			logger.Info("GET LIST", mdl.RequestID(r), "api", tmfManagementSystem, "type", tmfResource)

			if _, err := cc.GetHostAndPathFromResourcename(tmfResource); err != nil {
				proxy.ServeHTTP(w, r)
				return
			}

			// if tmfResource == "entrypoint" || tmfResource == "openapi" || tmfResource == "api-docs" {
			// 	proxyPass(w, r)
			// 	return
			// }

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
			mdl.ReplyTMF(w, out, additionalHeaders)

		})

	// Retrieve one object given its id
	mux.HandleFunc("GET /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}",
		func(w http.ResponseWriter, r *http.Request) {

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

			mdl.ReplyTMF(w, tmfObject.GetContentAsJSON(), additionalHeaders)

		})

	// Create one object, according to the body of the request
	mux.HandleFunc("POST /tmf-api/{tmfAPI}/{version}/{tmfResource}",
		func(w http.ResponseWriter, r *http.Request) {

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
			mdl.ReplyTMF(w, tmfObject.GetContentAsJSON(), additionalHeaders)

		})

	// Update one object
	mux.HandleFunc("PATCH /tmf-api/{tmfAPI}/{version}/{tmfResource}/{id}",
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
