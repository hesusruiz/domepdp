package tmapi

import (
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/goccy/go-json"

	"github.com/hesusruiz/domeproxy/constants"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/hesusruiz/domeproxy/tmfsync"
	slogformatter "github.com/samber/slog-formatter"
	"gitlab.com/greyxor/slogor"
)

// The standard HTTP error response for TMF APIs
type ErrorTMF struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

// tokenFromHeader retrieves the token string in the authorization header of an HTTP request
func tokenFromHeader(r *http.Request) string {
	// Get token from authorization header.
	bearer := r.Header.Get("Authorization")
	if len(bearer) > 7 && strings.ToUpper(bearer[0:6]) == "BEARER" {
		return bearer[7:]
	}
	return ""
}

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
	ruleEngine, err := pdp.NewPDP(environment, "auth_policies.star", debug)
	if err != nil {
		panic(err)
	}

	// Many routes share the same handlers, thanks to the consistency of the TMF APIs and underlying data model.
	// We have one for all LIST requests (get several objects), and another for GET requests (get one object)
	mux.HandleFunc("GET /catalog/category", handleGETlist("category", tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOffering", handleGETlist("productOffering", tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/catalog", handleGETlist("catalog", tmf, ruleEngine))

	mux.HandleFunc("GET /catalog/category/{id}", handleGET("category", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOffering/{id}", handleGET("productOffering", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productSpecification/{id}", handleGET("productSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOfferingPrice/{id}", handleGET("productOfferingPrice", logger, tmf, ruleEngine))

	mux.HandleFunc("GET /service/serviceSpecification/{id}", handleGET("serviceSpecification", logger, tmf, ruleEngine))
	mux.HandleFunc("GET /resource/resourceSpecification/{id}", handleGET("resourceSpecification", logger, tmf, ruleEngine))

	mux.HandleFunc("GET /party/organization/{id}", handleGET("organization", logger, tmf, ruleEngine))

	mux.HandleFunc("GET /authorize/v1/policies/httpapi/authz", handleGETAuthorization(logger, tmf, ruleEngine))

}

// handleGETlist retrieves a list of objects, subject to filtering
func handleGETlist(tmfType string, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// For the moment, a LIST request does not go through authorization, and are fully public
		_ = ruleEngine

		slog.Info("GET LIST", "type", tmfType)
		tmfObjectList, err := retrieveList(tmf, tmfType, r)
		if err != nil {
			sendError(w, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		var listMaps = []map[string]any{}
		for _, v := range tmfObjectList {
			listMaps = append(listMaps, v.ContentMap)
		}

		out, err := json.Marshal(listMaps)
		if err != nil {
			sendError(w, "error marshalling list", err.Error())
			slog.Error("error marshalling list", slogor.Err(err))
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(out)))
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.Header().Set("X-Powered-By", "MITM Proxy")
		w.Write(out)

	}
}

// handleGET retrieves a single TMF object, subject to authorization policy rules
func handleGET(tmfType string, logger *slog.Logger, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("GET", "type", tmfType, slog.Any("request", r))

		// Retrieve the Access Token from the request, if it exists.
		// We do not enforce here its existence or validity, and delegate enforcement (or not) to the policies in the rule engine
		tokString := tokenFromHeader(r)

		// Just some logs
		if tokString == "" {
			slog.Warn("no token found")
		} else {
			slog.Debug("Access Token found")
		}

		// Retrieve the object, either from our local database or remotely if it does not yet exist.
		// We need this so the rule engine can evaluate the policies using the data from the object.
		tmfObject, err := retrieveObject(tmf, tmfType, r)
		if err != nil {
			sendError(w, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		// Ask the rules engine for a decision on this request
		decision, err := ruleEngine.TakeAuthnDecision(pdp.Authorize, r, tokString, tmfObject)

		// An error is considered a rejection
		if err != nil {
			sendError(w, "error taking decision", err.Error())
			slog.Error("REJECTED REJECTED REJECTED 0000000000000000000000", slogor.Err(err))
			return
		}

		// The rules engine rejected the request
		if !decision {
			sendError(w, "not authenticated", "the policies said NOT!!!")
			slog.Warn("REJECTED REJECTED REJECTED 0000000000000000000000")
			return
		}

		// The rules engine accepted the request, return it to the caller
		slog.Info("Authorized Authorized")

		w.Header().Set("Content-Length", strconv.Itoa(len(tmfObject.Content)))
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.Write(tmfObject.Content)

	}
}

// handleGET retrieves a single TMF object, subject to authorization policy rules
func handleGETAuthorization(logger *slog.Logger, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		logger.Info("GETAuthorization", slog.Any("request", r))

		// Get the ininitial Request values from the header. These are the ones we use, with notation from NGINX
		// X-Original-URI $request_uri;
		// X-Original-Method $request_method
		// X-Original-Remote-Addr $remote_addr;
		// X-Original-Host $host;

		request_uri := r.Header.Get("X-Original-URI")

		// X-Original-URI is compulsory
		if len(request_uri) == 0 {
			http.Error(w, "X-Original-URI missing", http.StatusUnauthorized)
			return
		}

		reqURL, err := url.ParseRequestURI(request_uri)
		if err != nil {
			http.Error(w, "X-Original-URI invalid: "+err.Error(), http.StatusUnauthorized)
			return
		}

		// Get the type of object to retrieve from the request URI.
		// This is specialized for TMForum APIs in DOME
		stripped := strings.Trim(request_uri, "/")
		request_uri_parts := strings.Split(stripped, "/")
		if len(request_uri_parts) < 3 {
			http.Error(w, "X-Original-URI invalid", http.StatusUnauthorized)
			return

		}

		tmfType := request_uri_parts[1]

		// Set the original fields in the Request object before passing it to the rules engine
		r.RequestURI = request_uri
		r.URL = reqURL

		// And the rest of fields
		r.Method = r.Header.Get("X-Original-Method")
		r.Host = r.Header.Get("X-Original-Host")

		if r.Header.Get("X-Original-Remote-Addr") != "" {
			r.RemoteAddr = r.Header.Get("X-Original-Remote-Addr")
		}

		// Retrieve the Access Token from the request, if it exists.
		// We do not enforce here its existence or validity, and delegate enforcement (or not) to the policies in the rule engine
		tokString := tokenFromHeader(r)

		// Just some logs
		if tokString == "" {
			slog.Warn("no token found")
		} else {
			slog.Debug("Access Token found")
		}

		var tmfObject *tmfsync.TMFObject
		if r.Method == "GET" {
			// Retrieve the object, either from our local database or remotely if it does not yet exist.
			// We need this so the rule engine can evaluate the policies using the data from the object.
			id := request_uri_parts[2]

			slog.Debug("retrieving", "type", tmfType, "id", id)

			// Retrieve the product offerings
			var local bool
			tmfObject, local, err = tmf.RetrieveOrUpdateObject(nil, id, "", "", tmfsync.LocalOrRemote)
			if err != nil {
				slog.Error("retrieving", slogor.Err(err))
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
			if local {
				slog.Debug("object retrieved locally")
			} else {
				slog.Debug("object retrieved remotely")
			}
		}

		// Ask the rules engine for a decision on this request
		decision, err := ruleEngine.TakeAuthnDecision(pdp.Authorize, r, tokString, tmfObject)

		// An error is considered a rejection
		if err != nil {
			slog.Error("REJECTED REJECTED REJECTED 0000000000000000000000", slogor.Err(err))
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}

		// The rules engine rejected the request
		if !decision {
			sendError(w, "not authenticated", "the policies said NOT!!!")
			slog.Warn("REJECTED REJECTED REJECTED 0000000000000000000000")
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}

		// The rules engine accepted the request, return it to the caller
		slog.Info("Authorized Authorized")

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
	}
}

func sendError(w http.ResponseWriter, code string, reason string) {
	errtmf := &ErrorTMF{
		Code:   code,
		Reason: reason,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(errtmf)

}

func retrieveObject(tmf *tmfsync.TMFdb, tmfType string, r *http.Request) (*tmfsync.TMFObject, error) {

	id := r.PathValue("id")

	slog.Debug("retrieving", "type", tmfType, "id", id)

	// Retrieve the product offerings
	object, local, err := tmf.RetrieveOrUpdateObject(nil, id, "", "", tmfsync.LocalOrRemote)
	if err != nil {
		return nil, err
	}
	if local {
		slog.Debug("object retrieved locally")
	} else {
		slog.Debug("object retrieved remotely")
	}

	return object, nil

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
