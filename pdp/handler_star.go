package pdp

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/hesusruiz/domeproxy/internal/jpath"
	"github.com/hesusruiz/domeproxy/tmfsync"
	"gitlab.com/greyxor/slogor"
	st "go.starlark.net/starlark"
)

// handleGETAuthorization returns an [http.Handler] which asks for an authorization decision from the PDP by evaluation of the proper policy rules.
// The parameter tmf should be an already instantiated [tmfsync.TMFdb] databas manager. It also expects in ruleEngine an instance of a
// policy engine.
func handleGETAuthorization(logger *slog.Logger, tmf *tmfsync.TMFdb, rulesEngine *PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// **********************************************
		// Process the http.Request

		// To facilitate writing the rules, the object passed to the engine will be composed of:
		// - Some relevant fields of the received http.Request
		// - Some fields of the Access Token
		//
		// Some of the values come in special header fields set by the reverse proxy or any other component requesting authorization.
		// These are the ones we use, with notation from NGINX:
		// - X-Original-URI $request_uri;
		// - X-Original-Method $request_method
		// - X-Original-Remote-Addr $remote_addr;
		// - X-Original-Host $host;

		// X-Original-URI is compulsory
		request_uri := r.Header.Get("X-Original-URI")
		if len(request_uri) == 0 {
			slog.Error("X-Original-URI missing")
			http.Error(w, "X-Original-URI missing", http.StatusForbidden)
			return
		}

		reqURL, err := url.ParseRequestURI(request_uri)
		if err != nil {
			slog.Error("X-Original-URI invalid", slogor.Err(err), "URI", request_uri)
			http.Error(w, "X-Original-URI invalid: "+err.Error(), http.StatusForbidden)
			return
		}

		// X-Original-Method is compulsory
		original_method := r.Header.Get("X-Original-Method")
		if original_method != "GET" {
			slog.Error("X-Original-Methos missing or invalid")
			http.Error(w, "X-Original-Method missing or invalid", http.StatusForbidden)
			return
		}

		logger.Info("GETAuthorization", "URI", request_uri)

		// The Request elements will be represented to Starlark scripts as a StarTMFMap

		// Enrich the request object: "action: READ" is a synonym for the http method GET
		requestArgument := StarTMFMap{
			"action":      "READ",
			"host":        r.Header.Get("X-Original-Host"),
			"method":      r.Header.Get("X-Original-Method"),
			"remote_addr": r.Header.Get("X-Original-Remote-Addr"),
		}

		// In DOME, the TMForum API paths have the following structure:
		// - "GET /{prefix}/{object_type}?list_of_params" for retrieving a list of objects.
		// - "GET /{prefix}/{object_type}/{object_id}?list_of_params" for retrieving one object.
		//
		// To simplify writing rules, we pass the following:
		// - The raw path as a list of path components among the '/' separator.
		// - The interpreted components of the path for TMForum APIs in DOME
		stripped := strings.Trim(reqURL.Path, "/")
		request_uri_parts := strings.Split(stripped, "/")
		if len(request_uri_parts) < 3 {
			slog.Error("X-Original-URI invalid", slogor.Err(err), "URI", request_uri)
			http.Error(w, "X-Original-URI invalid", http.StatusForbidden)
			return

		}

		// To simplify processing by rules, the path is converted in a list of path segments
		var elems []st.Value
		for _, v := range request_uri_parts {
			elems = append(elems, st.String(v))
		}
		requestArgument["path"] = StarTMFList(elems)

		// In DOME, the type of object is the second path component
		tmfType := request_uri_parts[1]

		// In DOME, the identifier of the object is the third path component
		id := request_uri_parts[2]

		requestArgument["tmf_entity"] = tmfType
		requestArgument["tmf_id"] = id

		// The query, as a list of properties
		queryValues, err := parseQuery(reqURL.RawQuery)
		if err != nil {
			http.Error(w, "malformed URI", http.StatusForbidden)
			return
		}
		requestArgument["query"] = queryValues

		// **********************************************
		// Process the Access Token

		// Retrieve the Access Token from the request, if it exists.
		// We do not enforce here its existence, and delegate the enforcement to the rules engine.
		// But if it is sent, it must be valid.
		tokString := tokenFromHeader(r)

		// Just some logs
		if tokString == "" {
			slog.Warn("no token found")
		} else {
			slog.Debug("Access Token found")
		}

		// Verify the token if it was specified and extract the claims.
		// A verification error stops processing.

		var tokClaims map[string]any

		if len(tokString) > 0 {
			tokClaims, _, err = rulesEngine.getClaimsFromToken(tokString)
			if err != nil {
				slog.Error("invalid access token", slogor.Err(err), "token", tokString)
				http.Error(w, "invalid request", http.StatusForbidden)
				return
			}
		}

		var tokenArgument StarTMFMap

		if tokString == "" {
			tokenArgument = StarTMFMap{}
		} else {
			tokenArgument = StarTMFMap(tokClaims)
		}

		// **********************************************
		// Retrieve the intended TMForum object, to make it available to the rules engine for making the decision

		var tmfObject *tmfsync.TMFObject

		// Retrieve the object, either from our local database or remotely if it does not yet exist.
		// We need this so the rule engine can evaluate the policies using the data from the object.

		slog.Debug("retrieving", "type", tmfType, "id", id)

		var local bool
		tmfObject, local, err = tmf.RetrieveOrUpdateObject(nil, id, "", "", tmfsync.LocalOrRemote)
		if err != nil {
			slog.Error("retrieving TMF object", slogor.Err(err))
			http.Error(w, "not authorized", http.StatusForbidden)
			return
		}
		if local {
			slog.Debug("object retrieved locally")
		} else {
			slog.Debug("object retrieved remotely")
		}

		oMap := tmfObject.ContentMap
		oMap["type"] = tmfObject.Type
		oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier

		tmfArgument := StarTMFMap(oMap)

		// ****************************************
		// Set a simple User object from the received LEARCredential, so simple rules are simple to write.
		// The user always has the full power by accessing the complete token object.

		// Setup some fields about the remote User
		userOI := jpath.GetString(tokClaims, "vc.credentialSubject.mandate.mandator.organizationIdentifier")
		userArgument := &st.Dict{}
		userArgument.SetKey(st.String("organizationIdentifier"), st.String(userOI))

		if userOI == "" || tmfObject == nil {
			userArgument.SetKey(st.String("isOwner"), st.Bool(false))
		} else {
			userArgument.SetKey(st.String("isOwner"), st.Bool(userOI == tmfObject.OrganizationIdentifier))
		}

		requestArgument["user"] = userArgument

		// **********************************************
		// Ask the rules engine for an authorization decision on this request
		decision, err := rulesEngine.TakeAuthnDecision(Authorize, requestArgument, tokenArgument, tmfArgument)

		// An error is considered a rejection
		if err != nil {
			slog.Error("REJECTED REJECTED REJECTED 0000000000000000000000", slogor.Err(err))
			http.Error(w, "not authorized", http.StatusForbidden)
			return
		}

		// The rules engine rejected the request
		if !decision {
			slog.Warn("REJECTED REJECTED REJECTED 0000000000000000000000")
			http.Error(w, "not authorized", http.StatusForbidden)
			return
		}

		// The rules engine accepted the request, return it to the caller
		slog.Info("Authorized Authorized")

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
	}
}

func parseQuery(query string) (StarTMFMap, error) {
	var err error

	m := map[string]any{}

	for query != "" {
		var key string
		key, query, _ = strings.Cut(query, "&")
		if strings.Contains(key, ";") {
			err = fmt.Errorf("invalid semicolon separator in query")
			continue
		}
		if key == "" {
			continue
		}
		key, compoundValue, _ := strings.Cut(key, "=")
		key, err1 := url.QueryUnescape(key)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		compoundValue, err1 = url.QueryUnescape(compoundValue)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		compoundValue = strings.Trim(compoundValue, ",")
		values := strings.Split(compoundValue, ",")
		var elems []st.Value
		for _, v := range values {
			elems = append(elems, st.String(v))
		}
		elemList := StarTMFList(elems)
		m[key] = elemList
	}

	return m, err
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
