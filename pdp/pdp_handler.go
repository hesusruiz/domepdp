// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/hesusruiz/domeproxy/internal/jpath"
	"gitlab.com/greyxor/slogor"
	st "go.starlark.net/starlark"
)

// HandleGETAuthorization returns an [http.Handler] which asks for an authorization decision from the PDP
// by evaluation of the proper policy rules.
// The parameter tmf should be an already instantiated [TMFdb] database manager.
// It also expects in ruleEngine an instance of a policy engine.
func HandleGETAuthorization(
	logger *slog.Logger,
	tmf *TMFdb,
	ruleEngine *PDP,
) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// Check authorization as if we are reading the object, but we are only interested in
		// the authorization result, not in the object itself.
		_, err := HandleREADAuth(logger, tmf, ruleEngine, r)
		if err != nil {
			// The user can not access the object
			slog.Error("forbidden", slogor.Err(err), "URI", r.URL.RequestURI())
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		// The user is granted access to the object
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
	}
}

// HandleLISTAuth processes a GET request to retrieve a list of TMF objects
func HandleLISTAuth(
	logger *slog.Logger, tmf *TMFdb, ruleEngine *PDP, r *http.Request,
) ([]*TMFObject, error) {

	// ***********************************************************************************
	// 1. Parse the request and get the type of object we are processing.
	// ***********************************************************************************

	requestArgument, err := parseInputRequest(logger, r)
	if err != nil {
		slog.Error("HandleREADAuth: error in HTTP request", slogor.Err(err))
		return nil, fmt.Errorf("HandleREADAuth: error in HTTP request: %W", err)
	}
	tmfType := requestArgument["tmf_entity"].(string)

	// ******************************************************************************
	// 2. Process the Access Token if it comes with the request
	// ******************************************************************************

	// tokstring will be the empty string if no access token is found
	tokString, tokenArgument, err := processAccessToken(logger, ruleEngine, r, true)
	if err != nil {
		slog.Error("HandleREADAuth", slogor.Err(err))
		return nil, fmt.Errorf("processing the access token: %w", err)
	}

	// ****************************************************************************
	// 3. Build the user object from the Access Token.
	// ****************************************************************************

	// The returned user object always exists.
	// If the token was not provided or the user info does not exist, we get a default user object
	// probably only useful for reading public information.
	userArgument := parseUserInfo(tokString, tokenArgument)

	// ***************************************************************************************
	// 4. Retrieve the list of TMF objects locally.
	// ***************************************************************************************

	r.ParseForm()

	var finalObjects []*TMFObject
	var limit int

	// Check if there is a limit requested by the user
	limitStr := r.Form.Get("limit")
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}

	// Loop until we have retrieved enough objects or there are not any more
	for {

		// limit <= 0 means no limit in this context
		if limit > 0 && len(finalObjects) >= limit {
			break
		}

		// Retrieve the product offerings
		candidateObjects, _, err := tmf.RetrieveLocalListTMFObject(nil, tmfType, r.Form)
		if err != nil {
			return nil, err
		}

		// No more objects, process what we have
		if len(candidateObjects) == 0 {
			break
		}

		// Process each object in the candidate list
		for _, tmfObject := range candidateObjects {

			// Set the map representation
			oMap := tmfObject.ContentMap
			oMap["type"] = tmfObject.Type
			oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier

			tmfObjectArgument := StarTMFMap(oMap)

			// Update the isOwner attribute of the user according to the object information
			if len(userArgument["organizationIdentifier"].(string)) > 0 {
				userArgument["isOwner"] = (userArgument["organizationIdentifier"] == tmfObject.OrganizationIdentifier)
			}

			// *********************************************************************************
			// 5. Build the convenience data object from the usage terms embedded in the TMF object.
			// *********************************************************************************

			// We convert the complex structure into simple lists of countries and operator identifiers
			permittedLegalRegions := getRestrictionElements(tmfObjectArgument, "permittedLegalRegion")
			tmfObjectArgument["permittedCountries"] = permittedLegalRegions

			prohibitedLegalRegions := getRestrictionElements(tmfObjectArgument, "prohibitedLegalRegion")
			tmfObjectArgument["permittedCountries"] = prohibitedLegalRegions

			permittedOperators := getRestrictionElements(tmfObjectArgument, "permittedOperator")
			tmfObjectArgument["permittedCountries"] = permittedOperators

			prohibitedOperators := getRestrictionElements(tmfObjectArgument, "prohibitedOperator")
			tmfObjectArgument["permittedCountries"] = prohibitedOperators

			// *********************************************************************************
			// 6. Pass the request, the object and the user to the rules engine for a decision.
			// *********************************************************************************

			userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, tmfObjectArgument, userArgument)
			if userCanAccessObject {
				finalObjects = append(finalObjects, tmfObject)
			}

		}

	}

	// *********************************************************************************
	// 7. Reply to the caller with the list of authorised objects, which can be empty
	// *********************************************************************************

	return finalObjects, nil
}

/*
HandleREADAuth manages the read process of a single TMForum object (the GET method).
*/
func HandleREADAuth(
	logger *slog.Logger, tmf *TMFdb, ruleEngine *PDP, r *http.Request,
) (*TMFObject, error) {

	// ***********************************************************************************
	// 1. Parse the request and get the 'id' of the object from the path of the request.
	// ***********************************************************************************

	requestArgument, err := parseInputRequest(logger, r)
	if err != nil {
		slog.Error("HandleREADAuth: error in HTTP request", slogor.Err(err))
		return nil, fmt.Errorf("HandleREADAuth: error in HTTP request: %W", err)
	}
	tmfType := requestArgument["tmf_entity"].(string)
	id := requestArgument["tmf_id"].(string)

	// ******************************************************************************
	// 2. Process the Access Token if it comes with the request
	// ******************************************************************************

	// tokstring will be the empty string if no access token is found
	tokString, tokenArgument, err := processAccessToken(logger, ruleEngine, r, true)
	if err != nil {
		slog.Error("HandleREADAuth", slogor.Err(err), "id", id)
		return nil, fmt.Errorf("processing the access token: %w", err)
	}

	// ***************************************************************************************
	// 3. Retrieve the current object, either from the cache or remotely.
	// ***************************************************************************************

	var tmfObject *TMFObject

	slog.Debug("retrieving", "type", tmfType, "id", id)

	var local bool
	tmfObject, local, err = tmf.RetrieveOrUpdateObject(nil, id, "", "", LocalOrRemote)
	if err != nil {
		slog.Error("HandleUPDATEAuth", slogor.Err(err), "id", id)
		return nil, fmt.Errorf("not authorized: %w", err)
	}
	if local {
		slog.Debug("object retrieved locally", "id", id)
	} else {
		slog.Debug("object retrieved remotely", "id", id)
	}

	// Create a summary map object for the rules engine, to make rules simple to write
	oMap := tmfObject.ContentMap
	oMap["type"] = tmfObject.Type
	oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier

	tmfObjectArgument := StarTMFMap(oMap)

	// ****************************************************************************
	// 4. Build the user object, combining info from the Access Token and the retrieved object.
	// ****************************************************************************

	// The returned user object always exists.
	// If the token was not provided or the user info does not exist, we get a default user object
	// probably only useful for reading public information.
	userArgument := parseUserInfo(tokString, tokenArgument)

	// *********************************************************************************
	// 5. Build the convenience data object from the usage terms embedded in the TMF object.
	// *********************************************************************************

	// We convert the complex structure into simple lists of countries and operator identifiers
	permittedLegalRegions := getRestrictionElements(tmfObjectArgument, "permittedLegalRegion")
	tmfObjectArgument["permittedCountries"] = permittedLegalRegions

	prohibitedLegalRegions := getRestrictionElements(tmfObjectArgument, "prohibitedLegalRegion")
	tmfObjectArgument["permittedCountries"] = prohibitedLegalRegions

	permittedOperators := getRestrictionElements(tmfObjectArgument, "permittedOperator")
	tmfObjectArgument["permittedCountries"] = permittedOperators

	prohibitedOperators := getRestrictionElements(tmfObjectArgument, "prohibitedOperator")
	tmfObjectArgument["permittedCountries"] = prohibitedOperators

	// *********************************************************************************
	// 6. Pass the request, the object and the user to the rules engine for a decision.
	// *********************************************************************************

	userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, tmfObjectArgument, userArgument)

	// *********************************************************************************
	// 7. Reply to the caller with the object, if the rules engine did not deny the operation.
	// *********************************************************************************

	if userCanAccessObject {
		return tmfObject, nil
	} else {
		return nil, fmt.Errorf("not authorized")
	}
}

func getRestrictionElements(object any, concept string) []string {

	restrictedConcept := []string{}
	// If there is a list called productOfferingTerm
	if poTerms := jpath.GetList(object, "productOfferingTerm"); len(poTerms) > 0 {
		// Iterate each object which should be a map
		for _, term := range poTerms {
			// The object we need has a field called OperatorRestriction
			if jpath.GetString(term, "@type") != "OperatorRestriction" {
				continue
			}
			// Get the list called restrictionList
			restrictionList := jpath.GetList(term, concept)
			if len(restrictionList) == 0 {
				continue
			}

			// Iterate each element, which should be a country specification
			for _, countrySpec := range restrictionList {
				// The field country is the two letter country code
				country := jpath.GetString(countrySpec, "country")
				if country != "" {
					restrictedConcept = append(restrictedConcept, country)
				}
			}
		}
	}
	return restrictedConcept
}

/*
HandleUPDATEAuth manages the update process of a TMForum object (the http PATH verb).
*/
func HandleUPDATEAuth(
	logger *slog.Logger, tmf *TMFdb, ruleEngine *PDP, r *http.Request,
) (*TMFObject, error) {

	// ********************************************************************
	// 1. Parse the request and get the 'id' of the object to be updated from the path of the request.
	// ********************************************************************

	requestArgument, err := parseInputRequest(logger, r)
	if err != nil {
		return nil, err
	}
	tmfType := requestArgument["tmf_entity"].(string)
	id := requestArgument["tmf_id"].(string)

	// ******************************************************************************
	// 2. Process the Access Token if it comes with the request
	// ******************************************************************************

	tokString, tokenArgument, err := processAccessToken(logger, ruleEngine, r, true)
	if err != nil {
		slog.Error("HandleUPDATEAuth", slogor.Err(err))
		return nil, fmt.Errorf("access token missing or not valid")
	}

	// ***************************************************************************************
	// 3. Retrieve the current object from the local cache. The object must exist in the cache.
	// ***************************************************************************************

	var existingTmfObject *TMFObject

	slog.Debug("retrieving", "type", tmfType, "id", id)

	// Check if the object is already in the local database
	existingTmfObject, found, err := tmf.RetrieveLocalTMFObject(nil, id, "")
	if err != nil {
		return nil, fmt.Errorf("pdp: retrieving object from cache: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("pdp: object not found in local database: %s", id)
	}

	slog.Debug("object retrieved locally")

	// ****************************************************************************
	// 4. Check that the user is the owner of the object, using the organizationIdentifier in it.
	// ****************************************************************************

	// The returned user object always exists.
	// If the token was not provided or the user info does not exist, we get a default user object
	// probably only useful for reading public information.
	userArgument := parseUserInfo(tokString, tokenArgument)

	// TODO: check that this "hard rule" applies to all objects, like an Order
	userOrganizationIdentifier := userArgument["organizationIdentifier"].(string)
	if userArgument["organizationIdentifier"].(string) != existingTmfObject.OrganizationIdentifier {
		slog.Error("REJECTED: the user is not the owner", "user", userOrganizationIdentifier, "existing", existingTmfObject.OrganizationIdentifier)
		return nil, fmt.Errorf("not authorized")
	}

	// *******************************************************************************
	// 5. Retrieve the object from the request body, which will be used to update the existing one
	// *******************************************************************************

	incomingObjectArgument := StarTMFMap{}

	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	// Parse the request body into a StarTMFMap
	if err := json.Unmarshal(requestBody, &incomingObjectArgument); err != nil {
		return nil, fmt.Errorf("failed to parse request: %w", err)
	}

	// Set the user as the owner in the object being written
	incomingObjectArgument["organizationIdentifier"] = userOrganizationIdentifier

	slog.Debug("updating", "type", tmfType)

	// *********************************************************************************
	// 6. Check if the user can perform the operation on the object.
	// *********************************************************************************

	userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, incomingObjectArgument, userArgument)
	if !userCanAccessObject {
		return nil, fmt.Errorf("not authorized")
	}

	// **********************************************************************************
	// 7. Send the request to the central TMForum APIs, to update the object.
	// **********************************************************************************

	// We pass the same authorization token as the one we received from the caller
	remotepo, err := doPATCH(tmf.Server()+r.URL.Path, tokString, userOrganizationIdentifier, requestBody)
	if err != nil {
		slog.Error("pdp: performing PATCH", slogor.Err(err))
		return nil, fmt.Errorf("not authorized: %w", err)
	}

	// Set the owner id, because remote objects do not have it
	remotepo, err = remotepo.SetOwner(userOrganizationIdentifier, existingTmfObject.Organization)
	if err != nil {
		slog.Error("pdp: update object with oid", slogor.Err(err))
		return nil, fmt.Errorf("not authorized: %w", err)
	}

	// **********************************************************************************
	// 8. Update the cache with the response and respond to the caller.
	// **********************************************************************************

	// Update the object in the local database
	err = tmf.UpsertTMFObject(nil, remotepo)
	if err != nil {
		slog.Error("pdp: update local cache", slogor.Err(err))
		return nil, fmt.Errorf("not authorized: %w", err)
	}

	return remotepo, nil
}

func HandleCREATEAuth(
	logger *slog.Logger, tmf *TMFdb, ruleEngine *PDP, r *http.Request,
) (*TMFObject, error) {

	// ********************************************************************
	// 1. Parse the HTTP request.
	// ********************************************************************

	requestArgument, err := parseInputRequest(logger, r)
	if err != nil {
		return nil, err
	}
	tmfType := requestArgument["tmf_entity"].(string)

	// ******************************************************************************
	// 2. Process the Access Token if it comes with the request
	// ******************************************************************************

	tokString, tokenArgument, err := processAccessToken(logger, ruleEngine, r, true)
	if err != nil {
		slog.Error("HandleUPDATEAuth", slogor.Err(err))
		return nil, fmt.Errorf("access token missing or not valid")
	}

	// We do not allow a CREATE request to come without authorization info
	if len(tokString) == 0 {
		return nil, fmt.Errorf("not authorized")
	}

	// ****************************************************************************
	// 4. Retrieve the user sending the request.
	// ****************************************************************************

	// The returned user object always exists.
	// If the token was not provided or the user info does not exist, we get a default user object
	// probably only useful for reading public information.
	userArgument := parseUserInfo(tokString, tokenArgument)

	// For a CREATE request, the user requires an organizationIdentifier
	userOrganizationIdentifier := userArgument["organizationIdentifier"].(string)
	if len(userOrganizationIdentifier) == 0 {
		slog.Error("REJECTED: no user organization identifier", "user", userArgument)
		return nil, fmt.Errorf("not authorized")
	}

	// *******************************************************************************
	// 5. Retrieve the object from the request body, which will be used to update the existing one
	// *******************************************************************************

	incomingObjectArgument := StarTMFMap{}

	incomingRequestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	// Parse the request body into a StarTMFMap
	if err := json.Unmarshal(incomingRequestBody, &incomingObjectArgument); err != nil {
		return nil, fmt.Errorf("failed to parse request: %w", err)
	}

	// Perform some minimal checking. The real validation will be performed by TMForum implementation
	if len(incomingObjectArgument["name"].(string)) == 0 ||
		len(incomingObjectArgument["version"].(string)) == 0 ||
		len(incomingObjectArgument["lifecycleStatus"].(string)) == 0 {

		slog.Error("either name, version or lifecycleStatus is missing in the request body")
		return nil, fmt.Errorf("invalid TMF object")
	}

	// Set the user as the owner in the object being written
	incomingObjectArgument["organizationIdentifier"] = userOrganizationIdentifier

	slog.Debug("creating", "type", tmfType)

	// *********************************************************************************
	// 6. Check if the user can perform the operation on the object.
	// *********************************************************************************

	userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, incomingObjectArgument, userArgument)
	if !userCanAccessObject {
		return nil, fmt.Errorf("not authorized")
	}

	// **********************************************************************************
	// 7. Send the request to the central TMForum APIs, to update the object.
	// **********************************************************************************

	// Use the updated incoming object for the outgoing request
	outgoingRequestBody, err := json.Marshal(incomingObjectArgument)
	if err != nil {
		return nil, err
	}

	// Send the POST to the central server.
	// A POST in TMForum does not reply with any data.
	_, err = doPOST(tmf.Server()+r.URL.Path, tokString, userOrganizationIdentifier, outgoingRequestBody)
	if err != nil {
		slog.Error("pdp: performing POST", slogor.Err(err))
		return nil, fmt.Errorf("not authorized: %w", err)
	}

	// **********************************************************************************
	// 8. Update the cache with the object and respond to the caller.
	// **********************************************************************************

	tmfObject, err := NewTMFObject(incomingObjectArgument, nil)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// Insert the object in the local database
	err = tmf.UpsertTMFObject(nil, tmfObject)
	if err != nil {
		slog.Error("pdp: update local cache", slogor.Err(err))
		return nil, fmt.Errorf("not authorized: %w", err)
	}

	return tmfObject, nil
}

func doPOST(url string, auth_token string, organizationIdentifier string, requestBody []byte) (*TMFObject, error) {

	buf := bytes.NewReader(requestBody)

	// This is a POST
	req, err := http.NewRequest("POST", url, buf)
	if err != nil {
		return nil, err
	}

	// Set the headers for the outgoing request, including the authorization token
	req.Header.Set("X-Organization", organizationIdentifier)
	req.Header.Set("Authorization", "Bearer "+auth_token)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("content-type", "application/json")

	// Send the request
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("sending request", "object", url, slogor.Err(err))
		return nil, err
	}

	// Get the reply body and check possible return errors
	replyBody, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		slog.Error(err.Error())
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	if res.StatusCode > 299 {
		slog.Error("retrieving object", "status code", res.StatusCode)
		return nil, fmt.Errorf("retrieving object, status: %d", res.StatusCode)
	}

	// Create a TMFObject struct from the body
	var oMap = map[string]any{}
	err = json.Unmarshal(replyBody, &oMap)
	if err != nil {
		return nil, err
	}

	// Just in case, set the OrganizationIdentifier
	oMap["organizationIdentifier"] = organizationIdentifier

	tmfObject, err := NewTMFObject(oMap, nil)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return tmfObject, nil
}

// parseQuery returns the query part of the request as a StarTMFMap.
//
// It expands the standard query processing to support TMForum query arguments:
//   - for each repeated key in the query, it adds an element to the array associated to the map key.
//   - for a TMF query like 'lifecycleStatus=Launched,Active' it adds 'Launched' and 'Active' as
//     different entries in the list associated to the map key 'lifecycleStatus'
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

func marshallQuery(qmap StarTMFMap) string {
	var b strings.Builder
	for k, v := range qmap {
		if list, ok := v.([]any); ok {
			b.WriteString(k)
			b.WriteString("=")
			for i, e := range list {
				if i > 0 {
					b.WriteString(",")
				}
				elem, _ := e.(string)
				b.WriteString(elem)
			}
		}
	}
	return b.String()
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

func doPATCH(url string, auth_token string, organizationIdentifier string, request_body []byte) (*TMFObject, error) {

	buf := bytes.NewReader(request_body)

	req, err := http.NewRequest("PATCH", url, buf)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Organization", organizationIdentifier)
	req.Header.Set("Authorization", "Bearer "+auth_token)
	// req.Header.Set("Cookie", cookie)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("sending request", "object", url, slogor.Err(err))
		return nil, err
	}
	reply_body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		slog.Error("retrieving object", "status code", res.StatusCode)
		return nil, fmt.Errorf("retrieving object, status: %d", res.StatusCode)
	}
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	var oMap = map[string]any{}
	err = json.Unmarshal(reply_body, &oMap)
	if err != nil {
		return nil, err
	}

	// Create a TMFObject struct from the map
	po, err := NewTMFObject(oMap, nil)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return po, nil
}

var httpMethodAliases = map[string]string{
	"GET":   "READ",
	"POST":  "CREATE",
	"PATCH": "UPDATE",
}

// parseInputRequest converts the HTTP request into a StarTMFMap, processing the X-Original headers.
//
// To facilitate writing the rules, the StarTMFMap object will be composed of:
// - Some relevant fields of the received http.Request
// - Some fields of the Access Token
//
// Some of the values come in special header fields set by the reverse proxy or
// any other component requesting authorization.
// These are the ones we use, with notation from NGINX:
// - X-Original-URI $request_uri;
// - X-Original-Method $request_method
// - X-Original-Remote-Addr $remote_addr;
// - X-Original-Host $host;
func parseInputRequest(logger *slog.Logger, r *http.Request) (StarTMFMap, error) {

	// X-Original-URI is compulsory
	request_uri := r.Header.Get("X-Original-URI")
	if len(request_uri) == 0 {
		logger.Error("X-Original-URI missing")
		return nil, fmt.Errorf("X-Original-URI missing")
	}

	reqURL, err := url.ParseRequestURI(request_uri)
	if err != nil {
		logger.Error("X-Original-URI invalid", slogor.Err(err), "URI", request_uri)
		return nil, fmt.Errorf("X-Original-URI invalid: %W", err)
	}

	// X-Original-Method is compulsory
	original_method := r.Header.Get("X-Original-Method")
	methodAlias, found := httpMethodAliases[original_method]
	if !found {
		logger.Error("X-Original-Method missing or invalid", "method", original_method)
		return nil, fmt.Errorf("X-Original-Method missing or invalid: %v", original_method)
	}

	logger.Info("Request authorization", "URI", request_uri)

	// The Request elements will be represented to Starlark scripts as a StarTMFMap

	// Enrich the request object: "action" is a synonym for the http method received
	requestArgument := StarTMFMap{
		"action":      methodAlias,
		"host":        r.Header.Get("X-Original-Host"),
		"method":      r.Header.Get("X-Original-Method"),
		"remote_addr": r.Header.Get("X-Original-Remote-Addr"),
	}

	// In DOME, the TMForum GET API paths have the following structure:
	// - "GET /{prefix}/{object_type}/{id} for retrieving a single object.
	// - "GET /{prefix}/{object_type}/ for retrieving a listof objects of given type.
	//
	// The possible query parameters at the end of the URI are not present in the Path.
	//
	// To simplify writing rules, we pass the following:
	// - The raw path as a list of path components between the '/' separator.
	// - The interpreted components of the path for TMForum APIs in DOME

	stripped := strings.Trim(reqURL.Path, "/")
	request_uri_parts := strings.Split(stripped, "/")

	// We must have 2 or more components
	if len(request_uri_parts) < 2 {
		logger.Error("X-Original-URI invalid", slogor.Err(err), "URI", request_uri)
		return nil, fmt.Errorf("X-Original-URI invalid")
	}

	// To simplify processing by rules, the path is converted to a list of path segments
	// var elems []st.Value
	// for _, v := range request_uri_parts {
	// 	elems = append(elems, st.String(v))
	// }
	// requestArgument["path"] = StarTMFList(elems)

	requestArgument["path"] = request_uri_parts

	// This is a request for a list of objects. Set the alias accordingly
	if len(request_uri_parts) == 2 {
		requestArgument["action"] = "LIST"
	}

	// In DOME, the type of object is the second path component
	requestArgument["tmf_entity"] = request_uri_parts[1]

	// In DOME, the identifier of the object is the last path component
	if len(request_uri_parts) >= 3 {
		requestArgument["tmf_id"] = request_uri_parts[len(request_uri_parts)-1]
	}

	// The query, as a list of properties
	queryValues, err := parseQuery(reqURL.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("malformed URI: %w", err)
	}
	requestArgument["query"] = queryValues

	return requestArgument, nil

}

// processAccessToken retrieves the Access Token from the request and optionally verifies it and
// creates a StarTMFMap ready to be passed to the rules engine.
//
// The access token may not exist, but if verification is requested, then it must exist and must be valid.
// If verification is not requested, a nil tokenArgument is returned without an error.
func processAccessToken(
	logger *slog.Logger,
	ruleEngine *PDP,
	r *http.Request,
	verify bool,
) (tokString string, tokenArgument StarTMFMap, err error) {

	tokString = tokenFromHeader(r)

	if !verify || tokString == "" {
		return tokString, StarTMFMap{}, nil
	}

	// Just some logs
	logger.Debug("Access Token found")

	// Verify the token and extract the claims.
	// A verification error stops processing.
	var tokClaims map[string]any

	tokClaims, _, err = ruleEngine.getClaimsFromToken(tokString)
	if err != nil {
		logger.Error("invalid access token", slogor.Err(err), "token", tokString)
		return "", nil, fmt.Errorf("invalid access token: %w", err)
	}

	tokenArgument = StarTMFMap(tokClaims)
	return tokString, tokenArgument, nil

}

func parseUserInfo(tokString string, tokenArgument StarTMFMap) StarTMFMap {

	// Create the user with default values
	userArgument := StarTMFMap{
		"isAuthenticated":        false,
		"isLEAR":                 false,
		"isOwner":                false,
		"country":                "",
		"organizationIdentifier": "",
	}

	if len(tokString) > 0 {

		userArgument["isAuthenticated"] = true

		var isLEAR bool

		verifiableCredential := jpath.GetMap(tokenArgument, "vc")
		if len(verifiableCredential) > 0 {
			powers := jpath.GetList(verifiableCredential, "credentialSubject.mandate.power")
			for _, p := range powers {
				if jpath.GetString(p, "type") == "Domain" &&
					jpath.GetString(p, "domain") == "DOME" &&
					jpath.GetString(p, "function") == "Onboarding" &&
					jpath.GetString(p, "action") == "execute" {

					isLEAR = true
				}
			}

		} else {
			// This is to support old-format Verifiable Credentials
			verifiableCredential = jpath.GetMap(tokenArgument, "verifiableCredential")
			powers := jpath.GetList(verifiableCredential, "credentialSubject.mandate.power")
			for _, p := range powers {
				if jpath.GetString(p, "type") == "Domain" &&
					jpath.GetString(p, "domain") == "DOME" &&
					jpath.GetString(p, "function") == "Onboarding" &&
					jpath.GetString(p, "action") == "execute" {

					isLEAR = true
				}
			}

		}
		userArgument["isLEAR"] = isLEAR

		// Get the organizationIdentifier of the user
		userOrganizationIdentifier := jpath.GetString(verifiableCredential, "credentialSubject.mandate.mandator.organizationIdentifier")

		userArgument["organizationIdentifier"] = userOrganizationIdentifier

		userArgument["country"] = jpath.GetString(verifiableCredential, "credentialSubject.mandate.mandator.country")

	}

	return userArgument
}

func takeDecision(
	ruleEngine *PDP,
	requestArgument StarTMFMap,
	tokenArgument StarTMFMap,
	tmfArgument StarTMFMap,
	userArgument StarTMFMap,
) bool {
	// Assemble all data in a single "input" argument, to the style of OPA.
	// We mutate the predeclared identifier, so the policy can access the data for this request.
	// We can also service possible callbacks from the rules engine.
	input := StarTMFMap{
		"request": requestArgument,
		"token":   tokenArgument,
		"tmf":     tmfArgument,
		"user":    userArgument,
	}

	decision, err := ruleEngine.TakeAuthnDecision(Authorize, input)

	// An error is considered a rejection, continue with the next candidate object
	if err != nil {
		slog.Error("!!!!!! REJECTED due to an error", slogor.Err(err))
		return false
	}

	// The rules engine rejected the request, continue with the next candidate object
	if !decision {
		slog.Warn("!!!!!! REJECTED due to policy")
		return false
	}

	// The rules engine accepted the request, add the object to the final list
	slog.Info("PDP Authorized the request")
	return true
}
