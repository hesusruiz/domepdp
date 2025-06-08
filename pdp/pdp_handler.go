// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	"github.com/hesusruiz/domeproxy/tmfcache"
	"gitlab.com/greyxor/slogor"
	st "go.starlark.net/starlark"
)

// HandleGETAuthorization returns an [http.Handler] which asks for an authorization decision from the PDP
// by evaluation of the proper policy rules.
// The parameter tmf should be an already instantiated [TMFCache] database manager.
// It also expects in ruleEngine an instance of a policy engine.
func HandleGETAuthorization(
	logger *slog.Logger,
	tmf *tmfcache.TMFCache,
	ruleEngine *PDP,
) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// Check authorization as if we are reading the object, but we are only interested in
		// the authorization result, not in the object itself.
		// TODO: process the request to get the object id, type and resource
		_, err := AuthorizeREAD(logger, tmf, ruleEngine, r, "catalog", "productOffering", "")
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

// AuthorizeLIST processes a GET request to retrieve a list of TMF objects
func AuthorizeLIST(
	logger *slog.Logger, tmf *tmfcache.TMFCache, ruleEngine *PDP, r *http.Request, tmfAPI string, tmfResource string,
) ([]*tmfcache.TMFGeneralObject, error) {

	// ***********************************************************************************
	// Parse the request and get the type of object we are processing.
	// ***********************************************************************************

	requestArgument, err := parseHTTPRequest(logger, r)
	if err != nil {
		return nil, config.Error(err)
	}
	requestArgument["api"] = tmfAPI
	requestArgument["resource"] = tmfResource

	// ******************************************************************************
	// Process the Access Token if it comes with the request, and get the user info
	// ******************************************************************************

	// LIST requests can be unauthenticated, but individual returned objects are
	// subject to visibility policies.
	_, tokenArgument, userArgument, err := extractCallerInfo(logger, ruleEngine, r)
	if err != nil {
		return nil, config.Error(err)
	}

	// ***************************************************************************************
	// Retrieve the list of TMF objects locally, we assume the object is fresh in the cache
	// ***************************************************************************************

	r.ParseForm()

	var finalObjects []*tmfcache.TMFGeneralObject
	var limit int

	// Check if there is a limit requested by the user
	limitStr := r.Form.Get("limit")
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	} else {
		limit = 10
	}

	// Loop until we have retrieved enough objects or there are not any more
	for {

		// limit <= 0 means no limit in this context
		if limit > 0 && len(finalObjects) >= limit {
			break
		}

		// Retrieve the TMF objects of the given type only locally
		// We do not go to the upstream TMF API server, for performance reasons and to
		// implement policy rules easier.
		candidateObjects, _, err := tmf.LocalRetrieveListTMFObject(nil, tmfResource, r.Form)
		if err != nil {
			logger.Error("retrieving list of objects", "resource", tmfResource, slogor.Err(err))
			break
		}

		// No more objects, process what we have
		if len(candidateObjects) == 0 {
			break
		}

		// Process each object in the candidate list
		for _, tmfObject := range candidateObjects {

			// Set the map representation
			oMap := tmfObject.ContentAsMap
			oMap["resource"] = tmfObject.ResourceName
			oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier

			tmfObjectArgument := StarTMFMap(oMap)

			// Update the isOwner attribute of the user according to the object information
			userArgument["isSeller"] = (userArgument["organizationIdentifier"] == tmfObject.Seller)
			userArgument["isSellerOperator"] = (userArgument["organizationIdentifier"] == tmfObject.SellerOperator)
			userArgument["isOwner"] = (userArgument["organizationIdentifier"] == tmfObject.Seller) ||
				(userArgument["organizationIdentifier"] == tmfObject.SellerOperator)

			userArgument["isBuyer"] = (userArgument["organizationIdentifier"] == tmfObject.Buyer)
			userArgument["isBuyerOperator"] = (userArgument["organizationIdentifier"] == tmfObject.BuyerOperator)

			// *********************************************************************************
			// Build the convenience data object from the usage terms embedded in the TMF object.
			// *********************************************************************************

			// Update the TMF object with the restrictions on countries and operator identifiers
			tmfObjectArgument = getAllRestrictionElements(tmfObjectArgument)

			// *********************************************************************************
			// Pass the request, the object and the user to the rules engine for a decision.
			// *********************************************************************************

			userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, tmfObjectArgument, userArgument)
			if userCanAccessObject {
				finalObjects = append(finalObjects, tmfObject)
			}

		}

	}

	// *********************************************************************************
	// Reply to the caller with the list of authorised objects, which can be empty
	// *********************************************************************************

	return finalObjects, nil
}

/*
AuthorizeREAD manages the read process of a single TMForum object (the GET method).
*/
func AuthorizeREAD(
	logger *slog.Logger,
	tmfCache *tmfcache.TMFCache,
	ruleEngine *PDP,
	r *http.Request,
	tmfAPI string,
	tmfResource string,
	id string,
) (tmfcache.TMFObject, error) {

	// ***********************************************************************************
	// Parse the request and get the 'id' of the object from the path of the request.
	// ***********************************************************************************

	requestArgument, err := parseHTTPRequest(logger, r)
	if err != nil {
		return nil, config.Error(err)
	}

	requestArgument["api"] = tmfAPI
	requestArgument["resource"] = tmfResource
	requestArgument["id"] = id

	// ******************************************************************************
	// Process the Access Token if it comes with the request
	// ******************************************************************************

	// READ requests can be unauthenticated
	_, tokenArgument, userArgument, err := extractCallerInfo(logger, ruleEngine, r)
	if err != nil {
		return nil, config.Error(err)
	}

	// ***************************************************************************************
	// Retrieve the existing object from storage, either from the cache or remotely.
	//    This allows to apply the policies.
	// ***************************************************************************************

	slog.Debug("retrieving", "type", tmfResource, "id", id)

	// var tmfObject tmfcache.TMFObject
	// var local bool
	ro, local, err := tmfCache.RetrieveOrUpdateObject(nil, id, "", "", "", tmfcache.LocalOrRemote)
	if err != nil {
		slog.Error("HandleUPDATEAuth", "id", id, slogor.Err(err))
		return nil, config.Errorf("retrieving %s: %w", id, err)
	}
	if local {
		slog.Debug("object retrieved locally", "id", id)
	} else {
		slog.Debug("object retrieved remotely", "id", id)
	}

	tmfObject, _ := ro.(*tmfcache.TMFGeneralObject)

	// Create a summary map object for the rules engine, to make rules simple to write
	oMap := tmfObject.ContentAsMap
	oMap["resource"] = tmfObject.ResourceName
	oMap["organizationIdentifier"] = tmfObject.OrganizationIdentifier

	tmfObjectArgument := StarTMFMap(oMap)

	// ****************************************************************************************
	// Update the user object, combining info from the Access Token and the retrieved object.
	// ****************************************************************************************

	userArgument["isSeller"] = (userArgument["organizationIdentifier"] == tmfObject.Seller)
	userArgument["isSellerOperator"] = (userArgument["organizationIdentifier"] == tmfObject.SellerOperator)
	userArgument["isOwner"] = (userArgument["organizationIdentifier"] == tmfObject.Seller) ||
		(userArgument["organizationIdentifier"] == tmfObject.SellerOperator)

	userArgument["isBuyer"] = (userArgument["organizationIdentifier"] == tmfObject.Buyer)
	userArgument["isBuyerOperator"] = (userArgument["organizationIdentifier"] == tmfObject.BuyerOperator)

	// *************************************************************************************
	// Build the convenience data object from the usage terms embedded in the TMF object.
	// *************************************************************************************

	// Update the TMF object with the restrictions on countries and operator identifiers
	tmfObjectArgument = getAllRestrictionElements(tmfObjectArgument)

	// ********************************************************************************
	// 6. Pass the request, the object and the user to the rules engine for a decision.
	// ********************************************************************************

	userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, tmfObjectArgument, userArgument)

	// ***************************************************************************************
	// 7. Reply to the caller with the object, if the rules engine did not deny the operation.
	// ***************************************************************************************

	if userCanAccessObject {
		return tmfObject, nil
	} else {
		return nil, config.Errorf("not authorized")
	}
}

func getAllRestrictionElements(tmfObjectArgument StarTMFMap) StarTMFMap {

	permittedLegalRegions := getRestrictionElements(tmfObjectArgument, "permittedLegalRegion")
	tmfObjectArgument["permittedCountries"] = permittedLegalRegions

	prohibitedLegalRegions := getRestrictionElements(tmfObjectArgument, "prohibitedLegalRegion")
	tmfObjectArgument["prohibitedCountries"] = prohibitedLegalRegions

	permittedOperators := getRestrictionElements(tmfObjectArgument, "permittedOperator")
	tmfObjectArgument["permittedOperators"] = permittedOperators

	prohibitedOperators := getRestrictionElements(tmfObjectArgument, "prohibitedOperator")
	tmfObjectArgument["prohibitedOperators"] = prohibitedOperators

	return tmfObjectArgument
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
AuthorizeUPDATE manages the update process of a TMForum object (the http PATH verb).
*/
func AuthorizeUPDATE(
	logger *slog.Logger, tmf *tmfcache.TMFCache, ruleEngine *PDP, r *http.Request,
	tmfAPI string, tmfResource string, id string,
) (tmfcache.TMFObject, error) {

	// ********************************************************************
	// Parse the request and get the 'id' of the object to be updated.
	// ********************************************************************

	requestArgument, err := parseHTTPRequest(logger, r)
	if err != nil {
		return nil, err
	}
	requestArgument["api"] = tmfAPI
	requestArgument["resource"] = tmfResource
	requestArgument["id"] = id

	// ******************************************************************************
	// Process the Access Token and retrieve info about the user sending the request.
	// ******************************************************************************

	tokString, tokenArgument, userArgument, err := extractCallerInfo(logger, ruleEngine, r)
	if err != nil {
		return nil, config.Error(err)
	}

	// We do not allow a UPDATE request to come without authorization info
	if len(tokString) == 0 {
		return nil, config.Errorf("not authenticated")
	}

	// ***************************************************************************************
	// Retrieve the current object from the local cache. The object must exist in the cache.
	// ***************************************************************************************

	// var existingTmfObject tmfcache.TMFObject

	logger.Debug("AuthorizeUPDATE: retrieving", "type", tmfResource, "id", id)

	// Check if the object is already in the local database
	ro, found, err := tmf.LocalRetrieveTMFObject(nil, id, "")
	if err != nil {
		return nil, config.Errorf("retrieving from cache %s: %w", id, err)
	}
	if !found {
		return nil, config.Errorf("object not found in local database: %s", id)
	}

	existingTmfObject, _ := ro.(*tmfcache.TMFGeneralObject)

	logger.Debug("AuthorizeUPDATE: object retrieved locally")

	// ***************************************************************************************
	// Check that the user is the owner of the object, using the organizationIdentifier in it.
	// ***************************************************************************************

	userOrgId := userArgument["organizationIdentifier"].(string)

	if userOrgId != existingTmfObject.Seller && userOrgId != existingTmfObject.SellerOperator &&
		userOrgId != existingTmfObject.Buyer && userOrgId != existingTmfObject.BuyerOperator {
		slog.Error("REJECTED: the user is not the owner", "user", userOrgId,
			"seller", existingTmfObject.Seller, "sellerOperator", existingTmfObject.SellerOperator,
			"buyer", existingTmfObject.Buyer, "buyerOperator", existingTmfObject.BuyerOperator)
		return nil, config.Errorf("not authorized")
	}

	// ********************************************************************************************
	// 5. Retrieve the object from the request body, which will be used to update the existing one
	// ********************************************************************************************

	incomingObjectArgument := StarTMFMap{}

	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, config.Errorf("failed to read body: %w", err)
	}

	// Parse the request body into a StarTMFMap
	if err := json.Unmarshal(requestBody, &incomingObjectArgument); err != nil {
		return nil, config.Errorf("failed to parse request: %w", err)
	}

	// Set the user as the owner in the object being written
	incomingObjectArgument["organizationIdentifier"] = userOrgId

	logger.Debug("AuthorizeUPDATE: updating", "type", tmfResource)

	// *********************************************************************************
	// 6. Check if the user can perform the operation on the object.
	// *********************************************************************************

	userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, incomingObjectArgument, userArgument)
	if !userCanAccessObject {
		return nil, config.Errorf("take decision: not authorized")
	}

	// **********************************************************************************
	// 7. Send the request to the central TMForum APIs, to update the object.
	// **********************************************************************************

	hostAndPath, err := tmf.GetHostAndPathFromResourcename(tmfResource)
	if err != nil {
		return nil, config.Errorf("retrieving host and path for resource %s: %w", tmfResource, err)
	}

	// We pass the same authorization token as the one we received from the caller
	remotepo, err := doPATCH(logger, hostAndPath, tokString, userOrgId, requestBody)
	if err != nil {
		logger.Error("AuthorizeUPDATE: performing PATCH", slogor.Err(err))
		return nil, config.Errorf("not authorized: %w", err)
	}

	// The PATCH operation is on an existing object, so we assume that th elocal object has already
	// the owner info.
	// Set the owner id, just in case the remote object does not have it.
	remotepo.SetOwner(existingTmfObject.Owner())

	// **********************************************************************************
	// 8. Update the cache with the response and return to the caller.
	// **********************************************************************************

	// Update the object in the local database
	err = tmf.LocalUpsertTMFObject(nil, remotepo)
	if err != nil {
		logger.Error("AuthorizeUPDATE: update local cache", slogor.Err(err))
		return nil, config.Errorf("not authorized: %w", err)
	}

	return remotepo, nil
}

func AuthorizeCREATE(
	logger *slog.Logger, tmf *tmfcache.TMFCache, ruleEngine *PDP, r *http.Request,
	tmfAPI string, tmfResource string,
) (tmfcache.TMFObject, error) {

	// ********************************************************************
	// Parse the HTTP request.
	// ********************************************************************

	requestArgument, err := parseHTTPRequest(logger, r)
	if err != nil {
		return nil, config.Error(err)
	}
	requestArgument["api"] = tmfAPI
	requestArgument["resource"] = tmfResource

	// ******************************************************************************
	// Process the Access Token and retrieve info about the user sending the request.
	// ******************************************************************************

	tokString, tokenArgument, userArgument, err := extractCallerInfo(logger, ruleEngine, r)
	if err != nil {
		return nil, config.Error(err)
	}

	// We do not allow a CREATE request to come without authorization info
	if len(tokString) == 0 {
		return nil, config.Errorf("not authenticated")
	}

	// *******************************************************************************
	// Retrieve the new object from the request body
	// *******************************************************************************

	incomingObjectArgument := StarTMFMap{}

	incomingRequestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, config.Errorf("failed to read body: %w", err)
	}

	// Parse the request body into a StarTMFMap
	if err := json.Unmarshal(incomingRequestBody, &incomingObjectArgument); err != nil {
		return nil, config.Errorf("failed to parse request: %w", err)
	}

	// Perform some minimal checking. The real validation will be performed by TMForum implementation
	if len(incomingObjectArgument["name"].(string)) == 0 ||
		len(incomingObjectArgument["version"].(string)) == 0 ||
		len(incomingObjectArgument["lifecycleStatus"].(string)) == 0 {
		return nil, config.Errorf("either name, version or lifecycleStatus is missing in the request body")
	}

	// Set the user as the owner in the object being written

	// TODO: add the seller and sellerOperator relatedParties
	// incomingObjectArgument["organizationIdentifier"] = userOrganizationIdentifier

	logger.Debug("AuthorizeCREATE: creating", "resource", tmfResource)

	// *********************************************************************************
	// Check if the user can perform the operation on the object.
	// *********************************************************************************

	userCanCreateObject := takeDecision(ruleEngine, requestArgument, tokenArgument, incomingObjectArgument, userArgument)
	if !userCanCreateObject {
		return nil, config.Errorf("take decision: not authorized")
	}

	// **********************************************************************************
	// Create the object in the upstream TMForum API server.
	// **********************************************************************************

	// Use the updated incoming object for the outgoing request

	hostAndPath, err := tmf.GetHostAndPathFromResourcename(tmfResource)
	if err != nil {
		return nil, config.Errorf("retrieving host and path for resource %s: %w", tmfResource, err)
	}

	// Send the POST to the central server.
	// A POST in TMForum does not reply with any data.
	tmfObject, err := doTMFPOST(logger, tmf.HttpClient, hostAndPath, tokString, incomingObjectArgument)
	if err != nil {
		return nil, config.Errorf("creating object in upstream server: %w", err)
	}

	// **********************************************************************************
	// Update the cache with the object and respond to the caller.
	// **********************************************************************************

	// Insert the object in the local database
	err = tmf.LocalUpsertTMFObject(nil, tmfObject)
	if err != nil {
		return nil, config.Errorf("inserting object in local database: %w", err)
	}

	return tmfObject, nil
}

func doTMFPOST(
	logger *slog.Logger,
	httpClient *http.Client,
	url string,
	auth_token string,
	createObject StarTMFMap,
) (tmfcache.TMFObject, error) {

	organizationIdentifier := createObject["organizationIdentifier"].(string)

	outgoingRequestBody, err := json.Marshal(createObject)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewReader(outgoingRequestBody)

	// This is a POST
	req, err := http.NewRequest("POST", url, buf)
	if err != nil {
		return nil, config.Errorf("creating request: %s: %w", url, err)
	}

	// Set the headers for the outgoing request, including the authorization token
	req.Header.Set("X-Organization", organizationIdentifier)
	req.Header.Set("Authorization", "Bearer "+auth_token)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("content-type", "application/json")

	// Send the request using the provided http client
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, config.Errorf("sending request: %s: %w", url, err)
	}

	// Read the reply body and check possible return errors. We do not use the body.
	responseBody, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, config.Errorf("failed to read body: %s: %w", url, err)
	}

	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, config.Errorf("retrieving object: %s: status: %d", url, res.StatusCode)
	}

	if res.StatusCode != 201 {
		logger.Warn("doTMFPOST: status not 201", "url", url, "status code", res.StatusCode)
	}

	// The response must have the Location header, with the URI to the created resource
	location := res.Header.Get("Location")
	if len(location) == 0 {
		logger.Warn("doTMFPOST: missing Location header in reply", "url", url)
	}

	// The body of the response has the newly created object
	tmfObject, err := tmfcache.TMFObjectFromBytes(responseBody)
	if err != nil {
		return nil, config.Errorf("creating object from response: %w", err)
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
			err = config.Errorf("invalid semicolon separator in query")
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

// tokenFromHeader retrieves the token string in the Authorization header of an HTTP request.
// Returns the empty string if the Authorization header does not exist or has an invalid value.
func tokenFromHeader(r *http.Request) string {
	// Get token from authorization header.
	bearer := r.Header.Get("Authorization")
	if len(bearer) > 7 && strings.ToUpper(bearer[0:6]) == "BEARER" {
		return bearer[7:]
	}
	return ""
}

func doPATCH(logger *slog.Logger, url string, auth_token string, organizationIdentifier string, request_body []byte) (tmfcache.TMFObject, error) {

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
		logger.Error("sending request", "object", url, slogor.Err(err))
		return nil, err
	}
	reply_body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		logger.Error("retrieving object", "status code", res.StatusCode)
		return nil, config.Errorf("retrieving object, status: %d", res.StatusCode)
	}
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	po, err := tmfcache.TMFObjectFromBytes(reply_body)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	// var oMap = map[string]any{}
	// err = json.Unmarshal(reply_body, &oMap)
	// if err != nil {
	// 	return nil, err
	// }

	// // Create a TMFObject struct from the map
	// po, err := tmfcache.NewTMFObject(oMap, nil)
	// if err != nil {
	// 	logger.Error(err.Error())
	// 	return nil, err
	// }

	return po, nil
}

var httpMethodAliases = map[string]string{
	"GET":   "READ",
	"POST":  "CREATE",
	"PATCH": "UPDATE",
}

// parseHTTPRequest converts the HTTP request into a StarTMFMap, processing the X-Original headers.
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
// - X-Original-Operation this an alias for the operatio being requested
// - X-Original-Remote-Addr $remote_addr;
// - X-Original-Host $host;
func parseHTTPRequest(logger *slog.Logger, r *http.Request) (StarTMFMap, error) {

	// X-Original-URI is compulsory
	request_uri := r.Header.Get("X-Original-URI")
	if len(request_uri) == 0 {
		return nil, config.Errorf("X-Original-URI missing")
	}

	reqURL, err := url.ParseRequestURI(request_uri)
	if err != nil {
		return nil, config.Errorf("X-Original-URI (%s) invalid: %w", request_uri, err)
	}

	// X-Original-Method is compulsory
	original_method := r.Header.Get("X-Original-Method")
	if len(original_method) == 0 {
		return nil, config.Errorf("X-Original-Method missing")
	}
	original_operation := r.Header.Get("X-Original-Operation")

	// The Request elements will be represented to Starlark scripts as a StarTMFMap

	// Enrich the request object: "action" is a synonym for the http method received
	requestArgument := StarTMFMap{
		"action":      original_operation,
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

	request_uri_parts := strings.Split(strings.Trim(reqURL.Path, "/"), "/")

	// We must have 2 or more components
	if len(request_uri_parts) < 2 {
		logger.Error("X-Original-URI invalid", slogor.Err(err), "URI", request_uri)
		return nil, config.Errorf("X-Original-URI invalid: %s", request_uri)
	}

	requestArgument["path"] = request_uri_parts

	// The query, as a list of properties
	queryValues, err := parseQuery(reqURL.RawQuery)
	if err != nil {
		return nil, config.Errorf("malformed URI: %w", err)
	}
	requestArgument["query"] = queryValues

	return requestArgument, nil

}

// extractCallerInfo retrieves the Access Token from the request, verifies it if it exists and
// creates a StarTMFMap ready to be passed to the rules engine.
//
// The access token may not exist, but if it does then it must be valid.
// For convenience of the policies, some calculated fields are created and returned in the 'user' object.
func extractCallerInfo(
	logger *slog.Logger,
	ruleEngine *PDP,
	r *http.Request,
) (tokString string, tokenArgument StarTMFMap, user StarTMFMap, err error) {

	tokString = tokenFromHeader(r)
	if len(tokString) == 0 {
		// An empty token is not considered an error, and the caller should enforce its existence
		return tokString, StarTMFMap{}, StarTMFMap{}, nil
	}

	// It is an error to send an invaild token with the request, so we have to verify it.

	// Just some logs
	slog.Debug("Access Token found", "token", tokString)

	// Verify the token and extract the claims.
	// A verification error stops processing.
	var tokClaims map[string]any

	tokClaims, _, err = ruleEngine.getClaimsFromToken(tokString)
	if err != nil {
		logger.Error("invalid access token", slogor.Err(err), "token", tokString)
		return "", nil, nil, config.Errorf("invalid access token: %w", err)
	}

	tokenArgument = StarTMFMap(tokClaims)

	// Create the user with default values. We always return a value
	userArgument := StarTMFMap{
		"isAuthenticated":        false,
		"isLEAR":                 false,
		"isOwner":                false,
		"country":                "",
		"organizationIdentifier": "",
	}

	verifiableCredential := jpath.GetMap(tokenArgument, "vc")

	if len(verifiableCredential) > 0 {
		userArgument["isAuthenticated"] = true

		powers := jpath.GetList(verifiableCredential, "credentialSubject.mandate.power")
		for _, p := range powers {
			if jpath.GetString(p, "type") == "Domain" &&
				jpath.GetString(p, "domain") == "DOME" &&
				jpath.GetString(p, "function") == "Onboarding" &&
				jpath.GetString(p, "action") == "execute" {

				userArgument["isLEAR"] = true

			}
		}

	} else {
		// This is to support old-format Verifiable Credentials
		verifiableCredential = jpath.GetMap(tokenArgument, "verifiableCredential")

		if len(verifiableCredential) > 0 {
			userArgument["isAuthenticated"] = true

			powers := jpath.GetList(verifiableCredential, "credentialSubject.mandate.power")
			for _, p := range powers {
				if jpath.GetString(p, "type") == "Domain" &&
					jpath.GetString(p, "domain") == "DOME" &&
					jpath.GetString(p, "function") == "Onboarding" &&
					jpath.GetString(p, "action") == "execute" {

					userArgument["isLEAR"] = true

				}
			}

		} else {
			// There is not a Verifiable Credential inside the token
			err := config.Errorf("ccess token without verifiable credential: %s", tokString)
			logger.Error(err.Naked().Error())
			return "", nil, nil, err
		}

	}

	// Get the organizationIdentifier of the user
	userOrganizationIdentifier := jpath.GetString(verifiableCredential, "credentialSubject.mandate.mandator.organizationIdentifier")
	if len(userOrganizationIdentifier) == 0 {
		return "", nil, nil, config.Errorf("access token without organizationIdentifier: %s", tokString)
	}
	if !strings.HasPrefix(userOrganizationIdentifier, "did:elsi") {
		return "", nil, nil, config.Errorf("invalid organizationIdentifier: %s in token: %s", userOrganizationIdentifier, tokString)
	}
	userArgument["organizationIdentifier"] = userOrganizationIdentifier

	country := jpath.GetString(verifiableCredential, "credentialSubject.mandate.mandator.country")
	if len(country) == 0 {
		return "", nil, nil, config.Errorf("access token without country: %s", tokString)
	}
	userArgument["country"] = country

	return tokString, tokenArgument, userArgument, nil

}

func takeDecision(
	ruleEngine *PDP,
	requestArgument StarTMFMap,
	tokenArgument StarTMFMap,
	tmfObjectArgument StarTMFMap,
	userArgument StarTMFMap,
) bool {
	// Assemble all data in a single "input" argument, to the style of OPA.
	// We mutate the predeclared identifier, so the policy can access the data for this request.
	// We can also service possible callbacks from the rules engine.
	input := StarTMFMap{
		"request": requestArgument,
		"token":   tokenArgument,
		"tmf":     tmfObjectArgument,
		"user":    userArgument,
	}

	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		b, err := json.MarshalIndent(input, "", "  ")
		if err == nil {
			fmt.Println("PDP input:", string(b))
		}
	}

	decision, err := ruleEngine.TakeAuthnDecision(Authorize, input)

	// An error is considered a rejection, continue with the next candidate object
	if err != nil {
		slog.Error("PDP: request rejected due to an error", slogor.Err(err))
		return false
	}

	// The rules engine rejected the request, continue with the next candidate object
	if !decision {
		slog.Warn("PDP: request rejected due to policy")
		return false
	}

	// The rules engine accepted the request, add the object to the final list
	slog.Info("PDP: request authorised")
	return true
}
