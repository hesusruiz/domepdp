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

	"github.com/google/uuid"
	conf "github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/errl"
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
) ([]tmfcache.TMFObject, error) {

	// ***********************************************************************************
	// Parse the request and get the type of object we are processing.
	// ***********************************************************************************

	requestArgument, err := parseHTTPRequest(logger, r)
	if err != nil {
		return nil, errl.Error(err)
	}
	requestArgument["api"] = tmfAPI
	requestArgument["resource"] = tmfResource

	// ******************************************************************************
	// Process the Access Token if it comes with the request, and get the user info
	// ******************************************************************************

	// LIST requests can be unauthenticated, but individual returned objects are
	// subject to visibility policies.
	_, tokenArgument, userArgument, err := extractCallerInfo(logger, tmf, ruleEngine, r)
	if err != nil {
		return nil, errl.Error(err)
	}

	// ***************************************************************************************
	// Retrieve the list of TMF objects locally, we assume the object is fresh in the cache
	// ***************************************************************************************

	r.ParseForm()

	var finalObjects []tmfcache.TMFObject

	// Default offset
	offset := 0
	offsetStr := r.Form.Get("offset")
	if offsetStr != "" {
		offset, _ = strconv.Atoi(offsetStr)
	}

	// Default limit
	limit := 10
	limitStr := r.Form.Get("limit")
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}

	// Objects received, to account for the offset
	counter := 0

	perObject := func(tmfObject tmfcache.TMFObject) tmfcache.LoopControl {

		// Set the map representation
		oMap := tmfObject.GetContentAsMap()
		oMap["resource"] = tmfObject.GetType()
		oMap["organizationIdentifier"] = tmfObject.GetOrganizationIdentifier()

		tmfObjectArgument := StarTMFMap(oMap)

		// Update the isOwner attribute of the user according to the object information
		userArgument["isSeller"] = (userArgument["organizationIdentifier"] == tmfObject.GetSeller())
		userArgument["isSellerOperator"] = (userArgument["organizationIdentifier"] == tmfObject.GetSellerOperator())
		userArgument["isOwner"] = (userArgument["organizationIdentifier"] == tmfObject.GetSeller()) ||
			(userArgument["organizationIdentifier"] == tmfObject.GetSellerOperator())

		userArgument["isBuyer"] = (userArgument["organizationIdentifier"] == tmfObject.GetBuyer())
		userArgument["isBuyerOperator"] = (userArgument["organizationIdentifier"] == tmfObject.GetBuyerOperator())

		// *********************************************************************************
		// Build the convenience data object from the usage terms embedded in the TMF object.
		// *********************************************************************************

		// Update the TMF object with the restrictions on countries and operator identifiers
		tmfObjectArgument = getAllRestrictionElements(tmfObjectArgument)

		// *********************************************************************************
		// Pass the request, the object and the user to the rules engine for a decision.
		// *********************************************************************************

		userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, tmfObjectArgument, userArgument)

		if !userCanAccessObject {
			// This object is not a candidate, tell that we need another object
			return tmfcache.LoopContinue
		}

		// Check if we still did not reach the offset in the number of candidate objects
		if counter < offset {
			// We are still in the offset, do not add the object to the final list
			counter++
			return tmfcache.LoopContinue
		}

		counter++
		finalObjects = append(finalObjects, tmfObject)

		if len(finalObjects) >= limit {
			// We reached the limit, stop the loop
			return tmfcache.LoopStop
		}

		// We still need more objects
		return tmfcache.LoopContinue

	}

	// Retrieve the TMF objects of the given type only locally
	// We do not go to the upstream TMF API server, for performance reasons and to
	// implement policy rules easier.
	err = tmf.LocalRetrieveListTMFObject(nil, tmfResource, r.Form, perObject)
	if err != nil {
		return nil, errl.Errorf("retrieving list of objects: %w", err)
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
	tmf *tmfcache.TMFCache,
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
		return nil, errl.Error(err)
	}

	requestArgument["api"] = tmfAPI
	requestArgument["resource"] = tmfResource
	requestArgument["id"] = id

	// ******************************************************************************
	// Process the Access Token if it comes with the request
	// ******************************************************************************

	// READ requests can be unauthenticated
	_, tokenArgument, userArgument, err := extractCallerInfo(logger, tmf, ruleEngine, r)
	if err != nil {
		return nil, errl.Error(err)
	}

	// ***************************************************************************************
	// Retrieve the existing object from storage, either from the cache or remotely.
	//    This allows to apply the policies.
	// ***************************************************************************************

	slog.Debug("retrieving", "type", tmfResource, "id", id)

	// var tmfObject tmfcache.TMFObject
	// var local bool
	ro, local, err := tmf.RetrieveOrUpdateObject(nil, id, tmfResource, "", "", "", tmfcache.LocalOrRemote)
	if err != nil {
		return nil, errl.Errorf("retrieving %s: %w", id, err)
	}
	if local {
		slog.Debug("object retrieved locally", "id", id)
	} else {
		slog.Debug("object retrieved remotely", "id", id)
	}

	tmfObject, _ := ro.(*tmfcache.TMFGeneralObject)

	// Create a summary map object for the rules engine, to make rules simple to write
	oMap := tmfObject.GetContentAsMap()

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
		return nil, errl.Errorf("not authorized")
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
	// Parse the HTTP request.
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

	tokString, tokenArgument, userArgument, err := extractCallerInfo(logger, tmf, ruleEngine, r)
	if err != nil {
		return nil, errl.Error(err)
	}

	// We do not allow a UPDATE request to come without authorization info
	if len(tokString) == 0 {
		return nil, errl.Errorf("not authenticated")
	}

	// ***************************************************************************************
	// Retrieve the current object from the local cache. The object must exist in the cache.
	// ***************************************************************************************

	// var existingTmfObject tmfcache.TMFObject

	// TODO: allow user to specify the version of the object to update
	// For the moment, we update the latest version (empty string).
	version := ""

	logger.Debug("AuthorizeUPDATE: retrieving", "type", tmfResource, "id", id, "version", version)

	// Check if the object is already in the local database
	ro, found, err := tmf.LocalRetrieveTMFObject(nil, id, tmfResource, version)
	if err != nil {
		return nil, errl.Errorf("retrieving from cache %s: %w", id, err)
	}
	if !found {
		return nil, errl.Errorf("object not found in local database: %s", id)
	}

	existingTmfObject, _ := ro.(*tmfcache.TMFGeneralObject)

	logger.Debug("AuthorizeUPDATE: object retrieved locally")

	// ***************************************************************************************
	// Check that the user is the owner of the object, using the organizationIdentifier in it.
	// ***************************************************************************************

	userOrgId := userArgument["organizationIdentifier"].(string)

	userOrgDID := userOrgId
	if !strings.HasPrefix(userOrgId, "did:elsi:") {
		userOrgDID = "did:elsi:" + userOrgId
	}

	if userOrgDID != existingTmfObject.Seller && userOrgDID != existingTmfObject.SellerOperator &&
		userOrgDID != existingTmfObject.Buyer && userOrgDID != existingTmfObject.BuyerOperator {
		slog.Error("REJECTED: the user is not the owner", "user", userOrgDID,
			"seller", existingTmfObject.Seller, "sellerOperator", existingTmfObject.SellerOperator,
			"buyer", existingTmfObject.Buyer, "buyerOperator", existingTmfObject.BuyerOperator)
		return nil, errl.Errorf("not authorized")
	}

	// ********************************************************************************************
	// 5. Retrieve the object from the request body, which will be used to update the existing one
	// ********************************************************************************************

	incomingRequestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errl.Errorf("failed to read body: %w", err)
	}

	// Parse the request body into a StarTMFMap
	incomingObjectArgument := StarTMFMap{}
	if err := json.Unmarshal(incomingRequestBody, &incomingObjectArgument); err != nil {
		return nil, errl.Errorf("failed to parse request: %w", err)
	}

	logger.Debug("AuthorizeUPDATE: updating", "type", tmfResource)

	// *********************************************************************************
	// 6. Check if the user can perform the operation on the object.
	// *********************************************************************************

	userCanAccessObject := takeDecision(ruleEngine, requestArgument, tokenArgument, incomingObjectArgument, userArgument)
	if !userCanAccessObject {
		return nil, errl.Errorf("take decision: not authorized")
	}

	// **********************************************************************************
	// 7. Send the request to the central TMForum APIs, to update the object.
	// **********************************************************************************

	hostAndPath, err := tmf.UpstreamHostAndPathFromResource(tmfResource)
	if err != nil {
		return nil, errl.Errorf("retrieving host and path for resource %s: %w", tmfResource, err)
	}

	// Send the PATCH to the central server.
	tmfObject, err := doPATCH(logger, id, hostAndPath, tokString, userOrgId, incomingRequestBody, tmfResource)
	if err != nil {
		return nil, errl.Errorf("updating object in upstream server: %w", err)
	}

	// **********************************************************************************
	// 8. Update the cache with the response and return to the caller.
	// **********************************************************************************

	// Update the object in the local database
	err = tmf.LocalUpsertTMFObject(nil, tmfObject)
	if err != nil {
		return nil, errl.Errorf("inserting object in local database: %w", err)
	}

	return tmfObject, nil
}

var ErrorAlreadyExists = errl.Errorf("object already exists")

func AuthorizeCREATE(
	logger *slog.Logger, tmf *tmfcache.TMFCache, ruleEngine *PDP, r *http.Request,
	tmfAPI string, tmfResource string,
) (tmfcache.TMFObject, error) {

	// ********************************************************************
	// Parse the HTTP request.
	// ********************************************************************

	requestArgument, err := parseHTTPRequest(logger, r)
	if err != nil {
		return nil, errl.Error(err)
	}
	requestArgument["api"] = tmfAPI
	requestArgument["resource"] = tmfResource

	// ******************************************************************************
	// Process the Access Token and retrieve info about the user sending the request.
	// ******************************************************************************

	tokString, tokenArgument, userArgument, err := extractCallerInfo(logger, tmf, ruleEngine, r)
	if err != nil {
		return nil, errl.Error(err)
	}

	// We do not allow a CREATE request to come without authorization info
	if len(tokString) == 0 {
		return nil, errl.Errorf("not authenticated")
	}

	// *******************************************************************************
	// Retrieve the new object from the incoming request body
	// *******************************************************************************

	incomingRequestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, errl.Errorf("failed to read body: %w", err)
	}

	// Parse the request body into a StarTMFMap
	incomingObjectArgument := StarTMFMap{}
	if err := json.Unmarshal(incomingRequestBody, &incomingObjectArgument); err != nil {
		return nil, errl.Errorf("failed to parse request: %w", err)
	}

	// If the incoming object has an 'id', check if it is already in the cache and reject creation.
	if id, ok := incomingObjectArgument["id"].(string); ok && len(id) > 0 {
		// Check if the object is already in the local database
		_, found, _ := tmf.LocalRetrieveTMFObject(nil, id, tmfResource, "")
		if found {
			return nil, errl.Errorf("object already exists: %s", id)
		}
	} else {
		// If the incoming object does not have an 'id', we generate a new one
		// The format is "urn:ngsi-ld:{resource-in-kebab-case}:{uuid}"
		newID := fmt.Sprintf("urn:ngsi-ld:%s:%s", conf.ToKebabCase(tmfResource), uuid.NewString())
		incomingObjectArgument["id"] = newID
	}

	// Perform some minimal checking.
	if len(incomingObjectArgument["name"].(string)) == 0 ||
		len(incomingObjectArgument["version"].(string)) == 0 ||
		len(incomingObjectArgument["lifecycleStatus"].(string)) == 0 {
		return nil, errl.Errorf("either name, version or lifecycleStatus is missing in the request body")
	}

	// Set Seller and Buyer information, using the organization identifier from the user making the call
	userOrganizationIdentifier, _ := userArgument["organizationIdentifier"].(string)

	err = setSellerAndBuyerInfo(incomingObjectArgument, userOrganizationIdentifier)
	if err != nil {
		return nil, errl.Errorf("adding required fields: %w", err)
	}

	logger.Debug("AuthorizeCREATE: creating", "resource", tmfResource)

	// *********************************************************************************
	// Check if the user can perform the operation on the object.
	// *********************************************************************************

	userCanCreateObject := takeDecision(ruleEngine, requestArgument, tokenArgument, incomingObjectArgument, userArgument)
	if !userCanCreateObject {
		return nil, errl.Errorf("take decision: not authorized")
	}

	// **********************************************************************************
	// Create the object in the upstream TMForum API server.
	// **********************************************************************************

	// Use the updated incoming object for the outgoing request

	hostAndPath, err := tmf.UpstreamHostAndPathFromResource(tmfResource)
	if err != nil {
		return nil, errl.Errorf("retrieving host and path for resource %s: %w", tmfResource, err)
	}

	// Send the POST to the central server.
	tmfObject, err := doTMFPOST(logger, tmf.HttpClient, hostAndPath, tokString, incomingObjectArgument, tmfResource)
	if err != nil {
		return nil, errl.Errorf("creating object in upstream server: %w", err)
	}

	// **********************************************************************************
	// Update the cache with the object and respond to the caller.
	// **********************************************************************************

	// Insert the object in the local database
	err = tmf.LocalUpsertTMFObject(nil, tmfObject)
	if err != nil {
		return nil, errl.Errorf("inserting object in local database: %w", err)
	}

	return tmfObject, nil
}

// setSellerAndBuyerInfo adds the required fields to the incoming object argument
// Specifically, the Seller and SellerOperator roles are added to the relatedParty list
func setSellerAndBuyerInfo(tmfObjectMap map[string]any, organizationIdentifier string) (err error) {

	// Normalize all organization identifiers to the DID format
	if !strings.HasPrefix(organizationIdentifier, "did:elsi:") {
		organizationIdentifier = "did:elsi:" + organizationIdentifier
	}

	// TODO: add @schemalocation and @type
	// TODO: look in the database for an Organization with the user's organizationIdentifier

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties := jpath.GetList(tmfObjectMap, "relatedParty")

	// Build the two entries
	sellerEntry := map[string]any{
		"role":  "Seller",
		"@type": "RelatedPartyRefOrPartyRoleRef",
		"partyOrPartyRole": map[string]any{
			"@type":         "PartyRef",
			"href":          "urn:ngsi-ld:organization:" + organizationIdentifier,
			"id":            "urn:ngsi-ld:organization:" + organizationIdentifier,
			"name":          organizationIdentifier,
			"@referredType": "Organization",
		},
	}
	sellerOperator := map[string]any{
		"role":  "SellerOperator",
		"@type": "RelatedPartyRefOrPartyRoleRef",
		"partyOrPartyRole": map[string]any{
			"@type":         "PartyRef",
			"href":          "urn:ngsi-ld:organization:221f6434-ec82-4c62",
			"id":            "urn:ngsi-ld:organization:221f6434-ec82-4c62",
			"name":          "did:elsi:VATES-22222222",
			"@referredType": "Organization",
		},
	}

	if len(relatedParties) == 0 {
		slog.Debug("setSellerAndBuyerInfo: no relatedParty, adding seller and sellerOperator")
		tmfObjectMap["relatedParty"] = []any{sellerEntry, sellerOperator}
		return nil
	}

	foundSeller := false
	foundSellerOperator := false

	for _, rp := range relatedParties {

		// Convert entry to a map
		rpMap, _ := rp.(map[string]any)
		if len(rpMap) == 0 {
			return errl.Errorf("invalid relatedParty entry")
		}

		rpRole, _ := rpMap["role"].(string)
		rpRole = strings.ToLower(rpRole)

		if rpRole != "seller" && rpRole != "selleroperator" {
			// Go to next entry
			continue
		}

		if rpRole == "seller" {
			foundSeller = true
		}
		if rpRole == "selleroperator" {
			foundSellerOperator = true
		}

	}

	if !foundSeller {
		// Add the seller if it is not already in the list
		slog.Debug("setSellerAndBuyerInfo: adding seller", "organizationIdentifier", organizationIdentifier)
		relatedParties = append(relatedParties, sellerEntry)
	}

	if !foundSellerOperator {
		// Add the seller operator if it is not already in the list
		slog.Debug("setSellerAndBuyerInfo: adding seller operator", "organizationIdentifier", organizationIdentifier)
		relatedParties = append(relatedParties, sellerOperator)
	}

	tmfObjectMap["relatedParty"] = relatedParties

	return nil

}

func doTMFPOST(
	logger *slog.Logger,
	httpClient *http.Client,
	url string,
	auth_token string,
	createObject StarTMFMap,
	tmfResource string,
) (tmfcache.TMFObject, error) {

	outgoingRequestBody, err := json.Marshal(createObject)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewReader(outgoingRequestBody)

	// This is a POST
	req, err := http.NewRequest("POST", url, buf)
	if err != nil {
		return nil, errl.Errorf("creating request: %s: %w", url, err)
	}

	// Set the headers for the outgoing request, including the authorization token
	req.Header.Set("Authorization", "Bearer "+auth_token)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("content-type", "application/json")

	// Send the request using the provided http client
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, errl.Errorf("sending request: %s: %w", url, err)
	}

	// Read the reply body and check possible return errors. We do not use the body.
	responseBody, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, errl.Errorf("failed to read body: %s: %w", url, err)
	}

	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, errl.Errorf("retrieving object: %s: status: %d", url, res.StatusCode)
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
	tmfObject, err := tmfcache.TMFObjectFromBytes(responseBody, tmfResource)
	if err != nil {
		return nil, errl.Errorf("creating object from response: %w", err)
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
			err = errl.Errorf("invalid semicolon separator in query")
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

func doPATCH(logger *slog.Logger, id string, url string, auth_token string, organizationIdentifier string, request_body []byte, tmfResource string) (tmfcache.TMFObject, error) {

	url = url + "/" + id

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
		return nil, errl.Errorf("retrieving object, status: %d", res.StatusCode)
	}
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	po, err := tmfcache.TMFObjectFromBytes(reply_body, tmfResource)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

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
		return nil, errl.Errorf("X-Original-URI missing")
	}

	reqURL, err := url.ParseRequestURI(request_uri)
	if err != nil {
		return nil, errl.Errorf("X-Original-URI (%s) invalid: %w", request_uri, err)
	}

	// X-Original-Method is compulsory
	original_method := r.Header.Get("X-Original-Method")
	if len(original_method) == 0 {
		return nil, errl.Errorf("X-Original-Method missing")
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
		return nil, errl.Errorf("X-Original-URI invalid: %s", request_uri)
	}

	requestArgument["path"] = request_uri_parts

	// The query, as a list of properties
	queryValues, err := parseQuery(reqURL.RawQuery)
	if err != nil {
		return nil, errl.Errorf("malformed URI: %w", err)
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
	tmf *tmfcache.TMFCache,
	ruleEngine *PDP,
	r *http.Request,
) (tokString string, tokenArgument StarTMFMap, user StarTMFMap, err error) {

	// Check if we are testing the PDP, and if so, return a dummy token
	if ruleEngine.config.FakeClaims {

		slog.Debug("PDP: using fake claims for testing")

		// tokString = "fake-tokenstring"
		// fakeClaims := getFakeClaims(true, "did:elsi:VATES-00000000", "ES")
		// tokenArgument = StarTMFMap(fakeClaims)

		tokString = fakeAT
		if len(tokString) == 0 {
			// An empty token is not considered an error, and the caller should enforce its existence
			return tokString, StarTMFMap{}, StarTMFMap{}, nil
		}

		// It is an error to send an invaild token with the request, so we have to verify it.

		// Verify the token and extract the claims.
		// A verification error stops processing.
		var tokClaims map[string]any

		tokClaims, _, err = ruleEngine.getClaimsFromToken(tokString)
		if err != nil {
			logger.Error("invalid access token", slogor.Err(err), "token", tokString)
			return "", nil, nil, errl.Errorf("invalid access token: %w", err)
		}

		tokenArgument = StarTMFMap(tokClaims)

	} else {

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
			return "", nil, nil, errl.Errorf("invalid access token: %w", err)
		}

		tokenArgument = StarTMFMap(tokClaims)

	}

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
			ptype := jpath.GetString(p, "type")
			pdomain := jpath.GetString(p, "domain")
			pfunction := jpath.GetString(p, "function")
			paction := jpath.GetString(p, "action")

			// Check fields without regards to case
			if strings.EqualFold(ptype, "Domain") &&
				strings.EqualFold(pdomain, "DOME") &&
				strings.EqualFold(pfunction, "Onboarding") &&
				strings.EqualFold(paction, "execute") {

				userArgument["isLEAR"] = true
			}

			ptype = jpath.GetString(p, "tmf_type")
			pdomain = jpath.GetString(p, "tmf_domain")
			pfunction = jpath.GetString(p, "tmf_function")
			paction = jpath.GetString(p, "tmf_action")

			if strings.EqualFold(ptype, "Domain") &&
				strings.EqualFold(pdomain, "DOME") &&
				strings.EqualFold(pfunction, "Onboarding") &&
				strings.EqualFold(paction, "execute") {

				userArgument["isLEAR"] = true
			}

		}

	} else {

		// There is not a Verifiable Credential inside the token
		return "", nil, nil, errl.Errorf("access token without verifiable credential: %s", tokString)

	}

	// Get the organizationIdentifier of the user
	userOrganizationIdentifier := jpath.GetString(verifiableCredential, "credentialSubject.mandate.mandator.organizationIdentifier")
	if len(userOrganizationIdentifier) == 0 {
		return "", nil, nil, errl.Errorf("access token without organizationIdentifier: %s", tokString)
	}
	// if !strings.HasPrefix(userOrganizationIdentifier, "did:elsi") {
	// 	return "", nil, nil, errl.Errorf("invalid organizationIdentifier: %s in token: %s", userOrganizationIdentifier, tokString)
	// }
	userArgument["organizationIdentifier"] = userOrganizationIdentifier

	country := jpath.GetString(verifiableCredential, "credentialSubject.mandate.mandator.country")
	if len(country) == 0 {
		return "", nil, nil, errl.Errorf("access token without country: %s", tokString)
	}
	userArgument["country"] = country

	// *******************************************************************************************
	// Check if the organization of the user already exists in our database, and create it if not
	// *******************************************************************************************

	_, found, err := tmf.LocalRetrieveOrgByDid(nil, userOrganizationIdentifier)
	if err != nil {
		return "", nil, nil, errl.Error(err)
	}

	if !found {

		// Create the organization in memory
		newOrg, err := tmfcache.TMFOrganizationFromToken(tokenArgument)
		if err != nil {
			return "", nil, nil, errl.Error(err)
		}

		// **********************************************************************************
		// Create the object in the upstream TMForum API server.
		// **********************************************************************************

		_, err = tmf.CreateObject(logger, tokString, newOrg)
		if err != nil {
			return "", nil, nil, errl.Errorf("creating organization in upstream server: %w", err)
		}

	}

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
