// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/config"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type AccessType bool

const OnlyLocal AccessType = true
const LocalOrRemote AccessType = false

func (at AccessType) String() string {
	if at == OnlyLocal {
		return "OnlyLocal"
	}
	return "LocalOrRemote"
}

// TMFCache is a struct that holds a pool of connections to the database and the URL of the DOME server.
//
// The database connection is a pool of connections that is shared by all the requests in this object.
// The connection is returned to the pool when the object is closed.
// This object should be used by only one goroutine. Otherwise, multiple objects can be used concurrently
type TMFCache struct {
	dbpool         *sqlitex.Pool
	domeServer     string
	config         *config.Config
	Maxfreshness   int
	RefreshCounter int
	Dump           bool
	cloneMutex     sync.Mutex
	HttpClient     *http.Client
}

var ErrorRedirectsNotAllowed = errors.New("redirects not allowed")

func NewTMFCache(config *config.Config) (*TMFCache, error) {
	var err error

	tmf := &TMFCache{
		config:       config,
		domeServer:   "https://" + config.BAEProxyDomain,
		Maxfreshness: 60 * 60, // 1 hour
	}

	// Initialize the global pool of database connections
	if tmf.dbpool == nil {
		tmf.dbpool, err = sqlitex.NewPool(config.Dbname, sqlitex.PoolOptions{
			PoolSize: 10,
		})
		if err != nil {
			return nil, err
		}
	}

	// Create the tables if they do not exist
	if err := createTables(tmf.dbpool); err != nil {
		return nil, err
	}

	// Create the http client to send requests to the remore TMF server
	// This instance is safe for concurrent use and will be reused for performance
	tmf.HttpClient = &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return ErrorRedirectsNotAllowed
		},
	}

	return tmf, nil
}

func (tmf *TMFCache) Server() string {
	return tmf.domeServer
}

func (tmf *TMFCache) Close() {
	tmf.dbpool.Close()
}

func indentStr(indent int) string {
	return strings.Repeat(" ", indent)
}

// CloneRemoteProductOfferings visits the remote tree of objects starting from the lists of product offerings.
//
// This function is used only for performing a clone of the remote ProductOffering, so it always goes first
// to the remote TMF server and then creates or refreshes the local object in the database.
// It performs special processing to retrieve and set locally some information like the owner of the object.
func (tmf *TMFCache) CloneRemoteProductOfferings() ([]*TMFObject, map[string]bool, error) {

	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	// This is to avoid loops in the object graph
	visitedObjects := make(map[string]bool)

	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
	limit := 100
	offset := 0

	// We are only interested in ProductOfferings which are launched to the market (lifecycleStatus=Launched)
	lifecycleStatus := "Launched"

	var productOfferings []*TMFObject

	for {

		query := fmt.Sprintf("?limit=%d&offset=%d&lifecycleStatus=%s", limit, offset, lifecycleStatus)
		hostAndPath, err := tmf.config.GetHostAndPathFromResourcename("productOffering")
		if err != nil {
			return nil, nil, err
		}

		url := hostAndPath + query

		// // Get the list of product offerings from the DOME server
		// // TODO: make the base path configurable (or maybe not, given we are DOME-specific already??)
		// url := fmt.Sprintf("%s/catalog/productOffering?limit=%d&offset=%d&lifecycleStatus=%s", tmf.domeServer, limit, offset, lifecycleStatus)
		res, err := http.Get(url)
		if err != nil {
			// Log the error and stop the loop, returning whatever objects we have so far
			err = fmt.Errorf("retrieving %s: %w", url, err)
			slog.Error("cloning productOfferings", "url", url, slogor.Err(err))
			return productOfferings, visitedObjects, nil
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode > 299 {
			err = fmt.Errorf("retrieving %s, status code: %d and\nbody: %s", url, res.StatusCode, string(body))
			slog.Error("Response failed", "url", url, "status", res.StatusCode, "body", string(body))
			return productOfferings, visitedObjects, nil
		}
		if err != nil {
			err = fmt.Errorf("reading response body for %s: %w", url, err)
			slog.Error("reading response body", "url", url, slogor.Err(err))
			return productOfferings, visitedObjects, nil
		}

		// Parse the JSON response
		var poListAsMap []map[string]any
		err = json.Unmarshal(body, &poListAsMap)
		if err != nil {
			slog.Error("parsing JSON response", slogor.Err(err))
			return productOfferings, visitedObjects, nil
		}

		slog.Debug("cloning productOfferings", "url", url, "numObjects", len(poListAsMap))

		// Check if we should termninate the loop because there are no more objects
		if len(poListAsMap) == 0 {
			break
		}

		indent := 0

		// Process the list of product offerings
		for _, oMap := range poListAsMap {

			err := tmfObjectSanityCheck(oMap)
			if err != nil {
				slog.Error("invalid object", slogor.Err(err))
				if slog.Default().Enabled(context.Background(), slog.LevelInfo) {
					out, _ := json.MarshalIndent(oMap, "", "  ")
					fmt.Println(string(out))
				}
				continue
			}

			po, err := tmf.CloneOneProductOffering(oMap, indent, visitedObjects)
			if err != nil {
				slog.Error("cloning PO", slogor.Err(err))
				continue
			}

			productOfferings = append(productOfferings, po)

		}

		// Go and retrieve the next chunk of objects
		offset = offset + limit

	}

	return productOfferings, visitedObjects, nil

}

// CloneOneProductOffering receives a map object representing a ProductOffering and clones it.
// It assumes that the caller already retrieved the remote object, so this function contacts the remote server
// only if it needs to retrieve the owner information.
func (tmf *TMFCache) CloneOneProductOffering(oMap map[string]any, indent int, visitedObjects map[string]bool) (*TMFObject, error) {

	dbconn, err := tmf.dbpool.Take(context.Background())
	if err != nil {
		return nil, fmt.Errorf("CloneOneProductOffering: %w", err)
	}
	defer tmf.dbpool.Put(dbconn)

	// Wrap all db operations in a SQLite Savepoint (which is a nestable transaction)
	// TODO: is this necessary? We only need transactions for the individual objects
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Create the ProductOffering object with whatever info we have now. We will then retrieve the owner info,
	// update it and save it in the local database.
	po, err := TMFObjectFromMap(oMap)
	if err != nil {
		return nil, fmt.Errorf("CloneOneProductOffering: %w", err)
	}

	// At this moment, the ProductOffering object in DOME does not have
	// the identification of the owner organization.
	// We need that info to enable access control at the object level, so we retrieve
	// the owner information indirectly by retrieving the ProductSpecification associated
	// to the ProductOffering, and getting the relevant information from the RelatedParty
	// object associated to the ProductSpecification.
	oid, organization, err := tmf.DeduceProductOfferingOwner(dbconn, oMap)
	if err != nil {
		return nil, fmt.Errorf("CloneOneProductOffering: %w", err)
	} else {
		// Update our ProductOffering with the owner information retrieved
		po.OrganizationIdentifier = oid
		oMap["organizationIdentifier"] = oid
		po.Organization = organization
		oMap["organization"] = organization
		po.ContentAsMap = oMap
	}

	// Update or Insert the ProductOffering in our database
	if err := tmf.LocalUpsertTMFObject(dbconn, po); err != nil {
		return nil, fmt.Errorf("CloneOneProductOffering: %w", err)
	}

	visitedObjects[po.ID] = true

	// Recursively retrieve and save the sub-objects of this ProductOffering.
	// We pass the owner information so those objects can include it with them.
	tmf.visitMap(dbconn, oMap, po.OrganizationIdentifier, po.Organization, indent+3, visitedObjects)

	return po, nil

}

func (tmf *TMFCache) CloneAllRemoteResources() ([]*TMFObject, map[string]bool, error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	visitedObjects := make(map[string]bool)

	// Use the keys of the config.GeneratedDefaultResourceToPathPrefix map to clone all the resource types in the TMF database
	for resourceName := range config.GeneratedDefaultResourceToPathPrefix {
		_, err := tmf.CloneRemoteResource(resourceName, visitedObjects)
		if err != nil {
			slog.Error("cloning remote resource", "resourceName", resourceName, slog.Any("err", err))
			// We continue the loop to retrieve the remaining objects
			continue
		}

	}

	return nil, visitedObjects, nil

}

func (tmf *TMFCache) CloneAllRemoteBAEResources() ([]*TMFObject, map[string]bool, error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	visitedObjects := make(map[string]bool)

	// Use the keys of the config.GeneratedDefaultResourceToPathPrefix map to clone all the resource types in the TMF database
	for resourceName := range config.GeneratedDefaultResourceToBAEPathPrefix {

		_, err := tmf.CloneRemoteResource(resourceName, visitedObjects)
		if err != nil {
			slog.Error("cloning remote resource", "resourceName", resourceName, slog.Any("err", err))
			// We continue the loop to retrieve the remaining objects
			continue
		}

	}

	return nil, visitedObjects, nil

}

func (tmf *TMFCache) CloneRemoteResource(tmfResourceName string, visitedObjects map[string]bool) (objectList []*TMFObject, err error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	slog.Info("cloning all objects of type", "resourceName", tmfResourceName)

	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
	limit := 100
	offset := 0

	// We are only interested in ProductOfferings which are launched or active
	// to the market (lifecycleStatus=Launched,Active)
	lifecycleStatus := "Launched,Active"

	var poList []*TMFObject

	// Loop retrieving all objets of the given type
	for {

		hostAndPath, err := tmf.config.GetHostAndPathFromResourcename(tmfResourceName)
		if err != nil {
			slog.Error("retrieving host and path for resource", "resourceName", tmfResourceName, slogor.Err(err))
			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
			break
		}
		query := fmt.Sprintf("?limit=%d&offset=%d&lifecycleStatus=%s", limit, offset, lifecycleStatus)

		url := hostAndPath + query

		// Get the object from the DOME server
		slog.Debug("retrieving objects", "url", url)
		res, err := http.Get(url)
		if err != nil {
			slog.Error("performing GET", "url", url, slogor.Err(err))
			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
			break
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode > 299 {
			slog.Error("Response failed", "status", res.StatusCode, "body", string(body), "url", url)
			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
			break
		}
		if err != nil {
			slog.Error("reading response body", slogor.Err(err), "url", url)
			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
			break
		}

		// Check if it looks like a JSON object
		if body[0] != '{' && body[0] != '[' {
			slog.Error("reply does not look as a JSON object", "url", url)
			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
			break
		}

		// Parse the JSON response
		var resourceListMap []map[string]any
		err = json.Unmarshal(body, &resourceListMap)
		if err != nil {
			slog.Error("parsing JSON response", "url", url, slogor.Err(err))
			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
			break
		}

		// Check if we should terminate the loop because there are no more objects
		if len(resourceListMap) == 0 {
			break
		}

		slog.Info("retrieved objects", "url", url, "numObjects", len(resourceListMap))

		// Process each of the objects in the list
		for _, oMap := range resourceListMap {

			id, ok := oMap["id"].(string)
			if !ok {
				slog.Error("invalid id type", "id", id)
				continue
			}

			var local bool

			// With RetrieveOrUpdateObject, we go to the remote server only if the object is not in the local cache and
			// is not fresh enough
			tmfObject, local, err := tmf.RetrieveOrUpdateObject(nil, id, "", "", LocalOrRemote)
			if err != nil {
				slog.Error("RetrieveOrUpdateObject", slogor.Err(err), "id", id)
				b, err := json.MarshalIndent(oMap, "", "   ")
				if err == nil {
					fmt.Println(string(b))
				}
				continue
			}
			if local {
				slog.Debug("object retrieved locally", "id", id)
			} else {
				slog.Debug("object retrieved remotely", "id", id)
			}

			// TODO: there must be a related party with the Seller, Buyer, SellerOperator and BuyerOperator roles
			// If there is, then just continue.
			// Otherwise, if the object is a ProductOffering, try to deduce the owner, by using the info in
			// the associated ProductSpecification object.

			// // There must be a relatedParty object
			// relatedPartyList, ok := po.ContentMap["relatedParty"].([]any)
			// if !ok {
			// 	slog.Error("invalid relatedParty object")
			// 	out, _ := json.MarshalIndent(oMap, "", "   ")
			// 	fmt.Println(string(out))
			// 	continue
			// }

			// if relatedPartyList == nil {
			// 	slog.Error("relatedParty is nil")
			// 	return nil, nil, fmt.Errorf("relatedParty is nil")
			// }

			// // One of the relatedParty items must be the one defining the owner
			// for _, rp := range relatedPartyList {
			// 	rpMap := rp.(map[string]any)
			// 	if strings.ToLower(rpMap["role"].(string)) == "owner" {
			// 		owner, _, err := tmf.RetrieveOrUpdateObject(nil, rpMap["href"].(string), "", "", LocalOrRemote)
			// 		if err != nil {
			// 			slog.Error(err.Error())
			// 			return nil, nil, err
			// 		}

			// 		// The array 'externalReference' contains the ID of the organization
			// 		ownerReference := owner.ContentMap["externalReference"].([]any)
			// 		if ownerReference == nil {
			// 			slog.Info("externalReference is nil")
			// 			return nil, nil, fmt.Errorf("externalReference is nil")
			// 		}

			// 		for _, extRef := range ownerReference {
			// 			extRefMap := extRef.(map[string]any)
			// 			if extRefMap["externalReferenceType"] == "idm_id" {
			// 				oid := extRefMap["name"].(string)
			// 				organization := owner.ID

			// 				// Now that we have the owner, update the local database for the productSpecification object
			// 				if len(owner.OrganizationIdentifier) == 0 {
			// 					owner, _ = owner.SetOwner(oid, organization)
			// 					err := tmf.LocalUpsertTMFObject(nil, owner)
			// 					if err != nil {
			// 						slog.Error(err.Error())
			// 						return nil, nil, err
			// 					}
			// 				}
			// 				if len(po.OrganizationIdentifier) == 0 {
			// 					po, _ = po.SetOwner(oid, organization)
			// 					err := tmf.LocalUpsertTMFObject(nil, po)
			// 					if err != nil {
			// 						slog.Error(err.Error())
			// 						return nil, nil, err
			// 					}
			// 				}

			// 				poList = append(poList, po)
			// 				break
			// 			}
			// 		}

			// 	}
			// }

			visitedObjects[tmfObject.ID] = true

		}

		// Go and retrieve the next chunk of objects
		offset = offset + limit

	}

	return poList, nil

}

// DeduceProductOfferingOwner retrieves the owner of a product offering.
//
// The strategy is the following:
//  1. If the productOffering already includes information about the owner, we process it and save in the local database.
//  2. Otherwise, we retrieve the remote productSpecification object associated to the productOffering,
//     which always includes the owner information in the relatedParty object.
//
// The productOffering object has a 'productSpecification' field that points to the remote
// productSpecification object.
// The productSpecificationObject has a 'relatedParty' field which is an array of objects,
// one of which must have the role 'owner', with a 'href' field pointing to the remote 'organization' TMF object
// which is (finally) the one with the owner object.
func (tmf *TMFCache) DeduceProductOfferingOwner(dbconn *sqlite.Conn, productOfferingMap map[string]any) (oid string, organization string, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return "", "", err
		}
		defer func() {
			tmf.dbpool.Put(dbconn)
		}()
	}

	// If we have locally already the owner information, we return it. Once an object is created in the DOME
	// server, the owner never changes, so we can trust the local information.
	oid, _ = productOfferingMap["organizationIdentifier"].(string)
	organization, _ = productOfferingMap["organization"].(string)
	if oid != "" && organization != "" {
		return oid, organization, nil
	}

	// At this point we have to retrieve the owner information from the remote server

	// Get the info to retrieve the productSpecification object from the server
	psMap, ok := productOfferingMap["productSpecification"].(map[string]any)
	if !ok {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: productSpecification is nil or not a map for productOffering %s", productOfferingMap["id"])
	}

	if len(psMap) == 0 {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: productSpecification is empty for productOffering %s", productOfferingMap["id"])
	}

	// Get the href to retrieve the remote associated object
	href, ok := psMap["href"].(string)
	if !ok {

		// Try with the ID, as they are equal
		href, ok = psMap["id"].(string)
		if !ok {
			return "", "", fmt.Errorf("DeduceProductOfferingOwner: productSpecification 'id' is nil or not a string for productOffering %s", productOfferingMap["id"])
		}
	}

	if href == "" {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: productSpecification 'href' is nil or empty for productOffering %s", productOfferingMap["id"])
	}

	// Use the 'href' field to retrieve the productSpecification object from the server
	// After the call, the productSpecification object is already persisted locally with the owner information in the
	// standard TMF format. We need to update the database in the format we need for efficient SQL queries.
	productSpecification, _, err := tmf.RetrieveOrUpdateObject(dbconn, href, "", "", LocalOrRemote)
	if err != nil {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: %w", err)
	}

	// There must be a relatedParty object
	relatedPartyList, ok := productSpecification.ContentAsMap["relatedParty"].([]any)
	if !ok {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: relatedParty is nil for productSpecification %s", productSpecification.ID)
	}

	// One of the relatedParty items must be the one defining the owner
	for _, rp := range relatedPartyList {
		relatedParty, ok := rp.(map[string]any)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: relatedParty is not a map", "productSpecification", productSpecification.ID)
			continue
		}

		// We look for an entry "role" = "owner", in a case-insensitive way
		role, ok := relatedParty["role"].(string)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: relatedParty 'role' is nil or not a string", "productSpecification", productSpecification.ID)
			continue
		}
		if strings.ToLower(role) != "owner" {
			continue
		}

		// If "owner" entry found, use 'href' to retrieve the referenced object from the remote server.
		// 'href' points to an Organization TMF object
		href, ok := relatedParty["href"].(string)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: relatedParty 'href' is nil or not a string", "productSpecification", productSpecification.ID)
			continue
		}

		organizationObject, _, err := tmf.RetrieveOrUpdateObject(dbconn, href, "", "", LocalOrRemote)
		if err != nil {
			slog.Error("DeduceProductOfferingOwner: retrieving organization object", "href", href, "productSpecification", productSpecification.ID, slogor.Err(err))
			// We do not stop the loop. We may have other relatedParty objects
			// with the owner information we need.
			continue
		}

		// Inside Organization, the array externalReference contains the ID of the organization
		ownerReference, ok := organizationObject.ContentAsMap["externalReference"].([]any)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: externalReference is nil or not a list", "productSpecification", productSpecification.ID)
			continue
		}

		// The externalReference array must contain an entry with a map named "externalReferenceType"
		// where one of the keys is "idm_id".
		// We look at all entries in the array to find the one with "externalReferenceType" = "idm_id"
		for _, extRef := range ownerReference {
			extRefMap, ok := extRef.(map[string]any)
			if !ok {
				continue
			}
			externalReferenceType, ok := extRefMap["externalReferenceType"].(string)
			if !ok {
				continue
			}

			if strings.ToLower(externalReferenceType) == "idm_id" {

				oid, ok := extRefMap["name"].(string)
				if !ok {
					slog.Error("DeduceProductOfferingOwner: externalReference 'name' is nil or not a string", "productSpecification", productSpecification.ID)
					continue
				}
				organization := organizationObject.ID

				// Now that we have the owner, update the local database for the Organization object
				if len(organizationObject.OrganizationIdentifier) == 0 || len(organizationObject.Organization) == 0 {
					organizationObject, _ = organizationObject.SetOwner(oid, organization)
					err := tmf.LocalUpsertTMFObject(dbconn, organizationObject)
					if err != nil {
						return "", "", fmt.Errorf("DeduceProductOfferingOwner: %w", err)
					}
				}

				// And do the same with the ProductSpecification object
				if len(productSpecification.OrganizationIdentifier) == 0 || len(productSpecification.Organization) == 0 {
					productSpecification, _ = productSpecification.SetOwner(oid, organization)
					err := tmf.LocalUpsertTMFObject(dbconn, productSpecification)
					if err != nil {
						return "", "", fmt.Errorf("DeduceProductOfferingOwner: %w", err)
					}
				}

				return oid, organization, nil
			}
		}

	}

	return "", "", fmt.Errorf("relatedParty is nil")
}

// DeduceProductOfferingSeller retrieves the owner of a product offering for old entries not complying with the DOME rules.
//
// The strategy is the following:
//  1. If the productOffering already includes information about the owner, we process it and save in the local database.
//  2. Otherwise, we retrieve the remote productSpecification object associated to the productOffering,
//     which always includes the owner information in its relatedParty sub-resource.
//
// The productOffering object has a 'productSpecification' field that points to the remote
// productSpecification object.
// The productSpecificationObject has a 'relatedParty' field which is an array of objects,
// one of which must have the role 'owner', with a 'href' field pointing to the remote 'organization' TMF object
// which is (finally) the one with the owner object.
func (tmf *TMFCache) DeduceProductOfferingSeller(
	dbconn *sqlite.Conn,
	po *TMFObject,
) (organizationIdentifier string, organization string, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return "", "", err
		}
		defer func() {
			tmf.dbpool.Put(dbconn)
		}()
	}

	// Sanity check: this logic only applies to a ProductOffering
	if po.ResourceName != config.ProductOffering {
		return "", "", fmt.Errorf("DeduceProductOfferingSeller: not a ProductOffering: %s", po.ResourceName)
	}

	// If we have locally already the owner information, we return it. Once an object is created in the DOME
	// server, the owner never changes, so we can trust the local information.
	if po.Seller != "" {
		// Sanity check
		if !strings.HasPrefix(po.Seller, "did:elsi") {
			return "", "", fmt.Errorf("DeduceProductOfferingSeller: invalid Seller value: %s", po.Seller)
		}
		return po.Seller, po.Organization, nil
	}

	organizationIdentifier = po.OrganizationIdentifier
	organization = po.Organization
	if organizationIdentifier != "" && organization != "" {
		return organizationIdentifier, organization, nil
	}

	// At this point we have to retrieve the owner information from the remote server
	poMap := po.ContentAsMap

	// Get the info to retrieve the productSpecification object from the server
	prodSpecRefMap, ok := poMap["productSpecification"].(map[string]any)
	if !ok {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: productSpecification is nil or not a map for productOffering %s", po.ID)
	}

	if len(prodSpecRefMap) == 0 {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: productSpecification is empty for productOffering %s", po.ID)
	}

	// Get the prodSpecID to retrieve the remote associated object
	prodSpecID, _ := prodSpecRefMap["id"].(string)
	if prodSpecID == "" {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: productSpecification 'id' is nil or not a string for productOffering %s", po.ID)
	}

	// Use the 'id' field to retrieve the productSpecification object from the server
	// After the call, the productSpecification object is already persisted locally with the owner information in the
	// standard TMF format. We need to update the database in the format we need for efficient SQL queries.
	productSpecification, _, err := tmf.RetrieveOrUpdateObject(dbconn, prodSpecID, "", "", LocalOrRemote)
	if err != nil {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: %w", err)
	}

	// There must be a relatedParty object
	prodSpecRelatedParties, ok := productSpecification.ContentAsMap["relatedParty"].([]any)
	if !ok {
		return "", "", fmt.Errorf("DeduceProductOfferingOwner: relatedParty is nil for productSpecification %s", productSpecification.ID)
	}

	// One of the relatedParty items must be the one defining the owner
	for _, rp := range prodSpecRelatedParties {
		relatedParty, ok := rp.(map[string]any)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: relatedParty is not a map", "productSpecification", productSpecification.ID)
			continue
		}

		// We look for an entry "role" = "owner", in a case-insensitive way
		role, ok := relatedParty["role"].(string)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: relatedParty 'role' is nil or not a string", "productSpecification", productSpecification.ID)
			continue
		}
		if strings.ToLower(role) != "owner" {
			continue
		}

		// If "owner" entry found, use 'id' to retrieve the referenced object from the remote server.
		// 'id' points to an Organization TMF object
		ownerOrgID, ok := relatedParty["id"].(string)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: relatedParty 'href' is nil or not a string", "productSpecification", productSpecification.ID)
			continue
		}

		organizationObject, _, err := tmf.RetrieveOrUpdateObject(dbconn, ownerOrgID, "", "", LocalOrRemote)
		if err != nil {
			slog.Error("DeduceProductOfferingOwner: retrieving organization object", "href", ownerOrgID, "productSpecification", productSpecification.ID, slogor.Err(err))
			// We do not stop the loop. We may have other relatedParty objects
			// with the owner information we need.
			continue
		}

		// Inside Organization, the array externalReference contains the ID of the organization
		ownerReference, ok := organizationObject.ContentAsMap["externalReference"].([]any)
		if !ok {
			slog.Error("DeduceProductOfferingOwner: externalReference is nil or not a list", "productSpecification", productSpecification.ID)
			continue
		}

		// The externalReference array must contain an entry with a map named "externalReferenceType"
		// where one of the keys is "idm_id".
		// We look at all entries in the array to find the one with "externalReferenceType" = "idm_id"
		for _, extRef := range ownerReference {
			extRefMap, ok := extRef.(map[string]any)
			if !ok {
				continue
			}
			externalReferenceType, ok := extRefMap["externalReferenceType"].(string)
			if !ok {
				continue
			}

			if strings.ToLower(externalReferenceType) == "idm_id" {

				oid, ok := extRefMap["name"].(string)
				if !ok {
					slog.Error("DeduceProductOfferingOwner: externalReference 'name' is nil or not a string", "productSpecification", productSpecification.ID)
					continue
				}
				organization := organizationObject.ID

				// Now that we have the owner, update the local database for the Organization object
				if len(organizationObject.OrganizationIdentifier) == 0 || len(organizationObject.Organization) == 0 {
					organizationObject, _ = organizationObject.SetOwner(oid, organization)
					err := tmf.LocalUpsertTMFObject(dbconn, organizationObject)
					if err != nil {
						return "", "", fmt.Errorf("DeduceProductOfferingOwner: %w", err)
					}
				}

				// And do the same with the ProductSpecification object
				if len(productSpecification.OrganizationIdentifier) == 0 || len(productSpecification.Organization) == 0 {
					productSpecification, _ = productSpecification.SetOwner(oid, organization)
					err := tmf.LocalUpsertTMFObject(dbconn, productSpecification)
					if err != nil {
						return "", "", fmt.Errorf("DeduceProductOfferingOwner: %w", err)
					}
				}

				return oid, organization, nil
			}
		}

	}

	return "", "", fmt.Errorf("relatedParty is nil")
}

func (tmf *TMFCache) getRelatedPartyOwner(oMap map[string]any) (string, string, error) {

	// Check if there is a RelatedParty object
	relatedPartyList := oMap["relatedParty"].([]any)
	if relatedPartyList == nil {
		slog.Info("relatedParty is nil")
		return "", "", nil
	}

	// The RelatedParty must be like this:
	// "relatedParty": [
	// 	    {
	// 			"id": "urn:ngsi-ld:organization:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
	// 			"href": "urn:ngsi-ld:organization:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
	// 			"role": "Owner",
	// 			"name": "VATAT-U16848106",
	// 			"@referredType": ""
	// 		}
	// ]

	// One of the relatedParty items must be the one defining the owner
	for _, rp := range relatedPartyList {
		rpMap := rp.(map[string]any)

		// If there is an "owner" role, we can get the owner directly
		if strings.ToLower(rpMap["role"].(string)) == "owner" {

			oid, _ := rpMap["name"].(string)
			organization, _ := rpMap["id"].(string)
			if oid != "" && organization != "" {
				return oid, organization, nil
			}
		}
	}

	return "", "", nil
}

// RetrieveOrUpdateObject retrieves an object from the local database or from the server if it is not in the local database.
// The function returns the object and a boolean indicating if the object was retrieved from the local database.
func (tmf *TMFCache) RetrieveOrUpdateObject(
	dbconn *sqlite.Conn,
	id string,
	organizationId string,
	organization string,
	location AccessType,
) (localTmfObj *TMFObject, local bool, err error) {
	slog.Debug("RetrieveOrUpdateObject", "href", id, "organizationid", organizationId, "organization", organization, "location", location.String())

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, fmt.Errorf("RetrieveOrUpdateObject: taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Check if the object is already in the local database
	localTmfObj, found, err := tmf.LocalRetrieveTMFObject(dbconn, id, "")
	if err != nil {
		return nil, false, fmt.Errorf("retrieving local object: %w", err)
	}

	// Return with an error if the object was not found and caller specified 'local only search'
	if (location == OnlyLocal) && !found {
		return nil, false, fmt.Errorf("RetrieveOrUpdateObject: object not found in local database: %s", id)
	}

	// // TODO: remove this, as it is used only for diagnostics
	// if found && localTmfObj.Type == "productOfferingPrice" && localTmfObj.OrganizationIdentifier == "" {
	// 	slog.Error("no OrganizationIdentifier in retrieved object", "location", location.String(), "incoming", organizationId, "id", id)
	// }

	now := time.Now().Unix()

	// Return the local object if it was found and it is fresh enough
	if found && (int(now-localTmfObj.Updated) < tmf.Maxfreshness) {
		if localTmfObj.OrganizationIdentifier == "" && organizationId != "" {
			// Special case: we found the object, it does not have the organizationIdentifier, but the caller provides one.
			// We just replace the object, setting the organizationIdentifier to what the caller specifies.

			// Set the owner id
			localTmfObj, err = localTmfObj.SetOwner(organizationId, organization)
			if err != nil {
				return nil, false, err
			}

			// Update the object in the local database
			err = tmf.LocalUpsertTMFObject(dbconn, localTmfObj)
			if err != nil {
				return nil, false, err
			}

		}

		return localTmfObj, true, nil
	}

	//
	// In any other case we have to retrieve the object from the server
	//

	// Update some statistics counter
	if found && (int(now-localTmfObj.Updated) >= tmf.Maxfreshness) {
		// Update stats counter
		tmf.RefreshCounter++
	}

	// Save the organizationIdentifier if the object was found
	if found && localTmfObj.OrganizationIdentifier != "" {
		organizationId = localTmfObj.OrganizationIdentifier
	}

	// Get the object from the server
	remotepo, err := tmf.RemoteRetrieveTMFObject(id)
	if err != nil {
		return nil, false, err
	}

	// Set the owner id, because remote objects may not have it
	remotepo, err = remotepo.SetOwner(organizationId, organization)
	if err != nil {
		return nil, false, err
	}

	// Update the object in the local database
	err = tmf.LocalUpsertTMFObject(dbconn, remotepo)
	if err != nil {
		return nil, false, err
	}

	return remotepo, false, nil
}

// visitMap visits recursively the descendants of an object (representaed as a map).
// It handles cicles to avoid infinite loops.
func (tmf *TMFCache) visitMap(
	dbconn *sqlite.Conn,
	currentObject map[string]any,
	oid string,
	organization string,
	indent int,
	visitedObjects map[string]bool,
) {

	// A map object can contain an 'href' field that points to another object.
	// In this case we retrieve and visit the object, if it was not retrieved before.
	// For other map objects we print the relevant fields if enabled by the tmf.Dump variable.
	if currentObject["href"] != nil {
		href := currentObject["href"].(string)
		if tmf.Dump {
			fmt.Printf("%shref: %v\n", indentStr(indent), href)
		}
		if !visitedObjects[href] {
			visitedObjects[href] = true
			remoteObj, _, err := tmf.RetrieveOrUpdateObject(dbconn, href, oid, organization, LocalOrRemote)
			if err != nil {
				slog.Error(err.Error())
			} else {
				tmf.visitMap(dbconn, remoteObj.ContentAsMap, oid, organization, indent+3, visitedObjects)
			}
		}
	}

	for k, v := range currentObject {
		switch v := v.(type) {

		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%s:\n", indentStr(indent), k)
			}
			tmf.visitMap(dbconn, v, oid, organization, indent+3, visitedObjects)

		case []any:
			if tmf.Dump {
				fmt.Printf("%s%s: [\n", indentStr(indent), k)
			}
			tmf.visitArray(dbconn, v, oid, organization, indent+3, visitedObjects)
			if tmf.Dump {
				fmt.Printf("%s]\n", indentStr(indent))
			}

		}
	}
}

// visitArray is the complement to visitMap for recursive traversal of a TMForum object graph
func (tmf *TMFCache) visitArray(dbconn *sqlite.Conn, arr []any, oid string, organization string, indent int, visitedObjects map[string]bool) {
	for i, v := range arr {
		switch v := v.(type) {
		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			tmf.visitMap(dbconn, v, oid, organization, indent+3, visitedObjects)
		case []any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			tmf.visitArray(dbconn, v, oid, organization, indent+3, visitedObjects)
		}
	}
}

// RemoteRetrieveTMFObject retrieves a TMF object from the DOME server.
//
// In DOME the href parameter is also the ID of the object which has to be used in the URL of the object to
// retrieve it from the server.
// The id parameter also has embedded the type of the object in the form of urn:ngsi-ld:<type>:<id>
func (tmf *TMFCache) RemoteRetrieveTMFObject(id string) (*TMFObject, error) {

	// Parse the id to get the type of the object
	resourceName, err := config.FromIdToResourceName(id)
	if err != nil {
		return nil, fmt.Errorf("RetrieveRemoteTMFObject: %w", err)
	}

	hostAndPath, err := tmf.config.GetHostAndPathFromResourcename(resourceName)
	if err != nil {
		slog.Error("retrieving host and path for resource", "resourceName", resourceName, slogor.Err(err))
		return nil, fmt.Errorf("retrieving host and path for resource %s: %w", resourceName, err)
	}

	// Get the object from the server
	url := hostAndPath + "/" + id
	res, err := http.Get(url)
	if err != nil {
		slog.Error("retrieving remote", "object", url, slogor.Err(err))
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		return nil, fmt.Errorf("retrieving %s, status code: %d and\nbody: %s", url, res.StatusCode, body)
	}
	if err != nil {
		slog.Error("retrieving remote", "object", url, slogor.Err(err))
		return nil, fmt.Errorf("retrieving %s: %w", url, err)
	}

	// Check if it looks like a JSON object
	if body[0] != '{' && body[0] != '[' {
		slog.Error("reply does not look as a JSON object", "url", url)
		return nil, fmt.Errorf("reply does not look as a JSON object from %s", url)
	}

	// Create the in-memory object
	po, err := TMFObjectFromBytes(body)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return po, nil
}

var tmfIDtoType = map[string]string{
	"organization":           "organization",
	"individual":             "individual",
	"category":               "category",
	"catalog":                "catalog",
	"product-offering":       "productOffering",
	"product-specification":  "productSpecification",
	"product-offering-price": "productOfferingPrice",
	"service-specification":  "serviceSpecification",
	"resource-specification": "resourceSpecification",
}

var baePathPrefixForResourceName = map[string]string{
	"organization":          "/party/organization/",
	"category":              "/catalog/category/",
	"catalog":               "/catalog/catalog/",
	"productOffering":       "/catalog/productOffering/",
	"productSpecification":  "/catalog/productSpecification/",
	"productOfferingPrice":  "/catalog/productOfferingPrice/",
	"serviceSpecification":  "/service/serviceSpecification/",
	"resourceSpecification": "/resource/resourceSpecification/",
}

// func TMFObjectIDtoResourceName(id string) (string, error) {
// 	const prefix = "urn:ngsi-ld:"
// 	if !strings.HasPrefix(id, prefix) {
// 		return "", fmt.Errorf("invalid ID format: %s", id)
// 	}

// 	parts := strings.Split(id, ":")
// 	if len(parts) < 3 {
// 		return "", fmt.Errorf("invalid ID format: %s", id)
// 	}

// 	tmfType := tmfIDtoType[parts[2]]
// 	if tmfType == "" {
// 		return "", fmt.Errorf("unknown TMF type: %s", parts[2])
// 	}
// 	return tmfType, nil
// }

// This is for the new definitions
var ResourceToManagementSystem = map[string]string{
	"agreement":                  "agreementManagement",
	"agreementSpecification":     "agreementManagement",
	"appliedCustomerBillingRate": "customerBillManagement",
	"billFormat":                 "accountManagement",
	"billPresentationMedia":      "accountManagement",
	"billingAccount":             "accountManagement",
	"billingCycleSpecification":  "accountManagement",
	"cancelProductOrder":         "productOrderingManagement",
	"catalog":                    "productCatalogManagement",
	"category":                   "productCatalogManagement",
	"customer":                   "customerManagement",
	"customerBill":               "customerBillManagement",
	"customerBillOnDemand":       "customerBillManagement",
	"financialAccount":           "accountManagement",
	"heal":                       "resourceFunctionActivation",
	"individual":                 "party",
	"migrate":                    "resourceFunctionActivation",
	"monitor":                    "resourceFunctionActivation",
	"organization":               "party",
	"partyAccount":               "accountManagement",
	"partyRole":                  "partyRoleManagement",
	"product":                    "productInventory",
	"productOffering":            "productCatalogManagement",
	"productOfferingPrice":       "productCatalogManagement",
	"productOrder":               "productOrderingManagement",
	"productSpecification":       "productCatalogManagement",
	"quote":                      "quoteManagement",
	"resource":                   "resourceInventoryManagement",
	"resourceCandidate":          "resourceCatalog",
	"resourceCatalog":            "resourceCatalog",
	"resourceCategory":           "resourceCatalog",
	"resourceFunction":           "resourceFunctionActivation",
	"resourceSpecification":      "resourceCatalog",
	"scale":                      "resourceFunctionActivation",
	"service":                    "serviceInventory",
	"serviceCandidate":           "serviceCatalogManagement",
	"serviceCatalog":             "serviceCatalogManagement",
	"serviceCategory":            "serviceCatalogManagement",
	"serviceSpecification":       "serviceCatalogManagement",
	"settlementAccount":          "accountManagement",
	"usage":                      "usageManagement",
	"usageSpecification":         "usageManagement",
}

// func (tmf *TMFCache) CloneRemoteCatalogues() ([]*TMFObject, map[string]bool, error) {
// 	tmf.cloneMutex.Lock()
// 	defer tmf.cloneMutex.Unlock()

// 	visitedObjects := make(map[string]bool)

// 	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
// 	limit := 10
// 	offset := 0

// 	// We are only interested in ProductOfferings which are launched or active
// 	// to the market (lifecycleStatus=Launched,Active)
// 	lifecycleStatus := "Launched,Active"

// 	var poList []*TMFObject

// 	for {

// 		// Get the list of catalogues from the DOME server
// 		url := fmt.Sprintf("%s/catalog/catalog?limit=%d&offset=%d&lifecycleStatus=%s", tmf.domeServer, limit, offset, lifecycleStatus)
// 		res, err := http.Get(url)
// 		if err != nil {
// 			slog.Error(err.Error())
// 			return nil, nil, err
// 		}
// 		body, err := io.ReadAll(res.Body)
// 		res.Body.Close()
// 		if res.StatusCode > 299 {
// 			slog.Error("Response failed", "status", res.StatusCode, "body", body)
// 			return nil, nil, err
// 		}
// 		if err != nil {
// 			slog.Error("reading response body", slogor.Err(err))
// 			return nil, nil, err
// 		}

// 		// Parse the JSON response
// 		var poListMap []map[string]any
// 		err = json.Unmarshal(body, &poListMap)
// 		if err != nil {
// 			slog.Error("parsing JSOn response", slogor.Err(err))
// 			return nil, nil, err
// 		}

// 		// Check if we should termninate the loop because there are no more objects
// 		if len(poListMap) == 0 {
// 			break
// 		}

// 		// Process the list
// 		for _, oMap := range poListMap {

// 			po, err := NewTMFObject(oMap, nil)
// 			if err != nil {
// 				slog.Error("creating NewTMFObject", slogor.Err(err))
// 				continue
// 			}

// 			// There must be a relatedParty object
// 			relatedPartyList, ok := po.ContentAsMap["relatedParty"].([]any)
// 			if !ok {
// 				slog.Error("invalid relatedParty object")
// 				out, _ := json.MarshalIndent(oMap, "", "   ")
// 				fmt.Println(string(out))
// 				continue
// 			}

// 			if relatedPartyList == nil {
// 				slog.Error("relatedParty is nil")
// 				return nil, nil, fmt.Errorf("relatedParty is nil")
// 			}

// 			// One of the relatedParty items must be the one defining the owner
// 			for _, rp := range relatedPartyList {
// 				rpMap := rp.(map[string]any)
// 				if strings.ToLower(rpMap["role"].(string)) == "owner" {
// 					owner, _, err := tmf.RetrieveOrUpdateObject(nil, rpMap["href"].(string), "", "", LocalOrRemote)
// 					if err != nil {
// 						slog.Error(err.Error())
// 						return nil, nil, err
// 					}

// 					// The array 'externalReference' contains the ID of the organization
// 					ownerReference := owner.ContentAsMap["externalReference"].([]any)
// 					if ownerReference == nil {
// 						slog.Info("externalReference is nil")
// 						return nil, nil, fmt.Errorf("externalReference is nil")
// 					}

// 					for _, extRef := range ownerReference {
// 						extRefMap := extRef.(map[string]any)
// 						if extRefMap["externalReferenceType"] == "idm_id" {
// 							oid := extRefMap["name"].(string)
// 							organization := owner.ID

// 							// Now that we have the owner, update the local database for the productSpecification object
// 							if len(owner.OrganizationIdentifier) == 0 {
// 								owner, _ = owner.SetOwner(oid, organization)
// 								err := tmf.LocalUpsertTMFObject(nil, owner)
// 								if err != nil {
// 									slog.Error(err.Error())
// 									return nil, nil, err
// 								}
// 							}
// 							if len(po.OrganizationIdentifier) == 0 {
// 								po, _ = po.SetOwner(oid, organization)
// 								err := tmf.LocalUpsertTMFObject(nil, po)
// 								if err != nil {
// 									slog.Error(err.Error())
// 									return nil, nil, err
// 								}
// 							}

// 							poList = append(poList, po)
// 							break
// 						}
// 					}

// 				}
// 			}

// 			visitedObjects[po.ID] = true

// 		}

// 		// Go and retrieve the next chunk of objects
// 		offset = offset + limit

// 	}

// 	return poList, visitedObjects, nil

// }
