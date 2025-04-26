// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

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

// TMFdb is a struct that holds a pool of connections to the database and the URL of the DOME server.
//
// The database connection is a pool of connections that is shared by all the requests in this object.
// The connection is returned to the pool when the object is closed.
// This object should be used by only one goroutine. Otherwise, multiple objects can be used concurrently
type TMFdb struct {
	dbpool         *sqlitex.Pool
	domeServer     string
	Maxfreshness   int
	RefreshCounter int
	Dump           bool
	cloneMutex     sync.Mutex
	httpClient     *http.Client
}

var ErrorRedirectsNotAllowed = errors.New("redirects not allowed")

func NewTMFdb(config *Config) (*TMFdb, error) {
	var err error

	tmf := &TMFdb{
		domeServer:   config.DomeServer,
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
	tmf.httpClient = &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return ErrorRedirectsNotAllowed
		},
	}

	return tmf, nil
}

func createTables(dbpool *sqlitex.Pool) error {

	// Get a connection from the pool for all the requests.
	// The connection is returned to the pool when the object is closed
	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return err
	}
	defer dbpool.Put(conn)

	// Create the table if it does not exist
	if err := sqlitex.ExecuteScript(conn, createTMFTableSQL, nil); err != nil {
		slog.Error("createTables", slogor.Err(err))
		return fmt.Errorf("createTables: %w", err)
	}

	return nil
}

func (tmf *TMFdb) Server() string {
	return tmf.domeServer
}

func (tmf *TMFdb) RequestDB(dbconn *sqlite.Conn) (*sqlite.Conn, error) {
	if dbconn != nil {
		return dbconn, nil
	}

	var err error
	dbconn, err = tmf.dbpool.Take(context.Background())
	if err != nil {
		return nil, err
	}
	return dbconn, nil
}

func (tmf *TMFdb) ReleaseDB(dbconn *sqlite.Conn) {
	tmf.dbpool.Put(dbconn)
}

func (tmf *TMFdb) Close() {
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
func (tmf *TMFdb) CloneRemoteProductOfferings() ([]*TMFObject, map[string]bool, error) {

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

		// Get the list of product offerings from the DOME server
		// TODO: make the base path configurable (or maybe not, given we are DOME-specific already??)
		url := fmt.Sprintf("%s/catalog/productOffering?limit=%d&offset=%d&lifecycleStatus=%s", tmf.domeServer, limit, offset, lifecycleStatus)
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
func (tmf *TMFdb) CloneOneProductOffering(oMap map[string]any, indent int, visitedObjects map[string]bool) (*TMFObject, error) {

	// Wrap all db operations in a SQLite Savepoint (which is a nestable transaction)
	doWork := func() (po *TMFObject, err error) {

		dbconn, err := tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, err
		}
		defer tmf.dbpool.Put(dbconn)

		// Start a SAVEPOINT and defer its Commit/Rollback
		release := sqlitex.Save(dbconn)
		defer release(&err)

		// Create the ProductOffering object with whatever info we have now. We will then retrieve the owner info,
		// update it and save it in the local database.
		po, err = NewTMFObject(oMap, nil)
		if err != nil {
			slog.Error("creating NewTMFObject", slogor.Err(err))
			return nil, err
		}

		// At this moment, the ProductOffering object in DOME does not have
		// the identification of the owner organization.
		// We need that info to enable access control at the object level, so we retrieve
		// the owner information indirectly by retrieving the ProductSpecification associated
		// to the ProductOffering, and getting the relevant information from the RelatedParty
		// object associated to the ProductSpecification.
		oid, organization, err := tmf.GetProductOfferingOwner(dbconn, oMap)
		if err != nil {
			// We do not stop processing in case of error, just log it.
			slog.Error(err.Error())
		} else {
			// Update our ProductOffering with the owner information retrieved
			po.OrganizationIdentifier = oid
			oMap["organizationIdentifier"] = oid
			po.Organization = organization
			oMap["organization"] = organization
			po.ContentMap = oMap
		}

		// Update or Insert the ProductOffering in our database
		if err := tmf.UpsertTMFObject(dbconn, po); err != nil {
			slog.Error(err.Error())
			return nil, err
		}

		visitedObjects[po.ID] = true

		// Recursively retrieve and save the sub-objects of this ProductOffering.
		// We pass the owner information so those objects can include it with them.
		tmf.visitMap(dbconn, oMap, po.OrganizationIdentifier, po.Organization, indent+3, visitedObjects)

		return po, nil
	}

	// This actually executes the Savepoint
	po, err := doWork()
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return po, nil

}

func (tmf *TMFdb) CloneRemoteCatalogues() ([]*TMFObject, map[string]bool, error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	visitedObjects := make(map[string]bool)

	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
	limit := 10
	offset := 0

	// We are only interested in ProductOfferings which are launched or active
	// to the market (lifecycleStatus=Launched,Active)
	lifecycleStatus := "Launched,Active"

	var poList []*TMFObject

	for {

		// Get the list of catalogues from the DOME server
		url := fmt.Sprintf("%s/catalog/catalog?limit=%d&offset=%d&lifecycleStatus=%s", tmf.domeServer, limit, offset, lifecycleStatus)
		res, err := http.Get(url)
		if err != nil {
			slog.Error(err.Error())
			return nil, nil, err
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode > 299 {
			slog.Error("Response failed", "status", res.StatusCode, "body", body)
			return nil, nil, err
		}
		if err != nil {
			slog.Error("reading response body", slogor.Err(err))
			return nil, nil, err
		}

		// Parse the JSON response
		var poListMap []map[string]any
		err = json.Unmarshal(body, &poListMap)
		if err != nil {
			slog.Error("parsing JSOn response", slogor.Err(err))
			return nil, nil, err
		}

		// Check if we should termninate the loop because there are no more objects
		if len(poListMap) == 0 {
			break
		}

		// Process the list
		for _, oMap := range poListMap {

			po, err := NewTMFObject(oMap, nil)
			if err != nil {
				slog.Error("creating NewTMFObject", slogor.Err(err))
				continue
			}

			// There must be a relatedParty object
			relatedPartyList, ok := po.ContentMap["relatedParty"].([]any)
			if !ok {
				slog.Error("invalid relatedParty object")
				out, _ := json.MarshalIndent(oMap, "", "   ")
				fmt.Println(string(out))
				continue
			}

			if relatedPartyList == nil {
				slog.Error("relatedParty is nil")
				return nil, nil, fmt.Errorf("relatedParty is nil")
			}

			// One of the relatedParty items must be the one defining the owner
			for _, rp := range relatedPartyList {
				rpMap := rp.(map[string]any)
				if strings.ToLower(rpMap["role"].(string)) == "owner" {
					owner, _, err := tmf.RetrieveOrUpdateObject(nil, rpMap["href"].(string), "", "", LocalOrRemote)
					if err != nil {
						slog.Error(err.Error())
						return nil, nil, err
					}

					// The array 'externalReference' contains the ID of the organization
					ownerReference := owner.ContentMap["externalReference"].([]any)
					if ownerReference == nil {
						slog.Info("externalReference is nil")
						return nil, nil, fmt.Errorf("externalReference is nil")
					}

					for _, extRef := range ownerReference {
						extRefMap := extRef.(map[string]any)
						if extRefMap["externalReferenceType"] == "idm_id" {
							oid := extRefMap["name"].(string)
							organization := owner.ID

							// Now that we have the owner, update the local database for the productSpecification object
							if len(owner.OrganizationIdentifier) == 0 {
								owner, _ = owner.SetOwner(oid, organization)
								err := tmf.UpsertTMFObject(nil, owner)
								if err != nil {
									slog.Error(err.Error())
									return nil, nil, err
								}
							}
							if len(po.OrganizationIdentifier) == 0 {
								po, _ = po.SetOwner(oid, organization)
								err := tmf.UpsertTMFObject(nil, po)
								if err != nil {
									slog.Error(err.Error())
									return nil, nil, err
								}
							}

							poList = append(poList, po)
							break
						}
					}

				}
			}

			visitedObjects[po.ID] = true

		}

		// Go and retrieve the next chunk of objects
		offset = offset + limit

	}

	return poList, visitedObjects, nil

}

// GetProductOfferingOwner retrieves the owner of a product offering.
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
func (tmf *TMFdb) GetProductOfferingOwner(dbconn *sqlite.Conn, productOfferingMap map[string]any) (oid string, organization string, err error) {
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
		slog.Warn("productSpecification is nil or not a map", "productOffering id", productOfferingMap["id"])
		return "", "", fmt.Errorf("productSpecification is nil or not a map")
	}

	if len(psMap) == 0 {
		slog.Info("productSpecification is empty")
		return "", "", fmt.Errorf("productSpecification is empty")
	}

	// Get the href to retrieve the remote associated object
	href, ok := psMap["href"].(string)
	if !ok {
		slog.Warn("productSpecification 'href' is nil or not a string", "productOffering id", productOfferingMap["id"])

		// Try with the ID, as they are equal
		href, ok = psMap["id"].(string)
		if !ok {
			slog.Warn("productSpecification 'id' is nil or not a string", "productOffering id", productOfferingMap["id"])
			return "", "", fmt.Errorf("href is nil or not a string")
		}
	}

	if href == "" {
		return "", "", fmt.Errorf("href is nil or not a string")
	}

	// Use the 'href' field to retrieve the productSpecification object from the server
	// After the call, the productSpecification object is already persisted locally with the owner information in the
	// standard TMF format. We need to update the database in the format we need for efficient SQL queries.
	productSpecification, _, err := tmf.RetrieveOrUpdateObject(dbconn, href, "", "", LocalOrRemote)
	if err != nil {
		slog.Error(err.Error())
		return "", "", err
	}

	// There must be a relatedParty object
	relatedPartyList := productSpecification.ContentMap["relatedParty"].([]any)
	if relatedPartyList == nil {
		slog.Info("relatedParty is nil")
		return "", "", fmt.Errorf("relatedParty is nil")
	}

	// One of the relatedParty items must be the one defining the owner
	for _, rp := range relatedPartyList {
		rpMap := rp.(map[string]any)

		// We look for an entry "role" = "owner", in a case-insensitive way
		if strings.ToLower(rpMap["role"].(string)) == "owner" {

			// If "owner" entry found, use 'href' to retrieve the referenced object from the remote server.
			// 'href' points to an Organization TMF object
			organizationObject, _, err := tmf.RetrieveOrUpdateObject(dbconn, rpMap["href"].(string), "", "", LocalOrRemote)
			if err != nil {
				slog.Error(err.Error())
				return "", "", err
			}

			// Inside Organization, the array externalReference contains the ID of the organization
			ownerReference := organizationObject.ContentMap["externalReference"].([]any)
			if ownerReference == nil {
				slog.Info("externalReference is nil")
				return "", "", fmt.Errorf("externalReference is nil")
			}

			// The externalReference array must contain an entry with a map named "externalReferenceType"
			// where one of the keys is "idm_id".
			// We look at all entries in the array to find the one with "externalReferenceType" = "idm_id"
			for _, extRef := range ownerReference {
				extRefMap := extRef.(map[string]any)
				if extRefMap["externalReferenceType"] == "idm_id" {
					oid := extRefMap["name"].(string)
					organization := organizationObject.ID

					// Now that we have the owner, update the local database for the Organization object
					if len(organizationObject.OrganizationIdentifier) == 0 || len(organizationObject.Organization) == 0 {
						organizationObject, _ = organizationObject.SetOwner(oid, organization)
						err := tmf.UpsertTMFObject(dbconn, organizationObject)
						if err != nil {
							slog.Error(err.Error())
							return "", "", err
						}
					}

					// And do the same with the ProductSpecification object
					if len(productSpecification.OrganizationIdentifier) == 0 || len(productSpecification.Organization) == 0 {
						productSpecification, _ = productSpecification.SetOwner(oid, organization)
						err := tmf.UpsertTMFObject(dbconn, productSpecification)
						if err != nil {
							slog.Error(err.Error())
							return "", "", err
						}
					}

					return oid, organization, nil
				}
			}

		}
	}

	return "", "", fmt.Errorf("relatedParty is nil")
}

func (tmf *TMFdb) getRelatedPartyOwner(oMap map[string]any) (string, string, error) {

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
func (tmf *TMFdb) RetrieveOrUpdateObject(
	dbconn *sqlite.Conn,
	href string,
	oid string,
	organization string,
	location AccessType,
) (localpo *TMFObject, local bool, err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Check if the object is already in the local database
	localpo, found, err := tmf.RetrieveLocalTMFObject(dbconn, href, "")
	if err != nil {
		return nil, false, fmt.Errorf("retrieving local object: %w", err)
	}

	// Return with an error if the object was not found and caller specified 'local only search'
	if !found && (location == OnlyLocal) {
		return nil, false, fmt.Errorf("object not found in local database: %s", href)
	}

	// TODO: remove this, as it is used only for diagnostics
	if found && localpo.Type == "productOfferingPrice" && localpo.OrganizationIdentifier == "" {
		slog.Error("no OrganizationIdentifier in retrieved object", "location", location.String(), "incoming", oid, "id", href)
	}

	now := time.Now().Unix()

	// Return the local object if it was found and it is fresh enough
	if found && (int(now-localpo.Updated) < tmf.Maxfreshness) {
		if localpo.OrganizationIdentifier == "" && oid != "" {
			// Special case: we found the object, it does not have the organizationIdentifier, but the caller provides one.
			// We just replace the object, setting the organizationIdentifier to what the caller specifies.

			// Set the owner id
			localpo, err = localpo.SetOwner(oid, organization)
			if err != nil {
				return nil, false, err
			}

			// Update the object in the local database
			err = tmf.UpsertTMFObject(dbconn, localpo)
			if err != nil {
				return nil, false, err
			}

		}

		return localpo, true, nil
	}

	//
	// In any other case we have to retrieve the object from the server
	//

	// Update some statistics counter
	if found && (int(now-localpo.Updated) >= tmf.Maxfreshness) {
		// Update stats counter
		tmf.RefreshCounter++
	}

	// Save the organizationIdentifier if the object was found
	if found && localpo.OrganizationIdentifier != "" {
		oid = localpo.OrganizationIdentifier
	}

	// Get the object from the server
	remotepo, err := tmf.RetrieveRemoteTMFObject(href)
	if err != nil {
		return nil, false, err
	}

	// Set the owner id, because remote objects may not have it
	remotepo, err = remotepo.SetOwner(oid, organization)
	if err != nil {
		return nil, false, err
	}

	// Update the object in the local database
	err = tmf.UpsertTMFObject(dbconn, remotepo)
	if err != nil {
		return nil, false, err
	}

	return remotepo, false, nil
}

// visitMap visits recursively the descendants of an objec (representaed as a map).
// It handles cicles to avoid infinite loops.
func (tmf *TMFdb) visitMap(
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
				tmf.visitMap(dbconn, remoteObj.ContentMap, oid, organization, indent+3, visitedObjects)
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
func (tmf *TMFdb) visitArray(dbconn *sqlite.Conn, arr []any, oid string, organization string, indent int, visitedObjects map[string]bool) {
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

// RetrieveRemoteTMFObject retrieves a TMF object from the DOME server.
//
// In DOME the href parameter is also the ID of the object which has to be used in the URL of the object to
// retrieve it from the server.
// The href parameter also has embedded the type of the object in the form of urn:ngsi-ld:<type>:<id>
func (tmf *TMFdb) RetrieveRemoteTMFObject(href string) (*TMFObject, error) {

	// Parse the href to get the type of the object
	objectType, err := TMFObjectIDtoType(href)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// Each type of object has a different path prefix
	pathPrefix := pathPrefixForType[objectType]
	if pathPrefix == "" {
		slog.Error("unknown object type", "type", objectType)
		return nil, fmt.Errorf("unknown object type: %s", objectType)
	}

	// Get the object from the server
	res, err := http.Get(tmf.domeServer + pathPrefix + href)
	if err != nil {
		slog.Error("retrieving remote", "object", pathPrefix+href, slogor.Err(err))
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		return nil, fmt.Errorf("retrieving %s, status code: %d and\nbody: %s", href, res.StatusCode, body)
	}
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// Convert the JSON response to a map
	var oMap map[string]any
	err = json.Unmarshal(body, &oMap)
	if err != nil {
		slog.Error(err.Error())
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

var tmfIDtoType = map[string]string{
	"organization":           "organization",
	"category":               "category",
	"catalog":                "catalog",
	"product-offering":       "productOffering",
	"product-specification":  "productSpecification",
	"product-offering-price": "productOfferingPrice",
	"service-specification":  "serviceSpecification",
	"resource-specification": "resourceSpecification",
}

var pathPrefixForType = map[string]string{
	"organization":          "/party/organization/",
	"category":              "/catalog/category/",
	"catalog":               "/catalog/catalog/",
	"productOffering":       "/catalog/productOffering/",
	"productSpecification":  "/catalog/productSpecification/",
	"productOfferingPrice":  "/catalog/productOfferingPrice/",
	"serviceSpecification":  "/service/serviceSpecification/",
	"resourceSpecification": "/resource/resourceSpecification/",
}

func TMFObjectIDtoType(id string) (string, error) {
	const prefix = "urn:ngsi-ld:"
	if !strings.HasPrefix(id, prefix) {
		return "", fmt.Errorf("invalid ID format: %s", id)
	}

	parts := strings.Split(id, ":")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid ID format: %s", id)
	}

	tmfType := tmfIDtoType[parts[2]]
	if tmfType == "" {
		return "", fmt.Errorf("unknown TMF type: %s", parts[2])
	}
	return tmfType, nil
}
