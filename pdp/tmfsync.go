// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"context"
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
}

func New(config *Config) (*TMFdb, error) {
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
// This function is special in the sense that it retrieves a list of complete objects without their dependencies
func (tmf *TMFdb) CloneRemoteProductOfferings() ([]*TMFObject, map[string]bool, error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	visitedObjects := make(map[string]bool)

	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
	limit := 10
	offset := 0

	// We are only interested in ProductOfferings which are launched to the market (lifecycleStatus=Launched)
	lifecycleStatus := "Launched"

	var productOfferings []*TMFObject

	for {

		// Get the list of product offerings from the DOME server
		url := fmt.Sprintf("%s/catalog/productOffering?limit=%d&offset=%d&lifecycleStatus=%s", tmf.domeServer, limit, offset, lifecycleStatus)
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
		var poListAsMap []map[string]any
		err = json.Unmarshal(body, &poListAsMap)
		if err != nil {
			slog.Error("parsing JSON response", slogor.Err(err))
			return nil, nil, err
		}

		// Check if we should termninate the loop because there are no more objects
		if len(poListAsMap) == 0 {
			break
		}

		indent := 0

		// Process the list of product offerings
		for _, oMap := range poListAsMap {

			po, err := tmf.CloneRemoteProductOffering(oMap, indent, visitedObjects)
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

func (tmf *TMFdb) CloneRemoteProductOffering(oMap map[string]any, indent int, visitedObjects map[string]bool) (*TMFObject, error) {

	// Wrap all db operations in a SQLite Savepoint (which is a nestable transaction)
	doWork := func() (po *TMFObject, err error) {

		dbconn, err := tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, err
		}
		defer func() {
			tmf.dbpool.Put(dbconn)
		}()

		// Start the Savepoint and defer its Commint/Rollback
		release := sqlitex.Save(dbconn)
		defer func() {
			release(&err)
		}()

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
		owner, err := tmf.GetProductOfferingOwner(dbconn, oMap)
		if err != nil {
			// We do not stop processing in case of error, just log it.
			slog.Error(err.Error())
		} else {
			// Update our ProductOffering with the owner information retrieved
			po.OrganizationIdentifier = owner.OrganizationIdentifier
			oMap["organizationIdentifier"] = owner.OrganizationIdentifier
			po.Organization = owner.Organization
			oMap["organization"] = owner.Organization
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

func (tmf *TMFdb) GetProductOfferingOwner(dbconn *sqlite.Conn, productOfferingMap map[string]any) (*TMFObject, error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, err
		}
		defer func() {
			tmf.dbpool.Put(dbconn)
		}()
	}

	// Get the productSpecification object
	psMap := productOfferingMap["productSpecification"].(map[string]any)
	if psMap == nil {
		slog.Info("productSpecification is nil")
		return nil, fmt.Errorf("productSpecification is nil")
	}

	ps, _, err := tmf.RetrieveOrUpdateObject(dbconn, psMap["href"].(string), "", "", LocalOrRemote)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// There must be a relatedParty object
	relatedPartyList := ps.ContentMap["relatedParty"].([]any)
	if relatedPartyList == nil {
		slog.Info("relatedParty is nil")
		return nil, fmt.Errorf("relatedParty is nil")
	}

	// One of the relatedParty items must be the one defining the owner
	for _, rp := range relatedPartyList {
		rpMap := rp.(map[string]any)
		if strings.ToLower(rpMap["role"].(string)) == "owner" {
			owner, _, err := tmf.RetrieveOrUpdateObject(dbconn, rpMap["href"].(string), "", "", LocalOrRemote)
			if err != nil {
				slog.Error(err.Error())
				return nil, err
			}

			// The array externalReference contains the ID of the organization
			ownerReference := owner.ContentMap["externalReference"].([]any)
			if ownerReference == nil {
				slog.Info("externalReference is nil")
				return nil, fmt.Errorf("externalReference is nil")
			}

			for _, extRef := range ownerReference {
				extRefMap := extRef.(map[string]any)
				if extRefMap["externalReferenceType"] == "idm_id" {
					oid := extRefMap["name"].(string)
					organization := owner.ID

					// Now that we have the owner, update the local database for the productSpecification object
					if len(owner.OrganizationIdentifier) == 0 || len(owner.Organization) == 0 {
						owner, _ = owner.SetOwner(oid, organization)
						err := tmf.UpsertTMFObject(dbconn, owner)
						if err != nil {
							slog.Error(err.Error())
							return nil, err
						}
					}
					if len(ps.OrganizationIdentifier) == 0 || len(ps.Organization) == 0 {
						ps, _ = ps.SetOwner(oid, organization)
						err := tmf.UpsertTMFObject(dbconn, ps)
						if err != nil {
							slog.Error(err.Error())
							return nil, err
						}
					}

					return owner, nil
				}
			}

		}
	}

	return nil, fmt.Errorf("relatedParty is nil")
}

// RetrieveOrUpdateObject retrieves an object from the local database or from the server if it is not in the local database.
// The function returns the object and a boolean indicating if the object was retrieved from the local database.
func (tmf *TMFdb) RetrieveOrUpdateObject(dbconn *sqlite.Conn, href string, oid string, organization string, location AccessType) (localpo *TMFObject, local bool, err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, err
		}
		defer func() {
			tmf.dbpool.Put(dbconn)
		}()
	}

	// Check if the object is already in the local database
	localpo, found, err := tmf.RetrieveLocalTMFObject(dbconn, href, "")
	if err != nil {
		return nil, false, err
	}

	// We can only search in the local database, and the object was not found
	if !found && (location == OnlyLocal) {
		return nil, false, fmt.Errorf("object not found in local database: %s", href)
	}

	if found && localpo.Type == "productOfferingPrice" && localpo.OrganizationIdentifier == "" {
		slog.Error("no OrganizationIdentifier in retrieved object", "location", location.String(), "incoming", oid, "id", href)
	}

	now := time.Now().Unix()

	// The object was found in the local database and it is fresh enough, so we return it
	if found && (int(now-localpo.Updated) < tmf.Maxfreshness) {
		// Special case: we found the object, it does not have the organizationIdentifier, but the call provides one.
		// We just replace the object, setting the organizationIdentifier.
		if localpo.OrganizationIdentifier == "" && oid != "" {

			// Set the owner id
			localpo, err = localpo.SetOwner(oid, organization)
			if err != nil {
				return nil, false, err
			}

			// Insert the object in the local database
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

	// In any other case we have to retrieve the object from the server
	remotepo, err := tmf.RetrieveRemoteTMFObject(href)
	if err != nil {
		return nil, false, err
	}

	// Set the owner id, because remote objects do not have it
	remotepo, err = remotepo.SetOwner(oid, organization)
	if err != nil {
		return nil, false, err
	}

	// Insert the object in the local database
	err = tmf.UpsertTMFObject(dbconn, remotepo)
	if err != nil {
		return nil, false, err
	}

	return remotepo, false, nil
}

func (tmf *TMFdb) visitMap(dbconn *sqlite.Conn, currentObject map[string]any, oid string, organization string, indent int, visitedObjects map[string]bool) {

	// A map object can contain a 'href' field that points to another object.
	// In this case we retrieve and visit the object, if it was not retrieved before.
	// For other map objects we print the relevant fields
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

// func (tmf *TMFdb) visitObjectMapTrace(dbconn *sqlite.Conn, currentObject map[string]any, oid string, organization string, indent int) {

// 	// A map object can contain a 'href' field that points to another object.
// 	// In this case we retrieve and visit the object, if it was not retrieved before.
// 	// For other map objects we print the relevant fields
// 	if currentObject["href"] != nil {
// 		href := currentObject["href"].(string)
// 		if tmf.Dump {
// 			fmt.Printf("%shref: %v\n", indentStr(indent), href)
// 		}
// 		if !visitedObjects[href] {
// 			visitedObjects[href] = true
// 			remoteObj, _, err := tmf.RetrieveOrUpdateObject(dbconn, href, oid, organization, LocalOrRemote)
// 			if err != nil {
// 				slog.Error(err.Error())
// 			} else {
// 				tmf.visitMap(dbconn, remoteObj.ContentMap, oid, organization, indent+3)
// 			}
// 		}
// 	}

// 	for k, v := range currentObject {
// 		switch v := v.(type) {
// 		case string:
// 			switch k {
// 			case "href":
// 				// Skip the href field, as we have processed it before
// 				continue

// 			case "startDateTime", "lifecycleStatus", "version", "lastUpdate", "created", "updated", "id", "externalReference":
// 				if tmf.Dump {
// 					fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
// 				}

// 			case "role":
// 				if tmf.Dump {
// 					fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
// 				}

// 			default:
// 				if tmf.Dump {
// 					fmt.Printf("%s%s: %T\n", indentStr(indent), k, v)
// 				}
// 			}

// 		case float64:
// 			if tmf.Dump {
// 				fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
// 			}

// 		case bool:
// 			if tmf.Dump {
// 				fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
// 			}

// 		case map[string]any:
// 			if tmf.Dump {
// 				fmt.Printf("%s%s:\n", indentStr(indent), k)
// 			}
// 			tmf.visitMap(dbconn, v, oid, organization, indent+3)

// 		case []any:
// 			if tmf.Dump {
// 				fmt.Printf("%s%s: [\n", indentStr(indent), k)
// 			}
// 			tmf.visitArray(dbconn, v, oid, organization, indent+3)
// 			if tmf.Dump {
// 				fmt.Printf("%s]\n", indentStr(indent))
// 			}

// 		default:
// 			if tmf.Dump {
// 				fmt.Printf("%s%s: %T\n", indentStr(indent), k, v)
// 			}
// 		}
// 	}
// }

// func (tmf *TMFdb) visitArrayTrace(dbconn *sqlite.Conn, arr []any, oid string, organization string, indent int) {
// 	for i, v := range arr {
// 		switch v := v.(type) {
// 		case string:
// 			if v == "role" {
// 				if tmf.Dump {
// 					fmt.Printf("%s%d: %v\n", indentStr(indent), i, v)
// 				}
// 			} else {
// 				if tmf.Dump {
// 					fmt.Printf("%s%d: %T\n", indentStr(indent), i, v)
// 				}
// 			}

// 		case float64:
// 			if tmf.Dump {
// 				fmt.Printf("%s%d: %v\n", indentStr(indent), i, v)
// 			}
// 		case bool:
// 			if tmf.Dump {
// 				fmt.Printf("%s%d: %v\n", indentStr(indent), i, v)
// 			}
// 		case map[string]any:
// 			if tmf.Dump {
// 				fmt.Printf("%s%d:\n", indentStr(indent), i)
// 			}
// 			tmf.visitMap(dbconn, v, oid, organization, indent+3)
// 		case []any:
// 			if tmf.Dump {
// 				fmt.Printf("%s%d:\n", indentStr(indent), i)
// 			}
// 			tmf.visitArray(dbconn, v, oid, organization, indent+3)
// 		default:
// 			if tmf.Dump {
// 				fmt.Printf("%s%d: %T\n", indentStr(indent), i, v)
// 			}
// 		}
// 	}
// }

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
