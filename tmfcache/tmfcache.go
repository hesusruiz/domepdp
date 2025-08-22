// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/errl"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type AccessType bool

const LocalOnly AccessType = true
const LocalOrRemote AccessType = false

func (at AccessType) String() string {
	if at == LocalOnly {
		return "OnlyLocal"
	}
	return "LocalOrRemote"
}

// TMFCache is a disk-based cache of TMForum data, using SQLite as the database.
//
// The database connection is a pool of connections that is shared by all the requests in this object.
// The connection is returned to the pool when the object is closed.
// This object should be used by only one goroutine. Otherwise, multiple objects can be used concurrently
type TMFCache struct {
	dbpool           *sqlitex.Pool
	config           *config.Config
	Maxfreshness     int
	RefreshCounter   int
	Dump             bool
	MustFixInBackend FixLevel
	cloneMutex       sync.Mutex
	HttpClient       *http.Client
}

var ErrorRedirectsNotAllowed = errors.New("redirects not allowed")

func NewTMFCache(cfg *config.Config, delete bool) (*TMFCache, error) {
	var err error

	tmf := &TMFCache{
		config:       cfg,
		Maxfreshness: 60 * 60, // 1 hour
	}

	// Initialize the global pool of database connections
	if tmf.dbpool == nil {
		tmf.dbpool, err = sqlitex.NewPool(cfg.Dbname, sqlitex.PoolOptions{
			PoolSize: 10,
		})
		if err != nil {
			return nil, errl.Error(err)
		}
	}

	// Delete the tables first if requested to do so
	if delete {
		err = deleteTables(tmf.dbpool)
		if err != nil {
			return nil, errl.Error(err)
		}
	}

	// Create the tables if they do not exist
	if err := createTables(tmf.dbpool); err != nil {
		return nil, errl.Error(err)
	}

	// Create the http client to send requests to the remote TMF server
	// This instance is safe for concurrent use and will be reused for performance
	tmf.HttpClient = &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return ErrorRedirectsNotAllowed
		},
	}

	return tmf, nil
}

func (tmf *TMFCache) Config() *config.Config {
	return tmf.config
}

// UpstreamHostAndPathFromResource returns the upstream host and path associated with the given resource name.
// It delegates the lookup to the underlying configuration. If the resource is not found or an error occurs,
// an error is returned.
func (tmf *TMFCache) UpstreamHostAndPathFromResource(resourceName string) (string, error) {
	return tmf.config.UpstreamHostAndPathFromResource(resourceName)
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
func (tmf *TMFCache) CloneRemoteProductOfferings() ([]TMFObject, map[string]bool, error) {

	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	// This is to avoid loops in the object graph
	visitedObjects := make(map[string]bool)

	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
	limit := 100
	offset := 0

	// We are only interested in ProductOfferings which are launched to the market (lifecycleStatus=Launched)
	// lifecycleStatus := "Launched"

	var productOfferings []TMFObject

	for {

		// query := fmt.Sprintf("?limit=%d&offset=%d&lifecycleStatus=%s", limit, offset, lifecycleStatus)
		query := fmt.Sprintf("?limit=%d&offset=%d", limit, offset)
		hostAndPath, err := tmf.config.UpstreamHostAndPathFromResource("productOffering")
		if err != nil {
			return nil, nil, errl.Error(err)
		}

		url := hostAndPath + query

		// Get the list of product offerings from the TMForum server
		res, err := http.Get(url)
		if err != nil {
			// Log the error and stop the loop, returning whatever objects we have so far
			err = errl.Errorf("retrieving %s: %w", url, err)
			slog.Error(err.Error())
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

			err := tmfObjectSanityCheck(oMap, false)
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
// only if it needs to retrieve the owner information, or if it is not fresh in the local cache.
func (tmf *TMFCache) CloneOneProductOffering(oMap map[string]any, indent int, visitedObjects map[string]bool) (TMFObject, error) {

	dbconn, err := tmf.dbpool.Take(context.Background())
	if err != nil {
		return nil, errl.Error(err)
	}
	defer tmf.dbpool.Put(dbconn)

	// Wrap all db operations in a SQLite Savepoint (which is a nestable transaction)
	// TODO: is this necessary? We only need transactions for the individual objects
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Create the ProductOffering object with whatever info we have now. We will then retrieve the owner info,
	// update it and save it in the local database.
	po, err := TMFObjectFromMap(oMap, config.ProductOffering)
	if err != nil {
		return nil, errl.Errorf("CloneOneProductOffering: %w", err)
	}

	// At this moment, the ProductOffering object in DOME does not have
	// the identification of the owner sellerName.
	// We need that info to enable access control at the object level, so we retrieve
	// the owner information indirectly by retrieving the ProductSpecification associated
	// to the ProductOffering, and getting the relevant information from the RelatedParty
	// object associated to the ProductSpecification.
	sellerDid, sellerName, sellerHref, err := tmf.DeduceProductOfferingSeller(dbconn, po)
	if err != nil {
		return nil, errl.Errorf("CloneOneProductOffering: %w", err)
	} else {
		// Update our ProductOffering with the owner information retrieved
		po.SetOwner(sellerDid, sellerName, sellerHref)
	}

	// Update or Insert the ProductOffering in our database
	if err := tmf.LocalUpsertTMFObject(dbconn, po); err != nil {
		return nil, errl.Errorf("CloneOneProductOffering: %w", err)
	}

	visitedObjects[po.GetID()] = true

	// Recursively retrieve and save the sub-objects of this ProductOffering.
	// We pass the owner information so those objects can include it with them.
	tmf.visitMap(dbconn, oMap, sellerDid, sellerName, sellerHref, indent+3, visitedObjects)

	return po, nil

}

// CloneAllRemoteResources retrieves or updates all objects for all known resource types in DOME.
func (tmf *TMFCache) CloneAllRemoteResources() ([]TMFObject, map[string]bool, error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	visitedObjects := make(map[string]bool)

	// Use the keys of the config.GeneratedDefaultResourceToPathPrefix map to clone all the resource types in the TMF database
	for resourceName := range config.GeneratedDefaultResourceToPathPrefix {
		_, err := tmf.CloneRemoteResourceType(resourceName, visitedObjects)
		if err != nil {
			slog.Error("cloning remote resource", "resourceName", resourceName, slog.Any("err", err))
			// We continue the loop to retrieve the remaining objects
			continue
		}

	}

	return nil, visitedObjects, nil

}

// CloneAllRemoteBAEResources retrieves or updates all objects for all known resource types in DOME, but only those
// resource types which can be accessed via the BAE proxy.
// This is a temporary restriction until all DOME environments have implemented the external APIs for all objects.
func (tmf *TMFCache) CloneAllRemoteBAEResources() ([]TMFObject, map[string]bool, error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	visitedObjects := make(map[string]bool)

	// Use the keys of the config.GeneratedDefaultResourceToPathPrefix map to clone all the resource types in the TMF database
	for _, resourceName := range config.RootBAEObjects {

		_, err := tmf.CloneRemoteResourceType(resourceName, visitedObjects)
		if err != nil {
			slog.Error("cloning remote resource", "resourceName", resourceName, slog.Any("err", err))
			// We continue the loop to retrieve the remaining objects
			continue
		}

	}

	return nil, visitedObjects, nil

}

// CloneRemoteResourceTypes retrieves or updates all objects for the given resource types.
func (tmf *TMFCache) CloneRemoteResourceTypes(resources []string) ([]TMFObject, map[string]bool, error) {
	tmf.cloneMutex.Lock()
	defer tmf.cloneMutex.Unlock()

	visitedObjects := make(map[string]bool)

	// Use the keys of the config.GeneratedDefaultResourceToPathPrefix map to clone all the resource types in the TMF database
	for _, resourceName := range resources {
		slog.Info("cloning remote resource", "resourceName", resourceName)

		_, err := tmf.CloneRemoteResourceType(resourceName, visitedObjects)
		if err != nil {
			slog.Error("cloning remote resource", "resourceName", resourceName, slog.Any("err", err))
			// We continue the loop to retrieve the remaining objects
			continue
		}

	}

	return nil, visitedObjects, nil

}

func (tmf *TMFCache) CloneRemoteResourceType(tmfResourceType string, visitedObjects map[string]bool) (objectList []TMFObject, err error) {

	dbconn, err := tmf.dbpool.Take(context.Background())
	if err != nil {
		return nil, errl.Error(err)
	}
	defer tmf.dbpool.Put(dbconn)

	hostAndPath, err := tmf.config.UpstreamHostAndPathFromResource(tmfResourceType)
	if err != nil {
		slog.Error("retrieving host and path for resource", "resourceName", tmfResourceType, slogor.Err(err))
		return nil, errl.Error(err)
	}

	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
	limit := 100
	offset := 0

	// // We are only interested in objects which are launched or active
	// // to the market (lifecycleStatus=Launched,Active)
	// lifecycleStatus := "Launched,Active"

	var poList []TMFObject

	// Loop retrieving all objets of the given type
	for {

		query := fmt.Sprintf("?limit=%d&offset=%d", limit, offset)

		url := hostAndPath + query
		slog.Info("cloning all objects of type", "resourceName", tmfResourceType, "url", url)

		// Get the list of objects from the TMForum server
		res, err := http.Get(url)
		if err != nil {
			slog.Error("performing GET", "url", url, slogor.Err(err))
			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
			break
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode > 299 {
			slog.Error("Response failed", "status", res.StatusCode, "body", string(body), "url", url)
			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
			break
		}
		if err != nil {
			slog.Error("reading response body", slogor.Err(err), "url", url)
			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
			break
		}

		// Check if it looks like a JSON object. For some errors, the server may (incorrectly) return a string or HTML
		if body[0] != '{' && body[0] != '[' {
			slog.Error("reply does not look as a JSON object", "url", url)
			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
			break
		}

		// Parse the JSON response
		var resourceListMap []map[string]any
		err = json.Unmarshal(body, &resourceListMap)
		if err != nil {
			slog.Error("parsing JSON response", "url", url, slogor.Err(err))
			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
			break
		}

		slog.Info("retrieved objects", "numObjects", len(resourceListMap), "current", len(poList))

		// Process each of the objects in the list
		for _, oMap := range resourceListMap {

			tmfObject, err := TMFObjectFromMap(oMap, tmfResourceType)
			if err != nil {
				slog.Error("FromMapExt", slogor.Err(err))
				PrettyPrint(oMap)
				continue
			}

			// _, err = tmf.fixTMFObject(dbconn, tmfObject, FixHigh)
			// if err != nil {
			// 	slog.Error("FixTMFObject", "id", tmfObject.GetID(), slogor.Err(err))

			// 	if tmf.MustFixInBackend >= FixHigh {

			// 		url := hostAndPath + "/" + tmfObject.GetID()
			// 		fmt.Println("Deleting", url)
			// 		errorDel := doDELETE(url)
			// 		if errorDel != nil {
			// 			slog.Error("error deleting object")
			// 		}

			// 	}

			// 	continue
			// }

			err = tmfObject.LocalUpsertTMFObject(dbconn, 0)
			if err != nil {
				slog.Error("LocalUpsertTMFObject", "id", tmfObject.GetID(), slogor.Err(err))
				continue
			}

			visitedObjects[tmfObject.GetID()] = true
			poList = append(poList, tmfObject)

		}

		// If in this iteration we retrieved less objects than the limit, we are finished
		if len(resourceListMap) < limit {
			slog.Debug("this is the last chunk of objects", "numObjects", len(resourceListMap))
			break
		}

		// Go and retrieve the next chunk of objects
		offset = offset + limit

	}

	slog.Info("cloned", "resourceName", tmfResourceType, "numObjects", len(poList))

	return poList, nil

}

// func (tmf *TMFCache) CloneRemoteResourceTypeOld(tmfResourceName string, visitedObjects map[string]bool) (objectList []TMFObject, err error) {

// 	dbconn, err := tmf.dbpool.Take(context.Background())
// 	if err != nil {
// 		return nil, errl.Error(err)
// 	}
// 	defer tmf.dbpool.Put(dbconn)

// 	hostAndPath, err := tmf.config.UpstreamHostAndPathFromResource(tmfResourceName)
// 	if err != nil {
// 		slog.Error("retrieving host and path for resource", "resourceName", tmfResourceName, slogor.Err(err))
// 		return nil, errl.Error(err)
// 	}

// 	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
// 	limit := 100
// 	offset := 0

// 	// // We are only interested in objects which are launched or active
// 	// // to the market (lifecycleStatus=Launched,Active)
// 	// lifecycleStatus := "Launched,Active"

// 	var poList []TMFObject

// 	// Loop retrieving all objets of the given type
// 	for {

// 		query := fmt.Sprintf("?limit=%d&offset=%d", limit, offset)

// 		url := hostAndPath + query
// 		slog.Info("cloning all objects of type", "resourceName", tmfResourceName, "url", url)

// 		// Get the list of objects from the TMForum server
// 		res, err := http.Get(url)
// 		if err != nil {
// 			slog.Error("performing GET", "url", url, slogor.Err(err))
// 			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
// 			break
// 		}
// 		body, err := io.ReadAll(res.Body)
// 		res.Body.Close()
// 		if res.StatusCode > 299 {
// 			slog.Error("Response failed", "status", res.StatusCode, "body", string(body), "url", url)
// 			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
// 			break
// 		}
// 		if err != nil {
// 			slog.Error("reading response body", slogor.Err(err), "url", url)
// 			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
// 			break
// 		}

// 		// Check if it looks like a JSON object. For some errors, the server may (incorrectly) return a string or HTML
// 		if body[0] != '{' && body[0] != '[' {
// 			slog.Error("reply does not look as a JSON object", "url", url)
// 			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
// 			break
// 		}

// 		// Parse the JSON response
// 		var resourceListMap []map[string]any
// 		err = json.Unmarshal(body, &resourceListMap)
// 		if err != nil {
// 			slog.Error("parsing JSON response", "url", url, slogor.Err(err))
// 			// Just exit the loop, so we can return to the caller whatever objects have been retrieved until now
// 			break
// 		}

// 		// Check if we should terminate the loop because there are no more objects
// 		if len(resourceListMap) == 0 {
// 			break
// 		}

// 		slog.Info("retrieved objects", "numObjects", len(resourceListMap), "current", len(poList))

// 		// Process each of the objects in the list
// 		for _, oMap := range resourceListMap {

// 			tmfObject, err := TMFObjectFromMap(oMap)
// 			if err != nil {
// 				slog.Error("FromMapExt", slogor.Err(err))
// 				PrettyPrint(oMap)
// 				continue
// 			}

// 			fixed, err := tmf.FixTMFObject(dbconn, tmfObject, FixHigh)
// 			if err != nil {
// 				slog.Error("FixTMFObject", "id", tmfObject.GetID(), slogor.Err(err))
// 				PrettyPrint(oMap)

// 				if tmf.MustFixInBackend >= FixHigh {

// 					url := hostAndPath + "/" + tmfObject.GetID()
// 					fmt.Println("Deleting", url)
// 					errorDel := doDELETE(url)
// 					if errorDel != nil {
// 						slog.Error("error deleting object")
// 					}

// 				}

// 				continue
// 			}

// 			// Fixes for 'ProductOffering', when it does not have the field 'seller'
// 			if tmfObject.GetResourceName() == config.ProductOffering && tmfObject.GetSeller() == "" {

// 				// At this moment, the ProductOffering object in DOME does not have
// 				// the identification of the owner organization.
// 				// We need that info to enable access control at the object level, so we retrieve
// 				// the owner information indirectly by retrieving the ProductSpecification associated
// 				// to the ProductOffering, and getting the relevant information from the RelatedParty
// 				// object associated to the ProductSpecification.

// 				sellerDid, sellerName, sellerHref, err := tmf.DeduceProductOfferingSeller(dbconn, tmfObject)
// 				// organizationIdentifier, organization, err := tmf.DeduceProductOfferingOwner(dbconn, tmfObject.ContentAsMap)
// 				if err != nil || sellerDid == "" {
// 					slog.Error("no identification deduced", "id", tmfObject.GetID(), slogor.Err(err))

// 					if tmf.MustFixInBackend >= FixHigh {

// 						url := hostAndPath + "/" + tmfObject.GetID()
// 						fmt.Println("Deleting", url)
// 						errorDel := doDELETE(url)
// 						if errorDel != nil {
// 							slog.Error("error deleting object")
// 						}

// 					}
// 					continue
// 				} else {
// 					// Update our ProductOffering with the owner information retrieved
// 					slog.Info("updating ProductOffering with owner", "id", tmfObject.GetID(), "oid", sellerDid, "organization", sellerName)
// 					tmfObject.SetOwner(sellerDid, sellerName, sellerHref)
// 					tmfObject.SetSellerOperator("", config.DOMEOperatorDid)

// 					// Update or Insert the ProductOffering in our database
// 					if err := tmf.LocalUpsertTMFObject(nil, tmfObject); err != nil {
// 						slog.Error("CloneRemoteResource", slogor.Err(err))
// 					}

// 					fixed = true

// 				}

// 				visitedObjects[tmfObject.GetID()] = true

// 				// Recursively retrieve and save the sub-objects of this ProductOffering.
// 				// We pass the owner information so those objects can include it with them.
// 				tmf.visitMap(dbconn, tmfObject.GetContentAsMap(), sellerDid, sellerName, sellerHref, 3, visitedObjects)

// 			}

// 			// Fixes for 'Catalog', when it does not have the field 'seller'
// 			if tmfObject.GetResourceName() == config.Catalog && tmfObject.GetSeller() == "" {

// 				ownerDid, ownerName, ownerHref, err := tmf.getRelatedPartyOwner(dbconn, tmfObject)
// 				if err != nil {
// 					slog.Error("no identification deduced", "id", tmfObject.GetID(), slogor.Err(err))
// 					continue
// 				} else {
// 					// Update our resource with the owner information retrieved
// 					slog.Info("updating Catalog with owner", "id", tmfObject.GetID(), "oid", ownerDid, "organization", ownerName)
// 					tmfObject.SetOwner(ownerDid, ownerName, ownerHref)
// 					tmfObject.SetSellerOperator("", config.DOMEOperatorDid)

// 					// Update or Insert the object in our database
// 					if err := tmf.LocalUpsertTMFObject(nil, tmfObject); err != nil {
// 						slog.Error("CloneRemoteResource", slogor.Err(err))
// 					}

// 					fixed = true

// 				}

// 			}

// 			if fixed && tmf.MustFixInBackend > FixNone {
// 				resourceName := tmfObject.GetResourceName()
// 				hostAndPath, err := tmf.config.UpstreamHostAndPathFromResource(resourceName)
// 				if err != nil {
// 					return nil, errl.Errorf("retrieving host and path for resource %s: %w", resourceName, err)
// 				}

// 				url := hostAndPath + "/" + tmfObject.GetID()

// 				theMap := tmfObject.GetContentAsMap()
// 				delete(theMap, "id")
// 				delete(theMap, "href")

// 				body, err := json.Marshal(theMap)
// 				if err != nil {
// 					return nil, errl.Errorf("marshalling object %s: %w", tmfObject.GetID(), err)
// 				}
// 				fmt.Println(string(body))

// 				tmfObject, err = doPATCH(url, body, resourceName)
// 				if err != nil {
// 					return nil, errl.Errorf("performing remote PATCH for resource %s: %w", resourceName, err)
// 				}

// 				fmt.Println("******* FIXED Object ******")
// 				out, err := json.MarshalIndent(tmfObject, "", "   ")
// 				if err == nil {
// 					fmt.Println(string(out))
// 				}

// 			}

// 			err = tmfObject.LocalUpsertTMFObject(dbconn, 0)
// 			if err != nil {
// 				slog.Error("LocalUpsertTMFObject", "id", tmfObject.GetID(), slogor.Err(err))
// 				continue
// 			}

// 			visitedObjects[tmfObject.GetID()] = true
// 			poList = append(poList, tmfObject)

// 		}

// 		// If in this iteration we retrieved less objects than the limit, we are finished
// 		if len(resourceListMap) < limit {
// 			slog.Debug("this is the last chunk of objects", "numObjects", len(resourceListMap))
// 			break
// 		}

// 		// Go and retrieve the next chunk of objects
// 		offset = offset + limit

// 	}

// 	slog.Info("cloned", "resourceName", tmfResourceName, "numObjects", len(poList))

// 	return poList, nil

// }

// CloneRemoteObject retrieves a TMFObject by its ID and resource type, either from the local cache or remotely if not available or not fresh.
// If the object is a ProductOffering and lacks seller information, attempts to deduce and update the seller and organization fields
// by inspecting related ProductSpecification and RelatedParty objects. Updates the local cache with any deduced information.
// Recursively processes sub-objects for ProductOffering resources, passing along owner information.
// Tracks visited objects to prevent cycles during recursion.
//
// Parameters:
//   - dbconn: Optional SQLite connection. If nil, a connection is taken from the pool.
//   - id: The unique identifier of the TMFObject to clone.
//   - resource: The resource type of the TMFObject.
//   - visitedObjects: Map to track already visited object IDs to avoid processing cycles.
//
// Returns:
//   - object: The retrieved and possibly updated TMFObject.
//   - err: Error if retrieval, deduction, or update fails.
func (tmf *TMFCache) CloneRemoteObject(
	dbconn *sqlite.Conn,
	id string,
	resource string,
	visitedObjects map[string]bool,
) (object TMFObject, err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, errl.Error(fmt.Errorf("taking db connection: %w", err))
		}
		defer tmf.dbpool.Put(dbconn)
	}

	var local bool

	// With RetrieveOrUpdateObject, we go to the remote server only if the object is not in the local cache and
	// is not fresh enough
	tmfObject, local, err := tmf.RetrieveOrUpdateObject(dbconn, id, resource, "", "", "", LocalOrRemote)
	if err != nil {
		return nil, errl.Error(err)
	}
	if local {
		slog.Debug("object retrieved locally", "id", id)
	} else {
		slog.Debug("object retrieved remotely", "id", id)
	}

	visitedObjects[tmfObject.GetID()] = true

	// Until the TMForum APIs are updated to support all the required fields, we will try to "fix"
	// the objects by deducing it and do whatever is possible.

	// Fixes for 'ProductOffering', when it does not have the field 'seller'
	if tmfObject.GetType() == config.ProductOffering {

		// At this moment, the ProductOffering object in DOME does not have
		// the identification of the owner organization.
		// We need that info to enable access control at the object level, so we retrieve
		// the owner information indirectly by retrieving the ProductSpecification associated
		// to the ProductOffering, and getting the relevant information from the RelatedParty
		// object associated to the ProductSpecification.

		sellerDid, sellerName, sellerHref, err := tmf.DeduceProductOfferingSeller(dbconn, tmfObject)
		// organizationIdentifier, organization, err := tmf.DeduceProductOfferingOwner(dbconn, tmfObject.ContentAsMap)
		if err != nil || sellerDid == "" {
			slog.Warn("no identification deduced", "id", id, slogor.Err(err))
		} else {
			// Update our ProductOffering with the owner information retrieved
			slog.Info("updating ProductOffering with owner", "id", id, "oid", sellerDid, "organization", sellerName)
			tmfObject.SetSeller(sellerHref, sellerDid)
			tmfObject.SetSellerOperator("", config.DOMEOperatorDid)
			tmfObject.SetOrganization(sellerName)
			tmfObject.SetOrganizationIdentifier(sellerDid)

			// Update or Insert the ProductOffering in our database
			if err := tmf.LocalUpsertTMFObject(nil, tmfObject); err != nil {
				slog.Error("CloneRemoteResource", slogor.Err(err))
			}

		}

		visitedObjects[tmfObject.GetID()] = true

		// Recursively retrieve and save the sub-objects of this ProductOffering.
		// We pass the owner information so those objects can include it with them.
		tmf.visitMap(dbconn, tmfObject.GetContentAsMap(), sellerDid, sellerName, sellerHref, 3, visitedObjects)

	}

	return tmfObject, nil

}

func doPATCH(url string, request_body []byte, tmfResource string) (TMFObject, error) {

	buf := bytes.NewReader(request_body)

	req, err := http.NewRequest("PATCH", url, buf)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
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
		return nil, errl.Errorf("retrieving object, status: %d", res.StatusCode)
	}
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	po, err := TMFObjectFromBytes(reply_body, tmfResource)
	if err != nil {
		slog.Error(err.Error())
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

func doDELETE(url string) error {

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Error("sending request", "object", url, slogor.Err(err))
		return err
	}
	reply_body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		slog.Error("deleting object", "status code", res.StatusCode)
		return errl.Errorf("deleting object, status: %d", res.StatusCode)
	}
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	fmt.Println(string(reply_body))

	return nil
}

// func (tmf *TMFCache) fixRemoteResource(tmfResourceName string, visitedObjects map[string]bool, visitedStack Stack) (objectList []TMFObject, err error) {

// 	dbconn, err := tmf.dbpool.Take(context.Background())
// 	if err != nil {
// 		return nil, errl.Error(err)
// 	}
// 	defer tmf.dbpool.Put(dbconn)

// 	hostAndPath, err := tmf.config.UpstreamHostAndPathFromResource(tmfResourceName)
// 	if err != nil {
// 		slog.Error("retrieving host and path for resource", "resourceName", tmfResourceName, slogor.Err(err))
// 		return nil, errl.Error(err)
// 	}

// 	// We will retrieve the objects in chunks of 100, looping until we get a reply with no objects
// 	limit := 100
// 	offset := 0

// 	// We are only interested in ProductOfferings which are launched or active
// 	// to the market (lifecycleStatus=Launched,Active)
// 	lifecycleStatus := "Launched,Active"

// 	var poList []TMFObject

// 	// Loop retrieving all objets of the given type
// 	for {

// 		query := fmt.Sprintf("?limit=%d&offset=%d&lifecycleStatus=%s", limit, offset, lifecycleStatus)

// 		url := hostAndPath + query
// 		slog.Info("cloning all objects of type", "resourceName", tmfResourceName, "url", url)

// 		// Get the list of objects from the DOME server
// 		res, err := http.Get(url)
// 		if err != nil {
// 			slog.Error("performing GET", "url", url, slogor.Err(err))
// 			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
// 			break
// 		}
// 		body, err := io.ReadAll(res.Body)
// 		res.Body.Close()
// 		if res.StatusCode > 299 {
// 			slog.Error("Response failed", "status", res.StatusCode, "body", string(body), "url", url)
// 			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
// 			break
// 		}
// 		if err != nil {
// 			slog.Error("reading response body", slogor.Err(err), "url", url)
// 			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
// 			break
// 		}

// 		// Check if it looks like a JSON object
// 		if body[0] != '{' && body[0] != '[' {
// 			slog.Error("reply does not look as a JSON object", "url", url)
// 			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
// 			break
// 		}

// 		// Parse the JSON response
// 		var resourceListMap []map[string]any
// 		err = json.Unmarshal(body, &resourceListMap)
// 		if err != nil {
// 			slog.Error("parsing JSON response", "url", url, slogor.Err(err))
// 			// Just exit the loop, so we can return to caller whatever objects have been retrieved until now
// 			break
// 		}

// 		// Check if we should terminate the loop because there are no more objects
// 		if len(resourceListMap) == 0 {
// 			break
// 		}

// 		slog.Info("retrieved objects", "numObjects", len(resourceListMap), "current", len(poList))

// 		// Process each of the objects in the list
// 		for _, oMap := range resourceListMap {

// 			id, ok := oMap["id"].(string)
// 			if !ok {
// 				slog.Error("invalid object", "id", id)
// 				out, err := json.MarshalIndent(oMap, "", "   ")
// 				if err == nil {
// 					fmt.Println(string(out))
// 				}
// 				continue
// 			}

// 			tmfObject, err := TMFObjectFromMap(oMap)
// 			if err != nil {
// 				slog.Error("FromMapExt", "id", id, slogor.Err(err))
// 				continue
// 			}
// 			_, err = tmf.fixTMFObject(dbconn, tmfObject, FixHigh)
// 			if err != nil {
// 				slog.Error("FixTMFObject", "id", id, slogor.Err(err))
// 				continue
// 			}
// 			err = tmfObject.LocalUpsertTMFObject(dbconn, tmf.Maxfreshness)
// 			if err != nil {
// 				slog.Error("LocalUpsertTMFObject", "id", id, slogor.Err(err))
// 				continue
// 			}

// 			visitedObjects[tmfObject.GetID()] = true
// 			poList = append(poList, tmfObject)

// 		}

// 		// If in this iteration we retrieved less objects than the limit, we are finished
// 		if len(resourceListMap) < limit {
// 			slog.Debug("this is the last chunk of objects", "numObjects", len(resourceListMap))
// 			break
// 		}

// 		// Go and retrieve the next chunk of objects
// 		offset = offset + limit

// 	}

// 	slog.Info("cloned", "resourceName", tmfResourceName, "numObjects", len(poList))

// 	return poList, nil

// }

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
	po TMFObject,
) (organizationIdentifier string, organization string, href string, err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return "", "", "", errl.Error(fmt.Errorf("taking db connection: %w", err))
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Cast to a general object
	productOffering, ok := po.(*TMFGeneralObject)
	if !ok {
		return "", "", "", errl.Errorf("not a TMFGeneralObject")
	}

	// Sanity check: this logic only applies to a ProductOffering
	if productOffering.resourceType != config.ProductOffering {
		return "", "", "", errl.Errorf("not a ProductOffering: %s", productOffering.resourceType)
	}

	// If we have already the owner information, we return it.
	if did, name, href := productOffering.Owner(); did != "" {
		if len(name) == 0 {
			slog.Warn("DeduceProductOfferingSeller: owner name is empty")
		}
		return did, name, href, nil
	}

	// At this point we have to retrieve the owner information from the remote server
	// retrieve the remote productSpecification object associated to the productOffering,
	// which always includes the owner information in its relatedParty sub-resource.

	prodSpecRefMap := jpath.GetMap(productOffering.ContentAsMap, "productSpecification")
	PrettyPrint(productOffering.ContentAsMap)
	if len(prodSpecRefMap) == 0 {
		return "", "", "", errl.Errorf("productSpecification is empty or invalid for productOffering %s", productOffering.id)
	}

	// Get the prodSpecID to retrieve the remote associated object
	prodSpecID := jpath.GetString(prodSpecRefMap, "id")
	if prodSpecID == "" {
		return "", "", "", errl.Errorf("productSpecification 'id' is nil or not a string for productOffering %s", productOffering.id)
	}

	// Use the 'id' field to retrieve the productSpecification object from the server
	// After the call, the productSpecification object is already persisted locally with the owner information in the
	// standard TMF format. We need to update the database in the format we need for efficient SQL queries.
	productSpecification, _, err := tmf.RetrieveOrUpdateObject(dbconn, prodSpecID, config.ProductSpecification, "", "", "", LocalOrRemote)
	if err != nil {
		return "", "", "", errl.Errorf("retrieving productSpecification object: %w", err)
	}

	// The productSpecification may have already the owner information in the new format.
	if did, name, href := productSpecification.Owner(); did != "" && name != "" && href != "" {
		return did, name, href, nil
	}

	ownerDid, ownerName, ownerHref, err := tmf.getRelatedPartyOwner(dbconn, productSpecification)
	if err != nil {
		return "", "", "", errl.Errorf("retrieving relatedParty object: %w", err)
	}

	// Update the ProductSpecification object with the owner info
	productSpecification.SetSeller(ownerHref, ownerDid)
	productSpecification.SetOrganization(ownerName)

	if err := tmf.LocalUpsertTMFObject(dbconn, productSpecification); err != nil {
		return "", "", "", errl.Errorf("updating object locally: %w", err)
	}

	return ownerDid, ownerName, ownerHref, nil
}

func (tmf *TMFCache) getRelatedPartyOwner(dbconn *sqlite.Conn, o TMFObject) (did string, href string, name string, err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return "", "", "", errl.Error(fmt.Errorf("taking db connection: %w", err))
		}
		defer tmf.dbpool.Put(dbconn)
	}

	thisID := o.GetID()

	oMap := o.GetContentAsMap()

	// Check if there is a RelatedParty object
	relatedPartyList, _ := oMap["relatedParty"].([]any)
	if len(relatedPartyList) == 0 {
		return "", "", "", errl.Errorf("relatedParty is nil or invalid for object %s", thisID)
	}

	// The RelatedParty must be like this:
	// "relatedParty": [
	// 	    {
	// 			"id": "urn:ngsi-ld:organization:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
	// 			"href": "urn:ngsi-ld:organization:32611feb-6f78-4ccd-a4a2-547cb01cf33d",
	// 			"role": "Owner",
	// 			"@referredType": ""
	// 		}
	// ]

	// One of the relatedParty items must be the one defining the owner
	for _, rp := range relatedPartyList {

		relatedPartyRefMap, _ := rp.(map[string]any)
		if len(relatedPartyRefMap) == 0 {
			slog.Error("DeduceProductOfferingSeller: relatedParty is nil or invalid for productSpecification", "productSpecification", thisID)
			continue
		}

		// We look for an entry "role" = "owner", in a case-insensitive way
		role, _ := relatedPartyRefMap["role"].(string)
		if len(role) == 0 {
			slog.Error("DeduceProductOfferingSeller: relatedParty 'role' is nil or not a string", "productSpecification", thisID)
			continue
		}
		role = strings.ToLower(role)
		if role != "owner" {
			continue
		}

		// If "owner" or seller entry found, use 'href' to retrieve the referenced object from the remote server.
		// 'id' points to an Organization TMF object
		ownerOrgHref, _ := relatedPartyRefMap["href"].(string)
		if len(ownerOrgHref) == 0 {
			slog.Error("DeduceProductOfferingSeller: relatedParty 'href' is nil or not a string", "productSpecification", thisID)
			return "", "", "", errl.Errorf("relatedParty 'href' is nil or not a string: producSpecification: %s", thisID)
		}

		organizationObject, _, err := tmf.RetrieveOrUpdateObject(dbconn, ownerOrgHref, config.Organization, "", "", "", LocalOrRemote)
		if err != nil {
			slog.Error("DeduceProductOfferingSeller: retrieving organization object", "href", ownerOrgHref, "productSpecification", thisID, slogor.Err(err))
			return "", "", "", errl.Errorf("retrieving organization object: %s for productSpecification %s", ownerOrgHref, thisID)
		}

		organizationIdentifier, organizationName, err := organizationObject.GetIDMID()
		if err != nil {
			return "", "", "", errl.Error(err)
		}

		return organizationIdentifier, organizationName, ownerOrgHref, nil

	}

	return "", "", "", errl.Errorf("no entry with role 'owner' found for object %s", thisID)
}

func (tmf *TMFCache) CreateObject(logger *slog.Logger, tokString string, newOrg TMFObject) (TMFObject, error) {

	// TODO: for the moment, we do not use the server, but do it locally
	if newOrg.GetType() == config.Organization {
		// If the object is an Organization, we do not send it to the server,

		// Insert the object in the local database
		err := tmf.LocalUpsertTMFObject(nil, newOrg)
		if err != nil {
			return nil, errl.Errorf("inserting object in local database: %w", err)
		}

		return newOrg, nil
	}

	// **********************************************************************************
	// Create the object in the upstream TMForum API server.
	// **********************************************************************************

	hostAndPath, err := tmf.UpstreamHostAndPathFromResource(config.Organization)
	if err != nil {
		return nil, errl.Errorf("retrieving host and path for resource %s: %w", config.Organization, err)
	}

	// Send the POST to the central server.
	tmfObject, err := doTMFPOST(logger, tmf.HttpClient, hostAndPath, tokString, newOrg.GetContentAsMap(), config.Organization)
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

func doTMFPOST(
	logger *slog.Logger,
	httpClient *http.Client,
	url string,
	auth_token string,
	createObject map[string]any,
	tmfResource string,
) (TMFObject, error) {

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
	tmfObject, err := TMFObjectFromBytes(responseBody, tmfResource)
	if err != nil {
		return nil, errl.Errorf("creating object from response: %w", err)
	}

	return tmfObject, nil

}

// RetrieveOrUpdateObject retrieves an object from the local database or from the server if it is not in the local database.
// This behaviour can be tuned with the location parameter,
// The function returns the object and a boolean indicating if the object was retrieved from the local database.
func (tmf *TMFCache) RetrieveOrUpdateObject(
	dbconn *sqlite.Conn,
	id string,
	resource string,
	sellerDid string,
	sellerName string,
	sellerHref string,
	locationMustBe AccessType,
) (localTmfObj TMFObject, local bool, err error) {
	slog.Info("RetrieveOrUpdateObject", "href", id, "organizationid", sellerDid, "organization", sellerName, "location", locationMustBe.String())

	if sellerDid != "" && sellerName == "" {
		slog.Warn("RetrieveOrUpdateObject: sellerName is empty")
	}

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, errl.Error(fmt.Errorf("taking db connection: %w", err))
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Check if the object is already in the local database
	localTmfObj, found, err := tmf.LocalRetrieveTMFObject(dbconn, id, resource, "")
	if err != nil {
		if !errors.Is(err, ErrorNotFound) {
			return nil, false, errl.Error(fmt.Errorf("retrieving local object: %w", err))
		}
	}

	if !found {

		// Return with an error if the object was not found and caller specified 'local only search'
		if locationMustBe == LocalOnly {
			return nil, false, errl.Error(fmt.Errorf("object not found in local database: %s", id))
		}

		// Get the object from the server
		remotepo, err := tmf.RemoteRetrieveTMFObject(id, resource)
		if err != nil {
			return nil, false, errl.Error(err)
		}

		// Update the owner info if the remote does not have it (and we do).
		remoteDid, _, _ := remotepo.Owner()
		if remoteDid == "" && sellerDid != "" {

			remotepo.SetOwner(sellerDid, sellerName, sellerHref)
			remotepo.SetSellerOperator("", config.DOMEOperatorDid)

		}

		// Update the object in the local database
		err = tmf.LocalUpsertTMFObject(dbconn, remotepo)
		if err != nil {
			return nil, false, errl.Error(err)
		}

		slog.Info("RetrieveOrUpdateObject", "href", id, "organizationid", sellerDid, "organization", sellerName, "location", locationMustBe.String())
		return remotepo, false, nil

	}

	// We have retrieved a local object, but must check if it is fresh enough or if we
	// have to retrieve it again from the server

	now := time.Now().Unix()

	if int(now-localTmfObj.GetUpdated()) < tmf.Maxfreshness {
		// The local object is fresh and we can return it.
		// But we will check if we have to update it with owner info supplied in the call.
		localDid, _, _ := localTmfObj.Owner()
		if localDid == "" && sellerDid != "" {
			// Special case: we found the object, it does not have the owner info, but the caller provides one.
			// We just update the object in the cache, setting the organizationIdentifier to what the caller specifies.

			// Set the owner info with the one provided by the caller
			localTmfObj.SetOwner(sellerDid, sellerName, sellerHref)
			localTmfObj.SetSellerOperator("", config.DOMEOperatorDid)

			// Update the object in the local database
			err = tmf.LocalUpsertTMFObject(dbconn, localTmfObj)
			if err != nil {
				return nil, false, errl.Error(err)
			}

		}

		return localTmfObj, true, nil
	}

	slog.Debug("RetrieveOrUpdateObject: local object is not fresh enough, retrieving from server",
		"href", id,
		"organizationid", sellerDid,
		"organization", sellerName,
		"localTmfObjUpdated", localTmfObj.GetUpdated(),
		"now", now,
		"maxfreshness", tmf.Maxfreshness,
	)
	//
	// We have to retrieve the object from the server
	//

	// Update some statistics counter
	if int(now-localTmfObj.GetUpdated()) >= tmf.Maxfreshness {
		// Update stats counter
		tmf.RefreshCounter++
	}

	// Get the object from the server
	remotepo, err := tmf.RemoteRetrieveTMFObject(id, resource)
	if err != nil {
		return nil, false, errl.Error(err)
	}

	// Update the object in the local database
	err = tmf.LocalUpsertTMFObject(dbconn, remotepo)
	if err != nil {
		return nil, false, errl.Error(err)
	}

	return remotepo, false, nil
}

// visitMap visits recursively the descendants of an object (representaed as a map).
// It handles cicles to avoid infinite loops.
func (tmf *TMFCache) visitMap(
	dbconn *sqlite.Conn,
	currentObject map[string]any,
	sellerDid string,
	sellerName string,
	sellerHref string,
	indent int,
	visitedObjects map[string]bool,
) {

	// A map object can contain an 'href' field that points to another object.
	// In this case we retrieve and visit the object, if it was not retrieved before.
	// For other map objects we print the relevant fields if enabled by the tmf.Dump variable.
	href, _ := currentObject["href"].(string)
	if len(href) > 0 {
		if tmf.Dump {
			fmt.Printf("%shref: %v\n", indentStr(indent), href)
		}
		resource := jpath.GetString(currentObject, "resourceType")
		if !visitedObjects[href] {
			visitedObjects[href] = true
			remoteObj, _, err := tmf.RetrieveOrUpdateObject(dbconn, href, resource, sellerDid, sellerName, sellerHref, LocalOrRemote)
			if err != nil {
				slog.Error(err.Error())
			} else {
				tmf.visitMap(dbconn, remoteObj.GetContentAsMap(), sellerDid, sellerName, sellerHref, indent+3, visitedObjects)
			}
		}
	}

	for k, v := range currentObject {
		switch v := v.(type) {

		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%s:\n", indentStr(indent), k)
			}
			tmf.visitMap(dbconn, v, sellerDid, sellerName, sellerHref, indent+3, visitedObjects)

		case []any:
			if tmf.Dump {
				fmt.Printf("%s%s: [\n", indentStr(indent), k)
			}
			tmf.visitArray(dbconn, v, sellerDid, sellerName, sellerHref, indent+3, visitedObjects)
			if tmf.Dump {
				fmt.Printf("%s]\n", indentStr(indent))
			}

		case string:
			if tmf.Dump {
				fmt.Printf("%s%s: %s\n", indentStr(indent), k, v[:min(len(v), 60)])
			}

		}
	}
}

// visitArray is the complement to visitMap for recursive traversal of a TMForum object graph
func (tmf *TMFCache) visitArray(
	dbconn *sqlite.Conn,
	arr []any,
	sellerDid string,
	sellerName string,
	sellerHref string,
	indent int,
	visitedObjects map[string]bool,
) {
	for i, v := range arr {
		switch v := v.(type) {
		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			tmf.visitMap(dbconn, v, sellerDid, sellerName, sellerHref, indent+3, visitedObjects)
		case []any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			tmf.visitArray(dbconn, v, sellerDid, sellerName, sellerHref, indent+3, visitedObjects)

		case string:
			if tmf.Dump {
				fmt.Printf("%s%d: %s\n", indentStr(indent), i, v[:min(len(v), 60)])
			}

		}
	}
}

// RemoteRetrieveTMFObject retrieves a TMF object from the DOME server.
//
// In DOME the href parameter is also the ID of the object which has to be used in the URL of the object to
// retrieve it from the server.
// The id parameter also has embedded the type of the object in the form of urn:ngsi-ld:<type>:<id>
func (tmf *TMFCache) RemoteRetrieveTMFObject(id string, resourceName string) (TMFObject, error) {

	// // Parse the id to get the type of the object
	// resourceName, err := config.FromIdToResourceName(id)
	// if err != nil {
	// 	return nil, errl.Errorf("parsing the id: %w", err)
	// }

	hostAndPath, err := tmf.config.UpstreamHostAndPathFromResource(resourceName)
	if err != nil {
		return nil, errl.Errorf("retrieving host and path for resource %s: %w", resourceName, err)
	}

	// Get the object from the server
	url := hostAndPath + "/" + id
	res, err := http.Get(url)
	if err != nil {
		return nil, errl.Errorf("retrieving %s: %w", url, err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		return nil, errl.Errorf("retrieving %s, status code: %d and\nbody: %s", url, res.StatusCode, body)
	}
	if err != nil {
		slog.Error("retrieving remote", "object", url, slogor.Err(err))
		return nil, errl.Errorf("retrieving %s: %w", url, err)
	}

	// Check if it looks like a JSON object
	if body[0] != '{' && body[0] != '[' {
		return nil, errl.Errorf("reply does not look as a JSON object from %s", url)
	}

	dbconn, err := tmf.dbpool.Take(context.Background())
	if err != nil {
		return nil, errl.Error(fmt.Errorf("taking db connection: %w", err))
	}
	defer tmf.dbpool.Put(dbconn)
	// Create the in-memory object
	po, err := TMFObjectFromBytes(body, resourceName)
	if err != nil {
		slog.Error(err.Error())
		return nil, errl.Error(err)
	}

	// // Perform some verifications
	// switch resourceName {
	// case config.ProductOffering:
	// 	// Check the minimum conditions
	// case config.ProductSpecification:
	// 	ownerDid, ownerName, ownerHref, err := tmf.getRelatedPartyOwner(dbconn, po)
	// 	if err != nil {
	// 		return nil, errl.Errorf("retrieving relatedParty object: %w", err)
	// 	}

	// 	// Update the ProductSpecification object with the owner info
	// 	po.SetOwner(ownerDid, ownerName, ownerHref)

	// }

	return po, nil
}

// ************************************************************************
// ************************************************************************
// TMFCache database operations
// ************************************************************************
// ************************************************************************

// DeleteTables drops the table and performs a VACUUM to reclaim space
func (tmf *TMFCache) DeleteTables() error {
	return deleteTables(tmf.dbpool)
}

// LocalCheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func (tmf *TMFCache) LocalCheckIfExists(
	dbconn *sqlite.Conn, id string, resource string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return false, nil, 0, errl.Errorf("taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalCheckIfExists(dbconn, id, resource, version)

}

// LocalRetrieveTMFObject retrieves the object with the href (is the same as the id).
// The version is optional. If it is not provided, the most recently version (by lexicographic order) is retrieved.
func (tmf *TMFCache) LocalRetrieveTMFObject(dbconn *sqlite.Conn, id string, resource string, version string) (po TMFObject, found bool, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, errl.Errorf("taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalRetrieveTMFObject(dbconn, id, resource, version)

}

// LocalRetrieveOrgByDid retrieves an organization from the cache by its Organization DID.
// This DID is NOT the id of the object, but the DID of the organization.
// It returns the TMFObject, a boolean indicating whether the organization was found, and an error if any occurred.
func (tmf *TMFCache) LocalRetrieveOrgByDid(dbconn *sqlite.Conn, did string) (o TMFObject, found bool, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, errl.Errorf("taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalRetrieveOrgByDid(dbconn, did)

}

// LocalUpdateInStorage updates an object in the db with the contents of the po.
func (tmf *TMFCache) LocalUpdateInStorage(dbconn *sqlite.Conn, po *TMFGeneralObject) error {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return errl.Errorf("taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalUpdateInStorage(dbconn)

}

// LocalInsertInStorage inserts po into the database.
// id and version are primary keys, so their combination must be unique or the function returns and error.
func (tmf *TMFCache) LocalInsertInStorage(dbconn *sqlite.Conn, po *TMFGeneralObject) error {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return errl.Errorf("taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalInsertInStorage(dbconn)

}

// LocalUpsertTMFObject updates or inserts an object in the database.
// id and version are primary keys, so their combination must be unique or the function returns and error.
func (tmf *TMFCache) LocalUpsertTMFObject(dbconn *sqlite.Conn, po TMFObject) (err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return errl.Errorf("taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalUpsertTMFObject(dbconn, tmf.Maxfreshness)

}

// LocalRetrieveListTMFObject implements the TMForum functionality for retrieving a list of objects of a given type from the database.
func (tmf *TMFCache) LocalRetrieveListTMFObject(dbconn *sqlite.Conn, tmfResource string, queryValues url.Values, perObject func(tmfObject TMFObject) LoopControl) error {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return errl.Errorf("taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalRetrieveListTMFObject(dbconn, tmfResource, queryValues, perObject)

}

// ProcessRelatedParties inspects and fixes the "relatedParty" entries of a TMFObject.
// It performs several consistency checks and corrections, such as ensuring required fields
// ("id", "href", "@referredType", "did", "role") are present and valid. If missing or invalid
// data is found, it attempts to fix the entry and logs the correction. For related parties
// referring to organizations, it may retrieve additional information from the database to
// populate missing fields. The function also sets convenience fields in the TMFObject based
// on the roles of related parties (e.g., "seller", "buyer", "selleroperator", "buyeroperator").
// Returns true if any fixes were applied, or false if no changes were necessary. Returns an
// error if the input object is nil or if other critical issues are encountered.
//
// Parameters:
//   - dbconn: SQLite database connection used to retrieve referenced objects.
//   - po:     The TMFObject to process and potentially fix.
//   - level:  The FixLevel indicating whether and how to apply fixes.
//
// Returns:
//   - fixed:  True if any relatedParty entries were fixed, false otherwise.
//   - err:    Error if the operation failed or the input was invalid.
func (tmf *TMFCache) ProcessRelatedParties(dbconn *sqlite.Conn, po TMFObject, level FixLevel) (fixed bool, err error) {

	if level == FixNone {
		return false, nil
	}

	if po == nil {
		return false, errl.Errorf("object is nil")
	}

	id := po.GetID()
	resource := po.GetType()
	if resource == config.ProductOffering {
		_ = resource
	}

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties, _ := po.GetContentAsMap()["relatedParty"].([]any)

	for _, rp := range relatedParties {

		// Convert entry to a map
		rpMap, _ := rp.(map[string]any)
		if len(rpMap) == 0 {
			slog.Error("invalid relatedParty entry", "object", id)
			// Go to next entry
			continue
		}

		rpReferredType, _ := rpMap["@referredType"].(string)
		rpReferredType = strings.ToLower(rpReferredType)

		rpId, _ := rpMap["id"].(string)
		rpHref, _ := rpMap["href"].(string)

		// The did may come in any of these fields, depending on the version of the object.
		// New objects should have the did field.
		rpDid, _ := rpMap["did"].(string)
		rpName, _ := rpMap["name"].(string)

		rpOrganizationName := ""
		rpOrganizationIdentifier := ""

		// Just in case the object is old
		if rpDid == "" {
			rpDid = rpName
		}

		rpRole, _ := rpMap["role"].(string)
		rpRole = strings.ToLower(rpRole)

		// It is a hard error if there is not an id or an href (in principle, must be both)
		if len(rpId) == 0 && len(rpHref) == 0 {
			slog.Error("no id or href in related party", "object", id)
			if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
				fmt.Println(string(out))
			}
			continue
		}

		if len(rpId) == 0 {
			rpHref = rpId
			fixed = true
			slog.Error("empty 'id' in related party fixed with 'href'", "tmfObject", id)
		}
		if len(rpHref) == 0 {
			rpId = rpHref
			fixed = true
			slog.Error("empty 'href' in related party fixed with 'id'", "tmfObject", id)
		}

		// Fix related parties entries without the referredType entry
		if rpReferredType == "" {

			// Parse the id to get the type of the object
			referredResourceName, err := config.FromIdToResourceType(rpId)
			if err != nil {
				slog.Error("parsing id", "id", id, slogor.Err(err))
				continue
			}
			if len(referredResourceName) > 0 {
				rpReferredType = referredResourceName
				rpMap["@referredType"] = rpReferredType
				fixed = true
			}
			slog.Warn("empty '@referredType' in relatedParty fixed", "id", id, "referredType", rpReferredType)
		}

		// When the entry is pointing to an Organization object
		if rpReferredType == config.Organization {

			// Make sure the entry has an extension schemaLocation entry
			rpMap["@schemaLocation"] = config.SchemaLocationRelatedParty

			// If the DID value is not good, deduce it from the Organization object pointed by the entry
			if rpDid == "" || !strings.HasPrefix(rpDid, "did:elsi:") {

				org, _, err := tmf.RetrieveOrUpdateObject(dbconn, rpHref, config.Organization, "", "", "", LocalOrRemote)

				// org, err := localOrganizationByIdRetrieveOrUpdate(dbconn, rpHref)
				if err != nil {
					slog.Warn("referred organization not found", "id", id, "href", rpHref, slogor.Err(err))
					PrettyPrint(rp)
				} else {
					rpOrganizationIdentifier = org.GetOrganizationIdentifier()
					rpOrganizationName = org.GetOrganization()

					rpDid = rpOrganizationIdentifier
					rpName = rpOrganizationIdentifier

					rpMap["did"] = rpDid
					rpMap["name"] = rpName
					fixed = true
					slog.Warn("empty 'did' in relatedParty fixed", "id", id, "did", rpDid)
				}

			}

		}

		if resource == config.Individual {
			if len(rpRole) == 0 {
				// Fix individual objects. The role for the related party must be 'employer'.
				rpRole = "employer"
				rpMap["role"] = rpRole
				fixed = true
				slog.Warn("empty 'role' in related party fixed to 'employer'", "id", id)
			}
			po.SetOrganizationIdentifier(rpOrganizationIdentifier)
			po.SetOrganization(rpOrganizationName)
			fixed = true
			continue
		}

		// If we are processing the 'owner' relatedParty of an object which is not an Organization,
		// we try to retrieve the associated organization locally and update the object
		if rpRole == "owner" && resource != config.Organization {
			po.SetOrganizationIdentifier(rpOrganizationIdentifier)
			po.SetOrganization(rpOrganizationName)
			fixed = true
			continue
		}

		if rpRole != "seller" && rpRole != "selleroperator" && rpRole != "buyer" && rpRole != "buyeroperator" {
			// Go to next entry
			continue
		}

		if !strings.HasPrefix(rpDid, "did:elsi:") {
			slog.Error("invalid or not existent DID", "tmfObject", id)
			if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
				fmt.Println(string(out))
			}
			// Go to next entry
			continue
		}

		// // Add a new relatedParty entry to the object
		// rpEntry := RelatedPartyRef{
		// 	Id:   rpId,
		// 	Href: rpHref,
		// 	Role: rpRole,
		// 	Did:  rpDid,
		// }
		// po.RelatedParty = append(po.RelatedParty, rpEntry)

		// Set the convenience fields in the object
		switch rpRole {
		case "seller":
			po.SetOwner(rpDid, rpOrganizationName, rpHref)

		case "buyer":
			po.SetBuyer(rpHref, rpDid)

		case "selleroperator":
			po.SetSellerOperator(rpHref, rpDid)

		case "buyeroperator":
			po.SetBuyerOperator(rpHref, rpDid)
		}

	}

	if fixed {
		PrettyPrint(po.GetContentAsMap()["relatedParty"])
	}

	return fixed, nil

}
