package tmfsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type TMFObject struct {
	ID                     string         `json:"id"`
	Type                   string         `json:"type"`
	Name                   string         `json:"name"`
	Description            string         `json:"description"`
	LifecycleStatus        string         `json:"lifecycleStatus"`
	Version                string         `json:"version"`
	LastUpdate             string         `json:"lastUpdate"`
	ContentMap             map[string]any `json:"-"` // The content of the object as a map
	Content                []byte         `json:"-"` // The content of the object as a JSON byte array
	Organization           string         `json:"organization"`
	OrganizationIdentifier string         `json:"organizationIdentifier"`
	Updated                int64          `json:"updated"`
}

func NewTMFObject(oMap map[string]any, content []byte) (*TMFObject, error) {

	// Deduce the type of the object from the ID
	poType, err := TMFObjectIDtoType(oMap["id"].(string))
	if err != nil {
		return nil, err
	}

	// Canonicalize (if needed) the JSON object for the content field
	if content == nil {
		content, err = json.Marshal(oMap)
		if err != nil {
			return nil, err
		}
	}

	// Extract the fields from the map, if they exist
	name, _ := oMap["name"].(string)
	description, _ := oMap["description"].(string)
	lifecycleStatus, _ := oMap["lifecycleStatus"].(string)
	version, _ := oMap["version"].(string)
	lastUpdate, _ := oMap["lastUpdate"].(string)
	organizationIdentifier, _ := oMap["organizationIdentifier"].(string)
	organization, _ := oMap["organization"].(string)

	// Create a TMFObject struct from the map
	po := &TMFObject{
		ID:                     oMap["id"].(string),
		Type:                   poType,
		Name:                   name,
		Description:            description,
		LifecycleStatus:        lifecycleStatus,
		Version:                version,
		LastUpdate:             lastUpdate,
		ContentMap:             oMap,
		Content:                content,
		OrganizationIdentifier: organizationIdentifier,
		Organization:           organization,
	}

	return po, nil
}

func (po *TMFObject) String() string {
	return fmt.Sprintf("ID: %s\nType: %s\nName: %s\nLifecycleStatus: %s\nVersion: %s\nLastUpdate: %s\n", po.ID, po.Type, po.Name, po.LifecycleStatus, po.Version, po.LastUpdate)
}

func (po *TMFObject) UpdateInMemoryWithOwner(organizationIdentifier string, organization string) (*TMFObject, error) {
	po.OrganizationIdentifier = organizationIdentifier
	po.ContentMap["organizationIdentifier"] = organizationIdentifier
	po.Organization = organization
	po.ContentMap["organization"] = organization

	// Update the content field
	poJSON, err := json.Marshal(po.ContentMap)
	if err != nil {
		return nil, err
	}

	po.Content = poJSON

	return po, nil
}

func (po *TMFObject) Hash() []byte {
	hasher := sha256.New()
	hasher.Write(po.Content)
	return hasher.Sum(nil)
}

type AccessType bool

const OnlyLocal AccessType = true
const LocalOrRemote AccessType = false

func (at AccessType) String() string {
	if at == OnlyLocal {
		return "OnlyLocal"
	}
	return "LocalOrRemote"
}

// TMFdb is a struct that holds a pool of connections to the database and the URL of the DOME server
//
// The database connection is a pool of connections that is shared by all the requests in this object.
// The connection is returned to the pool when the object is closed.
// This object should be used by only one goroutine. Otherwise, multiple objects can be used concurrently
type TMFdb struct {
	dbpool         *sqlitex.Pool
	domeServer     string
	Maxfreshness   int
	RefreshCounter int
}

func New(config *Config) (*TMFdb, error) {
	var err error

	tmf := &TMFdb{
		domeServer:   config.domeServer,
		Maxfreshness: 60 * 60, // 1 hour
	}

	// Initialize the global pool of database connections
	if tmf.dbpool == nil {
		tmf.dbpool, err = sqlitex.NewPool(config.dbname, sqlitex.PoolOptions{
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
		return err
	}

	return nil
}

func (tmf *TMFdb) Close() {
	tmf.dbpool.Close()
}

func indentStr(indent int) string {
	return strings.Repeat(" ", indent)
}

var visitedObjects map[string]bool

// CloneRemoteProductOfferings visits the remote tree of objects starting from the lists of product offerings.
//
// This function is special in the sense that it retrieves a list of complete objects without their dependencies
func (tmf *TMFdb) CloneRemoteProductOfferings() ([]*TMFObject, error) {

	visitedObjects = make(map[string]bool)

	// Get the list of product offerings from the DOME server
	res, err := http.Get(tmf.domeServer + "/catalog/productOffering?limit=500&offset=0&lifecycleStatus=Launched")
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		slog.Error("Response failed", "status", res.StatusCode, "body", body)
		return nil, err
	}
	if err != nil {
		slog.Error("reading response body", slogor.Err(err))
		return nil, err
	}

	// Parse the JSON response
	var poListMap []map[string]any
	err = json.Unmarshal(body, &poListMap)
	if err != nil {
		slog.Error("parsing JSOn response", slogor.Err(err))
		return nil, err
	}

	indent := 0

	// Process the list of product offerings
	var productOfferings []*TMFObject
	for i, oMap := range poListMap {

		po, err := NewTMFObject(oMap, nil)
		if err != nil {
			slog.Error("creating NewTMFObject", slogor.Err(err))
			continue
		}

		owner, err := tmf.GetProductOfferingOwner(oMap)
		if err != nil {
			slog.Error(err.Error())
		} else {
			po.OrganizationIdentifier = owner.OrganizationIdentifier
			oMap["organizationIdentifier"] = owner.OrganizationIdentifier
			po.Organization = owner.Organization
			oMap["organization"] = owner.Organization
			po.ContentMap = oMap
		}

		productOfferings = append(productOfferings, po)

		// Save the object in the local database
		if err := tmf.UpsertTMFObject(nil, po); err != nil {
			slog.Error(err.Error())
		}

		visitedObjects[po.ID] = true

		fmt.Printf("Product Offering (%d):\n", i)

		tmf.visitObjectMap(oMap, po.OrganizationIdentifier, po.Organization, indent+3)

	}

	// Write some stats
	fmt.Println("############################################")

	var differentTypes = make(map[string]bool)

	fmt.Println("Visited objects:")
	for id := range visitedObjects {
		parts := strings.Split(id, ":")
		differentTypes[parts[2]] = true
		fmt.Println(id)
	}
	fmt.Println("############################################")

	fmt.Println("Different types:")
	for t := range differentTypes {
		fmt.Println(t)
	}

	return productOfferings, nil

}

func (tmf *TMFdb) CloneRemoteCatalogues() ([]*TMFObject, error) {

	visitedObjects = make(map[string]bool)

	// Get the list of catalogues from the DOME server
	res, err := http.Get(tmf.domeServer + "/catalog/catalog?limit=500&offset=0&lifecycleStatus=Launched,Active")
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		slog.Error("Response failed", "status", res.StatusCode, "body", body)
		return nil, err
	}
	if err != nil {
		slog.Error("reading response body", slogor.Err(err))
		return nil, err
	}

	// Parse the JSON response
	var poListMap []map[string]any
	err = json.Unmarshal(body, &poListMap)
	if err != nil {
		slog.Error("parsing JSOn response", slogor.Err(err))
		return nil, err
	}

	// Process the list
	var poList []*TMFObject
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
			return nil, fmt.Errorf("relatedParty is nil")
		}

		// One of the relatedParty items must be the one defining the owner
		for _, rp := range relatedPartyList {
			rpMap := rp.(map[string]any)
			if strings.ToLower(rpMap["role"].(string)) == "owner" {
				owner, _, err := tmf.RetrieveOrUpdateObject(nil, rpMap["href"].(string), "", "", LocalOrRemote)
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
						if len(owner.OrganizationIdentifier) == 0 {
							owner, _ = owner.UpdateInMemoryWithOwner(oid, organization)
							err := tmf.UpsertTMFObject(nil, owner)
							if err != nil {
								slog.Error(err.Error())
								return nil, err
							}
						}
						if len(po.OrganizationIdentifier) == 0 {
							po, _ = po.UpdateInMemoryWithOwner(oid, organization)
							err := tmf.UpsertTMFObject(nil, po)
							if err != nil {
								slog.Error(err.Error())
								return nil, err
							}
						}

						break
					}
				}

			}
		}

		visitedObjects[po.ID] = true

	}

	// Write some stats
	fmt.Println("############################################")

	var differentTypes = make(map[string]bool)

	fmt.Println("Visited objects:")
	for id := range visitedObjects {
		parts := strings.Split(id, ":")
		differentTypes[parts[2]] = true
		fmt.Println(id)
	}
	fmt.Println("############################################")

	fmt.Println("Different types:")
	for t := range differentTypes {
		fmt.Println(t)
	}

	return poList, nil

}

func (tmf *TMFdb) GetProductOfferingOwner(productOfferingMap map[string]any) (*TMFObject, error) {

	// Get the productSpecification object
	psMap := productOfferingMap["productSpecification"].(map[string]any)
	if psMap == nil {
		slog.Info("productSpecification is nil")
		return nil, fmt.Errorf("productSpecification is nil")
	}

	ps, _, err := tmf.RetrieveOrUpdateObject(nil, psMap["href"].(string), "", "", LocalOrRemote)
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
			owner, _, err := tmf.RetrieveOrUpdateObject(nil, rpMap["href"].(string), "", "", LocalOrRemote)
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
						owner, _ = owner.UpdateInMemoryWithOwner(oid, organization)
						err := tmf.UpsertTMFObject(nil, owner)
						if err != nil {
							slog.Error(err.Error())
							return nil, err
						}
					}
					if len(ps.OrganizationIdentifier) == 0 || len(ps.Organization) == 0 {
						ps, _ = ps.UpdateInMemoryWithOwner(oid, organization)
						err := tmf.UpsertTMFObject(nil, ps)
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

func (tmf *TMFdb) UpsertTMFObject(dbconn *sqlite.Conn, po *TMFObject) error {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Get the type of object from the ID
	objectType, err := TMFObjectIDtoType(po.ID)
	if err != nil {
		return err
	}
	po.Type = objectType

	// Categories do not have an owner. Make sure we do not set the owner field to something else
	if po.Type == "category" {
		po.OrganizationIdentifier = ""
		po.Organization = ""
	}

	// Check if the row already exists, with the same version
	hasRow, hash, freshness, err := tmf.CheckIfExists(dbconn, po.ID, po.Version)
	if err != nil {
		return err
	}

	// The id and version are the same, but we have to check the hash to see if we have to update the record
	if hasRow {

		slog.Debug("row already exists", "id", po.ID)

		// Check if the data is recent enough and the hash of the content is the same
		if freshness < tmf.Maxfreshness && bytes.Equal(hash, po.Hash()) {
			// The hash of the content is the same, so return immediately
			return nil
		}
		slog.Debug("hashes", "retrieved", hash, "new", po.Hash())

		// The row has to be updated.
		// We do not have to update the id and version fields.
		err = tmf.UpdateInStorage(dbconn, po)
		if err != nil {
			return err
		}

		slog.Debug("Updated row", "id", po.ID)

		return nil // Skip inserting if the row already exists
	}

	// There was no record with the same id and version, so insert the full object
	err = tmf.InsertInStorage(dbconn, po)
	if err != nil {
		return err
	}

	slog.Debug("Inserted row", "id", po.ID)

	return nil
}

// RetrieveOrUpdateObject retrieves an object from the local database or from the server if it is not in the local database.
// The function returns the object and a boolean indicating if the object was retrieved from the local database.
func (tmf *TMFdb) RetrieveOrUpdateObject(dbconn *sqlite.Conn, href string, oid string, organization string, location AccessType) (localpo *TMFObject, local bool, err error) {

	if href == "urn:ngsi-ld:product-specification:34639868-1d29-4c71-b868-d8e5c2cacaff" {
		slog.Error("")
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
			localpo, err = localpo.UpdateInMemoryWithOwner(oid, organization)
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
	remotepo, err = remotepo.UpdateInMemoryWithOwner(oid, organization)
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

func (tmf *TMFdb) visitObjectMap(currentObject map[string]any, oid string, organization string, indent int) {

	// A map object can contain a href field that points to another object
	// In this case we retrieve the object and visit the retrieved object, if it was not retrieved before
	// For other map objects we print the relevant fields
	if currentObject["href"] != nil {
		if currentObject["role"] != nil {
			_ = currentObject["role"].(string)
		}
		href := currentObject["href"].(string)
		fmt.Printf("%shref: %v\n", indentStr(indent), href)
		if !visitedObjects[href] {
			visitedObjects[href] = true
			remoteObj, _, err := tmf.RetrieveOrUpdateObject(nil, href, oid, organization, LocalOrRemote)
			if err != nil {
				slog.Error(err.Error())
			} else {
				tmf.visitObjectMap(remoteObj.ContentMap, oid, organization, indent+3)
			}
		}
	}

	for k, v := range currentObject {
		switch v := v.(type) {
		case string:
			switch k {
			case "href":
				// Skip the href field
				continue
			case "startDateTime", "lifecycleStatus", "version", "lastUpdate", "created", "updated", "id", "externalReference":
				fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
			case "role":
				fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
			default:
				fmt.Printf("%s%s: %T\n", indentStr(indent), k, v)
			}

		case float64:
			fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
		case bool:
			fmt.Printf("%s%s: %v\n", indentStr(indent), k, v)
		case map[string]any:
			fmt.Printf("%s%s:\n", indentStr(indent), k)
			tmf.visitObjectMap(v, oid, organization, indent+3)
		case []any:
			fmt.Printf("%s%s: [\n", indentStr(indent), k)
			tmf.visitArray(v, oid, organization, indent+3)
			fmt.Printf("%s]\n", indentStr(indent))
		default:
			fmt.Printf("%s%s: %T\n", indentStr(indent), k, v)
		}
	}
}

func (tmf *TMFdb) visitArray(arr []any, oid string, organization string, indent int) {
	for i, v := range arr {
		switch v := v.(type) {
		case string:
			if v == "role" {
				fmt.Printf("%s%d: %v\n", indentStr(indent), i, v)
			} else {
				fmt.Printf("%s%d: %T\n", indentStr(indent), i, v)
			}

		case float64:
			fmt.Printf("%s%d: %v\n", indentStr(indent), i, v)
		case bool:
			fmt.Printf("%s%d: %v\n", indentStr(indent), i, v)
		case map[string]any:
			fmt.Printf("%s%d:\n", indentStr(indent), i)
			tmf.visitObjectMap(v, oid, organization, indent+3)
		case []any:
			fmt.Printf("%s%d:\n", indentStr(indent), i)
			tmf.visitArray(v, oid, organization, indent+3)
		default:
			fmt.Printf("%s%d: %T\n", indentStr(indent), i, v)
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
		log.Printf("unknown object type: %s\n", objectType)
		return nil, fmt.Errorf("unknown object type: %s", objectType)
	}

	// Get the object from the server
	res, err := http.Get(tmf.domeServer + pathPrefix + href)
	if err != nil {
		log.Printf("error %s retrieving: %s\n", err, pathPrefix+href)
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
