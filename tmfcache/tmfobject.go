// Package tmfcache provides functionality for caching TMForum (TMF) objects in a local SQLite database.
//
// It defines the TMFObject interface, a generic TMFGeneralObject implementation, and related
// functions for creating, validating, storing, and retrieving TMF objects. The package also
// includes utilities for managing a SQLite database used as a persistent cache, including
// schema definition, table creation, and data access methods.
//
// The primary goal is to provide a mechanism for efficiently caching TMF objects retrieved
// from external sources, reducing latency and improving system performance. The cache supports
// upsert operations, freshness checks, and content-based hashing for efficient data management.
//
// Key components:
//
//   - TMFObject: An interface defining the common methods for all TMF object types.
//   - TMFGeneralObject: A generic implementation of the TMFObject interface suitable for
//     representing various TMForum objects.
//   - Database management functions: Functions for creating, deleting, and vacuuming the
//     SQLite database tables.
//   - Data access functions: Functions for inserting, updating, retrieving, and deleting
//     TMF objects from the database.
//   - Utility functions: Functions for validating TMF objects, calculating hashes, and
//     building SQL queries.
//
// The package leverages several external libraries, including:
//
//   - github.com/goccy/go-json: For JSON serialization and deserialization.
//   - github.com/hesusruiz/domeproxy/config: For configuration settings.
//   - github.com/hesusruiz/domeproxy/internal/errl: For error handling.
//   - github.com/huandu/go-sqlbuilder: For building SQL queries.
//   - zombiezen.com/go/sqlite: For interacting with the SQLite database.
//
// The package is designed to be used in conjunction with a TMForum-compliant system, providing
// a local caching layer to improve performance and reduce reliance on external data sources.
package tmfcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/errl"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	sqlb "github.com/huandu/go-sqlbuilder"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// This file implements the local database to be used for the persistent cache of TMForum objects.
// The functions in this file do not refresh the database, they only read from it or write to it.

type TMFObject interface {
	// Setters and getters for the fields
	SetID(id string)
	GetID() string
	SetHref(href string)
	GetHref() string
	GetResourceName() string
	SetUpdated(updated int64)
	GetUpdated() int64
	SetName(name string)
	GetName() string
	SetDescription(description string)
	GetDescription() string
	SetLifecycleStatus(lifecycleStatus string)
	GetLifecycleStatus() string
	SetVersion(version string)
	GetVersion() string
	SetLastUpdate(lastUpdate string)
	GetLastUpdate() string
	SetContentAsMap(contentAsMap map[string]any)
	GetContentAsMap() map[string]any
	SetContentAsJSON(contentAsJSON []byte)
	GetContentAsJSON() []byte
	SetOrganization(organization string)
	GetOrganization() string
	SetOrganizationIdentifier(organizationIdentifier string)
	GetOrganizationIdentifier() string
	SetSeller(href string, sellerDid string)
	GetSeller() string
	SetBuyer(href string, buyerDid string)
	GetBuyer() string
	SetSellerOperator(href string, sellerOperatorDid string)
	GetSellerOperator() string
	SetBuyerOperator(href string, buyerOperatorDid string)
	GetBuyerOperator() string
	GetIDMID() (organizationIdentifier string, organizationName string, err error)

	// Marshalling and Unmarshalling
	String() string
	// MarshalJSON() ([]byte, error)
	// UnmarshalJSON(data []byte) error

	// FromMap(oMap map[string]any) error
	Validate() errl.ValidationMessages

	// Hashes
	Hash() []byte
	ETag() string

	// Storage related
	LocalInsertInStorage(dbconn *sqlite.Conn) error
	LocalUpdateInStorage(dbconn *sqlite.Conn) error
	LocalUpsertTMFObject(dbconn *sqlite.Conn, maxFreshness int) error

	// Owner-related
	SetOwner(organizationIdentifier string, organization string, href string)
	Owner() (did string, name string, href string)
}

// TMFGeneralObject is the in-memory representation of a TMForum object.
//
// TMFGeneralObject can represent any arbitrary TMForum object, where the most important fields are in the struct.
// The whole object is always up-to-date in ContentAsMap and ContentAsJSON, to enable fast saving and retrieving
// from the database used as cache.
type TMFGeneralObject struct {
	ID                     string                  `json:"id"`           // Required: the id of the object
	Href                   string                  `json:"href"`         // Required: the href of the object
	ResourceName           string                  `json:"resourceName"` // Required: The type of resource
	Name                   string                  `json:"name"`
	Description            string                  `json:"description"`
	LifecycleStatus        string                  `json:"lifecycleStatus"`
	Version                string                  `json:"version"`
	LastUpdate             string                  `json:"lastUpdate"`
	ContentAsMap           map[string]any          `json:"-"` // The content of the object as a map
	ContentAsJSON          []byte                  `json:"-"` // The content of the object as a JSON byte array
	Organization           string                  `json:"organization"`
	OrganizationIdentifier string                  `json:"organizationIdentifier"`
	Seller                 string                  `json:"seller"`
	SellerHref             string                  `json:"sellerHref"`
	Buyer                  string                  `json:"buyer"`
	SellerOperator         string                  `json:"sellerOperator"`
	BuyerOperator          string                  `json:"buyerOperator"`
	RelatedParty           []RelatedPartyRef       `json:"relatedParty"`
	Updated                int64                   `json:"updated"`
	Messages               errl.ValidationMessages `json:"-"`
}

// Sentinel to make sure we implement the complete TMFObject interface
var _ TMFObject = (*TMFGeneralObject)(nil)

type RelatedPartyRef struct {
	Id   string `json:"id"`
	Href string `json:"href"`
	Role string `json:"role"`
	Did  string `json:"did"`
}

var (
	ErrorSellerEmpty         = errors.New("seller is empty")
	ErrorBuyerEmpty          = errors.New("buyer is empty")
	ErrorSellerOperatorEmpty = errors.New("seller operator is empty")
	ErrorBuyerOperatorEmpty  = errors.New("buyer operator is empty")
	ErrorLastUpdateEmpty     = errors.New("lastUpdate is empty")
)

var Verbose = false

/*
TMFObjectFromMap creates a TMFObject from a map[string]any.

It performs several operations:
  - Checks for the presence of a non-empty "id" field.
  - Determines the resource name, either from the "resourceName" field or by deducing it from the ID (in a DOME context).
  - Performs a sanity check on the input map using tmfObjectSanityCheck.
  - For Category objects, assigns the organization identifier and name to the DOME operator.
  - Extracts common TMF object fields (href, name, description, lifecycleStatus, version, lastUpdate, updated, organizationIdentifier, organization) from the map.
  - Assigns the extracted fields to the corresponding fields in the TMFObject.
  - Stores the entire input map in the TMFObject's ContentAsMap field.

Returns:
  - A TMFObject representing the data in the map, or nil if an error occurs.
  - An error if any of the checks fail or if the ID is missing.
*/
func TMFObjectFromMap(inputMap map[string]any) (TMFObject, error) {
	var err error
	po := &TMFGeneralObject{}

	id, _ := inputMap["id"].(string)
	if len(id) == 0 {
		return nil, errl.Errorf("id is nil or not a string: %v", inputMap["id"])
	}

	// Get the type of object if it is not set. The only way is to deduce it from the ID.
	// But this only works in DOME context, where the ID is a URN.
	resourceName := jpath.GetString(inputMap, "resourceName")
	if len(resourceName) == 0 {
		resourceName, err = config.FromIdToResourceName(po.ID)
		if err != nil {
			return nil, errl.Error(err)
		}
	}

	// Make sure the compulsory attributes exist and that they have sensible values
	err = tmfObjectSanityCheck(inputMap, false)
	if err != nil {
		slog.Error("invalid object", slogor.Err(err))
		PrettyPrint(inputMap)
		return nil, errl.Error(err)
	}

	// Special treatment for Category objects, which do not belong to any Seller or Buyer.
	// We assign them to the Operator of the ecosystem.
	if resourceName == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	// Extract the fields which are common to all TMF objects
	href, _ := inputMap["href"].(string)
	name, _ := inputMap["name"].(string)
	description, _ := inputMap["description"].(string)
	lifecycleStatus, _ := inputMap["lifecycleStatus"].(string)
	version, _ := inputMap["version"].(string)
	lastUpdate, _ := inputMap["lastUpdate"].(string)
	updated, _ := inputMap["updated"].(int64)

	// TODO: get rid of these fields, when Seller and SellerOperator are fully implemented
	organizationIdentifier, _ := inputMap["organizationIdentifier"].(string)
	organization, _ := inputMap["organization"].(string)

	// Assign the map fields to the struct
	po.ID = id
	po.Href = href
	po.ResourceName = resourceName
	po.Name = name
	po.Description = description
	po.LifecycleStatus = lifecycleStatus
	po.Version = version
	po.LastUpdate = lastUpdate
	po.OrganizationIdentifier = organizationIdentifier
	po.Organization = organization
	po.Updated = updated

	// Store the whole map, with any updated contents
	po.ContentAsMap = inputMap

	return po, nil

}

func (po *TMFGeneralObject) Validate() errl.ValidationMessages {

	var errorList []error
	if po.Seller == "" {
		errorList = append(errorList, ErrorSellerEmpty)
		po.Messages.Add(errl.WarnM, "seller is empty")
	}
	if po.SellerOperator == "" {
		errorList = append(errorList, ErrorSellerOperatorEmpty)
	}
	if po.Buyer == "" {
		errorList = append(errorList, ErrorBuyerEmpty)
	}
	if po.BuyerOperator == "" {
		errorList = append(errorList, ErrorBuyerOperatorEmpty)
	}
	if po.LastUpdate == "" {
		errorList = append(errorList, errl.Errorf("lastUpdate is empty"))
	}

	return po.Messages
}

// TMFObjectFromBytes unmarshals a byte slice into a TMFObject.
// It first unmarshals the byte slice into a map[string]any, then converts the map to a TMFObject using FromMapExt.
// It returns the TMFObject and an error, if any.
//
// Parameters:
//
//	content: A byte slice containing the JSON representation of the TMFObject.
//
// Returns:
//
//	TMFObject: The unmarshaled TMFObject.
//	error: An error, if any occurred during unmarshaling or conversion.
func TMFObjectFromBytes(content []byte, resourceName string) (TMFObject, error) {
	var oMap map[string]any
	var err error

	err = json.Unmarshal(content, &oMap)
	if err != nil {
		return nil, errl.Error(err)
	}

	// Make sure the resourceName is set in the map
	oMap["resourceName"] = resourceName

	tmfObject, err := TMFObjectFromMap(oMap)
	if err != nil {
		return nil, errl.Error(err)
	}

	return tmfObject, nil
}

func localOrganizationByDid(dbconn *sqlite.Conn, did string) (TMFObject, error) {
	const selectSQL = `SELECT * FROM tmfobject WHERE resource = 'organization' AND organizationIdentifier = :organizationIdentifier ORDER BY version DESC LIMIT 1;`
	stmt, err := dbconn.Prepare(selectSQL)
	if err != nil {
		return nil, errl.Error(err)
	}
	defer stmt.Reset()

	stmt.SetText(":organizationIdentifier", did)

	hasRow, err := stmt.Step()
	if err != nil {
		return nil, errl.Errorf("retrieving organization %s: %w", did, err)
	}

	if !hasRow {
		return nil, errl.Errorf("organization %s not found: %w", did, ErrorNotFound)
	}

	// Even if we store it also in the db, the map representation of the object is always
	// built from the JSON representation in the content field.
	// The system ensures that this field is in synch with the in-memory fields of the struct.
	var content = make([]byte, stmt.GetLen("content"))
	stmt.GetBytes("content", content)

	updated := stmt.GetInt64("updated")

	tmf, err := TMFObjectFromBytes(content, config.Organization)

	// var tmf *TMFGeneralObject
	// err = json.Unmarshal(content, &tmf)
	if err != nil {
		return nil, errl.Error(err)
	}

	tmf.SetUpdated(updated)

	return tmf, nil

}

// localOrganizationByIdRetrieveOrUpdate retrieves a TMFObject representing an organization by its ID from the local SQLite database.
// If the organization's identifier is missing (because the object is an old version), it attempts to
// update the object in the database with information found in the object's "externalReference" field.
// Returns the TMFObject (as *TMFGeneralObject) and an error if any step fails.
//
// Parameters:
//   - dbconn: SQLite database connection.
//   - id: The TMF unique identifier of the object.
//
// Returns:
//   - TMFObject: The retrieved or updated TMFObject representing the organization.
//   - error: An error if retrieval or update fails, or if the object is invalid.
func localOrganizationByIdRetrieveOrUpdate(dbconn *sqlite.Conn, id string) (TMFObject, error) {

	// Be optimistic and do not start a database transaction yet.
	// If the object has already the modern format, we can return it directly.

	oo, _, err := LocalRetrieveTMFObject(dbconn, id, config.Organization, "")
	if err != nil {
		return nil, errl.Error(err)
	}

	// If the Organization object already has the organizationIdentifier field set, we can return it directly
	if oo.GetOrganizationIdentifier() != "" {
		return oo, nil
	}

	// At this point, we know that the object is an old version, so we need to update it.
	// We start a db transaction and read again the object, just in case there are concurrent updates.

	// Start a SAVEPOINT and defer its Commit/Rollback
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Retrieve again the object inside the transaction, to make sure we operate with the latest data
	oo, _, err = LocalRetrieveTMFObject(dbconn, id, config.Organization, "")
	if err != nil {
		return nil, errl.Error(err)
	}

	tmf, _ := oo.(*TMFGeneralObject)
	if tmf == nil || tmf.ResourceName != config.Organization {
		return nil, errl.Errorf("invalid object type")
	}

	organizationIdentifier := tmf.OrganizationIdentifier
	if organizationIdentifier != "" {
		return oo, nil
	}

	organizationName := tmf.Organization

	mustUpdate := false

	// Extract identification info if it does not already include it
	if organizationIdentifier == "" {

		// Inside Organization, the array externalReference contains the ID of the organization
		ownerReferences, _ := tmf.ContentAsMap["externalReference"].([]any)
		if len(ownerReferences) == 0 {
			return nil, errl.Errorf("externalReference is nil or not a list")
		}

		// The externalReference array must contain an entry with a map named "externalReferenceType"
		// where one of the keys is "idm_id".
		// We look at all entries in the array to find the one with "externalReferenceType" = "idm_id"
		for _, extRef := range ownerReferences {

			extRefMap, ok := extRef.(map[string]any)
			if !ok {
				continue
			}
			externalReferenceType, ok := extRefMap["externalReferenceType"].(string)
			if !ok {
				continue
			}

			if strings.ToLower(externalReferenceType) == "idm_id" {

				vatId, _ := extRefMap["name"].(string)
				if len(vatId) == 0 {
					slog.Error("GetIDMID: externalReference 'name' is nil or not a string", "resource", id)
					continue
				}
				if len(vatId) > 0 && !strings.HasPrefix(vatId, "did:elsi:") {
					organizationIdentifier = "did:elsi:" + vatId
				}

				organizationName = tmf.ContentAsMap["tradingName"].(string)
				if len(organizationName) == 0 {
					slog.Warn("GetIDMID: externalReference 'tradingName' is nil or not a string", "resource", id)
					out, err := json.Marshal(tmf.ContentAsMap)
					if err == nil {
						fmt.Println(string(out))
					}
				}

			}
		}

		// If we found the organizationIdentifier, update the db
		if len(organizationIdentifier) > 0 {
			mustUpdate = true
			tmf.SetOrganizationIdentifier(organizationIdentifier)
			tmf.SetOrganization(organizationName)

		}

	}

	if mustUpdate {
		err = tmf.LocalUpdateInStorage(dbconn)
		if err != nil {
			return nil, errl.Error(err)
		}
	}

	return tmf, nil

}

// FromMapExt populates a TMFGeneralObject from a map[string]any.
// It performs several operations:
//   - It retrieves the 'id' from the map, or uses the existing ID if already set.
//   - It deduces the resource name from the ID using config.FromIdToResourceName.
//   - It performs a sanity check on the input map using tmfObjectSanityCheck.
//   - For Category resources, it assigns the DOME Operator as the organization.
//   - It extracts common TMF object fields (href, name, description, etc.) from the map.
//   - It assigns the extracted fields to the TMFGeneralObject.
//   - It stores the entire map in the ContentAsMap field.
//
// Parameters:
//
//	inputMap: A map[string]any containing the data to populate the TMFGeneralObject.
//
// Returns:
//
//	A TMFObject interface, which is the populated TMFGeneralObject, or an error if any operation fails.
//
// Errors:
//   - If the 'id' is missing or not a string.
//   - If config.FromIdToResourceName fails.
//   - If tmfObjectSanityCheck fails.
//   - If JSON marshaling fails during error handling.
// func (po *TMFGeneralObject) FromMapExt(inputMap map[string]any) (TMFObject, error) {
// 	var err error

// 	// Allocate a receiver object if it is nil, to allow unmarshal without previous allocation
// 	if po == nil {
// 		po = &TMFGeneralObject{}
// 	}

// 	id := po.ID
// 	if id == "" {
// 		id, _ = inputMap["id"].(string)
// 		if len(id) == 0 {
// 			return nil, errl.Errorf("id is nil or not a string: %v", inputMap["id"])
// 		}
// 	}

// 	// Get the type of object if it is not set. The only way is to deduce it from the ID.
// 	// But this only works in DOME context, where the ID is a URN.
// 	resourceName := jpath.GetString(inputMap, "resourceName")
// 	if len(resourceName) == 0 {
// 		resourceName, err = config.FromIdToResourceName(po.ID)
// 		if err != nil {
// 			return nil, errl.Error(err)
// 		}
// 	}

// 	// Make sure the compulsory attributes exist and that they have sensible values
// 	err = tmfObjectSanityCheck(inputMap, false)
// 	if err != nil {
// 		slog.Error("invalid object", slogor.Err(err))
// 		PrettyPrint(inputMap)
// 		return nil, errl.Error(err)
// 	}

// 	// Special treatment for Category objects, which do not belong to any Seller or Buyer.
// 	// We assign them to the Operator of the ecosystem.
// 	if resourceName == config.Category {
// 		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
// 		po.SetOrganization(config.DOMEOperatorName)
// 	}

// 	// Extract the fields which are common to all TMF objects
// 	href, _ := inputMap["href"].(string)
// 	name, _ := inputMap["name"].(string)
// 	description, _ := inputMap["description"].(string)
// 	lifecycleStatus, _ := inputMap["lifecycleStatus"].(string)
// 	version, _ := inputMap["version"].(string)
// 	lastUpdate, _ := inputMap["lastUpdate"].(string)
// 	updated, _ := inputMap["updated"].(int64)

// 	// TODO: get rid of these fields, when Seller and SellerOperator are fully implemented
// 	organizationIdentifier, _ := inputMap["organizationIdentifier"].(string)
// 	organization, _ := inputMap["organization"].(string)

// 	// Assign the map fields to the struct
// 	po.ID = id
// 	po.Href = href
// 	po.ResourceName = resourceName
// 	po.Name = name
// 	po.Description = description
// 	po.LifecycleStatus = lifecycleStatus
// 	po.Version = version
// 	po.LastUpdate = lastUpdate
// 	po.OrganizationIdentifier = organizationIdentifier
// 	po.Organization = organization
// 	po.Updated = updated

// 	// Store the whole map, with any updated contents
// 	po.ContentAsMap = inputMap

// 	return po, nil

// }

type FixLevel int

const (
	FixNone FixLevel = iota
	FixLow
	FixMedium
	FixHigh
)

func (po *TMFGeneralObject) ProcessRelatedParties(dbconn *sqlite.Conn, tmf TMFObject, level FixLevel) (fixed bool, err error) {

	if level == FixNone {
		return false, nil
	}

	if po == nil {
		return false, errl.Errorf("object is nil")
	}

	id := po.GetID()
	resource := po.GetResourceName()
	if resource == config.ProductOffering {
		_ = resource

	}

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties, _ := po.ContentAsMap["relatedParty"].([]any)

	for _, rp := range relatedParties {

		// Convert entry to a map
		rpMap, _ := rp.(map[string]any)
		if len(rpMap) == 0 {
			po.Messages.Addf(errl.ErrorM, "invalid relatedParty entry for object: %s", id)
			// if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
			// 	fmt.Println(string(out))
			// }
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
			slog.Error("no id or href in related party", "tmfObject", id)
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
			referredResourceName, err := config.FromIdToResourceName(rpId)
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

				org, err := localOrganizationByIdRetrieveOrUpdate(dbconn, rpHref)
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
		if rpRole == "owner" && po.ResourceName != config.Organization {
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
			po.Seller = rpDid
			po.SellerHref = rpHref
			po.OrganizationIdentifier = rpDid
			po.Organization = rpOrganizationName

		case "buyer":
			po.Buyer = rpDid

		case "selleroperator":
			po.SellerOperator = rpDid

		case "buyeroperator":
			po.BuyerOperator = rpDid
		}

	}

	if fixed {
		PrettyPrint(po.ContentAsMap["relatedParty"])
	}

	return fixed, nil

}

func FixTMFObject(dbconn *sqlite.Conn, tmf TMFObject, level FixLevel) (fixed bool, err error) {

	if level == FixNone {
		return false, nil
	}

	switch pp := tmf.(type) {
	case *TMFGeneralObject:
		_ = pp.ContentAsJSON
	case *TMFOrganization:
		_ = pp.ContentAsJSON
	default:
		return false, errl.Errorf("invalid object type")
	}

	po, _ := tmf.(*TMFGeneralObject)
	if po == nil {
		return false, errl.Errorf("invalid object type")
	}

	resource := po.GetResourceName()

	if resource == config.Category {
		return false, nil
	}

	if resource == config.Organization {

		did, organizationName, err := po.GetIDMID()
		if err != nil {
			return false, errl.Error(err)
		}
		identificationId := jpath.GetString(po.ContentAsMap, "organizationIdentification.identificationId")
		if identificationId == did {
			return false, nil
		}

		organizationIdentification := jpath.GetMap(po.ContentAsMap, "organizationIdentification")
		organizationIdentification["identificationId"] = did
		organizationIdentification["identificationType"] = elsiIdentificationType
		organizationIdentification["issuingAuthority"] = eIDASAuthority

		po.ContentAsMap["organizationIdentification"] = organizationIdentification

		po.OrganizationIdentifier = did
		po.Organization = organizationName

		PrettyPrint(po.ContentAsMap)

		return true, nil
	}

	if resource == config.ProductOffering {
		// Product Offerings must have a Product Specification
		productSpecification, _ := po.ContentAsMap["productSpecification"].(map[string]any)
		if len(productSpecification) == 0 {
			return false, errl.Errorf("productOffering must have a productSpecification sub-resource")
		}
	}

	fixed, err = po.ProcessRelatedParties(dbconn, tmf, level)
	if err != nil {
		return fixed, errl.Error(err)
	}

	return fixed, nil

}

func (po *TMFGeneralObject) MarshalJSON() ([]byte, error) {

	// We assume that the ContentAsMap field is always up to date
	content, err := json.Marshal(po.ContentAsMap)
	if err != nil {
		return nil, err
	}

	po.ContentAsJSON = content

	return content, nil

}

// String return the flat string representation of the object
func (po *TMFGeneralObject) String() string {
	s1 := fmt.Sprintf("ID: %s\nType: %s\nName: %s\nLifecycleStatus: %s\nVersion: %s\nLastUpdate: %s\n", po.ID, po.ResourceName, po.Name, po.LifecycleStatus, po.Version, po.LastUpdate)

	out, err := json.MarshalIndent(po.ContentAsMap, "", "  ")
	if err != nil {
		return s1
	}

	return s1 + string(out)

}

func tmfObjectSanityCheck(oMap map[string]any, strict bool) error {
	var errorList []error

	// id MUST exist
	id := jpath.GetString(oMap, "id")
	if len(id) == 0 {
		return errl.Errorf("id is nil or not a string: %v", oMap["id"])
	}

	// href MUST exist
	href := jpath.GetString(oMap, "href")
	if len(href) == 0 {
		return errl.Errorf("href is nil or not a string: %v", oMap["href"])
	}

	if strict {

		if _, ok := oMap["seller"]; !ok {
			errorList = append(errorList, ErrorSellerEmpty)
		}
		if _, ok := oMap["sellerOperator"]; !ok {
			errorList = append(errorList, ErrorSellerOperatorEmpty)
		}
		if _, ok := oMap["buyer"]; !ok {
			errorList = append(errorList, ErrorBuyerEmpty)
		}
		if _, ok := oMap["buyerOperator"]; !ok {
			errorList = append(errorList, ErrorBuyerOperatorEmpty)
		}
		if _, ok := oMap["lastUpdate"]; !ok {
			errorList = append(errorList, errl.Errorf("lastUpdate is empty"))
		}

		return errors.Join(errorList...)

	}

	return nil
}

// The following are typed methods to get values from the ContentAsMap field, to facilitate life to the user.
// path is a dot-separated path from the root of the object to the desired internal field.

func (po *TMFGeneralObject) SetID(id string) {
	if !strings.HasPrefix(id, "urn:ngsi-ld:") {
		return
	}
	po.ID = id
	po.Href = id
	po.ContentAsMap["id"] = id
	po.ContentAsMap["href"] = id
}
func (po *TMFGeneralObject) GetID() string {
	return po.ID
}

func (po *TMFGeneralObject) SetHref(href string) {
	po.SetID(href)
}
func (po *TMFGeneralObject) GetHref() string {
	return po.Href
}

func (po *TMFGeneralObject) GetResourceName() string {
	return po.ResourceName
}

func (po *TMFGeneralObject) SetUpdated(updated int64) {
	po.Updated = updated
}
func (po *TMFGeneralObject) GetUpdated() int64 {
	return po.Updated
}

func (po *TMFGeneralObject) SetName(name string) {
	po.Name = name
	po.ContentAsMap["name"] = name
}
func (po *TMFGeneralObject) GetName() string {
	return po.Name
}

func (po *TMFGeneralObject) SetDescription(description string) {
	po.Description = description
	po.ContentAsMap["description"] = description
}
func (po *TMFGeneralObject) GetDescription() string {
	return po.Description
}

func (po *TMFGeneralObject) SetLifecycleStatus(lifecycleStatus string) {
	po.LifecycleStatus = lifecycleStatus
	po.ContentAsMap["lifecycleStatus"] = lifecycleStatus
}
func (po *TMFGeneralObject) GetLifecycleStatus() string {
	return po.LifecycleStatus
}

func (po *TMFGeneralObject) SetVersion(version string) {
	po.Version = version
	po.ContentAsMap["version"] = version
}
func (po *TMFGeneralObject) GetVersion() string {
	return po.Version
}

func (po *TMFGeneralObject) SetLastUpdate(lastUpdate string) {
	po.LastUpdate = lastUpdate
	po.ContentAsMap["lastUpdate"] = lastUpdate
}
func (po *TMFGeneralObject) GetLastUpdate() string {
	return po.LastUpdate
}

func (po *TMFGeneralObject) SetContentAsMap(contentAsMap map[string]any) {
	po.ContentAsMap = contentAsMap
}
func (po *TMFGeneralObject) GetContentAsMap() map[string]any {
	return po.ContentAsMap
}

func (po *TMFGeneralObject) SetContentAsJSON(contentAsJSON []byte) {
	po.ContentAsJSON = contentAsJSON
}
func (po *TMFGeneralObject) GetContentAsJSON() []byte {
	// Update the content field
	poJSON, err := json.Marshal(po.ContentAsMap)
	if err != nil {
		return nil
	}

	po.ContentAsJSON = poJSON
	return po.ContentAsJSON
}

func (po *TMFGeneralObject) SetOrganization(organization string) {
	po.Organization = organization
}
func (po *TMFGeneralObject) GetOrganization() string {
	return po.Organization
}

func (po *TMFGeneralObject) SetOrganizationIdentifier(organizationIdentifier string) {
	po.OrganizationIdentifier = organizationIdentifier
}

func (po *TMFGeneralObject) GetOrganizationIdentifier() string {
	return po.OrganizationIdentifier
}

func (po *TMFGeneralObject) updateRelatedPartyRef(role string, did string, href string) {

	found := false
	rpMapList, _ := po.ContentAsMap["relatedParty"].([]any)
	for _, rpMap := range rpMapList {
		rpMap, _ := rpMap.(map[string]any)
		if rpMap["role"] == role {
			rpMap["@referredType"] = config.Organization
			rpMap["href"] = href
			rpMap["id"] = href
			rpMap["did"] = did
			rpMap["name"] = did
			found = true
			break
		}
	}
	if !found {
		po.ContentAsMap["relatedParty"] = append(rpMapList, map[string]any{
			"@referredType": config.Organization,
			"role":          role,
			"did":           did,
			"name":          did,
			"href":          href,
			"id":            href,
		})
	}

}

func (po *TMFGeneralObject) SetSeller(href string, sellerDid string) {
	if len(sellerDid) > 0 && !strings.HasPrefix(sellerDid, "did:elsi:") {
		sellerDid = "did:elsi:" + sellerDid
	}
	po.Seller = sellerDid
	// TODO: update properly the seller in the map

	po.updateRelatedPartyRef("seller", sellerDid, href)

}

func (po *TMFGeneralObject) GetSeller() string {
	return po.Seller
}

func (po *TMFGeneralObject) SetBuyer(href string, buyerDid string) {
	if len(buyerDid) > 0 && !strings.HasPrefix(buyerDid, "did:elsi:") {
		buyerDid = "did:elsi:" + buyerDid
	}
	po.Buyer = buyerDid

	po.updateRelatedPartyRef("buyer", buyerDid, href)
}

func (po *TMFGeneralObject) GetBuyer() string {
	return po.Buyer
}

func (po *TMFGeneralObject) SetSellerOperator(href string, sellerOperatorDid string) {
	if len(sellerOperatorDid) > 0 && !strings.HasPrefix(sellerOperatorDid, "did:elsi:") {
		sellerOperatorDid = "did:elsi:" + sellerOperatorDid
	}
	po.SellerOperator = sellerOperatorDid

	po.updateRelatedPartyRef("sellerOperator", sellerOperatorDid, href)
}

func (po *TMFGeneralObject) GetSellerOperator() string {
	return po.SellerOperator
}

func (po *TMFGeneralObject) SetBuyerOperator(href string, buyerOperatorDid string) {
	if len(buyerOperatorDid) > 0 && !strings.HasPrefix(buyerOperatorDid, "did:elsi:") {
		buyerOperatorDid = "did:elsi:" + buyerOperatorDid
	}
	po.BuyerOperator = buyerOperatorDid

	po.updateRelatedPartyRef("buyerOperator", buyerOperatorDid, href)
}

func (po *TMFGeneralObject) GetBuyerOperator() string {
	return po.BuyerOperator
}

// ****************

// These are setters which update the struct and the associated ContentAsMap.

func (po *TMFGeneralObject) SetOwner(organizationIdentifier string, organization string, href string) {
	po.OrganizationIdentifier = organizationIdentifier
	po.Organization = organization

	po.SetSeller(href, organizationIdentifier)

}

// For most resources, the Owner is the Seller.
// Specialized object can implement a different Owner logic.
func (po *TMFGeneralObject) Owner() (did string, name string, href string) {
	return po.Seller, po.Organization, po.SellerHref
}

// Hash calculates the hash of the canonical JSON representation of the object.
// It also updates the ContentAsJSON field with that JSON representation so it reflects the last status of the object.
func (po *TMFGeneralObject) Hash() []byte {
	// Update the content field
	poJSON, err := json.Marshal(po.ContentAsMap)
	if err != nil {
		return nil
	}

	po.ContentAsJSON = poJSON

	hasher := sha256.New()
	hasher.Write(po.ContentAsJSON)
	return hasher.Sum(nil)
}

func (po *TMFGeneralObject) ETag() string {
	etag := fmt.Sprintf(`"%x"`, po.Hash())
	return etag
}

// GetIDMID retrieves identification information from an Organization object
func (o *TMFGeneralObject) GetIDMID() (organizationDid string, organizationName string, err error) {

	if o.ResourceName != config.Organization {
		return "", "", errl.Errorf("resource is not an organization")
	}

	// Inside Organization, the array externalReference contains the ID of the organization
	ownerReferences, _ := o.ContentAsMap["externalReference"].([]any)
	if len(ownerReferences) == 0 {
		slog.Error("GetIDMID: externalReference is nil or not a list", "resource", o.ID)
		return "", "", errl.Errorf("externalReference is nil or not a list")
	}

	// The externalReference array must contain an entry with a map named "externalReferenceType"
	// where one of the keys is "idm_id".
	// We look at all entries in the array to find the one with "externalReferenceType" = "idm_id"
	for _, extRef := range ownerReferences {

		extRefMap, ok := extRef.(map[string]any)
		if !ok {
			continue
		}
		externalReferenceType, ok := extRefMap["externalReferenceType"].(string)
		if !ok {
			continue
		}

		if strings.ToLower(externalReferenceType) == "idm_id" {

			organizationIdentifier, _ := extRefMap["name"].(string)
			if len(organizationIdentifier) == 0 {
				slog.Error("GetIDMID: externalReference 'name' is nil or not a string", "resource", o.ID)
				continue
			}
			if len(organizationIdentifier) > 0 && !strings.HasPrefix(organizationIdentifier, "did:elsi:") {
				organizationIdentifier = "did:elsi:" + organizationIdentifier
			}

			organization, _ := o.ContentAsMap["tradingName"].(string)
			if len(organization) == 0 {
				slog.Warn("GetIDMID: externalReference 'tradingName' is nil or not a string", "resource", o.ID)
				out, err := json.Marshal(o.ContentAsMap)
				if err == nil {
					fmt.Println(string(out))
				}
			}

			return organizationIdentifier, organization, nil
		}
	}

	return "", "", errl.Error(ErrorNotFound)

}

// ******************************************************************************************
// *****************************************************************************************
// Database management.
// ******************************************************************************************
// ******************************************************************************************

// tmfobject Table Schema
//
// The `tmfobject` table serves as a local cache for TMForum objects retrieved from a remote server.
// It stores various attributes of TMF objects to allow for efficient local querying.
// The table is created with `WITHOUT ROWID` for performance reasons, using the composite primary key of `id` and `version`.
//
// # Columns
//
// `id` `TEXT` `NOT NULL`: The unique identifier of the TMF object, typically a URN like `urn:ngsi-ld:<type>:<id>`.
// `resource` `TEXT` `NOT NULL`: The name of the TMF resource type, like `productOffering` or `catalog`.
// `version` `TEXT`: The version of the TMF object. Paired with `id`, it uniquely identifies a specific version of an object.
// `organizationIdentifier` `TEXT`: The unique identifier of the organization that owns or is associated with the object.
// `organization` `TEXT`: The name of the organization associated with the `organizationIdentifier`.
//
// These are for access control
// `seller` `TEXT`: The DID of the seller party in a transaction or offering.
// `buyer` `TEXT`: The DID of the buyer party in a transaction or offering.
// `sellerOperator` `TEXT`: The DID of the operator for the seller party.
// `buyerOperator` `TEXT`: The DID of the operator for the buyer party.
//
// `name` `TEXT` `NOT NULL`: The human-readable name of the TMF object.
// `description` `TEXT`: A textual description of the TMF object.
// `lifecycleStatus` `TEXT`: The current stage in the lifecycle of the object, for example, `Launched` or `Active`.
// `lastUpdate` `TEXT` `NOT NULL`: A timestamp indicating when the object was last updated in the source system.
//
// `content` `BLOB` `NOT NULL`: The full JSON payload of the TMF object. We do some queries in the object when needed.
// `hash` `BLOB`: A SHA256 hash of the `content` field, used to quickly detect changes in the object's data.
//
// `created` `INTEGER`: A Unix timestamp representing when the record was first inserted into this cache table.
// `updated` `INTEGER`: A Unix timestamp representing when this record was last updated in the cache.
//
// # Indexes
//
// `CREATE INDEX IF NOT EXISTS idx_hash ON tmfobject (hash);`
//
// An index is created on the `hash` column to speed up lookups based on the object's content hash.
// This is useful for quickly checking if an object with the same content already exists in the cache.
const createTMFTableSQL = `
CREATE TABLE IF NOT EXISTS tmfobject (
	"id" TEXT NOT NULL,
	"resource" TEXT NOT NULL,
	"version" TEXT DEFAULT '0.1',
	"organizationIdentifier" TEXT,
	"organization" TEXT,
	"seller" TEXT,
	"buyer" TEXT,
	"sellerOperator" TEXT,
	"buyerOperator" TEXT,
	"name" TEXT NOT NULL,
	"description" TEXT,
	"lifecycleStatus" TEXT,
	"lastUpdate" TEXT NOT NULL,
	"content" BLOB NOT NULL,
	"hash" BLOB,
	"created" INTEGER,
	"updated" INTEGER,

	PRIMARY KEY ("id", "resource", "version")
);
PRAGMA journal_mode = WAL;
CREATE INDEX IF NOT EXISTS idx_hash ON tmfobject (hash);
`

const deleteTMFTableSQL = `
DROP TABLE IF EXISTS tmfobject;
`
const vacuumTMFTableSQL = `VACUUM;`

const InsertTMFObjectSQL = `INSERT INTO tmfobject (id, organizationIdentifier, organization, seller, buyer, sellerOperator, buyerOperator, resource, name, description, lifecycleStatus, version, lastUpdate, content, hash, created, updated) VALUES (:id, :organizationIdentifier, :organization, :seller, :buyer, :sellerOperator, :buyerOperator, :resource, :name, :description, :lifecycleStatus, :version, :lastUpdate, :content, :hash, :created, :updated);`

const UpdateTMFObjectSQL = `UPDATE tmfobject SET organizationIdentifier = :organizationIdentifier, organization = :organization, seller = :seller, buyer = :buyer, sellerOperator = :sellerOperator, buyerOperator = :buyerOperator, resource = :resource, name = :name, description = :description, lifecycleStatus = :lifecycleStatus, lastUpdate = :lastUpdate, content = :content, hash = :hash, updated = :updated WHERE id = :id AND version = :version;`

var ErrorNotFound = errors.New("not found")

// createTables creates the table if it does not exist
func createTables(dbpool *sqlitex.Pool) error {

	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return errl.Error(err)
	}
	defer dbpool.Put(conn)

	if err := sqlitex.ExecuteScript(conn, createTMFTableSQL, nil); err != nil {
		slog.Error("createTables", slogor.Err(err))
		return errl.Errorf("createTables: %w", err)
	}

	return nil
}

// deleteTables drops the table and performs a VACUUM to reclaim space
func deleteTables(dbpool *sqlitex.Pool) error {
	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return errl.Error(err)
	}
	defer dbpool.Put(conn)

	if err := sqlitex.ExecuteScript(conn, deleteTMFTableSQL, nil); err != nil {
		slog.Error("deleteTables", slogor.Err(err))
		return errl.Errorf("deleteTables: %w", err)
	}

	vacuumStmt, err := conn.Prepare(vacuumTMFTableSQL)
	if err != nil {
		return errl.Error(err)
	}
	defer vacuumStmt.Reset()

	_, err = vacuumStmt.Step()
	if err != nil {
		return errl.Error(err)
	}

	return nil
}

// LocalCheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func LocalCheckIfExists(
	dbconn *sqlite.Conn, id string, resource string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		return false, nil, 0, errl.Errorf("dbconn is nil")
	}

	// Check if the row already exists, with the same version
	const CheckIfExistsTMFObjectSQL = `SELECT hash, updated FROM tmfobject WHERE id = :id AND resource = :resource AND version = :version;`
	selectStmt, err := dbconn.Prepare(CheckIfExistsTMFObjectSQL)
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}
	defer selectStmt.Reset()

	selectStmt.SetText(":id", id)
	selectStmt.SetText(":resource", resource)
	selectStmt.SetText(":version", version)

	hasRow, err := selectStmt.Step()
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}

	// Each object has a hash to make sure it is the same object, even if the version is the same
	hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness = int(now - updated)

	return hasRow, hash, freshness, nil

}

// LocalCheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func LocalCheckIfExistssdfsdf(
	dbconn *sqlite.Conn, id string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		return false, nil, 0, errl.Errorf("dbconn is nil")
	}

	// Check if the row already exists, with the same version
	const CheckIfExistsTMFObjectSQL = `SELECT id, hash, updated FROM tmfobject WHERE id = :id AND version = :version;`
	selectStmt, err := dbconn.Prepare(CheckIfExistsTMFObjectSQL)
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}
	defer selectStmt.Reset()

	selectStmt.SetText(":id", id)
	selectStmt.SetText(":version", version)

	hasRow, err := selectStmt.Step()
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}

	// Each object has a hash to make sure it is the same object, even if the version is the same
	hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness = int(now - updated)

	return hasRow, hash, freshness, nil

}

// objectFromDbRecord constructs a TMFObject from a given sqlite.Stmt database record.
// It populates the TMFGeneralObject fields by extracting values from the statement,
// deserializes the "content" field from JSON into a map, and assigns both the raw JSON
// and the map representation to the object. Returns the populated TMFObject or an error
// if JSON unmarshalling fails.
func objectFromDbRecord(stmt *sqlite.Stmt) (TMFObject, error) {

	dbObject := &TMFGeneralObject{}

	dbObject.ID = stmt.GetText("id")
	dbObject.Version = stmt.GetText("version")
	dbObject.OrganizationIdentifier = stmt.GetText("organizationIdentifier")
	dbObject.Organization = stmt.GetText("organization")
	dbObject.Seller = stmt.GetText("seller")
	dbObject.Buyer = stmt.GetText("buyer")
	dbObject.SellerOperator = stmt.GetText("sellerOperator")
	dbObject.BuyerOperator = stmt.GetText("buyerOperator")
	dbObject.ResourceName = stmt.GetText("resource")
	dbObject.Name = stmt.GetText("name")
	dbObject.Description = stmt.GetText("description")
	dbObject.LifecycleStatus = stmt.GetText("lifecycleStatus")
	dbObject.LastUpdate = stmt.GetText("lastUpdate")

	dbObject.Updated = stmt.GetInt64("updated")

	// The map representation of the object is always
	// built from the JSON representation in the content field.
	// The system ensures that this field is in synch with the in-memory fields of the struct.
	var content = make([]byte, stmt.GetLen("content"))
	stmt.GetBytes("content", content)
	dbObject.ContentAsJSON = content

	var oMap map[string]any
	err := json.Unmarshal(content, &oMap)
	if err != nil {
		return nil, errl.Error(err)
	}

	dbObject.ContentAsMap = oMap

	return dbObject, nil

}

func LocalRetrieveTMFObject(dbconn *sqlite.Conn, id string, resource string, version string) (pod TMFObject, found bool, err error) {
	if dbconn == nil {
		return nil, false, errl.Errorf("dbconn is nil")
	}

	// We use a different SELECT statement depending on whether version is provided or not.
	// Except for admin users, normal users are given the latest version of the object.
	var stmt *sqlite.Stmt
	if len(version) == 0 {
		const RetrieveTMFObjectNoVersionSQL = `SELECT * FROM tmfobject WHERE id = :id AND resource = :resource ORDER BY version DESC LIMIT 1;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectNoVersionSQL)
		defer stmt.Reset()
		stmt.SetText(":id", id)
	} else {
		const RetrieveTMFObjectSQL = `SELECT * FROM tmfobject WHERE id = :id AND resource = :resource AND version = :version;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectSQL)
		defer stmt.Reset()
		stmt.SetText(":id", id)
		stmt.SetText(":version", version)
	}
	if err != nil {
		return nil, false, errl.Error(err)
	}

	hasRow, err := stmt.Step()
	if err != nil {
		slog.Error("RetrieveLocalTMFObject", "href", id, "error", err)
		return nil, false, errl.Error(err)
	}

	if !hasRow {
		return nil, false, errl.Error(ErrorNotFound)
	}

	dbObject, err := objectFromDbRecord(stmt)
	if err != nil {
		return nil, false, errl.Error(err)
	}

	return dbObject, true, nil

}

func LocalRetrieveListTMFObject(dbconn *sqlite.Conn, resource string, queryValues url.Values) (pos []TMFObject, found bool, err error) {
	if dbconn == nil {
		return nil, false, errl.Errorf("dbconn is nil")
	}

	// Build the SQL SELECT based on the query passed on the HTTP request, as specified in TMForum
	sql, args := BuildSelectFromParms(resource, queryValues)

	var resultPOs []TMFObject

	err = sqlitex.Execute(dbconn, sql, &sqlitex.ExecOptions{
		Args: args,

		// This function is called once for each record found in the database
		ResultFunc: func(stmt *sqlite.Stmt) error {

			dbObject, err := objectFromDbRecord(stmt)
			if err != nil {
				return errl.Error(err)
			}

			resultPOs = append(resultPOs, dbObject)

			return nil
		},
	})
	if err != nil {
		return nil, false, errl.Error(err)
	}

	slog.Debug("RetrieveLocalListTMFObject", "sql", sql, "args", args, "objects", resultPOs)
	return resultPOs, true, nil
}

func (po *TMFGeneralObject) LocalUpdateInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return errl.Errorf("dbconn is nil")
	}

	if po.ResourceName == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	// Calculate the hash, updating the ContentAsJSON at the same time
	hash := po.Hash()
	if hash == nil {
		return errl.Errorf("hash is nil")
	}

	updateStmt, err := dbconn.Prepare(UpdateTMFObjectSQL)
	if err != nil {
		return errl.Errorf("UpdateInStorage: %w", err)
	}
	defer updateStmt.Reset()

	// These are used for the WHERE clause
	updateStmt.SetText(":id", po.ID)
	updateStmt.SetText(":resource", po.ResourceName)
	updateStmt.SetText(":version", po.Version)

	// These are the updated fields
	updateStmt.SetText(":organizationIdentifier", po.OrganizationIdentifier)
	updateStmt.SetText(":organization", po.Organization)

	updateStmt.SetText(":seller", po.Seller)
	updateStmt.SetText(":buyer", po.Buyer)
	updateStmt.SetText(":sellerOperator", po.SellerOperator)
	updateStmt.SetText(":buyerOperator", po.BuyerOperator)

	updateStmt.SetText(":name", po.Name)
	updateStmt.SetText(":description", po.Description)
	updateStmt.SetText(":lifecycleStatus", po.LifecycleStatus)
	updateStmt.SetText(":lastUpdate", po.LastUpdate)
	updateStmt.SetBytes(":content", po.ContentAsJSON)
	updateStmt.SetBytes(":hash", hash)
	now := time.Now().Unix()
	updateStmt.SetInt64(":updated", now)

	_, err = updateStmt.Step()
	if err != nil {
		slog.Error("UpdateInStorage", "href", po.ID, "error", err)
		return errl.Errorf("UpdateInStorage: %w", err)
	}

	return nil
}

// LocalInsertInStorage inserts the TMFGeneralObject into the provided SQLite database connection.
// It provides default values for some of th efields, if they are not provided.
// The method also computes and stores the object's hash, and sets the creation and update timestamps.
// Returns an error if the database connection is nil, the hash is nil, or if any database operation fails.
func (po *TMFGeneralObject) LocalInsertInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return errl.Errorf("dbconn is nil")
	}

	if po.ResourceName == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	hash := po.Hash()
	if hash == nil {
		return errl.Errorf("hash is nil")
	}

	insertStmt, err := dbconn.Prepare(InsertTMFObjectSQL)
	if err != nil {
		return errl.Error(err)
	}
	defer insertStmt.Reset()

	insertStmt.SetText(":id", po.ID)
	insertStmt.SetText(":resource", po.ResourceName)
	insertStmt.SetText(":version", po.Version)
	insertStmt.SetText(":organizationIdentifier", po.OrganizationIdentifier)
	insertStmt.SetText(":organization", po.Organization)

	insertStmt.SetText(":seller", po.Seller)
	insertStmt.SetText(":buyer", po.Buyer)
	insertStmt.SetText(":sellerOperator", po.SellerOperator)
	insertStmt.SetText(":buyerOperator", po.BuyerOperator)

	insertStmt.SetText(":name", po.Name)
	insertStmt.SetText(":description", po.Description)
	insertStmt.SetText(":lifecycleStatus", po.LifecycleStatus)
	insertStmt.SetText(":lastUpdate", po.LastUpdate)
	insertStmt.SetBytes(":content", po.ContentAsJSON)
	insertStmt.SetBytes(":hash", hash)
	now := time.Now().Unix()
	insertStmt.SetInt64(":created", now)
	insertStmt.SetInt64(":updated", now)

	_, err = insertStmt.Step()
	if err != nil {
		slog.Error("InsertInStorage", "href", po.ID, "error", err)
		return errl.Error(err)
	}

	return nil
}

// LocalUpsertTMFObject inserts or updates a TMFGeneralObject in the local SQLite database.
// It first checks if a record with the same ID and version exists. If it exists and the data is
// fresh (based on maxFreshness) and the content hash matches, the function returns early.
// If the data is stale or the hash differs, it updates the existing record. If no such record exists,
// it inserts the object as a new row. The operation is wrapped in a SQLite SAVEPOINT for transactional safety.
//
// Parameters:
//   - dbconn: SQLite database connection.
//   - maxFreshness: Maximum allowed freshness (in seconds) for the existing record.
//
// Returns:
//   - err: An error if the operation fails, or nil on success.
func (po *TMFGeneralObject) LocalUpsertTMFObject(dbconn *sqlite.Conn, maxFreshness int) (err error) {
	if dbconn == nil {
		return errl.Errorf("dbconn is nil")
	}

	// Start a SAVEPOINT and defer its Commit/Rollback
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Get the type of object if it is not set. The only way is to deduce it from the ID.
	// But this only works in DOME context, where the ID is a URN.
	if len(po.ResourceName) == 0 {
		resourceName, err := config.FromIdToResourceName(po.ID)
		if err != nil {
			return errl.Error(err)
		}
		po.ResourceName = resourceName
	}

	// Check if the row already exists, with the same version
	objectExists, hash, freshness, err := LocalCheckIfExists(dbconn, po.ID, po.ResourceName, po.Version)
	if err != nil {
		return errl.Error(err)
	}

	// The id and version are the same, but we have to check the hash to see if we have to update the record
	if objectExists {

		fresh := freshness < maxFreshness
		newHash := po.Hash()

		// Check if the data is recent enough and the hash of the content is the same
		if fresh && bytes.Equal(hash, newHash) {
			// The hash of the content is the same, so return immediately
			slog.Debug("Upsert: row exists and fresh", "id", po.ID)
			return nil
		}

		// The row has to be updated.
		// We do not have to update the id and version fields.
		err = po.LocalUpdateInStorage(dbconn)
		if err != nil {
			return errl.Error(err)
		}

		if !fresh {
			slog.Debug("Upsert: row updated (not fresh)", "id", po.ID)
		} else {
			hashStr := fmt.Sprintf("%X", hash)
			newHashStr := fmt.Sprintf("%X", newHash)
			slog.Debug("Upsert: row updated (hash different)", "id", po.ID, "old", hashStr, "new", newHashStr)
		}

		return nil // Skip inserting if the row already exists
	}

	// There was no record with the same id and version, so insert the full object
	err = po.LocalInsertInStorage(dbconn)
	if err != nil {
		slog.Error("UpsertTMFObject", "href", po.ID, "version", po.Version, "error", err)
		return errl.Error(err)
	}

	slog.Debug("Upsert: row inserted", "id", po.ID)

	return nil
}

// BuildSelectFromParms creates a SELECT statement based on the query values.
// For objects with same id, selects the one with the latest version.
func BuildSelectFromParms(tmfResource string, queryValues url.Values) (string, []any) {

	// Default values if the user did not specify them. -1 is equivalent to no values provided.
	var limit = -1
	var offset = -1

	bu := sqlb.SQLite.NewSelectBuilder()

	// SELECT: for each object with a given id, select the latest version.
	// We use the 'max(version)' function, and will GROUP by id.
	bu.Select(
		"id",
		"max(version)",
		"organizationIdentifier",
		"organization",
		"seller",
		"buyer",
		"sellerOperator",
		"buyerOperator",
		"resource",
		"name",
		"description",
		"lifecycleStatus",
		"lastUpdate",
		"content",
		"hash",
		"created",
		"updated",
	).From("tmfobject")

	// WHERE: normally we expect the resource name of object to be specified, but we support a query for all object types
	if len(tmfResource) > 0 {
		bu.Where(bu.Equal("resource", tmfResource))
	}

	// Build the WHERE by processing the query values specified by the user
	whereClause := sqlb.NewWhereClause()
	cond := sqlb.NewCond()

	for key, values := range queryValues {

		switch key {
		case "limit":
			limitStr := queryValues.Get("limit")
			if l, err := strconv.Atoi(limitStr); err == nil {
				limit = l
			}
		case "offset":
			offsetStr := queryValues.Get("offset")
			if l, err := strconv.Atoi(offsetStr); err == nil {
				offset = l
			}
		case "lifecycleStatus":
			// Special processing because TMForum allows to specify multiple values
			// in the form 'lifecycleStatus=Launched,Active'
			var vals = []string{}
			// Allow several instances of 'lifecycleStatus' parameter in the query string
			for _, v := range values {
				parts := strings.Split(v, ",")
				// Allow for whitespace surrounding the elements
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				vals = append(vals, parts...)
			}

			// Use either an equality or an inclusion expression
			if len(vals) == 1 {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.Equal(key, sqlb.List(vals)),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In(key, sqlb.List(vals)),
				)
			}

		case "relatedParty.id", "relatedParty":
			// This is a special case, given that it is so frequent, we perform an optimization
			whereClause.AddWhereExpr(
				cond.Args,
				cond.Equal("organization", values[0]),
			)

		case "seller", "buyer", "sellerOperator", "buyerOperator":
			// A shortcut for DOME, to simplify life to applications (but can be also done in a TMF-compliant way).
			// Special processing to allow specifying multiple values in the form 'seller=id1,id2,id3'.
			// We also support the standard HTTP query strings like 'seller=id1,id2&seller=id3'
			var vals = []string{}
			// Allow several instances of the key in the query string (as in standard HTTP query strings)
			for _, v := range values {
				// Process each for several comma-separated values in the same key instance
				parts := strings.Split(v, ",")
				// Allow for whitespace surrounding the elements
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				vals = append(vals, parts...)
			}

			// Use either an equality (when one element) or an inclusion expression (when several)
			if len(vals) == 1 {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.Equal(key, sqlb.List(vals)),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In(key, sqlb.List(vals)),
				)
			}

		default:

			// We assume that the rest of parameters are not in the fields of the SQL database.
			// We have to use SQLite JSON expressions to search.
			if len(values) == 1 {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.Equal("content->>'$."+key+"'", values[0]),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In("content->>'$."+key+"'", sqlb.List(values)),
				)

			}

		}
	}

	// Add the WHERE to the SELECT
	bu.AddWhereClause(whereClause)

	// We need to GROUP by id, so we can SELECT the record with the latest version from each group
	bu.GroupBy("id")

	// For fairness of presenting results to customers, we want a random ordering, which is consistent and fair with the providers.
	// Ordering by the hash of the content of the TMF object complies with the requirements, as it is consistent across paginations
	// and nobody can predict the final ordering a-priory.
	// For a stable catalog, the ordering is the same for all users and at any time.
	// When a provider creates or modifies a product, it will be inserted at an unpredictable position in the catalog.
	//
	// TODO: we can consider a more advanced variation, where we add to the hash a random number which is
	// generated each day or week, and keeps the same until a new one is generated.
	// In this way, ordering is efficient, random, and changes every week (or whatever period is chosen)
	bu.OrderBy("hash")

	// Pagination support
	bu.Limit(limit).Offset(offset)

	// Build the query, with the statement and the arguments to be used
	sql, args := bu.Build()

	return sql, args
}

func PrettyPrint(o any) {
	out, err := json.MarshalIndent(o, "", "  ")
	if err == nil {
		fmt.Println(string(out))
	}
}
