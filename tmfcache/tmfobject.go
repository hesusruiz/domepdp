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
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/errl"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
)

// This file implements the local database to be used for the persistent cache of TMForum objects.
// The functions in this file do not refresh the database, they only read from it or write to it.

type TMFObject interface {
	// Setters and getters for the fields
	SetID(id string)
	GetID() string
	SetHref(href string)
	GetHref() string
	GetType() string
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
	id                     string         // Required: the id of the object
	href                   string         // Required: the href of the object
	resourceType           string         // Required: The type of resource
	Name                   string         `json:"name"`
	Description            string         `json:"description"`
	LifecycleStatus        string         `json:"lifecycleStatus"`
	Version                string         `json:"version"`
	LastUpdate             string         `json:"lastUpdate"`
	ContentAsMap           map[string]any `json:"-"` // The content of the object as a map
	ContentAsJSON          []byte         `json:"-"` // The content of the object as a JSON byte array
	Organization           string         `json:"organization"`
	organizationIdentifier string
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

// IPartyOrPartyRole is an interface implemented by PartyRef and PartyRoleRef.
type IPartyOrPartyRole interface {
	isPartyOrPartyRole()
}

// PartyRef is a reference to a party, as defined in TMF620.
type PartyRef struct {
	ID               string `json:"id"`
	Href             string `json:"href,omitempty"`
	Name             string `json:"name,omitempty"`
	AtReferredType   string `json:"@referredType"`
	AtType           string `json:"@type"`
	AtBaseType       string `json:"@baseType,omitempty"`
	AtSchemaLocation string `json:"@schemaLocation,omitempty"`
}

// isPartyOrPartyRole implements the IPartyOrPartyRole interface.
func (p PartyRef) isPartyOrPartyRole() {}

// PartyRoleRef is a reference to a party role, as defined in TMF620.
type PartyRoleRef struct {
	PartyRef
	PartyID   string `json:"partyId,omitempty"`
	PartyName string `json:"partyName,omitempty"`
}

// isPartyOrPartyRole implements the IPartyOrPartyRole interface.
func (p PartyRoleRef) isPartyOrPartyRole() {}

// RelatedPartyRefOrPartyRoleRef defines a party or party role, linked to a specific entity.
// It corresponds to the RelatedPartyRefOrPartyRoleRef schema in TMF620 Product Catalog Management API.
type RelatedPartyRefOrPartyRoleRef struct {
	Role             string            `json:"role,omitempty"`
	PartyOrPartyRole IPartyOrPartyRole `json:"partyOrPartyRole"`
	AtType           string            `json:"@type"`
	AtBaseType       string            `json:"@baseType,omitempty"`
	AtSchemaLocation string            `json:"@schemaLocation,omitempty"`
}

// UnmarshalJSON implements custom unmarshalling for RelatedPartyRefOrPartyRoleRef.
func (r *RelatedPartyRefOrPartyRoleRef) UnmarshalJSON(data []byte) error {
	// Use a temporary struct to unmarshal known fields and the raw partyOrPartyRole
	type tempRelatedParty struct {
		Role             string          `json:"role,omitempty"`
		PartyOrPartyRole json.RawMessage `json:"partyOrPartyRole"`
		AtType           string          `json:"@type"`
		AtBaseType       string          `json:"@baseType,omitempty"`
		AtSchemaLocation string          `json:"@schemaLocation,omitempty"`
	}

	var temp tempRelatedParty
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	r.Role = temp.Role
	r.AtType = temp.AtType
	r.AtBaseType = temp.AtBaseType
	r.AtSchemaLocation = temp.AtSchemaLocation

	if temp.PartyOrPartyRole == nil {
		return errors.New("partyOrPartyRole is missing")
	}

	// Now, unmarshal PartyOrPartyRole based on its @type
	var typeProbe struct {
		AtType string `json:"@type"`
	}
	if err := json.Unmarshal(temp.PartyOrPartyRole, &typeProbe); err != nil {
		return fmt.Errorf("failed to probe for @type in partyOrPartyRole: %w", err)
	}

	switch typeProbe.AtType {
	case "PartyRef":
		var pr PartyRef
		if err := json.Unmarshal(temp.PartyOrPartyRole, &pr); err != nil {
			return err
		}
		r.PartyOrPartyRole = pr
	case "PartyRoleRef":
		var prr PartyRoleRef
		if err := json.Unmarshal(temp.PartyOrPartyRole, &prr); err != nil {
			return err
		}
		r.PartyOrPartyRole = prr
	default:
		return fmt.Errorf("unknown @type for partyOrPartyRole: '%s'", typeProbe.AtType)
	}

	return nil
}

// MarshalJSON implements custom marshalling for RelatedPartyRefOrPartyRoleRef.
func (r RelatedPartyRefOrPartyRoleRef) MarshalJSON() ([]byte, error) {
	// Create a map to hold the fields, which is easier for custom marshalling
	m := map[string]any{
		"@type":            r.AtType,
		"partyOrPartyRole": r.PartyOrPartyRole,
	}
	if r.Role != "" {
		m["role"] = r.Role
	}
	if r.AtBaseType != "" {
		m["@baseType"] = r.AtBaseType
	}
	if r.AtSchemaLocation != "" {
		m["@schemaLocation"] = r.AtSchemaLocation
	}

	return json.Marshal(m)
}

// ToMap converts a RelatedPartyRefOrPartyRoleRef to a map[string]any.
func (r *RelatedPartyRefOrPartyRoleRef) ToMap() (map[string]any, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	var resultMap map[string]any
	err = json.Unmarshal(data, &resultMap)
	if err != nil {
		return nil, err
	}
	return resultMap, nil
}

// FromMap populates a RelatedPartyRefOrPartyRoleRef from a map[string]any.
func (r *RelatedPartyRefOrPartyRoleRef) FromMap(data map[string]any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, r)
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
func tmfObjectFromMap(inputMap map[string]any) (TMFObject, error) {
	var err error
	po := &TMFGeneralObject{}

	// the 'id' is mandatory
	po.id, _ = inputMap["id"].(string)
	if len(po.id) == 0 {
		return nil, errl.Errorf("id is nil or not a string: %v", inputMap["id"])
	}

	// We also expect the object to include its type
	po.resourceType = jpath.GetString(inputMap, "@type")

	// Just in case, convert the first letter to lowercase, to match the TMF API conventions
	po.resourceType = strings.ToLower(po.resourceType[:1]) + po.resourceType[1:]

	// Make sure the compulsory attributes exist and that they have sensible values
	err = tmfObjectSanityCheck(inputMap, false)
	if err != nil {
		slog.Error("invalid object", slogor.Err(err))
		PrettyPrint(inputMap)
		return nil, errl.Error(err)
	}

	// Special treatment for Category objects, which do not belong to any Seller or Buyer.
	// We assign them to the Operator of the ecosystem.
	if po.resourceType == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	// Retrieve the info from the relatedParty array, if present
	po.Seller, po.SellerOperator, po.Buyer, po.BuyerOperator = getSellerAndBuyerIds(inputMap)
	po.organizationIdentifier = po.Seller

	// Extract the fields which are common to all TMF objects
	po.href, _ = inputMap["href"].(string)
	po.Name, _ = inputMap["name"].(string)
	po.Description, _ = inputMap["description"].(string)
	po.LifecycleStatus, _ = inputMap["lifecycleStatus"].(string)
	po.Version, _ = inputMap["version"].(string)
	po.LastUpdate, _ = inputMap["lastUpdate"].(string)

	// TODO: get rid of these fields, when Seller and SellerOperator are fully implemented
	po.Organization, _ = inputMap["organization"].(string)

	// Store the whole map, with any updated contents
	po.ContentAsMap = inputMap

	// TODO: The 'updated' field is not TMForum, but from the DB, so maybe it does not belong here
	po.Updated, _ = inputMap["updated"].(int64)

	return po, nil

}

func getSellerAndBuyerIds(tmfObjectMap map[string]any) (seller, sellerOperator, buyer, buyerOperator string) {

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties, _ := tmfObjectMap["relatedParty"].([]any)
	id, _ := tmfObjectMap["id"].(string)

	if len(relatedParties) == 0 {
		return "", "", "", ""
	}

	for _, rp := range relatedParties {

		// Convert entry to a map
		rpMap, _ := rp.(map[string]any)
		if len(rpMap) == 0 {
			continue // Go to next entry
		}

		rpRole, _ := rpMap["role"].(string)
		rpRole = strings.ToLower(rpRole)

		if rpRole != "seller" && rpRole != "selleroperator" && rpRole != "buyer" && rpRole != "buyeroperator" {
			// Go to next entry
			continue
		}

		partyRef := jpath.GetMap(rpMap, "partyOrPartyRole")
		if len(partyRef) == 0 {
			slog.Error("relatedParty entry without partyOrPartyRole", "id", id)
			continue // Go to next entry
		}

		// Get the name of the party
		partyName, _ := partyRef["name"].(string)
		if len(partyName) == 0 {
			slog.Error("relatedParty entry without name", "id", id)
			continue // Go to next entry
		}

		switch rpRole {
		case "seller":
			seller = partyName
		case "selleroperator":
			sellerOperator = partyName
		case "buyer":
			buyer = partyName
		case "buyeroperator":
			buyerOperator = partyName
		}

	}

	return seller, sellerOperator, buyer, buyerOperator
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

func TMFObjectFromMap(oMap map[string]any, resourceType string) (TMFObject, error) {

	// Convert the first letter to lowercase, to match the TMF API conventions
	resourceType = strings.ToLower(resourceType[:1]) + resourceType[1:]

	// Make sure the type of the object is set in the map and is the correct one
	// We accept and fix maps without type info, but not with incorrect type info
	oType := jpath.GetString(oMap, "@type")
	if len(oType) == 0 {
		oMap["@type"] = resourceType
	} else if oType != resourceType {
		return nil, errl.Errorf("object type mismatch: expected %s, got %s", resourceType, oType)
	}

	switch resourceType {
	case config.Organization:
		return tmfOrganizationFromMap(oMap)
	default:
		return tmfObjectFromMap(oMap)
	}

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
func TMFObjectFromBytes(content []byte, resourceType string) (TMFObject, error) {
	var oMap map[string]any
	var err error

	err = json.Unmarshal(content, &oMap)
	if err != nil {
		return nil, errl.Error(err)
	}

	// Make sure the type of the object is set in the map and is the correct one
	// We accept and fix maps without type info, but not with incorrect type info
	oType := jpath.GetString(oMap, "@type")
	if len(oType) == 0 {
		oMap["@type"] = resourceType
	} else {
		// Convert the first letter to lowercase, to match the TMF API conventions
		oType = strings.ToLower(oType[:1]) + oType[1:]
		oMap["@type"] = oType

		if !strings.EqualFold(oType, resourceType) {
			return nil, errl.Errorf("object type mismatch: expected %s, got %s", resourceType, oType)
		}
	}

	switch resourceType {
	case config.Organization:
		return tmfOrganizationFromMap(oMap)
	default:
		return tmfObjectFromMap(oMap)
	}

}

type FixLevel int

const (
	FixNone FixLevel = iota
	FixLow
	FixMedium
	FixHigh
)

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
	s1 := fmt.Sprintf("ID: %s\nType: %s\nName: %s\nLifecycleStatus: %s\nVersion: %s\nLastUpdate: %s\n", po.id, po.resourceType, po.Name, po.LifecycleStatus, po.Version, po.LastUpdate)

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

// The following are typed methods to get/set values from the ContentAsMap field, to facilitate life to the user.

func (po *TMFGeneralObject) SetID(id string) {
	po.id = id
	po.ContentAsMap["id"] = id
}
func (po *TMFGeneralObject) GetID() string {
	return po.id
}

func (po *TMFGeneralObject) SetHref(href string) {
	po.href = href
	po.ContentAsMap["href"] = href
}
func (po *TMFGeneralObject) GetHref() string {
	return po.href
}

func (po *TMFGeneralObject) GetType() string {
	return po.resourceType
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
	po.organizationIdentifier = organizationIdentifier
}

func (po *TMFGeneralObject) GetOrganizationIdentifier() string {
	return po.organizationIdentifier
}

func (po *TMFGeneralObject) SetSeller(id string, sellerDid string) {
	if len(sellerDid) > 0 && !strings.HasPrefix(sellerDid, "did:elsi:") {
		sellerDid = "did:elsi:" + sellerDid
	}
	po.Seller = sellerDid
	// TODO: update properly the seller in the map

	po.updateRelatedPartyRef("seller", sellerDid, id)

}

func (po *TMFGeneralObject) GetSeller() string {
	return po.Seller
}

func (po *TMFGeneralObject) SetBuyer(id string, buyerDid string) {
	if len(buyerDid) > 0 && !strings.HasPrefix(buyerDid, "did:elsi:") {
		buyerDid = "did:elsi:" + buyerDid
	}
	po.Buyer = buyerDid

	po.updateRelatedPartyRef("buyer", buyerDid, id)
}

func (po *TMFGeneralObject) GetBuyer() string {
	return po.Buyer
}

func (po *TMFGeneralObject) SetSellerOperator(id string, sellerOperatorDid string) {
	if len(sellerOperatorDid) > 0 && !strings.HasPrefix(sellerOperatorDid, "did:elsi:") {
		sellerOperatorDid = "did:elsi:" + sellerOperatorDid
	}
	po.SellerOperator = sellerOperatorDid

	po.updateRelatedPartyRef("sellerOperator", sellerOperatorDid, id)
}

func (po *TMFGeneralObject) GetSellerOperator() string {
	return po.SellerOperator
}

func (po *TMFGeneralObject) SetBuyerOperator(id string, buyerOperatorDid string) {
	if len(buyerOperatorDid) > 0 && !strings.HasPrefix(buyerOperatorDid, "did:elsi:") {
		buyerOperatorDid = "did:elsi:" + buyerOperatorDid
	}
	po.BuyerOperator = buyerOperatorDid

	po.updateRelatedPartyRef("buyerOperator", buyerOperatorDid, id)
}

func (po *TMFGeneralObject) GetBuyerOperator() string {
	return po.BuyerOperator
}

func (po *TMFGeneralObject) updateRelatedPartyRef(role string, did string, id string) {

	found := false
	rpMapList, _ := po.ContentAsMap["relatedParty"].([]any)
	for _, rpMap := range rpMapList {
		rpMap, _ := rpMap.(map[string]any)
		if rpMap["role"] == role {
			rpMap["@referredType"] = config.Organization
			rpMap["href"] = id
			rpMap["id"] = id
			rpMap["name"] = did
			found = true
			break
		}
	}
	if !found {
		po.ContentAsMap["relatedParty"] = append(rpMapList, map[string]any{
			"@referredType": config.Organization,
			"role":          role,
			"name":          did,
			"href":          id,
			"id":            id,
		})
	}

}

// ****************

// These are setters which update the struct and the associated ContentAsMap.

func (po *TMFGeneralObject) SetOwner(organizationIdentifier string, organization string, tmfId string) {
	po.organizationIdentifier = organizationIdentifier
	po.Organization = organization

	po.SetSeller(tmfId, organizationIdentifier)

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

func PrettyPrint(o any) {
	out, err := json.MarshalIndent(o, "", "  ")
	if err == nil {
		fmt.Println(string(out))
	}
}
