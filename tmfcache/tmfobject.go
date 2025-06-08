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
	sqlb "github.com/huandu/go-sqlbuilder"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// This file implements the local database to be used for the persistent cache of TMForum objects.
// The functions in this file do not refresh the database, they only read from it or write to it.

var createTMFTableSQL = `
CREATE TABLE IF NOT EXISTS tmfobject (
	"id" TEXT NOT NULL,
	"version" TEXT,
	"organizationIdentifier" TEXT,
	"organization" TEXT,
	"seller" TEXT,
	"buyer" TEXT,
	"sellerOperator" TEXT,
	"buyerOperator" TEXT,
	"resource" TEXT NOT NULL,
	"name" TEXT NOT NULL,
	"description" TEXT,
	"lifecycleStatus" TEXT,
	"lastUpdate" TEXT NOT NULL,
	"content" BLOB NOT NULL,
	"hash" BLOB,
	"created" INTEGER,
	"updated" INTEGER,

	PRIMARY KEY ("id", "version")
) WITHOUT ROWID;
PRAGMA journal_mode = WAL;
CREATE INDEX IF NOT EXISTS idx_hash ON tmfobject (hash);
`

const deleteTMFTableSQL = `
DROP TABLE IF EXISTS tmfobject;
`
const vacuumTMFTableSQL = `VACUUM;`

const InsertTMFObjectSQL = `INSERT INTO tmfobject (id, organizationIdentifier, organization, seller, buyer, sellerOperator, buyerOperator, resource, name, description, lifecycleStatus, version, lastUpdate, content, hash, created, updated) VALUES (:id, :organizationIdentifier, :organization, :seller, :buyer, :sellerOperator, :buyerOperator, :resource, :name, :description, :lifecycleStatus, :version, :lastUpdate, :content, :hash, :created, :updated);`

const UpdateTMFObjectSQL = `UPDATE tmfobject SET organizationIdentifier = :organizationIdentifier, organization = :organization, seller = :seller, buyer = :buyer, sellerOperator = :sellerOperator, buyerOperator = :buyerOperator, resource = :resource, name = :name, description = :description, lifecycleStatus = :lifecycleStatus, lastUpdate = :lastUpdate, content = :content, hash = :hash, updated = :updated WHERE id = :id AND version = :version;`

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
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
	FromMap(oMap map[string]any) error
	Validate() error

	// Hashes
	Hash() []byte
	ETag() string

	// Storage related
	LocalInsertInStorage(dbconn *sqlite.Conn) error
	LocalUpdateInStorage(dbconn *sqlite.Conn) error
	LocalUpsertTMFObject(dbconn *sqlite.Conn, maxFreshness int) error

	// SetOwner(organizationIdentifier string, organization string)
	SetOwner(organizationIdentifier string, organization string, href string)
	Owner() (did string, name string, href string)
}

// TMFGeneralObject is the in-memory representation of a TMForum object.
//
// TMFGeneralObject can represent any arbitrary TMForum object, where the most important fields are in the struct.
// The whole object is always up-to-date in ContentAsMap and ContentAsJSON, to enable fast saving and retrieving
// from the database used as cache.
type TMFGeneralObject struct {
	ID                     string            `json:"id"`           // Required: the id of the object
	Href                   string            `json:"href"`         // Required: the href of the object
	ResourceName           string            `json:"resourceName"` // Required: The type of resource
	Name                   string            `json:"name"`
	Description            string            `json:"description"`
	LifecycleStatus        string            `json:"lifecycleStatus"`
	Version                string            `json:"version"`
	LastUpdate             string            `json:"lastUpdate"`
	ContentAsMap           map[string]any    `json:"-"` // The content of the object as a map
	ContentAsJSON          []byte            `json:"-"` // The content of the object as a JSON byte array
	Organization           string            `json:"organization"`
	OrganizationIdentifier string            `json:"organizationIdentifier"`
	Seller                 string            `json:"seller"`
	SellerHref             string            `json:"sellerHref"`
	Buyer                  string            `json:"buyer"`
	SellerOperator         string            `json:"sellerOperator"`
	BuyerOperator          string            `json:"buyerOperator"`
	RelatedParty           []RelatedPartyRef `json:"relatedParty"`
	Updated                int64             `json:"updated"`
}

// Sentinel to make sure we implement the complete TMFObject interface
var _ TMFObject = (*TMFGeneralObject)(nil)

type RelatedPartyRef struct {
	Id   string `json:"id"`
	Href string `json:"href"`
	Role string `json:"role"`
	Did  string `json:"did"`
}

// FromMap is the main TMFObject constructor, and is used by all other constructors.
// It performs validity checks when building the resulting TMFObject object.
func (po *TMFGeneralObject) FromMap(inputMap map[string]any) error {

	// Allocate a receiver object if it is nil, to allow unmarshal without previous allocation
	if po == nil {
		po = &TMFGeneralObject{}
	}

	// Make sure the compulsory objects exist and that they have sensible values
	err := tmfObjectSanityCheck(inputMap, false)
	if err != nil {
		slog.Error("invalid object", slogor.Err(err))
		out, err := json.MarshalIndent(inputMap, "", "  ")
		if err != nil {
			return config.Error(err)
		}
		fmt.Println(string(out))
		return config.Error(err)
	}

	// Special treatment for Category objects, which do not belong to any Seller or Buyer.
	// We assign them to the DOME Operator
	if po.ResourceName == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	// The first thing we do is to deduce the resource name of the object from the ID
	// In DOME, the rule is that the 'id' has embedded the resource name
	resourceName, err := config.FromIdToResourceName(inputMap["id"].(string))
	if err != nil {
		return config.Error(err)
	}

	// Extract the fields which are common to all TMF objects
	id, _ := inputMap["id"].(string)
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

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties, ok := inputMap["relatedParty"].([]any)
	if ok {

		for _, rp := range relatedParties {

			// Convert entry to a map
			rpMap, ok := rp.(map[string]any)
			if !ok {
				slog.Error("invalid relatedParty", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				// Go to next entry
				continue
			}

			rpId, _ := rpMap["id"].(string)
			rpHref, _ := rpMap["href"].(string)
			rpRole, _ := rpMap["role"].(string)
			rpDid, _ := rpMap["did"].(string)
			rpRole = strings.ToLower(rpRole)

			// TODO: Enable this rule when full implementation is performed

			// if len(rpId) == 0 {
			// 	slog.Error("no id in related party", "tmfObject", id)
			// 	if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
			// 		fmt.Println(string(out))
			// 	}
			// 	// Go to next entry
			// 	continue
			// }
			if len(rpRole) == 0 {
				slog.Error("no role in related party", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				// Go to next entry
				continue
			}

			if rpRole != "seller" && rpRole != "selleroperator" && rpRole != "buyer" && rpRole != "buyeroperator" {
				// Go to next entry
				continue
			}

			if !strings.HasPrefix(rpDid, "did:elsi:") {
				slog.Error("invalid DID prefix", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				// Go to next entry
				continue
			}

			// Add a new relatedParty entry to the object
			rpEntry := RelatedPartyRef{
				Id:   rpId,
				Href: rpHref,
				Role: rpRole,
				Did:  rpDid,
			}
			po.RelatedParty = append(po.RelatedParty, rpEntry)

			// Set the convenience fields in the object
			switch rpRole {
			case "seller":
				po.Seller = rpDid
				po.OrganizationIdentifier = rpDid

			case "buyer":
				po.Buyer = rpDid

			case "selleroperator":
				po.SellerOperator = rpDid

			case "buyeroperator":
				po.BuyerOperator = rpDid
			}

		}

	}

	// Store the whole map, with any updated contents
	po.ContentAsMap = inputMap

	return nil
}

var (
	ErrorSellerEmpty         = errors.New("seller is empty")
	ErrorBuyerEmpty          = errors.New("buyer is empty")
	ErrorSellerOperatorEmpty = errors.New("seller operator is empty")
	ErrorBuyerOperatorEmpty  = errors.New("buyer operator is empty")
	ErrorLastUpdateEmpty     = errors.New("lastUpdate is empty")
)

func (po *TMFGeneralObject) Validate() error {
	var errorList []error
	if po.Seller == "" {
		errorList = append(errorList, ErrorSellerEmpty)
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
		errorList = append(errorList, config.Errorf("lastUpdate is empty"))
	}

	return errors.Join(errorList...)
}

// UnmarshalJSON implements the [json.Unmarshall] interface.
// It unmarshalls first to a map[string]any and then uses [FromMap] to perform validity checks while building the TMFObject.
func (po *TMFGeneralObject) UnmarshalJSON(data []byte) error {
	var oMap map[string]any
	var err error

	err = json.Unmarshal(data, &oMap)
	if err != nil {
		return config.Error(err)
	}

	return po.FromMap(oMap)

}

func TMFObjectFromMap(oMap map[string]any) (TMFObject, error) {

	po := &TMFGeneralObject{}
	err := po.FromMap(oMap)
	if err != nil {
		return nil, err
	}

	return po, nil

}

func TMFObjectFromBytes(content []byte) (TMFObject, error) {

	var tmf *TMFGeneralObject
	err := json.Unmarshal(content, &tmf)
	if err != nil {
		return nil, err
	}

	return tmf, nil
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
	if oMap["id"] == nil {
		return config.Errorf("id field is nil")
	}

	id, ok := oMap["id"].(string)
	if !ok {
		return config.Errorf("invalid id type: %v", oMap["id"])
	}

	if !strings.HasPrefix(id, "urn:ngsi-ld:") {
		return config.Errorf("invalid id prefix: %s", id)
	}

	// href MUST exist
	if oMap["href"] == nil {
		return config.Errorf("href field is nil, id: %s", id)
	}

	href, ok := oMap["href"].(string)
	if !ok {
		return config.Errorf("invalid href type: %v", oMap["href"])
	}
	if !strings.HasPrefix(href, "urn:ngsi-ld:") {
		return config.Errorf("invalid href prefix: %s", href)
	}

	if id != href {
		return config.Errorf("id (%s) and href (%s) do not match", id, href)
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
			errorList = append(errorList, config.Errorf("lastUpdate is empty"))
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
	po.ContentAsMap["organizationIdentifier"] = organizationIdentifier
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
			rpMap["did"] = did
			found = true
			break
		}
	}
	if !found {
		po.ContentAsMap["relatedParty"] = append(rpMapList, map[string]any{
			"role": role,
			"did":  did,
			"href": href,
			"id":   href,
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

	return
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

func (o *TMFGeneralObject) GetIDMID() (organizationIdentifier string, organizationName string, err error) {
	// if o == nil {
	// 	return "", ""
	// }

	if o.ResourceName != config.Organization {
		return "", "", config.Errorf("resource is not an organization")
	}

	// Inside Organization, the array externalReference contains the ID of the organization
	ownerReferences, _ := o.ContentAsMap["externalReference"].([]any)
	if len(ownerReferences) == 0 {
		slog.Error("GetIDMID: externalReference is nil or not a list", "resource", o.ID)
		return "", "", config.Errorf("externalReference is nil or not a list")
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

			organizationIdentifier, ok := extRefMap["name"].(string)
			if !ok {
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

	return "", "", config.Errorf("no identification found")

}

func LocalRetrieveTMFObject(dbconn *sqlite.Conn, href string, version string) (po TMFObject, found bool, err error) {
	if dbconn == nil {
		return nil, false, config.Errorf("dbconn is nil")
	}

	// We use a different SELECT statement depending on whether version is provided or not.
	// Except for admin users, normal users are given the latest version of the object.
	var stmt *sqlite.Stmt
	if len(version) == 0 {
		const RetrieveTMFObjectNoVersionSQL = `SELECT * FROM tmfobject WHERE id = :id ORDER BY version DESC LIMIT 1;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectNoVersionSQL)
		defer stmt.Reset()
		stmt.SetText(":id", href)
	} else {
		const RetrieveTMFObjectSQL = `SELECT * FROM tmfobject WHERE id = :id AND version = :version;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectSQL)
		defer stmt.Reset()
		stmt.SetText(":id", href)
		stmt.SetText(":version", version)
	}

	hasRow, err := stmt.Step()
	if err != nil {
		slog.Error("RetrieveLocalTMFObject", "href", href, "error", err)
		return nil, false, config.Error(err)
	}

	if !hasRow {
		return nil, false, nil
	}

	// Even if we store it also in the db, the map representation of the object is always
	// built from the JSON representation in the content field.
	// The system ensures that this field is in synch with the in-memory fields of the struct.
	var content = make([]byte, stmt.GetLen("content"))
	stmt.GetBytes("content", content)

	updated := stmt.GetInt64("updated")

	tmf, err := TMFObjectFromBytes(content)

	// var tmf *TMFGeneralObject
	// err = json.Unmarshal(content, &tmf)
	if err != nil {
		return nil, false, config.Error(err)
	}

	tmf.SetUpdated(updated)

	return tmf, true, nil

}

func (po *TMFGeneralObject) LocalUpdateInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return config.Errorf("dbconn is nil")
	}

	if po.ResourceName == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	// Calculate the hash, updating the ContentAsJSON at the same time
	hash := po.Hash()
	if hash == nil {
		return config.Errorf("hash is nil")
	}

	updateStmt, err := dbconn.Prepare(UpdateTMFObjectSQL)
	if err != nil {
		return config.Errorf("UpdateInStorage: %w", err)
	}
	defer updateStmt.Reset()

	// These are used for the WHERE clause
	updateStmt.SetText(":id", po.ID)
	updateStmt.SetText(":version", po.Version)

	// These are the updated fields
	updateStmt.SetText(":organizationIdentifier", po.OrganizationIdentifier)
	updateStmt.SetText(":organization", po.Organization)

	updateStmt.SetText(":seller", po.Seller)
	updateStmt.SetText(":buyer", po.Buyer)
	updateStmt.SetText(":sellerOperator", po.SellerOperator)
	updateStmt.SetText(":buyerOperator", po.BuyerOperator)

	updateStmt.SetText(":resource", po.ResourceName)
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
		return config.Errorf("UpdateInStorage: %w", err)
	}

	return nil
}

func (po *TMFGeneralObject) LocalInsertInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return config.Errorf("dbconn is nil")
	}

	if po.ResourceName == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	hash := po.Hash()
	if hash == nil {
		return config.Errorf("hash is nil")
	}

	insertStmt, err := dbconn.Prepare(InsertTMFObjectSQL)
	if err != nil {
		return config.Error(err)
	}
	defer insertStmt.Reset()

	insertStmt.SetText(":id", po.ID)
	insertStmt.SetText(":version", po.Version)
	insertStmt.SetText(":organizationIdentifier", po.OrganizationIdentifier)
	insertStmt.SetText(":organization", po.Organization)

	insertStmt.SetText(":seller", po.Seller)
	insertStmt.SetText(":buyer", po.Buyer)
	insertStmt.SetText(":sellerOperator", po.SellerOperator)
	insertStmt.SetText(":buyerOperator", po.BuyerOperator)

	insertStmt.SetText(":resource", po.ResourceName)
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
		return config.Error(err)
	}

	return nil
}

func (po *TMFGeneralObject) LocalUpsertTMFObject(dbconn *sqlite.Conn, maxFreshness int) (err error) {
	if dbconn == nil {
		return config.Errorf("dbconn is nil")
	}

	// Start a SAVEPOINT and defer its Commit/Rollback
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Get the type of object from the ID
	resourceName, err := config.FromIdToResourceName(po.ID)
	if err != nil {
		return config.Error(err)
	}
	po.ResourceName = resourceName

	// Check if the row already exists, with the same version
	hasRow, hash, freshness, err := LocalCheckIfExists(dbconn, po.ID, po.Version)
	if err != nil {
		return config.Error(err)
	}

	// The id and version are the same, but we have to check the hash to see if we have to update the record
	if hasRow {

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
			return config.Error(err)
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
		return config.Error(err)
	}

	slog.Debug("Upsert: row inserted", "id", po.ID)

	return nil
}

func LocalCheckIfExists(
	dbconn *sqlite.Conn, id string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		return false, nil, 0, config.Errorf("dbconn is nil")
	}

	// Check if the row already exists, with the same version
	const CheckIfExistsTMFObjectSQL = `SELECT id, hash, updated FROM tmfobject WHERE id = :id AND version = :version;`
	selectStmt, err := dbconn.Prepare(CheckIfExistsTMFObjectSQL)
	if err != nil {
		return false, nil, 0, config.Errorf("CheckIfExists: %w", err)
	}
	defer selectStmt.Reset()

	selectStmt.SetText(":id", id)
	selectStmt.SetText(":version", version)

	hasRow, err := selectStmt.Step()
	if err != nil {
		return false, nil, 0, config.Errorf("CheckIfExists: %w", err)
	}

	// Each object has a hash to make sure it is the same object, even if the version is the same
	hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness = int(now - updated)

	return hasRow, hash, freshness, nil

}

func LocalRetrieveListTMFObject(dbconn *sqlite.Conn, tmfResource string, queryValues url.Values) (pos []*TMFGeneralObject, found bool, err error) {
	if dbconn == nil {
		return nil, false, config.Errorf("dbconn is nil")
	}

	// Build the SQL SELECT based on the query passed on the HTTP request, as specified in TMForum
	sql, args := BuildSelectFromParms(tmfResource, queryValues)

	var resultPOs []*TMFGeneralObject

	err = sqlitex.Execute(dbconn, sql, &sqlitex.ExecOptions{
		Args: args,

		// This function is called once for each record found in the database
		ResultFunc: func(stmt *sqlite.Stmt) error {

			// Even if we store it also in the db, the map representation of the object is always
			// built from the JSON representation in the content field.
			// The system ensures that this field is in synch with the in-memory fields of the struct.
			content := make([]byte, stmt.GetLen("content"))
			stmt.GetBytes("content", content)

			updated := stmt.GetInt64("updated")

			var tmf *TMFGeneralObject
			err = json.Unmarshal(content, &tmf)
			if err != nil {
				return config.Error(err)
			}

			tmf.Updated = updated

			// Complete the map representation with the relevant fields which are in our db but not in the DOME repo
			tmf.ContentAsMap["updated"] = updated
			tmf.ContentAsMap["organizationIdentifier"] = stmt.GetText("organizationIdentifier")
			tmf.ContentAsMap["organization"] = stmt.GetText("organization")

			resultPOs = append(resultPOs, tmf)

			return nil
		},
	})
	if err != nil {
		return nil, false, config.Error(err)
	}

	slog.Debug("RetrieveLocalListTMFObject", "sql", sql, "args", args, "objects", resultPOs)
	return resultPOs, true, nil
}

func createTables(dbpool *sqlitex.Pool) error {

	// Get a connection from the pool for all the requests.
	// The connection is returned to the pool when the object is closed
	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return config.Error(err)
	}
	defer dbpool.Put(conn)

	// Create the table if it does not exist
	if err := sqlitex.ExecuteScript(conn, createTMFTableSQL, nil); err != nil {
		slog.Error("createTables", slogor.Err(err))
		return config.Errorf("createTables: %w", err)
	}

	return nil
}

func deleteTables(dbpool *sqlitex.Pool) error {
	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return config.Error(err)
	}
	defer dbpool.Put(conn)

	if err := sqlitex.ExecuteScript(conn, deleteTMFTableSQL, nil); err != nil {
		slog.Error("deleteTables", slogor.Err(err))
		return config.Errorf("deleteTables: %w", err)
	}

	vacuumStmt, err := conn.Prepare(vacuumTMFTableSQL)
	if err != nil {
		return config.Error(err)
	}
	defer vacuumStmt.Reset()

	_, err = vacuumStmt.Step()
	if err != nil {
		return config.Error(err)
	}

	return nil
}

func (tmf *TMFCache) DeleteTables() error {
	return deleteTables(tmf.dbpool)
}

// LocalCheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func (tmf *TMFCache) LocalCheckIfExists(
	dbconn *sqlite.Conn, id string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return false, nil, 0, config.Errorf("LocalCheckIfExists: taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalCheckIfExists(dbconn, id, version)

}

// LocalUpdateInStorage updates an object in the db with the contents of the po.
func (tmf *TMFCache) LocalUpdateInStorage(dbconn *sqlite.Conn, po *TMFGeneralObject) error {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return config.Errorf("LocalUpdateInStorage: taking db connection: %w", err)
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
			return config.Errorf("LocalInsertInStorage: taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalInsertInStorage(dbconn)

}

// LocalUpsertTMFObject updates or insters an object in the database.
// id and version are primary keys, so their combination must be unique or the function returns and error.
func (tmf *TMFCache) LocalUpsertTMFObject(dbconn *sqlite.Conn, po TMFObject) (err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return config.Errorf("LocalUpsertTMFObject: taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalUpsertTMFObject(dbconn, tmf.Maxfreshness)

}

// LocalRetrieveTMFObject retrieves the object with the href (is the same as the id).
// The version is optional. If it is not provided, the most recently version (by lexicographic order) is retrieved.
func (tmf *TMFCache) LocalRetrieveTMFObject(dbconn *sqlite.Conn, href string, version string) (po TMFObject, found bool, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, config.Errorf("LocalRetrieveTMFObject: taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalRetrieveTMFObject(dbconn, href, version)

}

// LocalRetrieveListTMFObject implements the TMForum functionality for retrieving a list of objects of a given type from the database.
func (tmf *TMFCache) LocalRetrieveListTMFObject(dbconn *sqlite.Conn, tmfResource string, queryValues url.Values) (pos []*TMFGeneralObject, found bool, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, config.Errorf("LocalRetrieveListTMFObject: taking db connection: %w", err)
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalRetrieveListTMFObject(dbconn, tmfResource, queryValues)

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
