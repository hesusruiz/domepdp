package tmfcache

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/hesusruiz/domepdp/config"
	"github.com/hesusruiz/domepdp/internal/errl"
	"github.com/hesusruiz/domepdp/internal/jpath"
	"zombiezen.com/go/sqlite"
)

const eIDASAuthority = "eIDAS"
const elsiIdentificationType = "did:elsi"

func TMFOrganizationFromToken(accessToken map[string]any) (TMFObject, error) {

	// Get the Verifiable Credential object from the access token
	vc, ok := accessToken["vc"].(map[string]any)
	if !ok {
		return nil, errl.Errorf("accessToken does not contain 'vc' field")
	}

	// Get the credentialSubject from the Verifiable Credential
	credentialSubject, ok := vc["credentialSubject"].(map[string]any)
	if !ok {
		return nil, errl.Errorf("accessToken does not contain 'credentialSubject' field")
	}

	// Get the mandate from the credentialSubject
	mandate, ok := credentialSubject["mandate"].(map[string]any)
	if !ok {
		return nil, errl.Errorf("accessToken does not contain 'mandate' field")
	}

	// Get the mandator from the mandate
	mandator, ok := mandate["mandator"].(map[string]any)
	if !ok {
		return nil, errl.Errorf("accessToken does not contain 'mandator' field")
	}

	// Extract mandator fields
	organizationIdentifier, _ := mandator["organizationIdentifier"].(string)
	organizationName, _ := mandator["organization"].(string)
	if organizationName == "" {
		organizationName, _ = mandator["commonName"].(string)
	}
	emailAddress, _ := mandator["emailAddress"].(string)

	did := organizationIdentifier
	if !strings.HasPrefix(did, "did:elsi:") {
		did = "did:elsi:" + did
	}

	tokenJSON, err := json.Marshal(accessToken)
	if err != nil {
		return nil, errl.Errorf("error marshalling access token: %w", err)
	}

	attch := map[string]any{
		"@type":       "attachment",
		"name":        "verifiableCredential",
		"contentType": "application/json",
		"content":     base64.StdEncoding.EncodeToString(tokenJSON),
	}

	// Prepare organizationIdentification
	orgIdentification := []any{
		map[string]any{
			"@type":              "organizationIdentification",
			"identificationId":   did,
			"identificationType": elsiIdentificationType,
			"issuingAuthority":   eIDASAuthority,
			"attachment":         attch,
		},
	}

	// Prepare contactMedium
	var contactMedium []any
	if emailAddress != "" {
		contactMedium = append(contactMedium, map[string]any{
			"@type":        "EmailContactMedium",
			"preferred":    true,
			"emailAddress": emailAddress,
		})
	}

	orgMap := map[string]any{
		"@type":                      config.Organization,
		"isLegalEntity":              true,
		"id":                         fmt.Sprintf("urn:ngsi-ld:%s:%s", config.ToKebabCase(config.Organization), uuid.NewString()),
		"name":                       organizationName,
		"tradingName":                organizationName,
		"contactMedium":              contactMedium,
		"organizationIdentification": orgIdentification,
		"externalReference": []any{
			map[string]any{
				"externalReferenceType": "idm_id",
				"name":                  organizationIdentifier,
			},
		},
	}

	org, err := TMFObjectFromMap(orgMap, config.Organization)
	if err != nil {
		return nil, errl.Errorf("failed to create TMFObject from map: %w", err)
	}

	return org, nil
}

func tmfOrganizationFromMap(inputMap map[string]any) (TMFObject, error) {
	// Ensure the map has the correct type
	resourceType := jpath.GetString(inputMap, "@type")
	if resourceType != config.Organization {
		return nil, errl.Errorf("map is not of type %s", config.Organization)
	}

	po := &TMFGeneralObject{}

	// the 'id' is mandatory
	po.id, _ = inputMap["id"].(string)
	if len(po.id) == 0 {
		return nil, errl.Errorf("id is nil or not a string: %v", inputMap["id"])
	}

	po.resourceType = resourceType

	// Store the whole map
	po.ContentAsMap = inputMap

	// Retrieve the info from the relatedParty array, if present
	did, organizationName, err := po.GetIDMID()
	if err != nil {
		return nil, errl.Error(err)
	}

	po.organizationIdentifier = did
	po.Name = organizationName
	po.Organization = organizationName

	// Extract the fields which are common to all TMF objects
	po.href, _ = inputMap["href"].(string)
	po.Description, _ = inputMap["description"].(string)
	po.LifecycleStatus, _ = inputMap["lifecycleStatus"].(string)
	po.Version, _ = inputMap["version"].(string)
	po.LastUpdate, _ = inputMap["lastUpdate"].(string)

	// TODO: The 'updated' field is not TMForum, but from the DB, so maybe it does not belong here
	po.Updated, _ = inputMap["updated"].(int64)

	return po, nil

}

// Add or update an OrganizationIdentification entry
func (po *TMFGeneralObject) AddOrganizationIdentification(ident map[string]any) {
	ids, _ := po.ContentAsMap["organizationIdentification"].([]any)
	po.ContentAsMap["organizationIdentification"] = append(ids, ident)
}

// Remove OrganizationIdentification by identificationId
// At the same time, we cleanup the map from spurious entries
func (po *TMFGeneralObject) RemoveOrganizationIdentification(identificationId string) {
	ids, _ := po.ContentAsMap["organizationIdentification"].([]any)
	var newIds []any
	for _, id := range ids {
		idMap, ok := id.(map[string]any)
		// We ignore the entry if it is not a map, effectively deleting it
		// We also ignore the entry if it has the specified identificationId
		if ok && idMap["identificationId"] != identificationId {
			newIds = append(newIds, id)
		}
	}
	// Replace the old list with the new filtered list
	po.ContentAsMap["organizationIdentification"] = newIds
}

// Get all OrganizationIdentification entries
func (po *TMFGeneralObject) GetOrganizationIdentifications() []map[string]any {
	ids, _ := po.ContentAsMap["organizationIdentification"].([]any)
	var result []map[string]any
	for _, id := range ids {
		if idMap, ok := id.(map[string]any); ok {
			result = append(result, idMap)
		}
	}
	return result
}

// Similar helpers for IndividualIdentification
func (po *TMFGeneralObject) AddIndividualIdentification(ident map[string]any) {
	ids, _ := po.ContentAsMap["individualIdentification"].([]any)
	po.ContentAsMap["individualIdentification"] = append(ids, ident)
}

func (po *TMFGeneralObject) RemoveIndividualIdentification(identificationId string) {
	ids, _ := po.ContentAsMap["individualIdentification"].([]any)
	var newIds []any
	for _, id := range ids {
		idMap, ok := id.(map[string]any)
		if !ok || idMap["identificationId"] != identificationId {
			newIds = append(newIds, id)
		}
	}
	po.ContentAsMap["individualIdentification"] = newIds
}

func (po *TMFGeneralObject) GetIndividualIdentifications() []map[string]any {
	ids, _ := po.ContentAsMap["individualIdentification"].([]any)
	var result []map[string]any
	for _, id := range ids {
		if idMap, ok := id.(map[string]any); ok {
			result = append(result, idMap)
		}
	}
	return result
}

// GetIDMID retrieves identification information from an Organization object
func (o *TMFGeneralObject) GetIDMID() (organizationDid string, organizationName string, err error) {

	if o.resourceType != config.Organization {
		return "", "", errl.Errorf("resource is not an organization")
	}

	// Inside Organization, the array externalReference contains the ID of the organization
	ownerReferences, _ := o.ContentAsMap["externalReference"].([]any)
	if len(ownerReferences) == 0 {
		slog.Error("GetIDMID: externalReference is nil or not a list", "resource", o.id)
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
				slog.Error("GetIDMID: externalReference 'name' is nil or not a string", "resource", o.id)
				continue
			}
			if len(organizationIdentifier) > 0 && !strings.HasPrefix(organizationIdentifier, "did:elsi:") {
				organizationIdentifier = "did:elsi:" + organizationIdentifier
			}

			organization, _ := o.ContentAsMap["tradingName"].(string)
			if len(organization) == 0 {
				slog.Warn("GetIDMID: externalReference 'tradingName' is nil or not a string", "resource", o.id)
				PrettyPrint(o.ContentAsMap)
			}

			return organizationIdentifier, organization, nil
		}
	}

	return "", "", errl.Errorf("externalReference with 'idm_id' not found in Organization object: %s", o.id)

}

func LocalRetrieveOrgByDid(dbconn *sqlite.Conn, did string) (o TMFObject, found bool, err error) {

	if !strings.HasPrefix(did, "did:elsi:") {
		did = "did:elsi:" + did
	}

	const selectSQL = `SELECT * FROM tmfobject WHERE resource = 'organization' AND organizationIdentifier = :organizationIdentifier ORDER BY version DESC LIMIT 1;`
	stmt, err := dbconn.Prepare(selectSQL)
	if err != nil {
		return nil, false, errl.Errorf("preparing statement: %w", err)
	}
	defer stmt.Reset()

	stmt.SetText(":organizationIdentifier", did)

	rowReturned, err := stmt.Step()
	if err != nil {
		return nil, false, errl.Errorf("retrieving organization %s: %w", did, err)
	}

	if !rowReturned {
		return nil, false, nil
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
		return nil, false, errl.Errorf("unmarshalling organization %s: %w", did, err)
	}

	tmf.SetUpdated(updated)

	return tmf, true, nil

}
