package tmfcache

import (
	"log/slog"
	"strings"

	"github.com/goccy/go-json"
)

type TMFOrganization struct {
	TMFGeneralObject
	OrganizationIdentification []OrganizationIdentification `json:"organizationIdentification,omitempty"`
}

type OrganizationIdentification struct {
	IdentificationId   string `json:"identificationId,omitempty"`
	IdentificationType string `json:"identificationType,omitempty"`
	IssuingAuthority   string `json:"issuingAuthority,omitempty"`
}

const eIDASAuthority = "eIDAS"
const elsiIdentificationType = "did:elsi"

func (o *TMFOrganization) SetOrganizationIdentification(identificationId string) {

	// Check if we already have an entry with the did:elsi identification type
	for _, id := range o.OrganizationIdentification {
		if id.IdentificationType == elsiIdentificationType {
			id.IdentificationId = identificationId
			return
		}
	}

	// Otherwise, we create a new entry and append it
	o.OrganizationIdentification = append(o.OrganizationIdentification, OrganizationIdentification{
		IdentificationId:   identificationId,
		IdentificationType: elsiIdentificationType,
		IssuingAuthority:   eIDASAuthority,
	})

	// Update the ContentAsMap field
	o.ContentAsMap["organizationIdentification"] = o.OrganizationIdentification

}

func (o *TMFOrganization) GetOrganizationIdentification() string {
	for _, id := range o.OrganizationIdentification {
		if id.IdentificationType == elsiIdentificationType {
			return id.IdentificationId
		}
	}
	return ""
}

func (o *TMFOrganization) MarshalJSON() ([]byte, error) {

	// We assume that the ContentAsMap field is always up to date
	content, err := json.Marshal(o.ContentAsMap)
	if err != nil {
		return nil, err
	}

	o.ContentAsJSON = content

	return content, nil

}

func (o *TMFOrganization) GetIDMID() (organizationIdentifier string, organizationName string) {
	// if o == nil {
	// 	return "", ""
	// }

	// Inside Organization, the array externalReference contains the ID of the organization
	ownerReferences, ok := o.ContentAsMap["externalReference"].([]any)
	if !ok {
		slog.Error("getRelatedPartyOwner: externalReference is nil or not a list", "resource", o.ID)
		return "", ""
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
				slog.Error("getRelatedPartyOwner: externalReference 'name' is nil or not a string", "resource", o.ID)
				continue
			}
			if len(organizationIdentifier) > 0 && !strings.HasPrefix(organizationIdentifier, "did:elsi:") {
				organizationIdentifier = "did:elsi:" + organizationIdentifier
			}

			organization, _ := o.ContentAsMap["tradingName"].(string)

			return organizationIdentifier, organization
		}
	}

	return "", ""

}
