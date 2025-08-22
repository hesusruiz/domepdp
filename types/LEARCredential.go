package types

import "github.com/golang-jwt/jwt/v5"

type Mandate struct {
	Id       string `json:"id,omitempty"`
	Mandator struct {
		OrganizationIdentifier string `json:"organizationIdentifier,omitempty"` // OID 2.5.4.97
		CommonName             string `json:"commonName,omitempty"`             // OID 2.5.4.3
		GivenName              string `json:"givenName,omitempty"`
		Surname                string `json:"surname,omitempty"`
		EmailAddress           string `json:"emailAddress,omitempty"`
		SerialNumber           string `json:"serialNumber,omitempty"`
		Organization           string `json:"organization,omitempty"`
		Country                string `json:"country,omitempty"`
	} `json:"mandator"`
	Mandatee struct {
		Id           string `json:"id,omitempty"`
		FirstName    string `json:"first_name,omitempty"`
		LastName     string `json:"last_name,omitempty"`
		Email        string `json:"email,omitempty"`
		Mobile_phone string `json:"mobile_phone,omitempty"`
	} `json:"mandatee"`
	Power []struct {
		Id           string   `json:"id,omitempty"`
		Tmf_type     string   `json:"tmf_type,omitempty"`
		Tmf_domain   []string `json:"tmf_domain,omitempty"`
		Tmf_function string   `json:"tmf_function,omitempty"`
		Tmf_action   []string `json:"tmf_action,omitempty"`
	} `json:"power,omitempty"`
	LifeSpan struct {
		StartDateTime string `json:"start_date_time,omitempty"`
		EndDateTime   string `json:"end_date_time,omitempty"`
	} `json:"life_span"`
}

type LEARCredentialEmployee struct {
	Context        []string `json:"@context,omitempty"`
	Id             string   `json:"id,omitempty"`
	TypeCredential []string `json:"type,omitempty"`
	Issuer         struct {
		Id string `json:"id,omitempty"`
	} `json:"issuer"`
	ValidFrom         string `json:"validFrom,omitempty"`
	ValidUntil        string `json:"validUntil,omitempty"`
	CredentialSubject struct {
		Mandate Mandate `json:"mandate"`
	} `json:"credentialSubject"`
}

type LEARCredentialEmployeeJWTClaims struct {
	LEARCredentialEmployee
	jwt.RegisteredClaims
}
