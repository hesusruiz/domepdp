package common

import (
	"log/slog"
	"strings"

	"github.com/hesusruiz/domepdp/internal/errl"
	"github.com/hesusruiz/domepdp/internal/jpath"
	"gitlab.com/greyxor/slogor"
)

const FakeClaims = true
const fakeAT = `eyJraWQiOiJkaWQ6a2V5OnpEbmFlVlluV1RadTVuYnJIMXFtQlZNdk53U3J0S25rUmJDWjR4SDVoMkxRUG56ZHIiLCJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJhdWQiOiJkaWQ6a2V5OnpEbmFlVFUzOVd4OUtYZ21Fd21mWHNaU3lFVnhnQ3F3Q1Ztb1B5VlFVVEQ4YmhXOGEiLCJzdWIiOiJkaWQ6a2V5OnpEbmFlY0x2NWF5Q3J6V1JpWXlGM3ZhSGhLcVRTa0ZHV2lKSmRNZkFyV25aV2NxUm4iLCJzY29wZSI6Im9wZW5pZCBsZWFyY3JlZGVudGlhbCIsImlzcyI6Imh0dHBzOi8vdmVyaWZpZXIuZG9tZS1tYXJrZXRwbGFjZS5ldSIsImV4cCI6MTc1NTg1ODIyMywiaWF0IjoxNzU1ODU0NjIzLCJ2YyI6eyJAY29udGV4dCI6WyJodHRwczovL3d3dy53My5vcmcvbnMvY3JlZGVudGlhbHMvdjIiLCJodHRwczovL3RydXN0LWZyYW1ld29yay5kb21lLW1hcmtldHBsYWNlLmV1L2NyZWRlbnRpYWxzL2xlYXJjcmVkZW50aWFsZW1wbG95ZWUvdjEiXSwiY3JlZGVudGlhbFN1YmplY3QiOnsibWFuZGF0ZSI6eyJpZCI6IjUyY2VjZjc1LTc2YmEtNGRhZC04ZTg2LTBlNWMyY2FhNDA5NCIsImxpZmVfc3BhbiI6eyJlbmRfZGF0ZV90aW1lIjoiMjAyNS0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIiwic3RhcnRfZGF0ZV90aW1lIjoiMjAyNC0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIn0sIm1hbmRhdGVlIjp7ImVtYWlsIjoiamVzdXNAYWxhc3RyaWEuaW8iLCJmaXJzdF9uYW1lIjoiSmVzdXMiLCJpZCI6ImRpZDprZXk6ekRuYWVjTHY1YXlDcnpXUmlZeUYzdmFIaEtxVFNrRkdXaUpKZE1mQXJXblpXY3FSbiIsImxhc3RfbmFtZSI6IlJ1aXoiLCJtb2JpbGVfcGhvbmUiOiIifSwibWFuZGF0b3IiOnsiY29tbW9uTmFtZSI6Ikplc3VzIFJ1aXoiLCJjb3VudHJ5IjoiRVMiLCJlbWFpbEFkZHJlc3MiOiJqZXN1cy5ydWl6QGluMi5lcyIsIm9yZ2FuaXphdGlvbiI6IklOMiBJTkdFTklFUklBIERFIExBIElORk9STUFDSU9OIFNPQ0lFREFEIExJTUlUQURBIiwib3JnYW5pemF0aW9uSWRlbnRpZmllciI6IlZBVEVTLUI2MDY0NTkwMCIsInNlcmlhbE51bWJlciI6Ijg3NjU0MzIxSyJ9LCJwb3dlciI6W3siaWQiOiIwNTRiOGY2Zi0wZjE3LTQ2NjItYWUyNi0zMTcxZGZhZGRiODciLCJ0bWZfYWN0aW9uIjoiRXhlY3V0ZSIsInRtZl9kb21haW4iOiJET01FIiwidG1mX2Z1bmN0aW9uIjoiT25ib2FyZGluZyIsInRtZl90eXBlIjoiRG9tYWluIn0seyJpZCI6IjNmNjQyZTg4LWQzYzAtNDI2My04NzE4LWE0MzYzNzJkMWE1NiIsInRtZl9hY3Rpb24iOlsiQ3JlYXRlIiwiVXBkYXRlIiwiRGVsZXRlIl0sInRtZl9kb21haW4iOiJET01FIiwidG1mX2Z1bmN0aW9uIjoiUHJvZHVjdE9mZmVyaW5nIiwidG1mX3R5cGUiOiJEb21haW4ifV0sInNpZ25lciI6eyJjb21tb25OYW1lIjoiNTY1NjU2NTZQIEplc3VzIFJ1aXoiLCJjb3VudHJ5IjoiRVMiLCJlbWFpbEFkZHJlc3MiOiJqZXN1cy5ydWl6QGluMi5lcyIsIm9yZ2FuaXphdGlvbiI6IkRPTUUgQ3JlZGVudGlhbCBJc3N1ZXIiLCJvcmdhbml6YXRpb25JZGVudGlmaWVyIjoiVkFURVMtUTAwMDAwMDBKIiwic2VyaWFsTnVtYmVyIjoiSURDRVMtNTY1NjU2NTZQIn19fSwiaWQiOiIxNjI3NGZjYy0wNzQ0LTQyNjYtYjhiMS00ZjQ4ZmY4MjdlMTQiLCJpc3N1ZXIiOiJkaWQ6ZWxzaTpWQVRFUy1RMDAwMDAwMEoiLCJ0eXBlIjpbIkxFQVJDcmVkZW50aWFsRW1wbG95ZWUiLCJWZXJpZmlhYmxlQ3JlZGVudGlhbCJdLCJ2YWxpZEZyb20iOiIyMDI0LTEyLTIzVDEwOjE1OjQ4Ljg2ODI5Mzg2M1oiLCJ2YWxpZFVudGlsIjoiMjAyNS0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIn0sImp0aSI6IjU5MDU1MWI3LWYxNTQtNGMxYi04MTEwLTE5MWUwMzg4MzI2NiIsImNsaWVudF9pZCI6Imh0dHBzOi8vdmVyaWZpZXIuZG9tZS1tYXJrZXRwbGFjZS5ldSJ9.KJ89dcb2N60GIrq0ESmQkQzL15JI3PulkJEDPMIhuFqunVKA7AAoVsi4lDD8rri5ZT1fCIjXPRbUHJk4wXN-mw`

// extractCallerInfo retrieves the Access Token from the request, verifies it if it exists and
// creates a map ready to be passed to the rules engine.
//
// The access token may not exist, but if it does then it must be valid.
// For convenience of the policies, some calculated fields are created and returned in the 'user' object.
func (r *Request) extractCallerInfo() (tokenClaims map[string]any, err error) {

	var authUser *AuthUser

	// Check if we are testing the PDP, and if so, use a fake access token
	if FakeClaims && len(r.JWTToken) == 0 {

		slog.Debug("PDP: using fake claims for testing")
		r.JWTToken = fakeAT

	}

	if len(r.JWTToken) == 0 {
		// An empty token is not considered an error, and the caller should enforce its existence
		return nil, nil
	}

	// It is an error to send an invaild token with the request, so we have to verify it.

	// Verify the token and extract the claims.
	// A verification error stops processing.

	tokenClaims, authUser, err = ParseJWT(r.JWTToken)
	if err != nil {
		slog.Error("invalid access token", slogor.Err(err), "token", r.JWTToken)
		return nil, errl.Errorf("invalid access token: %w", err)
	}

	verifiableCredential := jpath.GetMap(tokenClaims, "vc")

	if len(verifiableCredential) > 0 {
		authUser.isAuthenticated = true

		powers := jpath.GetList(verifiableCredential, "credentialSubject.mandate.power")
		for _, p := range powers {

			// This is to support old version of the Verifiable Credential
			ptype := jpath.GetString(p, "type")
			pdomain := jpath.GetString(p, "domain")
			pfunction := jpath.GetString(p, "function")
			paction := jpath.GetString(p, "action")

			// Check fields without regards to case
			if strings.EqualFold(ptype, "Domain") &&
				strings.EqualFold(pdomain, "DOME") &&
				strings.EqualFold(pfunction, "Onboarding") &&
				strings.EqualFold(paction, "execute") {

				authUser.isLEAR = true

			}

			// And this for the new version of the Verifiable Credential
			ptype = jpath.GetString(p, "tmf_type")
			pdomain = jpath.GetString(p, "tmf_domain")
			pfunction = jpath.GetString(p, "tmf_function")
			paction = jpath.GetString(p, "tmf_action")

			if strings.EqualFold(ptype, "Domain") &&
				strings.EqualFold(pdomain, "DOME") &&
				strings.EqualFold(pfunction, "Onboarding") &&
				strings.EqualFold(paction, "execute") {

				authUser.isLEAR = true
			}

		}

	} else {

		// There is not a Verifiable Credential inside the token
		return nil, errl.Errorf("access token without verifiable credential: %s", r.JWTToken)

	}

	r.AuthUser = authUser

	return tokenClaims, nil

}

// setSellerAndBuyerInfo adds the required fields to the incoming object argument
// Specifically, the Seller and SellerOperator roles are added to the relatedParty list
func setSellerAndBuyerInfo(tmfObjectMap map[string]any, organizationIdentifier string) (err error) {

	// Normalize all organization identifiers to the DID format
	if !strings.HasPrefix(organizationIdentifier, "did:elsi:") {
		organizationIdentifier = "did:elsi:" + organizationIdentifier
	}

	// TODO: add @schemalocation and @type

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties := jpath.GetList(tmfObjectMap, "relatedParty")

	// Build the two entries
	sellerEntry := map[string]any{
		"role":  "Seller",
		"@type": "RelatedPartyRefOrPartyRoleRef",
		"partyOrPartyRole": map[string]any{
			"@type":         "PartyRef",
			"href":          "urn:ngsi-ld:organization:" + organizationIdentifier,
			"id":            "urn:ngsi-ld:organization:" + organizationIdentifier,
			"name":          organizationIdentifier,
			"@referredType": "Organization",
		},
	}
	sellerOperator := map[string]any{
		"role":  "SellerOperator",
		"@type": "RelatedPartyRefOrPartyRoleRef",
		"partyOrPartyRole": map[string]any{
			"@type":         "PartyRef",
			"href":          "urn:ngsi-ld:organization:221f6434-ec82-4c62",
			"id":            "urn:ngsi-ld:organization:221f6434-ec82-4c62",
			"name":          "did:elsi:VATES-22222222",
			"@referredType": "Organization",
		},
	}

	if len(relatedParties) == 0 {
		slog.Debug("setSellerAndBuyerInfo: no relatedParty, adding seller and sellerOperator")
		tmfObjectMap["relatedParty"] = []any{sellerEntry, sellerOperator}
		return nil
	}

	foundSeller := false
	foundSellerOperator := false

	newRelatedParties := []any{}

	for _, rp := range relatedParties {

		// Convert entry to a map
		rpMap, _ := rp.(map[string]any)
		if len(rpMap) == 0 {
			return errl.Errorf("invalid relatedParty entry")
		}

		rpRole, _ := rpMap["role"].(string)
		rpRole = strings.ToLower(rpRole)

		if rpRole != "seller" && rpRole != "selleroperator" {
			newRelatedParties = append(newRelatedParties, rp)
			// Go to next entry
			continue
		}

		if rpRole == "seller" {
			// Overwrite the entry, because we can not allow the user to create fake info
			newRelatedParties = append(newRelatedParties, sellerEntry)
			foundSeller = true
			continue
		}
		if rpRole == "selleroperator" {
			// Overwrite the entry, because we can not allow the user to create fake info
			newRelatedParties = append(newRelatedParties, sellerOperator)
			foundSellerOperator = true
			continue
		}

	}

	if !foundSeller {
		// Add the seller if it is not already in the list
		slog.Debug("setSellerAndBuyerInfo: adding seller", "organizationIdentifier", organizationIdentifier)
		relatedParties = append(relatedParties, sellerEntry)
	}

	if !foundSellerOperator {
		// Add the seller operator if it is not already in the list
		slog.Debug("setSellerAndBuyerInfo: adding seller operator", "organizationIdentifier", organizationIdentifier)
		relatedParties = append(relatedParties, sellerOperator)
	}

	tmfObjectMap["relatedParty"] = newRelatedParties

	return nil

}

func getSellerAndBuyerInfo(tmfObjectMap map[string]any) (sellerDid string, sellerOperatorDid string, err error) {

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties := jpath.GetList(tmfObjectMap, "relatedParty")

	if len(relatedParties) == 0 {
		err = errl.Errorf("no relatedParty")
		return
	}

	for _, rp := range relatedParties {

		// Convert entry to a map
		rpMap, _ := rp.(map[string]any)
		if len(rpMap) == 0 {
			return "", "", errl.Errorf("invalid relatedParty entry")
		}

		rpRole, _ := rpMap["role"].(string)
		rpRole = strings.ToLower(rpRole)

		if rpRole != "seller" && rpRole != "selleroperator" {
			// Go to next entry
			continue
		}

		if rpRole == "seller" {
			party, _ := rpMap["partyOrPartyRole"].(map[string]any)
			sellerDid, _ = party["name"].(string)
			continue
		}
		if rpRole == "selleroperator" {
			party, _ := rpMap["partyOrPartyRole"].(map[string]any)
			sellerDid, _ = party["name"].(string)
			continue
		}

	}

	if sellerDid == "" {
		err = errl.Errorf("no seller")
		return
	}
	if sellerOperatorDid == "" {
		err = errl.Errorf("no seller operator")
		return
	}

	return

}
