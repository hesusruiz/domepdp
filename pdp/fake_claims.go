// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

// This file contains helper functions for testing.

package pdp

import (
	"encoding/json"
	"strings"
	"time"
)

// getFakeClaims returns a map representing JWT claims for testing.
// It allows specifying if the user is a LEAR, their organization identifier, and country.
func getFakeClaims(isLear bool, organizationIdentifier, country string) map[string]any {
	claims := map[string]any{
		"iss": "https://fake-issuer.com",
		"sub": "fake-subject-did",
		"aud": []string{"fake-audience"},
		"exp": float64(time.Now().Add(time.Hour).Unix()),
		"iat": float64(time.Now().Unix()),
		"vc": map[string]any{
			"credentialSubject": map[string]any{
				"mandate": map[string]any{
					"mandator": map[string]any{
						"organizationIdentifier": organizationIdentifier,
						"country":                country,
					},
				},
			},
		},
	}

	onboardingPower := map[string]any{
		"type":     "Domain",
		"domain":   "DOME",
		"function": "Onboarding",
		"action":   "execute",
	}

	if isLear {
		// This is a bit verbose, but it ensures we are modifying the nested map correctly.
		vc, _ := claims["vc"].(map[string]any)
		credentialSubject, _ := vc["credentialSubject"].(map[string]any)
		mandate, _ := credentialSubject["mandate"].(map[string]any)
		mandate["power"] = []any{
			onboardingPower,
		}
	}

	return claims
}

// getFakeClaimsFromToken can be used in tests to replace the real getClaimsFromToken method.
// It returns a canned claims object for testing purposes.
// It doesn't perform any validation, just returns a fake claims map.
func (m *PDP) getFakeClaimsFromToken(tokString string) (claims map[string]any, found bool, err error) {
	if tokString == "" {
		return nil, false, nil
	}

	// For testing, you can use different fake tokens to get different claims.
	// For example: "fake-lear-token" vs "fake-normal-user-token"
	var fakeClaims map[string]any
	if strings.Contains(tokString, "lear") {
		fakeClaims = getFakeClaims(true, "did:elsi:fake-lear-org-id", "FR")
	} else {
		fakeClaims = getFakeClaims(false, "did:elsi:fake-user-org-id", "ES")
	}

	return fakeClaims, true, nil
}

func getFakeClaimsJR() map[string]any {
	// Unmarshall the fakeATPayload into a map
	claims := make(map[string]any)
	err := json.Unmarshal([]byte(fakeATPayload), &claims)
	if err != nil {
		panic("Failed to unmarshal fake AT payload: " + err.Error())
	}
	return claims
}

const fakeATPayload = `{
  "aud": "did:key:zDnaeTU39Wx9KXgmEwmfXsZSyEVxgCqwCVmoPyVQUTD8bhW8a",
  "sub": "did:key:zDnaecLv5ayCrzWRiYyF3vaHhKqTSkFGWiJJdMfArWnZWcqRn",
  "scope": "openid learcredential",
  "iss": "https://verifier.dome-marketplace.eu",
  "exp": 1755672779,
  "iat": 1755669179,
  "vc": {
    "@context": [
      "https://www.w3.org/ns/credentials/v2",
      "https://trust-framework.dome-marketplace.eu/credentials/learcredentialemployee/v1"
    ],
    "credentialSubject": {
      "mandate": {
        "id": "52cecf75-76ba-4dad-8e86-0e5c2caa4094",
        "life_span": {
          "end_date_time": "2025-12-23T10:15:48.868293863Z",
          "start_date_time": "2024-12-23T10:15:48.868293863Z"
        },
        "mandatee": {
          "email": "jesus@alastria.io",
          "first_name": "Jesus",
          "id": "did:key:zDnaecLv5ayCrzWRiYyF3vaHhKqTSkFGWiJJdMfArWnZWcqRn",
          "last_name": "Ruiz",
          "mobile_phone": ""
        },
        "mandator": {
          "commonName": "Jesus Ruiz",
          "country": "ES",
          "emailAddress": "jesus.ruiz@in2.es",
          "organization": "IN2 INGENIERIA DE LA INFORMACION SOCIEDAD LIMITADA",
          "organizationIdentifier": "VATES-B60645900",
          "serialNumber": "87654321K"
        },
        "power": [
          {
            "id": "054b8f6f-0f17-4662-ae26-3171dfaddb87",
            "tmf_action": "Execute",
            "tmf_domain": "DOME",
            "tmf_function": "Onboarding",
            "tmf_type": "Domain"
          },
          {
            "id": "3f642e88-d3c0-4263-8718-a436372d1a56",
            "tmf_action": [
              "Create",
              "Update",
              "Delete"
            ],
            "tmf_domain": "DOME",
            "tmf_function": "ProductOffering",
            "tmf_type": "Domain"
          }
        ],
        "signer": {
          "commonName": "56565656P Jesus Ruiz",
          "country": "ES",
          "emailAddress": "jesus.ruiz@in2.es",
          "organization": "DOME Credential Issuer",
          "organizationIdentifier": "VATES-Q0000000J",
          "serialNumber": "IDCES-56565656P"
        }
      }
    },
    "id": "16274fcc-0744-4266-b8b1-4f48ff827e14",
    "issuer": "did:elsi:VATES-Q0000000J",
    "type": [
      "LEARCredentialEmployee",
      "VerifiableCredential"
    ],
    "validFrom": "2024-12-23T10:15:48.868293863Z",
    "validUntil": "2025-12-23T10:15:48.868293863Z"
  },
  "jti": "4c492f01-6d18-43d2-a040-f351cb813ee0",
  "client_id": "https://verifier.dome-marketplace.eu"
}`

const fakeAT = `eyJraWQiOiJkaWQ6a2V5OnpEbmFlVlluV1RadTVuYnJIMXFtQlZNdk53U3J0S25rUmJDWjR4SDVoMkxRUG56ZHIiLCJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJhdWQiOiJkaWQ6a2V5OnpEbmFlVFUzOVd4OUtYZ21Fd21mWHNaU3lFVnhnQ3F3Q1Ztb1B5VlFVVEQ4YmhXOGEiLCJzdWIiOiJkaWQ6a2V5OnpEbmFlY0x2NWF5Q3J6V1JpWXlGM3ZhSGhLcVRTa0ZHV2lKSmRNZkFyV25aV2NxUm4iLCJzY29wZSI6Im9wZW5pZCBsZWFyY3JlZGVudGlhbCIsImlzcyI6Imh0dHBzOi8vdmVyaWZpZXIuZG9tZS1tYXJrZXRwbGFjZS5ldSIsImV4cCI6MTc1NTg1ODIyMywiaWF0IjoxNzU1ODU0NjIzLCJ2YyI6eyJAY29udGV4dCI6WyJodHRwczovL3d3dy53My5vcmcvbnMvY3JlZGVudGlhbHMvdjIiLCJodHRwczovL3RydXN0LWZyYW1ld29yay5kb21lLW1hcmtldHBsYWNlLmV1L2NyZWRlbnRpYWxzL2xlYXJjcmVkZW50aWFsZW1wbG95ZWUvdjEiXSwiY3JlZGVudGlhbFN1YmplY3QiOnsibWFuZGF0ZSI6eyJpZCI6IjUyY2VjZjc1LTc2YmEtNGRhZC04ZTg2LTBlNWMyY2FhNDA5NCIsImxpZmVfc3BhbiI6eyJlbmRfZGF0ZV90aW1lIjoiMjAyNS0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIiwic3RhcnRfZGF0ZV90aW1lIjoiMjAyNC0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIn0sIm1hbmRhdGVlIjp7ImVtYWlsIjoiamVzdXNAYWxhc3RyaWEuaW8iLCJmaXJzdF9uYW1lIjoiSmVzdXMiLCJpZCI6ImRpZDprZXk6ekRuYWVjTHY1YXlDcnpXUmlZeUYzdmFIaEtxVFNrRkdXaUpKZE1mQXJXblpXY3FSbiIsImxhc3RfbmFtZSI6IlJ1aXoiLCJtb2JpbGVfcGhvbmUiOiIifSwibWFuZGF0b3IiOnsiY29tbW9uTmFtZSI6Ikplc3VzIFJ1aXoiLCJjb3VudHJ5IjoiRVMiLCJlbWFpbEFkZHJlc3MiOiJqZXN1cy5ydWl6QGluMi5lcyIsIm9yZ2FuaXphdGlvbiI6IklOMiBJTkdFTklFUklBIERFIExBIElORk9STUFDSU9OIFNPQ0lFREFEIExJTUlUQURBIiwib3JnYW5pemF0aW9uSWRlbnRpZmllciI6IlZBVEVTLUI2MDY0NTkwMCIsInNlcmlhbE51bWJlciI6Ijg3NjU0MzIxSyJ9LCJwb3dlciI6W3siaWQiOiIwNTRiOGY2Zi0wZjE3LTQ2NjItYWUyNi0zMTcxZGZhZGRiODciLCJ0bWZfYWN0aW9uIjoiRXhlY3V0ZSIsInRtZl9kb21haW4iOiJET01FIiwidG1mX2Z1bmN0aW9uIjoiT25ib2FyZGluZyIsInRtZl90eXBlIjoiRG9tYWluIn0seyJpZCI6IjNmNjQyZTg4LWQzYzAtNDI2My04NzE4LWE0MzYzNzJkMWE1NiIsInRtZl9hY3Rpb24iOlsiQ3JlYXRlIiwiVXBkYXRlIiwiRGVsZXRlIl0sInRtZl9kb21haW4iOiJET01FIiwidG1mX2Z1bmN0aW9uIjoiUHJvZHVjdE9mZmVyaW5nIiwidG1mX3R5cGUiOiJEb21haW4ifV0sInNpZ25lciI6eyJjb21tb25OYW1lIjoiNTY1NjU2NTZQIEplc3VzIFJ1aXoiLCJjb3VudHJ5IjoiRVMiLCJlbWFpbEFkZHJlc3MiOiJqZXN1cy5ydWl6QGluMi5lcyIsIm9yZ2FuaXphdGlvbiI6IkRPTUUgQ3JlZGVudGlhbCBJc3N1ZXIiLCJvcmdhbml6YXRpb25JZGVudGlmaWVyIjoiVkFURVMtUTAwMDAwMDBKIiwic2VyaWFsTnVtYmVyIjoiSURDRVMtNTY1NjU2NTZQIn19fSwiaWQiOiIxNjI3NGZjYy0wNzQ0LTQyNjYtYjhiMS00ZjQ4ZmY4MjdlMTQiLCJpc3N1ZXIiOiJkaWQ6ZWxzaTpWQVRFUy1RMDAwMDAwMEoiLCJ0eXBlIjpbIkxFQVJDcmVkZW50aWFsRW1wbG95ZWUiLCJWZXJpZmlhYmxlQ3JlZGVudGlhbCJdLCJ2YWxpZEZyb20iOiIyMDI0LTEyLTIzVDEwOjE1OjQ4Ljg2ODI5Mzg2M1oiLCJ2YWxpZFVudGlsIjoiMjAyNS0xMi0yM1QxMDoxNTo0OC44NjgyOTM4NjNaIn0sImp0aSI6IjU5MDU1MWI3LWYxNTQtNGMxYi04MTEwLTE5MWUwMzg4MzI2NiIsImNsaWVudF9pZCI6Imh0dHBzOi8vdmVyaWZpZXIuZG9tZS1tYXJrZXRwbGFjZS5ldSJ9.KJ89dcb2N60GIrq0ESmQkQzL15JI3PulkJEDPMIhuFqunVKA7AAoVsi4lDD8rri5ZT1fCIjXPRbUHJk4wXN-mw`
