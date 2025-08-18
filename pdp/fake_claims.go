// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

// This file contains helper functions for testing.

package pdp

import (
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

	if isLear {
		// This is a bit verbose, but it ensures we are modifying the nested map correctly.
		vc, _ := claims["vc"].(map[string]any)
		credentialSubject, _ := vc["credentialSubject"].(map[string]any)
		mandate, _ := credentialSubject["mandate"].(map[string]any)
		mandate["power"] = []map[string]any{
			{
				"type":     "Domain",
				"domain":   "DOME",
				"function": "Onboarding",
				"action":   "execute",
			},
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
