// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-jose/go-jose/v4"
	"github.com/goccy/go-json"
	"gitlab.com/greyxor/slogor"
)

var domeVerifierStaticConf = `{
    "issuer": "https://verifier.dome-marketplace-prd.org",
    "authorization_endpoint": "https://verifier.dome-marketplace-prd.org/oidc/authorize",
    "device_authorization_endpoint": "https://verifier.dome-marketplace-prd.org/oidc/device_authorization",
    "token_endpoint": "https://verifier.dome-marketplace-prd.org/oidc/token",
    "token_endpoint_auth_methods_supported": [
        "client_secret_basic",
        "client_secret_post",
        "client_secret_jwt",
        "private_key_jwt",
        "tls_client_auth",
        "self_signed_tls_client_auth"
    ],
    "jwks_uri": "https://verifier.dome-marketplace-prd.org/oidc/jwks",
    "userinfo_endpoint": "https://verifier.dome-marketplace-prd.org/oidc/userinfo",
    "end_session_endpoint": "https://verifier.dome-marketplace-prd.org/oidc/logout",
    "response_types_supported": [
        "code"
    ],
    "grant_types_supported": [
        "authorization_code",
        "client_credentials",
        "refresh_token",
        "urn:ietf:params:oauth:grant-type:device_code",
        "urn:ietf:params:oauth:grant-type:token-exchange"
    ],
    "revocation_endpoint": "https://verifier.dome-marketplace-prd.org/oidc/revoke",
    "revocation_endpoint_auth_methods_supported": [
        "client_secret_basic",
        "client_secret_post",
        "client_secret_jwt",
        "private_key_jwt",
        "tls_client_auth",
        "self_signed_tls_client_auth"
    ],
    "introspection_endpoint": "https://verifier.dome-marketplace-prd.org/oidc/introspect",
    "introspection_endpoint_auth_methods_supported": [
        "client_secret_basic",
        "client_secret_post",
        "client_secret_jwt",
        "private_key_jwt",
        "tls_client_auth",
        "self_signed_tls_client_auth"
    ],
    "code_challenge_methods_supported": [
        "S256"
    ],
    "tls_client_certificate_bound_access_tokens": true,
    "subject_types_supported": [
        "public"
    ],
    "id_token_signing_alg_values_supported": [
        "RS256"
    ],
    "scopes_supported": [
        "openid"
    ]
}`

const DOME_JWKS_JSON = `{
    "keys": [
        {
            "kty": "EC",
            "use": "sig",
            "crv": "P-256",
            "kid": "did:key:zDnaeVYnWTZu5nbrH1qmBVMvNwSrtKnkRbCZ4xH5h2LQPnzdr",
            "x": "TAmV5htgfwIOjgaDENCqSKUOsYvmIW_dHPXtYNpa-GU",
            "y": "OOxoUKEbvt-GZqc2296Kdxr6Ez4osae77J6T-JllKkA"
        }
    ]
}`

type OpenIDConfig struct {
	Issuer                                    string   `json:"issuer,omitempty"`
	AuthorizationEndpoint                     string   `json:"authorization_endpoint,omitempty"`
	DeviceAuthorizationEndpoint               string   `json:"device_authorization_endpoint,omitempty"`
	TokenEndpoint                             string   `json:"token_endpoint,omitempty"`
	TokenEndpointAuthMethodsSupported         []string `json:"token_endpoint_auth_methods_supported,omitempty"`
	JwksUri                                   string   `json:"jwks_uri,omitempty"`
	UserinfoEndpoint                          string   `json:"userinfo_endpoint,omitempty"`
	EndSessionEndpoint                        string   `json:"end_session_endpoint,omitempty"`
	ResponseTypesSupported                    []string `json:"response_types_supported,omitempty"`
	GrantTypesSupported                       []string `json:"grant_types_supported,omitempty"`
	RevocationEndpoint                        string   `json:"revocation_endpoint,omitempty"`
	RevocationEndpointAuthMethodsSupported    []string `json:"revocation_endpoint_auth_methods_supported,omitempty"`
	IntrospectionEndpoint                     string   `json:"introspection_endpoint,omitempty"`
	IntrospectionEndpointAuthMethodsSupported []string `json:"introspection_endpoint_auth_methods_supported,omitempty"`
	CodeChallengeMethodsSupported             []string `json:"code_challenge_methods_supported,omitempty"`
	TlsClientCertificateBoundAccessTokens     bool     `json:"tls_client_certificate_bound_access_tokens,omitempty"`
	SubjectTypesSupported                     []string `json:"subject_types_supported,omitempty"`
	IdTokenSigningAlgValuesSupported          []string `json:"id_token_signing_alg_values_supported,omitempty"`
	ScopesSupported                           []string `json:"scopes_supported,omitempty"`
}

func NewOpenIDConfigFromBytes(serialized []byte) (*OpenIDConfig, error) {
	oid := &OpenIDConfig{}
	err := json.Unmarshal(serialized, oid)
	if err != nil {
		return nil, err
	}

	return oid, nil
}

func MustNewOpenIDConfigFromBytes(serialized []byte) *OpenIDConfig {
	oid, err := NewOpenIDConfigFromBytes(serialized)
	if err != nil {
		panic(err)
	}
	return oid
}

var DOMEVerifierConfig = MustNewOpenIDConfigFromBytes([]byte(domeVerifierStaticConf))

func DOME_JWKS() (jose.JSONWebKeySet, error) {
	var jwks = jose.JSONWebKeySet{}
	err := json.Unmarshal([]byte(DOME_JWKS_JSON), &jwks)
	if err != nil {
		return jwks, err
	}
	return jwks, nil
}

const DOME_LCL_WellKnown = "https://verifier.dome-marketplace-lcl.org/.well-known/openid-configuration"
const DOME_DEV2_WellKnown = "https://verifier.dome-marketplace-dev2.org/.well-known/openid-configuration"
const DOME_PRO_WellKnown = "https://verifier.dome-marketplace.eu/.well-known/openid-configuration"

func NewOpenIDConfig(environment Environment) (*OpenIDConfig, error) {

	var verifierWellKnownURL string

	switch environment {
	case DOME_PRO:
		verifierWellKnownURL = DOME_PRO_WellKnown
	case DOME_DEV2:
		verifierWellKnownURL = DOME_DEV2_WellKnown
	case DOME_LCL:
		verifierWellKnownURL = DOME_LCL_WellKnown
	default:
		return nil, fmt.Errorf("invalid environment: %v", environment)
	}

	res, err := http.Get(verifierWellKnownURL)
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

	oid := &OpenIDConfig{}
	err = json.Unmarshal(body, oid)
	if err != nil {
		slog.Error("unmarshalling", slogor.Err(err))
		return nil, err
	}

	if oid.JwksUri == "" {
		return nil, fmt.Errorf("no JwksUri")
	}

	return oid, nil
}

func (oid *OpenIDConfig) VerificationJWK() (*jose.JSONWebKey, error) {

	if oid.JwksUri == "" {
		return nil, fmt.Errorf("no JwksUri")
	}

	res, err := http.Get(oid.JwksUri)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode > 299 {
		err := fmt.Errorf("response failed with status: %d", res.StatusCode)
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	var jwks = &jose.JSONWebKeySet{}
	err = json.Unmarshal(body, jwks)
	if err != nil {
		slog.Error("unmarshalling JWKS", slogor.Err(err))
		return nil, err
	}

	if len(jwks.Keys) == 0 {
		err := fmt.Errorf("no JWK keys returned")
		return nil, err
	}

	return &jwks.Keys[0], nil

}
