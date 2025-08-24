package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// ExtractJWTToken extracts the JWT token from the Authorization header.
// It handles both "Bearer <token>" and raw token formats.
func ExtractJWTToken(authHeader string) string {
	jwtToken := ""
	if after, ok := strings.CutPrefix(authHeader, "Bearer "); ok {
		jwtToken = after
	} else if authHeader != "" {
		jwtToken = authHeader
	}
	return jwtToken
}

// ParseJWT parses a JWT string, extracts the mandator information, and returns an AuthUser.
// It does NOT verify the JWT signature.
func ParseJWT(tokenString string) (tokString map[string]any, u *AuthUser, err error) {

	// Parse the token without signature verification
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		slog.Error("Failed to parse JWT unverified", slog.Any("error", err))
		return nil, nil, fmt.Errorf("failed to parse JWT: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		slog.Error("JWT claims are not of type MapClaims")
		return nil, nil, errors.New("invalid JWT claims format")
	}

	// Extract mandator object from vc.credentialSubject.mandate.mandator
	vc, ok := claims["vc"].(map[string]any)
	if !ok {
		slog.Debug("JWT payload does not contain 'vc' field or it's not a map")
		return nil, nil, errors.New("missing 'vc' in JWT claims")
	}

	credentialSubject, ok := vc["credentialSubject"].(map[string]any)
	if !ok {
		slog.Debug("JWT payload does not contain 'credentialSubject' field or it's not a map")
		return nil, nil, errors.New("missing 'credentialSubject' in JWT claims")
	}

	mandate, ok := credentialSubject["mandate"].(map[string]any)
	if !ok {
		slog.Debug("JWT payload does not contain 'mandate' field or it's not a map")
		return nil, nil, errors.New("missing 'mandate' in JWT claims")
	}

	mandatorData, ok := mandate["mandator"].(map[string]any)
	if !ok {
		slog.Debug("JWT payload does not contain 'mandator' field or it's not a map")
		return nil, nil, errors.New("missing 'mandator' in JWT claims")
	}

	// Marshal and unmarshal to AuthUser struct for type safety and JSON tag mapping
	mandatorJSON, err := json.Marshal(mandatorData)
	if err != nil {
		slog.Error("Failed to marshal mandator data", slog.Any("error", err))
		return nil, nil, fmt.Errorf("failed to process mandator data: %w", err)
	}

	var authUser AuthUser
	if err := json.Unmarshal(mandatorJSON, &authUser); err != nil {
		slog.Error("Failed to unmarshal mandator data to AuthUser", slog.Any("error", err))
		return nil, nil, fmt.Errorf("failed to process mandator data: %w", err)
	}

	slog.Debug("Successfully parsed AuthUser from JWT",
		slog.String("organizationIdentifier", authUser.OrganizationIdentifier),
		slog.String("country", authUser.Country))

	return claims, &authUser, nil
}
