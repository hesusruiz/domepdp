package tmapi

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/goccy/go-json"

	"github.com/golang-jwt/jwt/v5"
	"github.com/hesusruiz/domeproxy/pdp"
	"github.com/hesusruiz/domeproxy/tmfsync"
	"gitlab.com/greyxor/slogor"
)

// The standard HTTP error response for TMF APIs
type ErrorTMF struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

// tokenFromHeader retrieves the token string in the authorization header of an HTTP request
func tokenFromHeader(r *http.Request) string {
	// Get token from authorization header.
	bearer := r.Header.Get("Authorization")
	if len(bearer) > 7 && strings.ToUpper(bearer[0:6]) == "BEARER" {
		return bearer[7:]
	}
	return ""
}

func addHttpRoutes(
	mux *http.ServeMux,
	config *Config,
	tmf *tmfsync.TMFdb,
) {

	// Create an instance of the rules engine for the evaluation of the authorization policy rules
	ruleEngine, err := pdp.NewPDP("auth_policies.star")
	if err != nil {
		panic(err)
	}

	// Many routes share the same handlers, thanks to the consistency of the TMF APIs and underlying data model.
	// We have one for all LIST requests (get several objects), and another for GET requests (get one object)
	mux.HandleFunc("GET /catalog/category", handleGETlist("category", tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOffering", handleGETlist("productOffering", tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/catalog", handleGETlist("catalog", tmf, ruleEngine))

	mux.HandleFunc("GET /catalog/category/{id}", handleGET("category", tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOffering/{id}", handleGET("productOffering", tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productSpecification/{id}", handleGET("productSpecification", tmf, ruleEngine))
	mux.HandleFunc("GET /catalog/productOfferingPrice/{id}", handleGET("productOfferingPrice", tmf, ruleEngine))

	mux.HandleFunc("GET /service/serviceSpecification/{id}", handleGET("serviceSpecification", tmf, ruleEngine))
	mux.HandleFunc("GET /resource/resourceSpecification/{id}", handleGET("resourceSpecification", tmf, ruleEngine))

	mux.HandleFunc("GET /party/organization/{id}", handleGET("organization", tmf, ruleEngine))

}

// handleGETlist retrieves a list of objects, subject to filtering
func handleGETlist(tmfType string, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// For the moment, a LIST request does not go through authorization, and are fully public
		_ = ruleEngine

		slog.Info("!!!!!!!!!!!!!!!!!!!!! LIST", "type", tmfType)
		tmfObjectList, err := retrieveList(tmf, tmfType, r)
		if err != nil {
			sendError(w, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		var listMaps = []map[string]any{}
		for _, v := range tmfObjectList {
			listMaps = append(listMaps, v.ContentMap)
		}

		out, err := json.Marshal(listMaps)
		if err != nil {
			sendError(w, "error marshalling list", err.Error())
			slog.Error("error marshalling list", slogor.Err(err))
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(out)))
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.Header().Set("X-Powered-By", "MITM Proxy")
		w.Write(out)

	}
}

// getClaimsFromRequest verifies the Access Token received with the request, and extracts the claims in its payload.
// The most important claim in the payload is the LEARCredential that was used for authentication.
func getClaimsFromRequest(r *http.Request) (claims string, found bool, err error) {
	var token *jwt.Token

	publicKeyFunc := func(*jwt.Token) (interface{}, error) {

		// Get the Verifier keys
		domeJWKS, err := pdp.DOME_JWKS()
		if err != nil {
			return nil, err
		}

		domeJWK := domeJWKS.Keys[0]

		return domeJWK.Key, nil

	}

	claims = "{}"

	tokString := tokenFromHeader(r)
	if tokString != "" {

		token, err = jwt.NewParser().ParseWithClaims(tokString, jwt.MapClaims{}, publicKeyFunc)

		// token, _, err = jwt.NewParser().ParseUnverified(tokString, jwt.MapClaims{})
		if err != nil {
			return "{}", false, err
		}
		cl, err := json.Marshal(token.Claims)
		if err != nil {
			return "{}", false, err
		}
		claims = string(cl)
	}

	return claims, true, nil

}

// handleGET retrieves a single TMF object, subject to authorization policy rules
func handleGET(tmfType string, tmf *tmfsync.TMFdb, ruleEngine *pdp.PDP) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		// Retrieve the claims in the Access Token from the request, if they exists.
		// We do not enforce here its existence, and delegate enforcement (or not) to the policies in the rule engine
		claimsString, found, err := getClaimsFromRequest(r)
		if err != nil {
			slog.Error("error parsing token", slogor.Err(err))
			return
		}

		// Just some logs
		if !found {
			slog.Warn("no token found")
		} else {
			slog.Debug("Access Token found", "claims", claimsString)
		}

		// Retrieve the object, either from our local database or remotely it it does not yet exist
		tmfObject, err := retrieveLocalObject(tmf, tmfType, r)
		if err != nil {
			sendError(w, "error retrieving", err.Error())
			slog.Error("retrieving", slogor.Err(err))
			return
		}

		// Ask the rules engine for a decision on this request
		decision, err := ruleEngine.TakeAuthnDecision(pdp.Authenticate, r, claimsString, tmfObject)

		// An error is considered a rejection
		if err != nil {
			sendError(w, "error taking decision", err.Error())
			slog.Warn("REJECTED REJECTED REJECTED 0000000000000000000000")
			return
		}

		// The rules engine rejected the request
		if !decision {
			sendError(w, "not authenticated", "the policies said NOT!!!")
			slog.Warn("REJECTED REJECTED REJECTED 0000000000000000000000")
			return
		}

		// The rules engine accepted the request, return it to the caller
		slog.Info("ACCEPTED ACCEPTED ACCEPTED 1111111111111111111111")

		w.Header().Set("Content-Length", strconv.Itoa(len(tmfObject.Content)))
		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.Write(tmfObject.Content)

	}
}

func sendError(w http.ResponseWriter, code string, reason string) {
	errtmf := &ErrorTMF{
		Code:   code,
		Reason: reason,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(errtmf)

}

func retrieveLocalObject(tmf *tmfsync.TMFdb, tmfType string, r *http.Request) (*tmfsync.TMFObject, error) {

	id := r.PathValue("id")

	slog.Debug("retrieving", "type", tmfType, "id", id)

	// Retrieve the product offerings
	object, local, err := tmf.RetrieveOrUpdateObject(nil, id, "", "", tmfsync.LocalOrRemote)
	if err != nil {
		return nil, err
	}
	if local {
		slog.Debug("object retrieved locally")
	} else {
		slog.Debug("object retrieved remotely")
	}

	return object, nil

}

func retrieveList(tmf *tmfsync.TMFdb, tmfType string, r *http.Request) ([]*tmfsync.TMFObject, error) {

	r.ParseForm()

	slog.Debug("retrieving", "type", tmfType)

	// Retrieve the product offerings
	objects, _, err := tmf.RetrieveLocalListTMFObject(nil, tmfType, r.Form)
	if err != nil {
		return nil, err
	}

	slog.Debug("object retrieved locally")

	return objects, nil

}
