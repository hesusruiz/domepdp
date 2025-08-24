package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"log/slog"

	"github.com/google/uuid"
	"github.com/hesusruiz/domepdp/internal/errl"
	"github.com/hesusruiz/domepdp/pkg/apierror"
	repo "github.com/hesusruiz/domepdp/tmfserver/repository"
	svc "github.com/hesusruiz/domepdp/tmfserver/service"
)

// AuthUser represents the authenticated user's information from the JWT mandator object.
type AuthUser struct {
	CommonName             string `json:"commonName"`
	Country                string `json:"country"`
	EmailAddress           string `json:"emailAddress"`
	Organization           string `json:"organization"`
	OrganizationIdentifier string `json:"organizationIdentifier"`
	SerialNumber           string `json:"serialNumber"`
	isAuthenticated        bool
	isLEAR                 bool
	isOwner                bool
}

func (u *AuthUser) ToMap() map[string]any {
	return map[string]any{
		"commonName":             u.CommonName,
		"country":                u.Country,
		"emailAddress":           u.EmailAddress,
		"organization":           u.Organization,
		"organizationIdentifier": u.OrganizationIdentifier,
		"serialNumber":           u.SerialNumber,
		"isAuthenticated":        u.isAuthenticated,
		"isLEAR":                 u.isLEAR,
		"isOwner":                u.isOwner,
	}
}

// Request represents a generic HTTP request. All possible handlers must convert to this representation.
// In this way, we support easily any HTTP framework (currently Fiber and Echo), but also other
// future channels like JSON-RPC or even non-HTTP channels like GRPC.
type Request struct {
	Method       string
	Action       string
	APIfamily    string
	ResourceName string
	ID           string
	QueryParams  url.Values
	Body         []byte
	AuthUser     *AuthUser // Added field for authenticated user
	JWTToken     string    // Added field for raw JWT token
}

func (r *Request) ToMap() map[string]any {
	return map[string]any{
		"method":   r.Method,
		"action":   r.Action,
		"api":      r.APIfamily,
		"resource": r.ResourceName,
		"id":       r.ID,
	}
}

var HttpMethodAliases = map[string]string{
	"GET":    "READ",
	"POST":   "CREATE",
	"PATCH":  "UPDATE",
	"DELETE": "DELETE",
}

// Response represents a generic HTTP response.
type Response struct {
	StatusCode int
	Headers    map[string]string
	Body       any
}

// ToKebabCase converts a camelCase string to kebab-case.
// For example: "productOffering" becomes "product-offering".
func ToKebabCase(s string) string {
	var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(s, "${1}-${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}-${2}")
	return strings.ToLower(snake)
}

// CreateGenericObject creates a new TMF object using generalized parameters.
func CreateGenericObject(req *Request, service *svc.Service) *Response {
	slog.Debug("CreateGenericObject called", slog.String("apiFamily", req.APIfamily), slog.String("resourceName", req.ResourceName))

	// Before doing anything, check if we can extract the calling user info
	token, err := req.extractCallerInfo()
	if err != nil {
		err = errl.Errorf("invalid access token: %w", err)
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request", slog.Any("error", err))
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	// Creating an object can not be done without authentication
	if len(token) == 0 {
		err = errl.Errorf("user not authenticated")
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request")
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	var data map[string]any
	if err := json.Unmarshal(req.Body, &data); err != nil {
		err = errl.Errorf("failed to bind request body: %w", err)
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Failed to bind request body", slog.Any("error", err), slog.String("apiFamily", req.APIfamily), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
	}

	id, _ := data["id"].(string)
	if id == "" {
		// If the incoming object does not have an 'id', we generate a new one
		// The format is "urn:ngsi-ld:{resource-in-kebab-case}:{uuid}"
		id = fmt.Sprintf("urn:ngsi-ld:%s:%s", ToKebabCase(req.ResourceName), uuid.NewString())
		data["id"] = id
		slog.Debug("Generated new ID for object", "id", id)

	}

	href, _ := data["href"].(string)
	if href == "" {
		// Add href to the object
		data["href"] = fmt.Sprintf("/tmf-api/%s/v5/%s/%s", req.APIfamily, req.ResourceName, id)
		slog.Debug("Set href", slog.String("href", data["href"].(string)))
	}

	// Check and process '@type' field
	if typeVal, typeOk := data["@type"].(string); typeOk {
		if !strings.EqualFold(typeVal, req.ResourceName) {
			err = errl.Errorf("@type mismatch: expected %s, got %s", req.ResourceName, typeVal)
			apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
			slog.Error("@type mismatch", slog.String("expected", req.ResourceName), slog.String("got", typeVal), slog.String("apiFamily", req.APIfamily), slog.String("resourceName", req.ResourceName))
			return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
		}
	} else {
		// If @type is not specified, add it
		data["@type"] = req.ResourceName
		slog.Debug("Added missing @type field", slog.String("type", req.ResourceName))
	}

	// Set default version if not provided
	version, versionOk := data["version"].(string)
	if !versionOk || version == "" {
		version = "1.0"
		data["version"] = version // Update data map for content marshaling
		slog.Debug("Set default version", slog.String("version", version))
	}

	// Populate other common first-level attributes if not present
	var lastUpdate string
	if lu, ok := data["lastUpdate"].(string); ok {
		lastUpdate = lu
	} else if data["lastUpdate"] == nil {
		lastUpdate = time.Now().Format(time.RFC3339Nano)
		data["lastUpdate"] = lastUpdate
		slog.Debug("Set default lastUpdate", slog.String("lastUpdate", lastUpdate))
	} else {
		err = errl.Errorf("lastUpdate field must be a string or null")
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Invalid lastUpdate field type", slog.Any("lastUpdate", data["lastUpdate"]))
		return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
	}

	err = setSellerAndBuyerInfo(data, req.AuthUser.OrganizationIdentifier)
	if err != nil {
		err = errl.Errorf("failed to add Seller and Buyer info: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to add Seller and Buyer info", slog.Any("error", err), slog.String("apiFamily", req.APIfamily), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	content, err := json.Marshal(data)
	if err != nil {
		err = errl.Errorf("failed to marshal object content: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to marshal object content", slog.Any("error", err), slog.String("apiFamily", req.APIfamily), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	obj := repo.NewTMFObject(id, req.ResourceName, version, lastUpdate, content)

	// ************************************************************************************************
	// Before performing the action, check if the user can perform the operation on the object.
	// ************************************************************************************************

	userCanPerformAction := takeDecision(nil, req, token, obj)
	if !userCanPerformAction {
		err = errl.Errorf("user not authorized")
		apiErr := apierror.NewError("403", "Forbidden", err.Error(), fmt.Sprintf("%d", http.StatusForbidden), "")
		slog.Error("Unauthorized request")
		return &Response{StatusCode: http.StatusForbidden, Body: apiErr}
	}

	// ************************************************************************************************
	// Now we can proceed.
	// ************************************************************************************************

	if err := service.CreateObject(obj); err != nil {
		err = errl.Errorf("failed to create object in service: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to create object in service", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	headers := make(map[string]string)
	headers["Location"] = data["href"].(string)
	slog.Info("Object created successfully", slog.String("id", id), slog.String("resourceName", req.ResourceName), slog.String("location", data["href"].(string)))

	return &Response{
		StatusCode: http.StatusCreated,
		Headers:    headers,
		Body:       data,
	}
}

// GetGenericObject retrieves a TMF object using generalized parameters.
func GetGenericObject(req *Request, service *svc.Service) *Response {
	slog.Debug("GetGenericObject called", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))

	// Before doing anything, check if we can extract the calling user info
	token, err := req.extractCallerInfo()
	if err != nil {
		err = errl.Errorf("invalid access token: %w", err)
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request", slog.Any("error", err))
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	obj, err := service.GetObject(req.ID, req.ResourceName)
	if err != nil {
		err = errl.Errorf("failed to get object from service: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to get object from service", slog.Any("error", err), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	if obj == nil {
		err = errl.Errorf("object not found")
		apiErr := apierror.NewError("404", "Not Found", err.Error(), fmt.Sprintf("%d", http.StatusNotFound), "")
		slog.Info("Object not found", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusNotFound, Body: apiErr}
	}

	// ************************************************************************************************
	// Before performing the action, check if the user can perform the operation on the object.
	// ************************************************************************************************

	userCanPerformAction := takeDecision(nil, req, token, obj)
	if !userCanPerformAction {
		apiErr := apierror.NewError("403", "Forbidden", "user not authorized", fmt.Sprintf("%d", http.StatusForbidden), "")
		slog.Error("Unauthorized request")
		return &Response{StatusCode: http.StatusForbidden, Body: apiErr}
	}

	// ************************************************************************************************
	// Now we can proceed.
	// ************************************************************************************************

	var responseData map[string]any
	err = json.Unmarshal(obj.Content, &responseData)
	if err != nil {
		err = errl.Errorf("failed to unmarshal object content: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to unmarshal object content", slog.Any("error", err), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	// Handle partial field selection
	fieldsParam := req.QueryParams.Get("fields")
	if fieldsParam != "" {
		var fields []string
		if fieldsParam == "none" {
			fields = []string{"id", "href", "lastUpdate", "version"}
		} else {
			fields = strings.Split(fieldsParam, ",")
		}

		// Create a set of fields for quick lookup
		fieldSet := make(map[string]bool)
		for _, f := range fields {
			fieldSet[strings.TrimSpace(f)] = true
		}

		// Always include id, href, lastUpdate, version and @type
		fieldSet["id"] = true
		fieldSet["href"] = true
		fieldSet["lastUpdate"] = true
		fieldSet["version"] = true
		fieldSet["@type"] = true

		filteredItem := make(map[string]any)
		for key, value := range responseData {
			if fieldSet[key] {
				filteredItem[key] = value
			}
		}
		responseData = filteredItem
	}

	slog.Info("Object retrieved successfully", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
	return &Response{StatusCode: http.StatusOK, Body: responseData}
}

// UpdateGenericObject updates an existing TMF object using generalized parameters.
func UpdateGenericObject(req *Request, service *svc.Service) *Response {
	slog.Debug("UpdateGenericObject called", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))

	// Before doing anything, check if we can extract the calling user info
	token, err := req.extractCallerInfo()
	if err != nil {
		err = errl.Errorf("invalid access token: %w", err)
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request", slog.Any("error", err))
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	// Updating an object can not be done without authentication
	if len(token) == 0 {
		err = errl.Errorf("user not authenticated")
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request")
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	var data map[string]any
	if err := json.Unmarshal(req.Body, &data); err != nil {
		err = errl.Errorf("failed to bind request body: %w", err)
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Failed to bind request body for update", slog.Any("error", err), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
	}

	// If the ID is present in the body, ensure it matches the ID in the URL
	if bodyID, ok := data["id"]; ok {
		bodyIDStr, ok := bodyID.(string)
		if !ok || bodyIDStr != req.ID {
			err = errl.Errorf("ID in body must match ID in URL")
			apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
			slog.Error("ID mismatch in update request", slog.String("url_id", req.ID), slog.Any("body_id", bodyID), slog.String("resourceName", req.ResourceName))
			return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
		}
	}

	// Check and process '@type' field
	if typeVal, typeOk := data["@type"].(string); typeOk {
		if !strings.EqualFold(typeVal, req.ResourceName) {
			err = errl.Errorf("@type field in body must match resource name in URL")
			apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
			slog.Error("@type mismatch in update request", slog.String("expected", req.ResourceName), slog.String("got", typeVal), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
			return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
		}
	} else {
		// If @type is not specified, add it
		data["@type"] = req.ResourceName
		slog.Debug("Added missing @type field to update request", slog.String("type", req.ResourceName))
	}

	// Version must be specified for update operations
	version, versionOk := data["version"].(string)
	if !versionOk || version == "" {
		err = errl.Errorf("version field is required for update operations")
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Version missing from update request", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
	}

	var lastUpdate string
	if lu, ok := data["lastUpdate"].(string); ok {
		lastUpdate = lu
	} else if data["lastUpdate"] == nil {
		lastUpdate = time.Now().Format(time.RFC3339Nano)
		data["lastUpdate"] = lastUpdate
		slog.Debug("Set default lastUpdate for update", slog.String("lastUpdate", lastUpdate))
	} else {
		err = errl.Errorf("lastUpdate field must be a string or null")
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Invalid lastUpdate field type for update", slog.Any("lastUpdate", data["lastUpdate"]))
		return &Response{StatusCode: http.StatusBadRequest, Body: apiErr}
	}

	content, err := json.Marshal(data)
	if err != nil {
		err = errl.Errorf("failed to marshal object content for update: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to marshal object content for update", slog.Any("error", err), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	// Get existing object to preserve CreatedAt
	existingObj, err := service.GetObject(req.ID, req.ResourceName)
	if err != nil {
		err = errl.Errorf("failed to get existing object for update: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to get existing object for update", slog.Any("error", err), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	if existingObj == nil {
		err = errl.Errorf("object not found")
		apiErr := apierror.NewError("404", "Not Found", err.Error(), fmt.Sprintf("%d", http.StatusNotFound), "")
		slog.Info("Object not found for update", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusNotFound, Body: apiErr}
	}

	obj := &repo.TMFObject{
		ID:         req.ID,
		Type:       req.ResourceName,
		Version:    version,
		LastUpdate: lastUpdate,
		Content:    content,
		CreatedAt:  existingObj.CreatedAt,
		UpdatedAt:  time.Now(),
	}

	if err := service.UpdateObject(obj); err != nil {
		err = errl.Errorf("failed to update object in service: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to update object in service", slog.Any("error", err), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	slog.Info("Object updated successfully", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
	return &Response{StatusCode: http.StatusOK, Body: data}
}

// DeleteGenericObject deletes a TMF object using generalized parameters.
func DeleteGenericObject(req *Request, service *svc.Service) *Response {
	slog.Debug("DeleteGenericObject called", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))

	// Before doing anything, check if we can extract the calling user info
	token, err := req.extractCallerInfo()
	if err != nil {
		err = errl.Errorf("invalid access token: %w", err)
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request", slog.Any("error", err))
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	// Deleting an object can not be done without authentication
	if len(token) == 0 {
		err = errl.Errorf("user not authenticated")
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request")
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	if err := service.DeleteObject(req.ID, req.ResourceName); err != nil {
		err = errl.Errorf("failed to delete object from service: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to delete object from service", slog.Any("error", err), slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	slog.Info("Object deleted successfully", slog.String("id", req.ID), slog.String("resourceName", req.ResourceName))
	return &Response{StatusCode: http.StatusNoContent}
}

// ListGenericObjects retrieves all TMF objects of a given type using generalized parameters.
func ListGenericObjects(req *Request, service *svc.Service) *Response {
	slog.Debug("ListGenericObjects called", slog.String("resourceName", req.ResourceName))

	// Before doing anything, check if we can extract the calling user info
	_, err := req.extractCallerInfo()
	if err != nil {
		err = errl.Errorf("invalid access token: %w", err)
		apiErr := apierror.NewError("401", "Unauthorized", err.Error(), fmt.Sprintf("%d", http.StatusUnauthorized), "")
		slog.Error("Unauthorized request", slog.Any("error", err))
		return &Response{StatusCode: http.StatusUnauthorized, Body: apiErr}
	}

	objs, totalCount, err := service.ListObjects(req.ResourceName, req.QueryParams)
	if err != nil {
		err = errl.Errorf("failed to list objects from service: %w", err)
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to list objects from service", slog.Any("error", err), slog.String("resourceName", req.ResourceName))
		return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
	}

	headers := make(map[string]string)
	headers["X-Total-Count"] = strconv.Itoa(totalCount)

	var responseData []map[string]any
	for _, obj := range objs {
		var item map[string]any
		err := json.Unmarshal(obj.Content, &item)
		if err != nil {
			err = errl.Errorf("failed to unmarshal object content for listing: %w", err)
			apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
			slog.Error("Failed to unmarshal object content for listing", slog.Any("error", err), slog.String("resourceName", req.ResourceName))
			return &Response{StatusCode: http.StatusInternalServerError, Body: apiErr}
		}
		responseData = append(responseData, item)
	}

	// Handle partial field selection
	fieldsParam := req.QueryParams.Get("fields")
	if fieldsParam != "" {
		var fields []string
		if fieldsParam == "none" {
			fields = []string{"id", "href", "lastUpdate", "version"}
		} else {
			fields = strings.Split(fieldsParam, ",")
		}

		// Create a set of fields for quick lookup
		fieldSet := make(map[string]bool)
		for _, f := range fields {
			fieldSet[strings.TrimSpace(f)] = true
		}

		// Always include id, href, lastUpdate, version and @type
		fieldSet["id"] = true
		fieldSet["href"] = true
		fieldSet["lastUpdate"] = true
		fieldSet["version"] = true
		fieldSet["@type"] = true

		var filteredResponseData []map[string]any
		for _, item := range responseData {
			filteredItem := make(map[string]any)
			for key, value := range item {
				if fieldSet[key] {
					filteredItem[key] = value
				}
			}
			filteredResponseData = append(filteredResponseData, filteredItem)
		}
		responseData = filteredResponseData
	}

	slog.Info("Objects listed successfully", slog.Int("count", len(responseData)), slog.String("resourceName", req.ResourceName))
	return &Response{StatusCode: http.StatusOK, Headers: headers, Body: responseData}
}
