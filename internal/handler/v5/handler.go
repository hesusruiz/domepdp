package v5

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	repo "github.com/hesusruiz/domeproxy/internal/repository/v5"
	svc "github.com/hesusruiz/domeproxy/internal/service/v5"
	"github.com/hesusruiz/domeproxy/pkg/apierror"
)

// Handler is the handler for the v5 API.
type Handler struct {
	service *svc.Service
}

// NewHandler creates a new handler.
func NewHandler(s *svc.Service) *Handler {
	return &Handler{service: s}
}

// HelloWorld is a simple hello world handler.
func (h *Handler) HelloWorld(c *fiber.Ctx) error {
	slog.Info("Hello World endpoint hit")
	return c.Status(http.StatusOK).SendString("Hello, World!")
}

// CreateGenericObject creates a new TMF object using generalized parameters.
func (h *Handler) CreateGenericObject(c *fiber.Ctx) error {
	apiFamily := c.Params("apiFamily")
	resourceName := c.Params("resourceName")

	slog.Debug("CreateGenericObject called", slog.String("apiFamily", apiFamily), slog.String("resourceName", resourceName))

	var data map[string]any
	if err := c.BodyParser(&data); err != nil {
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Failed to bind request body", slog.Any("error", err), slog.String("apiFamily", apiFamily), slog.String("resourceName", resourceName))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}

	id, ok := data["id"].(string)
	if !ok || id == "" {
		// If the incoming object does not have an 'id', we generate a new one
		// The format is "urn:ngsi-ld:{resource-in-kebab-case}:{uuid}"
		id = fmt.Sprintf("urn:ngsi-ld:%s:%s", ToKebabCase(resourceName), uuid.NewString())
		data["id"] = id
		slog.Debug("Generated new ID for object", "id", id)
	}

	// Check and process '@type' field
	if typeVal, typeOk := data["@type"].(string); typeOk {
		if !strings.EqualFold(typeVal, resourceName) {
			apiErr := apierror.NewError("400", "Bad Request", "@type field in body must match resource name in URL (case-insensitive)", fmt.Sprintf("%d", http.StatusBadRequest), "")
			slog.Error("@type mismatch", slog.String("expected", resourceName), slog.String("got", typeVal), slog.String("apiFamily", apiFamily), slog.String("resourceName", resourceName))
			return c.Status(http.StatusBadRequest).JSON(apiErr)
		}
	} else {
		// If @type is not specified, add it
		data["@type"] = resourceName
		slog.Debug("Added missing @type field", slog.String("type", resourceName))
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
		apiErr := apierror.NewError("400", "Bad Request", "lastUpdate field must be a string or null", fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Invalid lastUpdate field type", slog.Any("lastUpdate", data["lastUpdate"]))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}

	// Add href to the object
	data["href"] = fmt.Sprintf("/tmf-api/%s/v5/%s/%s", apiFamily, resourceName, id)
	slog.Debug("Set href", slog.String("href", data["href"].(string)))

	content, err := json.Marshal(data)
	if err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to marshal object content", slog.Any("error", err), slog.String("apiFamily", apiFamily), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	obj := repo.NewTMFObject(id, resourceName, version, lastUpdate, content)

	if err := h.service.CreateObject(obj); err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to create object in service", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	// Set Location header
	c.Set("Location", data["href"].(string))
	slog.Info("Object created successfully", slog.String("id", id), slog.String("resourceName", resourceName), slog.String("location", data["href"].(string)))

	return c.Status(http.StatusCreated).JSON(data)
}

// GetGenericObject retrieves a TMF object using generalized parameters.
func (h *Handler) GetGenericObject(c *fiber.Ctx) error {
	resourceName := c.Params("resourceName")
	id := c.Params("id")

	slog.Debug("GetGenericObject called", slog.String("id", id), slog.String("resourceName", resourceName))

	obj, err := h.service.GetObject(id, resourceName)
	if err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to get object from service", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	if obj == nil {
		apiErr := apierror.NewError("404", "Not Found", "object not found", fmt.Sprintf("%d", http.StatusNotFound), "")
		slog.Info("Object not found", slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusNotFound).JSON(apiErr)
	}

	var responseData map[string]any
	err = json.Unmarshal(obj.Content, &responseData)
	if err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", "failed to unmarshal object content", fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to unmarshal object content", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	// Handle partial field selection
	queryParams, err := url.ParseQuery(string(c.Request().URI().QueryString()))
	if err != nil {
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Failed to parse query params", slog.Any("error", err), slog.String("resourceName", resourceName))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}
	fieldsParam := queryParams.Get("fields")
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

	slog.Info("Object retrieved successfully", slog.String("id", id), slog.String("resourceName", resourceName))
	return c.Status(http.StatusOK).JSON(responseData)
}

// UpdateGenericObject updates an existing TMF object using generalized parameters.
func (h *Handler) UpdateGenericObject(c *fiber.Ctx) error {
	resourceName := c.Params("resourceName")
	id := c.Params("id")

	slog.Debug("UpdateGenericObject called", slog.String("id", id), slog.String("resourceName", resourceName))

	var data map[string]any
	if err := c.BodyParser(&data); err != nil {
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Failed to bind request body for update", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}

	// If the body specifies an ID, then it must match the ID in the URL.
	bodyID, _ := data["id"].(string)
	if bodyID != "" && bodyID != id {
		apiErr := apierror.NewError("400", "Bad Request", "ID in body must match ID in URL", fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("ID mismatch in update request", slog.String("url_id", id), slog.String("body_id", bodyID), slog.String("resourceName", resourceName))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}

	// Check and process '@type' field
	if typeVal, typeOk := data["@type"].(string); typeOk {
		if !strings.EqualFold(typeVal, resourceName) {
			apiErr := apierror.NewError("400", "Bad Request", "@type field in body must match resource name in URL (case-insensitive)", fmt.Sprintf("%d", http.StatusBadRequest), "")
			slog.Error("@type mismatch in update request", slog.String("expected", resourceName), slog.String("got", typeVal), slog.String("id", id), slog.String("resourceName", resourceName))
			return c.Status(http.StatusBadRequest).JSON(apiErr)
		}
	} else {
		// If @type is not specified, add it
		data["@type"] = resourceName
		slog.Debug("Added missing @type field to update request", slog.String("type", resourceName))
	}

	// Version must be specified for update operations
	version, versionOk := data["version"].(string)
	if !versionOk || version == "" {
		apiErr := apierror.NewError("400", "Bad Request", "version field is required for update operations", fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Version missing from update request", slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}

	var lastUpdate string
	if lu, ok := data["lastUpdate"].(string); ok {
		lastUpdate = lu
	} else if data["lastUpdate"] == nil {
		lastUpdate = time.Now().Format(time.RFC3339Nano)
		data["lastUpdate"] = lastUpdate
		slog.Debug("Set default lastUpdate for update", slog.String("lastUpdate", lastUpdate))
	} else {
		apiErr := apierror.NewError("400", "Bad Request", "lastUpdate field must be a string or null", fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Invalid lastUpdate field type for update", slog.Any("lastUpdate", data["lastUpdate"]))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}

	content, err := json.Marshal(data)
	if err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to marshal object content for update", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	// Get existing object to preserve CreatedAt
	existingObj, err := h.service.GetObject(id, resourceName)
	if err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to get existing object for update", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	if existingObj == nil {
		apiErr := apierror.NewError("404", "Not Found", "object not found", fmt.Sprintf("%d", http.StatusNotFound), "")
		slog.Info("Object not found for update", slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusNotFound).JSON(apiErr)
	}

	obj := &repo.TMFObject{
		ID:         id,
		Type:       resourceName,
		Version:    version,
		LastUpdate: lastUpdate,
		Content:    content,
		CreatedAt:  existingObj.CreatedAt,
		UpdatedAt:  time.Now(),
	}

	if err := h.service.UpdateObject(obj); err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to update object in service", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	slog.Info("Object updated successfully", slog.String("id", id), slog.String("resourceName", resourceName))
	return c.Status(http.StatusOK).JSON(data)
}

// DeleteGenericObject deletes a TMF object using generalized parameters.
func (h *Handler) DeleteGenericObject(c *fiber.Ctx) error {
	resourceName := c.Params("resourceName")
	id := c.Params("id")

	slog.Debug("DeleteGenericObject called", slog.String("id", id), slog.String("resourceName", resourceName))

	if err := h.service.DeleteObject(id, resourceName); err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to delete object from service", slog.Any("error", err), slog.String("id", id), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	slog.Info("Object deleted successfully", slog.String("id", id), slog.String("resourceName", resourceName))
	return c.SendStatus(http.StatusNoContent)
}

// ListGenericObjects retrieves all TMF objects of a given type using generalized parameters.
func (h *Handler) ListGenericObjects(c *fiber.Ctx) error {
	resourceName := c.Params("resourceName")

	slog.Debug("ListGenericObjects called", slog.String("resourceName", resourceName))

	// Extract query parameters for filtering, pagination, and sorting
	queryParams, err := url.ParseQuery(string(c.Request().URI().QueryString()))
	if err != nil {
		apiErr := apierror.NewError("400", "Bad Request", err.Error(), fmt.Sprintf("%d", http.StatusBadRequest), "")
		slog.Error("Failed to parse query params", slog.Any("error", err), slog.String("resourceName", resourceName))
		return c.Status(http.StatusBadRequest).JSON(apiErr)
	}

	objs, totalCount, err := h.service.ListObjects(resourceName, queryParams)
	if err != nil {
		apiErr := apierror.NewError("500", "Internal Server Error", err.Error(), fmt.Sprintf("%d", http.StatusInternalServerError), "")
		slog.Error("Failed to list objects from service", slog.Any("error", err), slog.String("resourceName", resourceName))
		return c.Status(http.StatusInternalServerError).JSON(apiErr)
	}

	c.Set("X-Total-Count", strconv.Itoa(totalCount))

	var responseData []map[string]any
	for _, obj := range objs {
		var item map[string]any
		err := json.Unmarshal(obj.Content, &item)
		if err != nil {
			apiErr := apierror.NewError("500", "Internal Server Error", "failed to unmarshal object content for listing", fmt.Sprintf("%d", http.StatusInternalServerError), "")
			slog.Error("Failed to unmarshal object content for listing", slog.Any("error", err), slog.String("resourceName", resourceName))
			return c.Status(http.StatusInternalServerError).JSON(apiErr)
		}
		responseData = append(responseData, item)
	}

	// Handle partial field selection
	fieldsParam := queryParams.Get("fields")
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

	slog.Info("Objects listed successfully", slog.Int("count", len(responseData)), slog.String("resourceName", resourceName))
	return c.Status(http.StatusOK).JSON(responseData)
}
