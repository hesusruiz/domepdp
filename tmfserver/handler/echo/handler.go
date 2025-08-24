package echo

import (
	"io"

	common "github.com/hesusruiz/domepdp/tmfserver/common"
	svc "github.com/hesusruiz/domepdp/tmfserver/service"
	"github.com/labstack/echo/v4"
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
func (h *Handler) HelloWorld(c echo.Context) error {
	resp := &common.Response{
		StatusCode: 200,
		Body:       "Hello, World!",
	}
	return sendResponse(c, resp)
}

// CreateGenericObject creates a new TMF object using generalized parameters.
func (h *Handler) CreateGenericObject(c echo.Context) error {
	body, _ := io.ReadAll(c.Request().Body)
	jwtToken := common.ExtractJWTToken(c.Request().Header.Get("Authorization"))

	req := &common.Request{
		Method:       c.Request().Method,
		Action:       common.HttpMethodAliases[c.Request().Method],
		APIfamily:    c.Param("apiFamily"),
		ResourceName: c.Param("resourceName"),
		Body:         body,
		JWTToken:     jwtToken, // Store the raw JWT token
	}

	resp := common.CreateGenericObject(req, h.service)
	return sendResponse(c, resp)
}

// GetGenericObject retrieves a TMF object using generalized parameters.
func (h *Handler) GetGenericObject(c echo.Context) error {
	jwtToken := common.ExtractJWTToken(c.Request().Header.Get("Authorization"))

	req := &common.Request{
		Method:       c.Request().Method,
		Action:       common.HttpMethodAliases[c.Request().Method],
		ResourceName: c.Param("resourceName"),
		ID:           c.Param("id"),
		QueryParams:  c.QueryParams(),
		JWTToken:     jwtToken, // Store the raw JWT token
	}

	resp := common.GetGenericObject(req, h.service)
	return sendResponse(c, resp)
}

// UpdateGenericObject updates an existing TMF object using generalized parameters.
func (h *Handler) UpdateGenericObject(c echo.Context) error {
	body, _ := io.ReadAll(c.Request().Body)
	jwtToken := common.ExtractJWTToken(c.Request().Header.Get("Authorization"))

	req := &common.Request{
		Method:       c.Request().Method,
		Action:       common.HttpMethodAliases[c.Request().Method],
		ResourceName: c.Param("resourceName"),
		ID:           c.Param("id"),
		Body:         body,
		JWTToken:     jwtToken, // Store the raw JWT token
	}

	resp := common.UpdateGenericObject(req, h.service)
	return sendResponse(c, resp)
}

// DeleteGenericObject deletes a TMF object using generalized parameters.
func (h *Handler) DeleteGenericObject(c echo.Context) error {
	jwtToken := common.ExtractJWTToken(c.Request().Header.Get("Authorization"))

	req := &common.Request{
		Method:       c.Request().Method,
		Action:       common.HttpMethodAliases[c.Request().Method],
		ResourceName: c.Param("resourceName"),
		ID:           c.Param("id"),
		JWTToken:     jwtToken, // Store the raw JWT token
	}

	resp := common.DeleteGenericObject(req, h.service)
	return sendResponse(c, resp)
}

// ListGenericObjects retrieves all TMF objects of a given type using generalized parameters.
func (h *Handler) ListGenericObjects(c echo.Context) error {
	jwtToken := common.ExtractJWTToken(c.Request().Header.Get("Authorization"))

	req := &common.Request{
		Method:       c.Request().Method,
		Action:       "LIST",
		ResourceName: c.Param("resourceName"),
		QueryParams:  c.QueryParams(),
		JWTToken:     jwtToken, // Store the raw JWT token
	}

	resp := common.ListGenericObjects(req, h.service)
	return sendResponse(c, resp)
}

func sendResponse(c echo.Context, resp *common.Response) error {
	for key, value := range resp.Headers {
		c.Response().Header().Set(key, value)
	}
	if resp.Body != nil {
		return c.JSON(resp.StatusCode, resp.Body)
	}
	return c.NoContent(resp.StatusCode)
}
