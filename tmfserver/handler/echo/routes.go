package echo

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// RegisterRoutes registers the routes for the v5 API.
func (h *Handler) RegisterRoutes(e *echo.Echo) {
	e.Pre(middleware.RemoveTrailingSlash())

	// Group routes for TMF API
	tmfApi := e.Group("/tmf-api/:apiFamily/v5")

	// Generalized routes for TMF API resources
	// Collection operations (List and Create)
	tmfApi.GET("/:resourceName", h.ListGenericObjects)
	tmfApi.POST("/:resourceName", h.CreateGenericObject)

	// Individual resource operations (Get, Update, Delete)
	tmfApi.GET("/:resourceName/:id", h.GetGenericObject)
	tmfApi.PATCH("/:resourceName/:id", h.UpdateGenericObject)
	tmfApi.DELETE("/:resourceName/:id", h.DeleteGenericObject)

	// HelloWorld route
	e.GET("/", h.HelloWorld)
}
