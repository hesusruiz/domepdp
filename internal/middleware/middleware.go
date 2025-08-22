package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"log/slog"

	"github.com/gofiber/template/html/v2"
	"github.com/google/uuid"
)

type customAttributesCtxKeyType struct{}
type requestIDCtxKeyType struct{}

var customAttributesCtxKey = customAttributesCtxKeyType{}
var requestIDCtxKey = requestIDCtxKeyType{}

var (
	RequestIDKey = "reqid"

	// Formatted with http.CanonicalHeaderKey
	RequestIDHeaderKey = "X-Request-Id"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

func RequestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		method := r.Method
		uri := r.RequestURI

		// Get the info about the original client making the request.
		// First try to get the standard field, then the defacto standard.
		xforwardedfor := r.Header.Get("Forwarded")
		if xforwardedfor == "" {
			xforwardedfor = r.Header.Get("X-Forwarded-For")
		}

		requestLogMessage := "HTTP " + method + " " + uri

		// Retrieve or create a request ID and set it in the request header and request context for downstream handlers.
		// This is useful for tracing requests across services.
		// After this, the request ID can be retrieved using GetRequestID or GetRequestIDFromContext.
		requestID := r.Header.Get(RequestIDHeaderKey)
		if requestID == "" {
			requestID = uuid.New().String()
			r.Header.Set(RequestIDHeaderKey, requestID)
		}
		r = r.WithContext(context.WithValue(r.Context(), requestIDCtxKey, requestID))

		// Make sure we create a map only once per request (in case we have multiple middleware instances)
		if v := r.Context().Value(customAttributesCtxKey); v == nil {
			r = r.WithContext(context.WithValue(r.Context(), customAttributesCtxKey, &sync.Map{}))
		}

		// Log the request
		logger.LogAttrs(r.Context(), slog.LevelInfo,
			requestLogMessage,
			slog.String(RequestIDKey, requestID),
			slog.Time("time", start.UTC()),
			slog.String("X-Forwarded-For", xforwardedfor),
		)

		// Setup our response writer to capture the status code and body
		rec := statusRecorder{w, 200}

		// Pass control to the next handler in the chain
		// This will call the next handler and write the response to the statusRecorder
		next.ServeHTTP(&rec, r)

		// Get the additional info from the reply
		end := time.Now()
		latency := end.Sub(start)

		status := rec.status

		responseLogMessage := "HTTP " + strconv.Itoa(status) + ": " + http.StatusText(status)

		responseAttributes := []slog.Attr{
			slog.String(RequestIDKey, requestID),
			slog.Time("time", end.UTC()),
			slog.Duration("latency", latency),
			slog.Int("status", status),
		}

		level := slog.LevelInfo
		if status >= http.StatusInternalServerError {
			level = slog.LevelError
		} else if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
			level = slog.LevelWarn
		}

		logger.LogAttrs(r.Context(), level, responseLogMessage, responseAttributes...)

	})

}

func LogHTTPRequest(logger *slog.Logger, r *http.Request) (start time.Time) {
	start = time.Now()
	method := r.Method
	uri := r.RequestURI

	// Get the info about the original client making the request.
	// First try to get the standard field, then the defacto standard.
	xforwardedfor := r.Header.Get("Forwarded")
	if xforwardedfor == "" {
		xforwardedfor = r.Header.Get("X-Forwarded-For")
	}

	requestLogMessage := "HTTP " + method + " " + uri

	// Retrieve or create a request ID and set it in the request header and request context for downstream handlers.
	// This is useful for tracing requests across services.
	// After this, the request ID can be retrieved using GetRequestID or GetRequestIDFromContext.
	requestID := r.Header.Get(RequestIDHeaderKey)
	if requestID == "" {
		requestID = uuid.New().String()
		r.Header.Set(RequestIDHeaderKey, requestID)
	}
	r = r.WithContext(context.WithValue(r.Context(), requestIDCtxKey, requestID))

	// Log the request
	logger.LogAttrs(r.Context(), slog.LevelInfo,
		requestLogMessage,
		slog.String(RequestIDKey, requestID),
		slog.Time("time", start.UTC()),
		slog.String("x-forwarded-for", xforwardedfor),
	)

	return start

}

func LogHTTPReply(logger *slog.Logger, r *http.Request, start time.Time, status int) {

	requestID := r.Header.Get(RequestIDHeaderKey)

	// Get the additional info from the reply
	end := time.Now()
	latency := end.Sub(start)

	responseLogMessage := "HTTP " + strconv.Itoa(status) + ": " + http.StatusText(status)

	responseAttributes := []slog.Attr{
		slog.String(RequestIDKey, requestID),
		slog.Time("time", end.UTC()),
		slog.Duration("latency", latency),
		slog.Int("status", status),
	}

	level := slog.LevelInfo
	if status >= http.StatusInternalServerError {
		level = slog.LevelError
	} else if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
		level = slog.LevelWarn
	}

	logger.LogAttrs(r.Context(), level, responseLogMessage, responseAttributes...)

}

func ResponseSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()

	h.Set("Content-Security-Policy", "frame-ancestors 'none';")
	h.Set("X-Frame-Options", "DENY")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
	h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
	h.Set("Cross-Origin-Opener-Policy", "same-origin")
	h.Set("Cross-Origin-Embedder-Policy", "require-corp")
	h.Set("Cross-Origin-Resource-Policy", "same-site")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), interest-cohort=()")
	h.Set("X-Powered-By", "webserver")

}

// GetRequestID returns the request identifier.
func GetRequestID(r *http.Request) string {
	return GetRequestIDFromContext(r.Context())
}

func RequestID(r *http.Request) slog.Attr {
	requestID := r.Header.Get(RequestIDHeaderKey)
	return slog.String(RequestIDKey, requestID)

}

// GetRequestIDFromContext returns the request identifier from the context.
func GetRequestIDFromContext(ctx context.Context) string {
	requestID := ctx.Value(requestIDCtxKey)
	if id, ok := requestID.(string); ok {
		return id
	}

	return ""
}

// AddCustomAttributes adds custom attributes to the request context. This func can be called from any handler or middleware, as long as the slog-http middleware is already mounted.
func AddCustomAttributes(r *http.Request, attr slog.Attr) {
	AddContextAttributes(r.Context(), attr)
}

// AddContextAttributes is the same as AddCustomAttributes, but it doesn't need access to the request struct.
func AddContextAttributes(ctx context.Context, attr slog.Attr) {
	if v := ctx.Value(customAttributesCtxKey); v != nil {
		if m, ok := v.(*sync.Map); ok {
			m.Store(attr.Key, attr.Value)
		}
	}
}

// The standard HTTP error response for TMF APIs
type errorTMFObject struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

// ErrorTMF sends back an HTTP error response using the TMForum standard format
func ErrorTMF(w http.ResponseWriter, statusCode int, code string, reason string) {
	errtmf := &errorTMFObject{
		Code:   code,
		Reason: reason,
	}

	h := w.Header()

	// Delete the Content-Length header, just in case the handler panicking already set it.
	h.Del("Content-Length")
	h.Set("X-Powered-By", "JRM Proxy")

	// There might be content type already set, but we reset it
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(errtmf)

}

// ReplyTMF sends an HTTP response in the TMForum format
func ReplyTMF(w http.ResponseWriter, statusCode int, data []byte, additionalHeaders map[string]string) {

	h := w.Header()

	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set("X-Powered-By", "JRM Proxy")

	// There might be content type already set, but we reset it to
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")

	// TODO: set Last-Modified
	for k, v := range additionalHeaders {
		h.Set(k, v)
	}

	w.WriteHeader(statusCode)
	w.Write(data)

}

func RenderHTML(engine *html.Engine, w http.ResponseWriter, templateName string, data map[string]any, layout ...string) int {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	ResponseSecurityHeaders(w)

	if err := engine.Render(w, templateName, data, layout...); err != nil {
		slog.Error("Error rendering template",
			slog.String("error", err.Error()),
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return http.StatusInternalServerError
	}
	return http.StatusOK

}

// PanicHandler is a simple http handler for recovering panics in downstream handlers
func PanicHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				buf = buf[:n]

				fmt.Printf("panic recovered: %v\n %s", err, buf)
				ErrorTMF(w, http.StatusInternalServerError, "unknown error", "unknown error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}
