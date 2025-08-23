package apierror

// Error represents a standardized TMForum API error response.
type Error struct {
	// Gorm related fields
	BaseType        string `json:"@baseType,omitempty"`
	SchemaLocation  string `json:"@schemaLocation,omitempty"`
	Type            string `json:"@type,omitempty"`

	Code           string `json:"code"`
	Reason         string `json:"reason"`
	Message        string `json:"message,omitempty"`
	Status         string `json:"status,omitempty"`
	ReferenceError string `json:"referenceError,omitempty"`
}

// NewError creates a new Error instance.
func NewError(code, reason, message, status, referenceError string) *Error {
	return &Error{
		Code:           code,
		Reason:         reason,
		Message:        message,
		Status:         status,
		ReferenceError: referenceError,
		Type:           "Error", // Default @type for Error object
	}
}
