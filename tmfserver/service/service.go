package service

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/hesusruiz/domepdp/internal/errl"
	repo "github.com/hesusruiz/domepdp/tmfserver/repository"
	"github.com/jmoiron/sqlx"
)

// Service is the service for the API.
type Service struct {
	db *sqlx.DB
}

// NewService creates a new service.
func NewService(db *sqlx.DB) *Service {
	return &Service{db: db}
}

// CreateObject creates a new TMF object.
func (s *Service) CreateObject(obj *repo.TMFObject) error {
	slog.Debug("Service: Creating object", slog.String("id", obj.ID), slog.String("type", obj.Type), slog.String("version", obj.Version))
	_, err := s.db.NamedExec(`INSERT INTO tmf_object (id, type, version, last_update, content, created_at, updated_at)
		VALUES (:id, :type, :version, :last_update, :content, :created_at, :updated_at)`, obj)
	if err != nil {
		err = errl.Errorf("failed to create object: %v", err)
		slog.Error("Service: Failed to create object", slog.Any("error", err), slog.String("id", obj.ID), slog.String("type", obj.Type))
	}
	return err
}

// GetObject retrieves a TMF object by its ID and type, returning the latest version.
func (s *Service) GetObject(id, objectType string) (*repo.TMFObject, error) {
	slog.Debug("Service: Getting object", slog.String("id", id), slog.String("type", objectType))
	var obj repo.TMFObject
	err := s.db.Get(&obj, "SELECT * FROM tmf_object WHERE id = ? AND type = ? ORDER BY version DESC LIMIT 1", id, objectType)
	if err == sql.ErrNoRows {
		slog.Info("Service: Object not found", slog.String("id", id), slog.String("type", objectType))
		return nil, nil // Object not found
	} else if err != nil {
		err = errl.Errorf("failed to get object: %v", err)
		slog.Error("Service: Failed to get object", slog.Any("error", err), slog.String("id", id), slog.String("type", objectType))
	}
	return &obj, err
}

// UpdateObject updates an existing TMF object.
func (s *Service) UpdateObject(obj *repo.TMFObject) error {
	slog.Debug("Service: Updating object", slog.String("id", obj.ID), slog.String("type", obj.Type), slog.String("version", obj.Version))
	_, err := s.db.NamedExec(`UPDATE tmf_object SET version = :version, last_update = :last_update, content = :content, updated_at = :updated_at WHERE id = :id AND type = :type`, obj)
	if err != nil {
		err = errl.Errorf("failed to update object: %v", err)
		slog.Error("Service: Failed to update object", slog.Any("error", err), slog.String("id", obj.ID), slog.String("type", obj.Type))
	}
	return err
}

// DeleteObject deletes a TMF object by its ID and type.
func (s *Service) DeleteObject(id, objectType string) error {
	slog.Debug("Service: Deleting object", slog.String("id", id), slog.String("type", objectType))
	_, err := s.db.Exec("DELETE FROM tmf_object WHERE id = ? AND type = ?", id, objectType)
	if err != nil {
		err = errl.Errorf("failed to delete object: %v", err)
		slog.Error("Service: Failed to delete object", slog.Any("error", err), slog.String("id", id), slog.String("type", objectType))
	}
	return err
}

// ListObjects retrieves all TMF objects of a given type, returning only the latest version for each unique ID.
// It supports pagination, filtering, and sorting according to TMF630 guidelines.
func (s *Service) ListObjects(objectType string, queryParams url.Values) ([]repo.TMFObject, int, error) {
	slog.Debug("Service: Listing objects", "type", objectType, "queryParams", queryParams)
	var objs []repo.TMFObject
	var totalCount int

	// Base query to get the latest version for each unique ID and type
	baseQuery := `
		SELECT t1.*
		FROM tmf_object t1
		INNER JOIN (
			SELECT id, type, MAX(version) AS max_version
			FROM tmf_object
			WHERE type = ?
			GROUP BY id, type
		) AS t2
		ON t1.id = t2.id AND t1.type = t2.type AND t1.version = t2.max_version
		WHERE t1.type = ?
	`
	countQuery := `
		SELECT COUNT(DISTINCT t1.id)
		FROM tmf_object t1
		INNER JOIN (
			SELECT id, type, MAX(version) AS max_version
			FROM tmf_object
			WHERE type = ?
			GROUP BY id, type
		) AS t2
		ON t1.id = t2.id AND t1.type = t2.type AND t1.version = t2.max_version
		WHERE t1.type = ?
	`

	args := []any{objectType, objectType}
	countArgs := []any{objectType, objectType}

	// Add filters
	filterClauses := []string{}
	for key, values := range queryParams {
		// TMF630 reserved words for query parameters
		if key == "limit" || key == "offset" || key == "sort" || key == "fields" {
			continue
		}
		// Assuming simple equality filter for now
		filterClauses = append(filterClauses, fmt.Sprintf("json_extract(t1.content, '$.%s') = ?", key))
		args = append(args, values[0])
		countArgs = append(countArgs, values[0])
	}

	if len(filterClauses) > 0 {
		filterString := " AND " + strings.Join(filterClauses, " AND ")
		baseQuery += filterString
		countQuery += filterString
	}

	// Get total count before pagination
	err := s.db.Get(&totalCount, countQuery, countArgs...)
	if err != nil {
		err = errl.Errorf("failed to get total count: %v", err)
		slog.Error("Service: Failed to get total count of objects", "error", err, "type", objectType, "queryParams", queryParams)
		return nil, 0, err
	}

	// Add sorting
	sortParam := queryParams.Get("sort")
	if sortParam != "" {
		sortFields := strings.Split(sortParam, ",")
		orderByClauses := []string{}
		for _, field := range sortFields {
			direction := "ASC"
			if strings.HasPrefix(field, "-") {
				direction = "DESC"
				field = field[1:]
			}
			orderByClauses = append(orderByClauses, fmt.Sprintf("json_extract(t1.content, '$.%s') %s", field, direction))
		}
		baseQuery += " ORDER BY " + strings.Join(orderByClauses, ", ")
	}

	// Add pagination
	limitStr := queryParams.Get("limit")
	if limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err == nil && limit > 0 {
			baseQuery += fmt.Sprintf(" LIMIT %d", limit)
		}
	}

	offsetStr := queryParams.Get("offset")
	if offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err == nil && offset >= 0 {
			baseQuery += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	err = s.db.Select(&objs, baseQuery, args...)
	if err != nil {
		err = errl.Errorf("failed to list objects: %v", err)
		slog.Error("Service: Failed to list objects", "error", err, "type", objectType, "queryParams", queryParams)
		return nil, 0, err
	}

	// TODO: Implement partial field selection based on "fields" query parameter.
	// This would involve unmarshalling and then selectively marshalling the content.
	// Currently, this is done at a higher level in th eimplementation

	return objs, totalCount, err
}
