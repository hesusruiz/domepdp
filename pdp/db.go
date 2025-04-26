// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package pdp

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	sqlb "github.com/huandu/go-sqlbuilder"
)

var createTMFTableSQL = `
CREATE TABLE IF NOT EXISTS tmfobject (
	"id" TEXT NOT NULL,
	"version" TEXT,
	"organizationIdentifier" TEXT,
	"organization" TEXT,
	"type" TEXT NOT NULL,
	"name" TEXT NOT NULL,
	"description" TEXT,
	"lifecycleStatus" TEXT,
	"lastUpdate" TEXT NOT NULL,
	"content" BLOB NOT NULL,
	"hash" BLOB,
	"created" INTEGER,
	"updated" INTEGER,

	PRIMARY KEY ("id", "version")
) WITHOUT ROWID;
PRAGMA journal_mode = WAL;
CREATE INDEX IF NOT EXISTS idx_hash ON tmfobject (hash);
`

// CheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func (tmf *TMFdb) CheckIfExists(dbconn *sqlite.Conn, id string, version string) (bool, []byte, int, error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return false, nil, 0, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Check if the row already exists, with the same version
	const CheckIfExistsTMFObjectSQL = `SELECT id, hash, updated FROM tmfobject WHERE id = :id AND version = :version;`
	selectStmt, err := dbconn.Prepare(CheckIfExistsTMFObjectSQL)
	if err != nil {
		return false, nil, 0, fmt.Errorf("CheckIfExists: %w", err)
	}
	defer selectStmt.Reset()

	selectStmt.SetText(":id", id)
	selectStmt.SetText(":version", version)

	hasRow, err := selectStmt.Step()
	if err != nil {
		return false, nil, 0, fmt.Errorf("CheckIfExists: %w", err)
	}

	// Each object has a hash to make sure it is the same object, even if the version is the same
	var hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness := int(now - updated)

	return hasRow, hash, freshness, nil

}

// UpdateInStorage updates an object in the db with the contents of the po.
func (tmf *TMFdb) UpdateInStorage(dbconn *sqlite.Conn, po *TMFObject) error {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	const UpdateTMFObjectSQL = `UPDATE tmfobject SET organizationIdentifier = :organizationIdentifier, organization = :organization, type = :type, name = :name, description = :description, lifecycleStatus = :lifecycleStatus, lastUpdate = :lastUpdate, content = :content, hash = :hash, updated = :updated WHERE id = :id AND version = :version;`
	updateStmt, err := dbconn.Prepare(UpdateTMFObjectSQL)
	if err != nil {
		return fmt.Errorf("UpdateInStorage: %w", err)
	}
	defer updateStmt.Reset()

	// These are used for the WHERE clause
	updateStmt.SetText(":id", po.ID)
	updateStmt.SetText(":version", po.Version)

	// These are the updated fields
	updateStmt.SetText(":organizationIdentifier", po.OrganizationIdentifier)
	updateStmt.SetText(":organization", po.Organization)
	updateStmt.SetText(":type", po.Type)
	updateStmt.SetText(":name", po.Name)
	updateStmt.SetText(":description", po.Description)
	updateStmt.SetText(":lifecycleStatus", po.LifecycleStatus)
	updateStmt.SetText(":lastUpdate", po.LastUpdate)
	updateStmt.SetBytes(":content", po.Content)
	updateStmt.SetBytes(":hash", po.Hash())
	now := time.Now().Unix()
	updateStmt.SetInt64(":updated", now)

	_, err = updateStmt.Step()
	if err != nil {
		slog.Error("UpdateInStorage", "href", po.ID, "error", err)
		return fmt.Errorf("UpdateInStorage: %w", err)
	}

	return nil
}

// InsertInStorage inserts po into the database.
func (tmf *TMFdb) InsertInStorage(dbconn *sqlite.Conn, po *TMFObject) error {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	const InsertTMFObjectSQL = `INSERT INTO tmfobject (id, organizationIdentifier, organization, type, name, description, lifecycleStatus, version, lastUpdate, content, hash, created, updated) VALUES (:id, :organizationIdentifier, :organization, :type, :name, :description, :lifecycleStatus, :version, :lastUpdate, :content, :hash, :created, :updated);`
	insertStmt, err := dbconn.Prepare(InsertTMFObjectSQL)
	if err != nil {
		return err
	}
	defer insertStmt.Reset()

	insertStmt.SetText(":id", po.ID)
	insertStmt.SetText(":version", po.Version)
	insertStmt.SetText(":organizationIdentifier", po.OrganizationIdentifier)
	insertStmt.SetText(":organization", po.Organization)
	insertStmt.SetText(":type", po.Type)
	insertStmt.SetText(":name", po.Name)
	insertStmt.SetText(":description", po.Description)
	insertStmt.SetText(":lifecycleStatus", po.LifecycleStatus)
	insertStmt.SetText(":lastUpdate", po.LastUpdate)
	insertStmt.SetBytes(":content", po.Content)
	insertStmt.SetBytes(":hash", po.Hash())
	now := time.Now().Unix()
	insertStmt.SetInt64(":created", now)
	insertStmt.SetInt64(":updated", now)

	_, err = insertStmt.Step()
	if err != nil {
		slog.Error("InsertInStorage", "href", po.ID, "error", err)
		return err
	}

	return nil
}

func (tmf *TMFdb) UpsertTMFObject(dbconn *sqlite.Conn, po *TMFObject) (err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Start a SAVEPOINT and defer its Commit/Rollback
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Get the type of object from the ID
	objectType, err := TMFObjectIDtoType(po.ID)
	if err != nil {
		return err
	}
	po.Type = objectType

	// Categories do not have an owner. Make sure we do not set the owner field to something else
	if po.Type == "category" {
		po.OrganizationIdentifier = ""
		po.Organization = ""
	}

	// TODO: use a SAVEPOINT to wrap the check and the update

	// Check if the row already exists, with the same version
	hasRow, hash, freshness, err := tmf.CheckIfExists(dbconn, po.ID, po.Version)
	if err != nil {
		return err
	}

	// The id and version are the same, but we have to check the hash to see if we have to update the record
	if hasRow {

		fresh := freshness < tmf.Maxfreshness
		newHash := po.Hash()

		// Check if the data is recent enough and the hash of the content is the same
		if fresh && bytes.Equal(hash, newHash) {
			// The hash of the content is the same, so return immediately
			slog.Debug("Upsert: row exists and fresh", "id", po.ID)
			return nil
		}

		// The row has to be updated.
		// We do not have to update the id and version fields.
		err = tmf.UpdateInStorage(dbconn, po)
		if err != nil {
			return err
		}

		if !fresh {
			slog.Debug("Upsert: row updated (not fresh)", "id", po.ID)
		} else {
			hashStr := fmt.Sprintf("%X", hash)
			newHashStr := fmt.Sprintf("%X", newHash)
			slog.Debug("Upsert: row updated (hash different)", "id", po.ID, "old", hashStr, "new", newHashStr)
		}

		return nil // Skip inserting if the row already exists
	}

	// There was no record with the same id and version, so insert the full object
	err = tmf.InsertInStorage(dbconn, po)
	if err != nil {
		slog.Error("UpsertTMFObject", "href", po.ID, "version", po.Version, "error", err)
		return err
	}

	// slog.Debug("Inserted row", "id", po.ID)
	slog.Debug("Upsert: row inserted", "id", po.ID)

	return nil
}

// RetrieveLocalTMFObject retrieves the object with the href (is the same as the id).
// The version is optional. If it is not provided, the most recently version (by lexicographic order) is retrieved.
func (tmf *TMFdb) RetrieveLocalTMFObject(dbconn *sqlite.Conn, href string, version string) (po *TMFObject, found bool, err error) {

	if dbconn == nil {
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// We use a different SELECT statement depending on whether version is provided or not.
	// Except for admin users, normal users are given the latest version of the object.
	var stmt *sqlite.Stmt
	if len(version) == 0 {
		const RetrieveTMFObjectNoVersionSQL = `SELECT * FROM tmfobject WHERE id = :id ORDER BY version DESC;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectNoVersionSQL)
		defer stmt.Reset()
		stmt.SetText(":id", href)
	} else {
		const RetrieveTMFObjectSQL = `SELECT * FROM tmfobject WHERE id = :id AND version = :version;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectSQL)
		defer stmt.Reset()
		stmt.SetText(":id", href)
		stmt.SetText(":version", version)
	}

	hasRow, err := stmt.Step()
	if err != nil {
		slog.Error("RetrieveLocalTMFObject", "href", href, "error", err)
		return nil, false, err
	}

	if !hasRow {
		return nil, false, nil
	}

	// Return the first match, ignoring any other rows that may be in the database
	var oMap map[string]any

	// Even if we store it also in the db, the map representation of the object is always
	// built from the JSON representation in the content field.
	var content = make([]byte, stmt.GetLen("content"))
	stmt.GetBytes("content", content)
	err = json.Unmarshal(content, &oMap)
	if err != nil {
		log.Println(err)
		return nil, false, err
	}

	// Complete the map representation with the relevant fields which are in our db but not in the DOME repo
	oMap["updated"] = stmt.GetInt64("updated")
	oMap["organizationIdentifier"] = stmt.GetText("organizationIdentifier")
	oMap["organization"] = stmt.GetText("organization")

	po, err = NewTMFObject(oMap, content)
	if err != nil {
		log.Println(err)
		return nil, false, err
	}
	po.Updated = stmt.GetInt64("updated")

	return po, true, nil

}

// RetrieveLocalListTMFObject implements the TMForum functionality for retrieving a list of objects of a given type from the database.
func (tmf *TMFdb) RetrieveLocalListTMFObject(dbconn *sqlite.Conn, tmfType string, queryValues url.Values) (pos []*TMFObject, found bool, err error) {
	if dbconn == nil {
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// Build the SQL SELECT based on the query passed on the HTTP request, as specified in TMForum
	sql, args := buildSelectFromParms(tmfType, queryValues)

	var resultPOs []*TMFObject

	err = sqlitex.Execute(dbconn, sql, &sqlitex.ExecOptions{
		Args: args,

		// This function is called once for each record found in the database
		ResultFunc: func(stmt *sqlite.Stmt) error {

			var oMap map[string]any

			// Retrieve the JSON representation and build the map from it
			var content = make([]byte, stmt.GetLen("content"))
			stmt.GetBytes("content", content)
			err = json.Unmarshal(content, &oMap)
			if err != nil {
				log.Println(err)
				return err
			}

			// Complete the object with the relevant fields which are in our db but not in the DOME repo
			oMap["updated"] = stmt.GetInt64("updated")
			oMap["organizationIdentifier"] = stmt.GetText("organizationIdentifier")
			oMap["organization"] = stmt.GetText("organization")

			po, err := NewTMFObject(oMap, content)
			if err != nil {
				log.Println(err)
				return err
			}

			resultPOs = append(resultPOs, po)

			return nil
		},
	})
	if err != nil {
		fmt.Println(err)
		return nil, false, err
	}

	slog.Debug("RetrieveLocalListTMFObject", "sql", sql, "args", args, "objects", resultPOs)
	return resultPOs, true, nil
}

// buildSelectFromParms creates a SELECT statement based on the query values.
// For objects with same id, selects the one with the latest version.
func buildSelectFromParms(tmfType string, queryValues url.Values) (string, []any) {

	// Default values if the user did not specify them. -1 is equivalent to no values provided.
	var limit = -1
	var offset = -1

	bu := sqlb.SQLite.NewSelectBuilder()

	// SELECT: for each object with a given id, select the latest version.
	// We use the 'max(version)' function, and will GROUP by id.
	bu.Select("id", "max(version)", "organizationIdentifier", "organization", "content", "updated").From("tmfobject")

	// WHERE: normally we expect the type of object to be specified, but we support a query for all object types
	if len(tmfType) > 0 {
		bu.Where(bu.Equal("type", tmfType))
	}

	// Add to WHERE: process the query values specified by the user
	whereClause := sqlb.NewWhereClause()
	cond := sqlb.NewCond()

	for key, values := range queryValues {

		switch key {
		case "limit":
			limitStr := queryValues.Get("limit")
			if l, err := strconv.Atoi(limitStr); err == nil {
				limit = l
			}
		case "offset":
			offsetStr := queryValues.Get("offset")
			if l, err := strconv.Atoi(offsetStr); err == nil {
				offset = l
			}
		case "lifecycleStatus":
			// Special processing because TMForum allows to specify multiple values
			// in the form 'lifecycleStatus=Launched,Active'
			var vals = []string{}
			// Allow several instances of 'lifecycleStatus' parameter in the query string
			for _, v := range values {
				parts := strings.Split(v, ",")
				// Allow for whitespace surrounding the elements
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				vals = append(vals, parts...)
			}

			// Use either an equality or an inclusion expression
			if len(vals) == 1 {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.Equal("lifecycleStatus", sqlb.List(vals)),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In("lifecycleStatus", sqlb.List(vals)),
				)
			}

		case "relatedParty.id", "relatedParty":
			// This is a special case, given that it is so frequent, we perform an optimization
			whereClause.AddWhereExpr(
				cond.Args,
				cond.Equal("organization", values[0]),
			)

		default:

			// The rest of parameters are not in the fields of the SQL database.
			// We have to use SQLite JSON expressions to search.
			if len(values) == 1 {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.Equal("content->>'$."+key+"'", values[0]),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In("content->>'$."+key+"'", sqlb.List(values)),
				)

			}

		}
	}

	// Add the WHERE to the SELECT
	bu.AddWhereClause(whereClause)

	// We need to GROUP by id, so we can SELECT the record with the latest version from each group
	bu.GroupBy("id")

	// For fairness of presenting results to customers, we want a random ordering, which is consistent and fair with the providers.
	// Ordering by the hash of the content of the TMF object complies with the requirements, as it is consistent across paginations
	// and nobody can predict the final ordering a-priory. For a stable catalog, the ordering is the same for all users and at any time.
	// When a provider creates or modifies a product, it will be inserted at an unpredictable position in the catalog.
	// TODO: we can consider a more advanced variation, where we add to the hash a random number which is
	// generated each day or week, and keeps the same until a new one is generated.
	// In this way, ordering is efficient, random, and changes every week (or whatever period is chosen)
	bu.OrderBy("hash")

	// Pagination support
	bu.Limit(limit).Offset(offset)

	// Build the query, with the statement and the arguments to be used
	sql, args := bu.Build()

	return sql, args
}
