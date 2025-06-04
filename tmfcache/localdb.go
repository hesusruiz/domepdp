// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package tmfcache

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	sqlb "github.com/huandu/go-sqlbuilder"
)

// This file implements the local database to be used for the persistent cache of TMForum objects.
// The functions in this file do not refresh the database, they only read from it or write to it.

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

func createTables(dbpool *sqlitex.Pool) error {

	// Get a connection from the pool for all the requests.
	// The connection is returned to the pool when the object is closed
	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return err
	}
	defer dbpool.Put(conn)

	// Create the table if it does not exist
	if err := sqlitex.ExecuteScript(conn, createTMFTableSQL, nil); err != nil {
		slog.Error("createTables", slogor.Err(err))
		return fmt.Errorf("createTables: %w", err)
	}

	return nil
}

// LocalCheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func (tmf *TMFCache) LocalCheckIfExists(
	dbconn *sqlite.Conn, id string, version string,
) (exists bool, hash []byte, freshness int, err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return false, nil, 0, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalCheckIfExists(dbconn, id, version)

}

// LocalUpdateInStorage updates an object in the db with the contents of the po.
func (tmf *TMFCache) LocalUpdateInStorage(dbconn *sqlite.Conn, po *TMFObject) error {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalUpdateInStorage(dbconn)

}

// LocalInsertInStorage inserts po into the database.
// id and version are primary keys, so their combination must be unique or the function returns and error.
func (tmf *TMFCache) LocalInsertInStorage(dbconn *sqlite.Conn, po *TMFObject) error {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalInsertInStorage(dbconn)

}

// LocalUpsertTMFObject updates or insters an object in the database.
// id and version are primary keys, so their combination must be unique or the function returns and error.
func (tmf *TMFCache) LocalUpsertTMFObject(dbconn *sqlite.Conn, po *TMFObject) (err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return po.LocalUpsertTMFObject(dbconn, tmf.Maxfreshness)

}

// LocalRetrieveTMFObject retrieves the object with the href (is the same as the id).
// The version is optional. If it is not provided, the most recently version (by lexicographic order) is retrieved.
func (tmf *TMFCache) LocalRetrieveTMFObject(dbconn *sqlite.Conn, href string, version string) (po *TMFObject, found bool, err error) {

	if dbconn == nil {
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalRetrieveTMFObject(dbconn, href, version)

}

// LocalRetrieveListTMFObject implements the TMForum functionality for retrieving a list of objects of a given type from the database.
func (tmf *TMFCache) LocalRetrieveListTMFObject(dbconn *sqlite.Conn, tmfType string, queryValues url.Values) (pos []*TMFObject, found bool, err error) {
	if dbconn == nil {
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	return LocalRetrieveListTMFObject(dbconn, tmfType, queryValues)
}

// BuildSelectFromParms creates a SELECT statement based on the query values.
// For objects with same id, selects the one with the latest version.
func BuildSelectFromParms(tmfType string, queryValues url.Values) (string, []any) {

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

	// Build the WHERE by processing the query values specified by the user
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
