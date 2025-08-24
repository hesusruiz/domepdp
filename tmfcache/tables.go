package tmfcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hesusruiz/domepdp/config"
	"github.com/hesusruiz/domepdp/internal/errl"
	sqlb "github.com/huandu/go-sqlbuilder"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// ******************************************************************************************
// *****************************************************************************************
// Database management.
// ******************************************************************************************
// ******************************************************************************************

// tmfobject Table Schema
//
// The `tmfobject` table serves as a local cache for TMForum objects retrieved from a remote server.
// It stores various attributes of TMF objects to allow for efficient local querying.
// The table is created with `WITHOUT ROWID` for performance reasons, using the composite primary key of `id` and `version`.
//
// # Columns
//
// `id` `TEXT` `NOT NULL`: The unique identifier of the TMF object, typically a URN like `urn:ngsi-ld:<type>:<id>`.
// `resource` `TEXT` `NOT NULL`: The name of the TMF resource type, like `productOffering` or `catalog`.
// `version` `TEXT`: The version of the TMF object. Paired with `id`, it uniquely identifies a specific version of an object.
// `organizationIdentifier` `TEXT`: The unique identifier of the organization that owns or is associated with the object.
// `organization` `TEXT`: The name of the organization associated with the `organizationIdentifier`.
//
// These are for access control
// `seller` `TEXT`: The DID of the seller party in a transaction or offering.
// `buyer` `TEXT`: The DID of the buyer party in a transaction or offering.
// `sellerOperator` `TEXT`: The DID of the operator for the seller party.
// `buyerOperator` `TEXT`: The DID of the operator for the buyer party.
//
// `name` `TEXT` `NOT NULL`: The human-readable name of the TMF object.
// `description` `TEXT`: A textual description of the TMF object.
// `lifecycleStatus` `TEXT`: The current stage in the lifecycle of the object, for example, `Launched` or `Active`.
// `lastUpdate` `TEXT` `NOT NULL`: A timestamp indicating when the object was last updated in the source system.
//
// `content` `BLOB` `NOT NULL`: The full JSON payload of the TMF object. We do some queries in the object when needed.
// `hash` `BLOB`: A SHA256 hash of the `content` field, used to quickly detect changes in the object's data.
//
// `created` `INTEGER`: A Unix timestamp representing when the record was first inserted into this cache table.
// `updated` `INTEGER`: A Unix timestamp representing when this record was last updated in the cache.
//
// # Indexes
//
// `CREATE INDEX IF NOT EXISTS idx_hash ON tmfobject (hash);`
//
// An index is created on the `hash` column to speed up lookups based on the object's content hash.
// This is useful for quickly checking if an object with the same content already exists in the cache.
const createTMFTableSQL = `
CREATE TABLE IF NOT EXISTS tmfobject (
	"id" TEXT NOT NULL,
	"resource" TEXT NOT NULL,
	"version" TEXT DEFAULT '0.1',
	"organizationIdentifier" TEXT,
	"organization" TEXT,
	"seller" TEXT,
	"buyer" TEXT,
	"sellerOperator" TEXT,
	"buyerOperator" TEXT,
	"name" TEXT NOT NULL,
	"description" TEXT,
	"lifecycleStatus" TEXT,
	"lastUpdate" TEXT NOT NULL,
	"content" BLOB NOT NULL,
	"hash" BLOB,
	"created" INTEGER,
	"updated" INTEGER,

	PRIMARY KEY ("id", "resource", "version")
);
PRAGMA journal_mode = WAL;
CREATE INDEX IF NOT EXISTS idx_hash ON tmfobject (hash);
`

const deleteTMFTableSQL = `
DROP TABLE IF EXISTS tmfobject;
`
const vacuumTMFTableSQL = `VACUUM;`

const InsertTMFObjectSQL = `INSERT INTO tmfobject (id, organizationIdentifier, organization, seller, buyer, sellerOperator, buyerOperator, resource, name, description, lifecycleStatus, version, lastUpdate, content, hash, created, updated) VALUES (:id, :organizationIdentifier, :organization, :seller, :buyer, :sellerOperator, :buyerOperator, :resource, :name, :description, :lifecycleStatus, :version, :lastUpdate, :content, :hash, :created, :updated);`

const UpdateTMFObjectSQL = `UPDATE tmfobject SET organizationIdentifier = :organizationIdentifier, organization = :organization, seller = :seller, buyer = :buyer, sellerOperator = :sellerOperator, buyerOperator = :buyerOperator, resource = :resource, name = :name, description = :description, lifecycleStatus = :lifecycleStatus, lastUpdate = :lastUpdate, content = :content, hash = :hash, updated = :updated WHERE id = :id AND version = :version;`

var ErrorNotFound = errors.New("not found")

// createTables creates the table if it does not exist
func createTables(dbpool *sqlitex.Pool) error {

	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return errl.Error(err)
	}
	defer dbpool.Put(conn)

	if err := sqlitex.ExecuteScript(conn, createTMFTableSQL, nil); err != nil {
		slog.Error("createTables", slogor.Err(err))
		return errl.Errorf("createTables: %w", err)
	}

	return nil
}

// deleteTables drops the table and performs a VACUUM to reclaim space
func deleteTables(dbpool *sqlitex.Pool) error {
	conn, err := dbpool.Take(context.Background())
	if err != nil {
		return errl.Error(err)
	}
	defer dbpool.Put(conn)

	if err := sqlitex.ExecuteScript(conn, deleteTMFTableSQL, nil); err != nil {
		slog.Error("deleteTables", slogor.Err(err))
		return errl.Errorf("deleteTables: %w", err)
	}

	vacuumStmt, err := conn.Prepare(vacuumTMFTableSQL)
	if err != nil {
		return errl.Error(err)
	}
	defer vacuumStmt.Reset()

	_, err = vacuumStmt.Step()
	if err != nil {
		return errl.Error(err)
	}

	return nil
}

// LocalCheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func LocalCheckIfExists(
	dbconn *sqlite.Conn, id string, resource string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		return false, nil, 0, errl.Errorf("dbconn is nil")
	}

	// Check if the row already exists, with the same version
	const CheckIfExistsTMFObjectSQL = `SELECT hash, updated FROM tmfobject WHERE id = :id AND resource = :resource AND version = :version;`
	selectStmt, err := dbconn.Prepare(CheckIfExistsTMFObjectSQL)
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}
	defer selectStmt.Reset()

	selectStmt.SetText(":id", id)
	selectStmt.SetText(":resource", resource)
	selectStmt.SetText(":version", version)

	hasRow, err := selectStmt.Step()
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}

	// Each object has a hash to make sure it is the same object, even if the version is the same
	hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness = int(now - updated)

	return hasRow, hash, freshness, nil

}

// LocalCheckIfExists reports if there is an object in the database with a given id and version.
// It returns in addition its hash and freshness to enable comparisons with other objects.
func LocalCheckIfExistssdfsdf(
	dbconn *sqlite.Conn, id string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		return false, nil, 0, errl.Errorf("dbconn is nil")
	}

	// Check if the row already exists, with the same version
	const CheckIfExistsTMFObjectSQL = `SELECT id, hash, updated FROM tmfobject WHERE id = :id AND version = :version;`
	selectStmt, err := dbconn.Prepare(CheckIfExistsTMFObjectSQL)
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}
	defer selectStmt.Reset()

	selectStmt.SetText(":id", id)
	selectStmt.SetText(":version", version)

	hasRow, err := selectStmt.Step()
	if err != nil {
		return false, nil, 0, errl.Errorf("CheckIfExists: %w", err)
	}

	// Each object has a hash to make sure it is the same object, even if the version is the same
	hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness = int(now - updated)

	return hasRow, hash, freshness, nil

}

func LocalRetrieveTMFObject(dbconn *sqlite.Conn, id string, resourceType string, version string) (pod TMFObject, found bool, err error) {
	if dbconn == nil {
		return nil, false, errl.Errorf("dbconn is nil")
	}

	// We use a different SELECT statement depending on whether version is provided or not.
	// Except for admin users, normal users are given the latest version of the object.
	var stmt *sqlite.Stmt
	if len(version) == 0 {
		const RetrieveTMFObjectNoVersionSQL = `SELECT * FROM tmfobject WHERE id = :id AND resource = :resource ORDER BY version DESC LIMIT 1;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectNoVersionSQL)
		defer stmt.Reset()
		stmt.SetText(":id", id)
		stmt.SetText(":resource", resourceType)
	} else {
		const RetrieveTMFObjectSQL = `SELECT * FROM tmfobject WHERE id = :id AND resource = :resource AND version = :version;`
		stmt, err = dbconn.Prepare(RetrieveTMFObjectSQL)
		defer stmt.Reset()
		stmt.SetText(":id", id)
		stmt.SetText(":resource", resourceType)
		stmt.SetText(":version", version)
	}
	if err != nil {
		return nil, false, errl.Error(err)
	}

	hasRow, err := stmt.Step()
	if err != nil {
		slog.Error("RetrieveLocalTMFObject", "href", id, "error", err)
		return nil, false, errl.Error(err)
	}

	if !hasRow {
		return nil, false, errl.Error(ErrorNotFound)
	}

	// The map representation of the object is always
	// built from the JSON representation in the content field.
	// The system ensures that this field is in synch with the in-memory fields of the struct.
	var content = make([]byte, stmt.GetLen("content"))
	stmt.GetBytes("content", content)

	dbObject, err := TMFObjectFromBytes(content, resourceType)
	if err != nil {
		return nil, false, errl.Error(err)
	}

	return dbObject, true, nil

}

var ErrorStopLoop = errors.New("stop loop")

type LoopControl bool

const (
	LoopContinue LoopControl = true
	LoopStop     LoopControl = false
)

func LocalRetrieveListTMFObject(dbconn *sqlite.Conn, resourceType string, queryValues url.Values, perObject func(tmfObject TMFObject) LoopControl) error {
	if dbconn == nil {
		return errl.Errorf("dbconn is nil")
	}

	// Build the SQL SELECT based on the query passed on the HTTP request, as specified in TMForum
	sql, args := BuildSelectFromParms(resourceType, queryValues)

	err := sqlitex.Execute(dbconn, sql, &sqlitex.ExecOptions{
		Args: args,

		// This function is called once for each record found in the database
		ResultFunc: func(stmt *sqlite.Stmt) error {

			// The map representation of the object is always
			// built from the JSON representation in the content field.
			// The system ensures that this field is in synch with the in-memory fields of the struct.
			var content = make([]byte, stmt.GetLen("content"))
			stmt.GetBytes("content", content)

			dbObject, err := TMFObjectFromBytes(content, resourceType)
			if err != nil {
				return errl.Error(err)
			}

			if perObject(dbObject) == LoopContinue {
				return nil
			} else {
				return ErrorStopLoop
			}

		},
	})

	// An error indicating that the loop was stopped is not an error
	if err != nil && !errors.Is(err, ErrorStopLoop) {
		return errl.Error(err)
	}

	return nil
}

func (po *TMFGeneralObject) LocalUpdateInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return errl.Errorf("dbconn is nil")
	}

	if po.resourceType == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	// Calculate the hash, updating the ContentAsJSON at the same time
	hash := po.Hash()
	if hash == nil {
		return errl.Errorf("hash is nil")
	}

	updateStmt, err := dbconn.Prepare(UpdateTMFObjectSQL)
	if err != nil {
		return errl.Errorf("UpdateInStorage: %w", err)
	}
	defer updateStmt.Reset()

	// These are used for the WHERE clause
	updateStmt.SetText(":id", po.id)
	updateStmt.SetText(":resource", po.resourceType)
	updateStmt.SetText(":version", po.Version)

	// These are the updated fields
	updateStmt.SetText(":organizationIdentifier", po.organizationIdentifier)
	updateStmt.SetText(":organization", po.Organization)

	updateStmt.SetText(":seller", po.Seller)
	updateStmt.SetText(":buyer", po.Buyer)
	updateStmt.SetText(":sellerOperator", po.SellerOperator)
	updateStmt.SetText(":buyerOperator", po.BuyerOperator)

	updateStmt.SetText(":name", po.Name)
	updateStmt.SetText(":description", po.Description)
	updateStmt.SetText(":lifecycleStatus", po.LifecycleStatus)
	updateStmt.SetText(":lastUpdate", po.LastUpdate)
	updateStmt.SetBytes(":content", po.ContentAsJSON)
	updateStmt.SetBytes(":hash", hash)
	now := time.Now().Unix()
	updateStmt.SetInt64(":updated", now)

	_, err = updateStmt.Step()
	if err != nil {
		slog.Error("UpdateInStorage", "href", po.id, "error", err)
		return errl.Errorf("UpdateInStorage: %w", err)
	}

	return nil
}

// LocalInsertInStorage inserts the TMFGeneralObject into the provided SQLite database connection.
// It provides default values for some of th efields, if they are not provided.
// The method also computes and stores the object's hash, and sets the creation and update timestamps.
// Returns an error if the database connection is nil, the hash is nil, or if any database operation fails.
func (po *TMFGeneralObject) LocalInsertInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return errl.Errorf("dbconn is nil")
	}

	if po.resourceType == config.Category {
		po.SetOrganizationIdentifier(config.DOMEOperatorDid)
		po.SetOrganization(config.DOMEOperatorName)
	}

	hash := po.Hash()
	if hash == nil {
		return errl.Errorf("hash is nil")
	}

	insertStmt, err := dbconn.Prepare(InsertTMFObjectSQL)
	if err != nil {
		return errl.Error(err)
	}
	defer insertStmt.Reset()

	insertStmt.SetText(":id", po.id)
	insertStmt.SetText(":resource", po.resourceType)
	insertStmt.SetText(":version", po.Version)
	insertStmt.SetText(":organizationIdentifier", po.organizationIdentifier)
	insertStmt.SetText(":organization", po.Organization)

	insertStmt.SetText(":seller", po.Seller)
	insertStmt.SetText(":buyer", po.Buyer)
	insertStmt.SetText(":sellerOperator", po.SellerOperator)
	insertStmt.SetText(":buyerOperator", po.BuyerOperator)

	insertStmt.SetText(":name", po.Name)
	insertStmt.SetText(":description", po.Description)
	insertStmt.SetText(":lifecycleStatus", po.LifecycleStatus)
	insertStmt.SetText(":lastUpdate", po.LastUpdate)
	insertStmt.SetBytes(":content", po.ContentAsJSON)
	insertStmt.SetBytes(":hash", hash)
	now := time.Now().Unix()
	insertStmt.SetInt64(":created", now)
	insertStmt.SetInt64(":updated", now)

	_, err = insertStmt.Step()
	if err != nil {
		slog.Error("InsertInStorage", "href", po.id, "error", err)
		return errl.Error(err)
	}

	return nil
}

// LocalUpsertTMFObject inserts or updates a TMFGeneralObject in the local SQLite database.
// It first checks if a record with the same ID and version exists. If it exists and the data is
// fresh (based on maxFreshness) and the content hash matches, the function returns early.
// If the data is stale or the hash differs, it updates the existing record. If no such record exists,
// it inserts the object as a new row. The operation is wrapped in a SQLite SAVEPOINT for transactional safety.
//
// Parameters:
//   - dbconn: SQLite database connection.
//   - maxFreshness: Maximum allowed freshness (in seconds) for the existing record.
//
// Returns:
//   - err: An error if the operation fails, or nil on success.
func (po *TMFGeneralObject) LocalUpsertTMFObject(dbconn *sqlite.Conn, maxFreshness int) (err error) {
	if dbconn == nil {
		return errl.Errorf("dbconn is nil")
	}

	// Start a SAVEPOINT and defer its Commit/Rollback
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Check if the row already exists, with the same version
	objectExists, hash, freshness, err := LocalCheckIfExists(dbconn, po.id, po.resourceType, po.Version)
	if err != nil {
		return errl.Error(err)
	}

	// The id and version are the same, but we have to check the hash to see if we have to update the record
	if objectExists {

		fresh := freshness < maxFreshness
		newHash := po.Hash()

		// Check if the data is recent enough and the hash of the content is the same
		if fresh && bytes.Equal(hash, newHash) {
			// The hash of the content is the same, so return immediately
			slog.Debug("Upsert: row exists and fresh", "id", po.id)
			return nil
		}

		// The row has to be updated.
		// We do not have to update the id and version fields.
		err = po.LocalUpdateInStorage(dbconn)
		if err != nil {
			return errl.Error(err)
		}

		if !fresh {
			slog.Debug("Upsert: row updated (not fresh)", "id", po.id)
		} else {
			hashStr := fmt.Sprintf("%X", hash)
			newHashStr := fmt.Sprintf("%X", newHash)
			slog.Debug("Upsert: row updated (hash different)", "id", po.id, "old", hashStr, "new", newHashStr)
		}

		return nil // Skip inserting if the row already exists
	}

	// There was no record with the same id and version, so insert the full object
	err = po.LocalInsertInStorage(dbconn)
	if err != nil {
		slog.Error("UpsertTMFObject", "href", po.id, "version", po.Version, "error", err)
		return errl.Error(err)
	}

	slog.Debug("Upsert: row inserted", "id", po.id)

	return nil
}

// BuildSelectFromParms creates a SELECT statement based on the query values.
// For objects with same id, selects the one with the latest version.
func BuildSelectFromParmsClassic(tmfResource string, queryValues url.Values) (string, []any) {

	// Default values if the user did not specify them. -1 is equivalent to no values provided.
	var limit = -1
	var offset = -1

	bu := sqlb.SQLite.NewSelectBuilder()

	// SELECT: for each object with a given id, select the latest version.
	// We use the 'max(version)' function, and will GROUP by id.
	bu.Select(
		"id",
		"max(version)",
		"organizationIdentifier",
		"organization",
		"seller",
		"buyer",
		"sellerOperator",
		"buyerOperator",
		"resource",
		"name",
		"description",
		"lifecycleStatus",
		"lastUpdate",
		"content",
		"hash",
		"created",
		"updated",
	).From("tmfobject")

	// WHERE: normally we expect the resource name of object to be specified, but we support a query for all object types
	if len(tmfResource) > 0 {
		bu.Where(bu.Equal("resource", tmfResource))
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
					cond.Equal(key, sqlb.List(vals)),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In(key, sqlb.List(vals)),
				)
			}

		case "relatedParty.id", "relatedParty":
			// This is a special case, given that it is so frequent, we perform an optimization
			whereClause.AddWhereExpr(
				cond.Args,
				cond.Equal("organization", values[0]),
			)

		case "seller", "buyer", "sellerOperator", "buyerOperator":
			// A shortcut for DOME, to simplify life to applications (but can be also done in a TMF-compliant way).
			// Special processing to allow specifying multiple values in the form 'seller=id1,id2,id3'.
			// We also support the standard HTTP query strings like 'seller=id1,id2&seller=id3'
			var vals = []string{}
			// Allow several instances of the key in the query string (as in standard HTTP query strings)
			for _, v := range values {
				// Process each for several comma-separated values in the same key instance
				parts := strings.Split(v, ",")
				// Allow for whitespace surrounding the elements
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				vals = append(vals, parts...)
			}

			// Use either an equality (when one element) or an inclusion expression (when several)
			if len(vals) == 1 {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.Equal(key, sqlb.List(vals)),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In(key, sqlb.List(vals)),
				)
			}

		default:

			// We assume that the rest of parameters are not in the fields of the SQL database.
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
	// and nobody can predict the final ordering a-priory.
	// For a stable catalog, the ordering is the same for all users and at any time.
	// When a provider creates or modifies a product, it will be inserted at an unpredictable position in the catalog.
	//
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

// BuildSelectFromParms creates a SELECT statement based on the query values.
// For objects with same id, selects the one with the latest version.
func BuildSelectFromParms(tmfResource string, queryValues url.Values) (string, []any) {

	// Default values if the user did not specify them. -1 is equivalent to no values provided.
	var limit = -1
	var offset = -1

	bu := sqlb.SQLite.NewSelectBuilder()

	// SELECT: for each object with a given id, select the latest version.
	// We use the 'max(version)' function, and will GROUP by id.
	bu.Select(
		"id",
		"max(version)",
		"organizationIdentifier",
		"organization",
		"seller",
		"buyer",
		"sellerOperator",
		"buyerOperator",
		"resource",
		"name",
		"description",
		"lifecycleStatus",
		"lastUpdate",
		"content",
		"hash",
		"created",
		"updated",
	).From("tmfobject")

	// WHERE: normally we expect the resource name of object to be specified, but we support a query for all object types
	if len(tmfResource) > 0 {
		bu.Where(bu.Equal("resource", tmfResource))
	}

	// Build the WHERE by processing the query values specified by the user
	whereClause := sqlb.NewWhereClause()
	cond := sqlb.NewCond()

	for key, values := range queryValues {

		switch key {
		// case "limit":
		// 	limitStr := queryValues.Get("limit")
		// 	if l, err := strconv.Atoi(limitStr); err == nil {
		// 		limit = l
		// 	}
		// case "offset":
		// 	offsetStr := queryValues.Get("offset")
		// 	if l, err := strconv.Atoi(offsetStr); err == nil {
		// 		offset = l
		// 	}
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
					cond.Equal(key, sqlb.List(vals)),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In(key, sqlb.List(vals)),
				)
			}

		case "relatedParty.id", "relatedParty":
			// This is a special case, given that it is so frequent, we perform an optimization
			whereClause.AddWhereExpr(
				cond.Args,
				cond.Equal("organization", values[0]),
			)

		case "seller", "buyer", "sellerOperator", "buyerOperator":
			// A shortcut for DOME, to simplify life to applications (but can be also done in a TMF-compliant way).
			// Special processing to allow specifying multiple values in the form 'seller=id1,id2,id3'.
			// We also support the standard HTTP query strings like 'seller=id1,id2&seller=id3'
			var vals = []string{}
			// Allow several instances of the key in the query string (as in standard HTTP query strings)
			for _, v := range values {
				// Process each for several comma-separated values in the same key instance
				parts := strings.Split(v, ",")
				// Allow for whitespace surrounding the elements
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				vals = append(vals, parts...)
			}

			// Use either an equality (when one element) or an inclusion expression (when several)
			if len(vals) == 1 {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.Equal(key, sqlb.List(vals)),
				)
			} else {
				whereClause.AddWhereExpr(
					cond.Args,
					cond.In(key, sqlb.List(vals)),
				)
			}

		default:

			// We assume that the rest of parameters are not in the fields of the SQL database.
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
	// and nobody can predict the final ordering a-priory.
	// For a stable catalog, the ordering is the same for all users and at any time.
	// When a provider creates or modifies a product, it will be inserted at an unpredictable position in the catalog.
	//
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
