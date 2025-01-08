package tmfsync

import (
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
	"id" TEXT NOT NULL PRIMARY KEY,
	"organizationIdentifier" TEXT,
	"organization" TEXT,
	"type" TEXT NOT NULL,
	"name" TEXT NOT NULL,
	"description" TEXT,
	"lifecycleStatus" TEXT,
	"version" TEXT,
	"lastUpdate" TEXT NOT NULL,
	"content" BLOB NOT NULL,
	"hash" BLOB,
	"created" INTEGER,
	"updated" INTEGER
);
PRAGMA journal_mode = WAL;`

const UpdateTMFObjectSQL = `UPDATE tmfobject SET organizationIdentifier = :organizationIdentifier, organization = :organization, type = :type, name = :name, description = :description, lifecycleStatus = :lifecycleStatus, lastUpdate = :lastUpdate, content = :content, hash = :hash, updated = :updated WHERE id = :id AND version = :version;`
const InsertTMFObjectSQL = `INSERT INTO tmfobject (id, organizationIdentifier, organization, type, name, description, lifecycleStatus, version, lastUpdate, content, hash, created, updated) VALUES (:id, :organizationIdentifier, :organization, :type, :name, :description, :lifecycleStatus, :version, :lastUpdate, :content, :hash, :created, :updated);`

const CheckIfExistsTMFObjectSQL = `SELECT id, hash, updated FROM tmfobject WHERE id = :id AND version = :version;`

func (tmf *TMFdb) CheckIfExists(dbconn *sqlite.Conn, id string, version string) (bool, []byte, int, error) {

	// if dbconn == nil {
	// 	var err error
	// 	dbconn, err = tmf.dbpool.Take(context.Background())
	// 	if err != nil {
	// 		return false, nil, 0, err
	// 	}
	// 	defer tmf.dbpool.Put(dbconn)
	// }

	dbconn, err := tmf.RequestDB(dbconn)
	if err != nil {
		return false, nil, 0, err
	}
	defer tmf.ReleaseDB(dbconn)

	// Check if the row already exists, with the same version
	selectStmt, err := dbconn.Prepare(CheckIfExistsTMFObjectSQL)
	if err != nil {
		return false, nil, 0, err
	}
	defer selectStmt.Reset()

	selectStmt.SetText(":id", id)
	selectStmt.SetText(":version", version)

	hasRow, err := selectStmt.Step()
	if err != nil {
		return false, nil, 0, err
	}

	// Each object has a hash to make sure it is the same object, even if the version is the same
	var hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness := int(now - updated)

	return hasRow, hash, freshness, nil

}

func (tmf *TMFdb) UpdateInStorage(dbconn *sqlite.Conn, po *TMFObject) error {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// The hash of the content is different, so update the row.
	// We do not have to update the id and version fields.
	updateStmt, err := dbconn.Prepare(UpdateTMFObjectSQL)
	if err != nil {
		return err
	}
	defer updateStmt.Reset()

	updateStmt.SetText(":id", po.ID)
	updateStmt.SetText(":version", po.Version)

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
		return err
	}

	return nil
}

func (tmf *TMFdb) InsertInStorage(dbconn *sqlite.Conn, po *TMFObject) error {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// There was no record with the same id and version, so insert the full object
	insertStmt, err := dbconn.Prepare(InsertTMFObjectSQL)
	if err != nil {
		return err
	}
	defer insertStmt.Reset()

	insertStmt.SetText(":id", po.ID)
	insertStmt.SetText(":organizationIdentifier", po.OrganizationIdentifier)
	insertStmt.SetText(":organization", po.Organization)
	insertStmt.SetText(":type", po.Type)
	insertStmt.SetText(":name", po.Name)
	insertStmt.SetText(":description", po.Description)
	insertStmt.SetText(":lifecycleStatus", po.LifecycleStatus)
	insertStmt.SetText(":version", po.Version)
	insertStmt.SetText(":lastUpdate", po.LastUpdate)
	insertStmt.SetBytes(":content", po.Content)
	insertStmt.SetBytes(":hash", po.Hash())
	now := time.Now().Unix()
	insertStmt.SetInt64(":created", now)
	insertStmt.SetInt64(":updated", now)

	_, err = insertStmt.Step()
	if err != nil {
		return err
	}

	return nil
}

const RetrieveTMFObjectSQL = `SELECT * FROM tmfobject WHERE id = :id AND version = :version;`
const RetrieveTMFObjectNoVersionSQL = `SELECT * FROM tmfobject WHERE id = :id ORDER BY version DESC;`

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

	// Check if the object is already in the local database
	var stmt *sqlite.Stmt
	if len(version) == 0 {
		stmt, err = dbconn.Prepare(RetrieveTMFObjectNoVersionSQL)
		defer stmt.Reset()
		stmt.SetText(":id", href)
	} else {
		stmt, err = dbconn.Prepare(RetrieveTMFObjectSQL)
		defer stmt.Reset()
		stmt.SetText(":id", href)
		stmt.SetText(":version", href)
	}

	hasRow, err := stmt.Step()
	if err != nil {
		return nil, false, err
	}

	if !hasRow {
		return nil, false, nil
	}

	// Return the first match, ignoring any other rows that may be in the database
	var oMap map[string]any

	var content = make([]byte, stmt.GetLen("content"))
	stmt.GetBytes("content", content)
	err = json.Unmarshal(content, &oMap)
	if err != nil {
		log.Println(err)
		return nil, false, err
	}

	// Complete the object with the relevant fields which are in our db but not in the DOME repo
	oMap["updated"] = stmt.GetInt64("updated")
	oMap["organizationIdentifier"] = stmt.GetText("organizationIdentifier")
	oMap["organization"] = stmt.GetText("organization")

	po, err = NewTMFObject(oMap, content)
	if err != nil {
		log.Println(err)
		return nil, false, err
	}

	return po, true, nil

}

func buildSelectFromParms(tmfType string, qv url.Values) (string, []interface{}) {

	// Process the queryValues and convert to a SQL query
	var limit = -1
	var offset = -1

	bu := sqlb.SQLite.NewSelectBuilder()

	bu.Select("*").From("tmfobject")
	if len(tmfType) > 0 {
		bu.Where(bu.Equal("type", tmfType))
	}

	whereClause := sqlb.NewWhereClause()
	cond := sqlb.NewCond()

	for key, values := range qv {

		switch key {
		case "limit":
			limitStr := qv.Get("limit")
			if l, err := strconv.Atoi(limitStr); err == nil {
				limit = l
			}
		case "offset":
			offsetStr := qv.Get("offset")
			if l, err := strconv.Atoi(offsetStr); err == nil {
				offset = l
			}
		case "lifecycleStatus":
			var vals = []string{}
			for _, v := range values {
				parts := strings.Split(v, ",")
				vals = append(vals, parts...)
			}

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

		default:
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

	bu.AddWhereClause(whereClause)
	bu.OrderBy("updated DESC").Limit(limit).Offset(offset)
	sql, args := bu.Build()

	return sql, args
}

func (tmf *TMFdb) RetrieveLocalListTMFObject(dbconn *sqlite.Conn, tmfType string, qv url.Values) (pos []*TMFObject, found bool, err error) {
	if dbconn == nil {
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, false, err
		}
		defer tmf.dbpool.Put(dbconn)
	}

	sql, args := buildSelectFromParms(tmfType, qv)

	// slog.Info("RetrieveLocalListTMFObject", "sql", sql, "args", args)

	var resultPOs []*TMFObject

	err = sqlitex.Execute(dbconn, sql, &sqlitex.ExecOptions{
		Args: args,

		ResultFunc: func(stmt *sqlite.Stmt) error {

			var oMap map[string]any

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
