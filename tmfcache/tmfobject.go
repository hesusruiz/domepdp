package tmfcache

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// TMFObject is the in-memory representation of a TMForum object.
//
// TMFObject can represent any arbitrary TMForum object, where the most important fields are in the struct.
// The whole object is always up-to-date in ContentAsMap and ContentAsJSON, to enable fast saving and retrieving
// from the database used as cache.
type TMFObject struct {
	ID                     string         `json:"id"`
	ResourceName           string         `json:"resourceName"`
	Name                   string         `json:"name"`
	Description            string         `json:"description"`
	LifecycleStatus        string         `json:"lifecycleStatus"`
	Version                string         `json:"version"`
	LastUpdate             string         `json:"lastUpdate"`
	ContentAsMap           map[string]any `json:"-"` // The content of the object as a map
	ContentAsJSON          []byte         `json:"-"` // The content of the object as a JSON byte array
	Organization           string         `json:"organization"`
	OrganizationIdentifier string         `json:"organizationIdentifier"`
	Seller                 string         `json:"seller"`
	Buyer                  string         `json:"buyer"`
	SellerOperator         string         `json:"sellerOperator"`
	BuyerOperator          string         `json:"buyerOperator"`
	Updated                int64          `json:"updated"`
}

// FromMap is the main TMFObject constructor, and is used by all other constructors.
// It performs validity checks when building the resulting TMFObject object.
func (po *TMFObject) FromMap(oMap map[string]any) error {

	// Allocate an object if it is nil
	if po == nil {
		po = &TMFObject{}
	}

	err := tmfObjectSanityCheck(oMap)
	if err != nil {
		slog.Error("invalid object", slogor.Err(err))
		out, err := json.MarshalIndent(oMap, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return err
	}

	// Deduce the resource name of the object from the ID
	resourceName, err := config.FromIdToResourceName(oMap["id"].(string))
	if err != nil {
		return err
	}

	// Extract the fields from the map, if they exist
	id, _ := oMap["id"].(string)
	name, _ := oMap["name"].(string)
	description, _ := oMap["description"].(string)
	lifecycleStatus, _ := oMap["lifecycleStatus"].(string)
	version, _ := oMap["version"].(string)
	lastUpdate, _ := oMap["lastUpdate"].(string)
	updated, _ := oMap["updated"].(int64)
	organizationIdentifier, _ := oMap["organizationIdentifier"].(string)
	organization, _ := oMap["organization"].(string)

	// Create a TMFObject struct from the map
	po.ID = id
	po.ResourceName = resourceName
	po.Name = name
	po.Description = description
	po.LifecycleStatus = lifecycleStatus
	po.Version = version
	po.LastUpdate = lastUpdate
	po.OrganizationIdentifier = organizationIdentifier
	po.Organization = organization
	po.Updated = updated

	// Store the whole map
	po.ContentAsMap = oMap

	// Look for the "Seller", "SellerOperator", "Buyer" and "BuyerOperator" roles
	relatedParties, ok := oMap["relatedParty"].([]any)
	if ok {
		for _, rp := range relatedParties {

			// Convert entry to a map
			rpMap, ok := rp.(map[string]any)
			if !ok {
				slog.Error("invalid relatedParty", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}

			// The entry MUST have a 'role' field
			if rpMap["role"] == nil {
				slog.Error("mo role in related party", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}

			role, ok := rpMap["role"].(string)
			if !ok {
				slog.Error("invalid role type", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}

			role = strings.ToLower(role)
			if role != "seller" && role != "selleroperator" && role != "buyer" && role != "buyeroperator" {
				continue
			}

			// TODO: maybe we can be be paranoid and check that the "name" field complies with the ELSI format completely,
			// including the organizationIdentifier part

			did, ok := rpMap["did"].(string)
			if !ok {
				slog.Error("DID does not exist or has invalid type", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}
			if !strings.HasPrefix(did, "did:elsi:") {
				slog.Error("invalid DID prefix", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}

			switch role {
			case "seller":
				po.Seller = did

			case "buyer":
				po.Buyer = did

			case "selleroperator":
				po.SellerOperator = did

			case "buyeroperator":
				po.BuyerOperator = did
			}
		}
	}

	return nil
}

var (
	ErrorSellerEmpty         = errors.New("seller is empty")
	ErrorBuyerEmpty          = errors.New("buyer is empty")
	ErrorSellerOperatorEmpty = errors.New("seller operator is empty")
	ErrorBuyerOperatorEmpty  = errors.New("buyer operator is empty")
	ErrorLastUpdateEmpty     = errors.New("lastUpdate is empty")
)

func (po *TMFObject) Validate() error {
	var errorList []error
	if po.Seller == "" {
		errorList = append(errorList, ErrorSellerEmpty)
	}
	if po.SellerOperator == "" {
		errorList = append(errorList, ErrorSellerOperatorEmpty)
	}
	if po.Buyer == "" {
		errorList = append(errorList, ErrorBuyerEmpty)
	}
	if po.BuyerOperator == "" {
		errorList = append(errorList, ErrorBuyerOperatorEmpty)
	}
	if po.LastUpdate == "" {
		errorList = append(errorList, fmt.Errorf("lastUpdate is empty"))
	}

	return errors.Join(errorList...)
}

// UnmarshalJSON implements the [json.Unmarshall] interface.
// It unmarshalls first to a map[string]any and then uses [FromMap] to perform validity checks while building the TMFObject.
func (po *TMFObject) UnmarshalJSON(data []byte) error {
	var oMap map[string]any
	var err error

	err = json.Unmarshal(data, &oMap)
	if err != nil {
		return err
	}

	return po.FromMap(oMap)

}

func TMFObjectFromMap(oMap map[string]any) (*TMFObject, error) {

	po := &TMFObject{}
	err := po.FromMap(oMap)
	if err != nil {
		return nil, err
	}

	return po, nil

}

func TMFObjectFromBytes(content []byte) (*TMFObject, error) {

	var tmf *TMFObject
	err := json.Unmarshal(content, &tmf)
	if err != nil {
		return nil, err
	}

	return tmf, nil
}

func (po *TMFObject) MarshalJSON() ([]byte, error) {

	// We assume that the ContentAsMap field is always up to date
	content, err := json.Marshal(po.ContentAsMap)
	if err != nil {
		return nil, err
	}

	po.ContentAsJSON = content

	return content, nil

}

func tmfObjectSanityCheck(oMap map[string]any) error {

	// id MUST exist
	if oMap["id"] == nil {
		return fmt.Errorf("id field is nil")
	}

	id, ok := oMap["id"].(string)
	if !ok {
		return fmt.Errorf("invalid id type: %v", oMap["id"])
	}

	if !strings.HasPrefix(id, "urn:ngsi-ld:") {
		return fmt.Errorf("invalid id prefix: %s", id)
	}

	// href MUST exist
	if oMap["href"] == nil {
		return fmt.Errorf("href field is nil, id: %s", id)
	}

	href, ok := oMap["href"].(string)
	if !ok {
		return fmt.Errorf("invalid href type: %v", oMap["href"])
	}
	if !strings.HasPrefix(href, "urn:ngsi-ld:") {
		return fmt.Errorf("invalid href prefix: %s", href)
	}

	if id != href {
		return fmt.Errorf("id (%s) and href (%s) do not match", id, href)
	}

	return nil
}

func (po *TMFObject) GetMap(path string) map[string]any {
	return jpath.GetMap(po.ContentAsMap, path)
}

func (po *TMFObject) GetString(path string) string {
	return jpath.GetString(po.ContentAsMap, path)
}

func (po *TMFObject) GetList(path string) []any {
	return jpath.GetList(po.ContentAsMap, path)
}

func (po *TMFObject) GetListString(path string) []string {
	return jpath.GetListString(po.ContentAsMap, path)
}

func (po *TMFObject) GetBool(path string) bool {
	return jpath.GetBool(po.ContentAsMap, path)
}

func (po *TMFObject) GetInt(path string) int {
	return jpath.GetInt(po.ContentAsMap, path)
}

func (po *TMFObject) SetID(id string) {
	po.ID = id
	po.ContentAsMap["id"] = id
}

func (po *TMFObject) SetHref(href string) {
	po.ID = href
	po.ContentAsMap["href"] = href
}

func (po *TMFObject) SetName(name string) {
	po.Name = name
	po.ContentAsMap["name"] = name
}

func (po *TMFObject) SetDescription(description string) {
	po.Description = description
	po.ContentAsMap["description"] = description
}

func (po *TMFObject) SetLifecycleStatus(lifecycleStatus string) {
	po.LifecycleStatus = lifecycleStatus
	po.ContentAsMap["lifecycleStatus"] = lifecycleStatus
}

func (po *TMFObject) SetVersion(version string) {
	po.Version = version
	po.ContentAsMap["version"] = version
}

func (po *TMFObject) SetLastUpdate(lastUpdate string) {
	po.LastUpdate = lastUpdate
	po.ContentAsMap["lastUpdate"] = lastUpdate
}

func (po *TMFObject) SetOrganization(organization string) {
	po.Organization = organization
	po.ContentAsMap["organization"] = organization
}

func (po *TMFObject) SetOrganizationIdentifier(organizationIdentifier string) {
	po.OrganizationIdentifier = organizationIdentifier
}

func (po *TMFObject) SetSeller(organizationIdentifier string) {

}

func (po *TMFObject) String() string {
	return fmt.Sprintf("ID: %s\nType: %s\nName: %s\nLifecycleStatus: %s\nVersion: %s\nLastUpdate: %s\n", po.ID, po.ResourceName, po.Name, po.LifecycleStatus, po.Version, po.LastUpdate)
}

func (po *TMFObject) SetOwner(organizationIdentifier string, organization string) (*TMFObject, error) {
	po.OrganizationIdentifier = organizationIdentifier
	po.ContentAsMap["organizationIdentifier"] = organizationIdentifier
	po.Organization = organization
	po.ContentAsMap["organization"] = organization

	// Update the content field
	poJSON, err := json.Marshal(po.ContentAsMap)
	if err != nil {
		return nil, err
	}

	po.ContentAsJSON = poJSON

	return po, nil
}

// Hash calculates the hash of the canonical JSON representation of the object.
// It also updates the ContentAsJSON field with that JSON representation so it reflects the last status of the object.
func (po *TMFObject) Hash() []byte {
	// Update the content field
	poJSON, err := json.Marshal(po.ContentAsMap)
	if err != nil {
		return nil
	}

	po.ContentAsJSON = poJSON

	hasher := sha256.New()
	hasher.Write(po.ContentAsJSON)
	return hasher.Sum(nil)
}

func (po *TMFObject) ETag() string {
	etag := fmt.Sprintf(`"%x"`, po.Hash())
	return etag
}

func LocalRetrieveTMFObject(dbconn *sqlite.Conn, href string, version string) (po *TMFObject, found bool, err error) {
	if dbconn == nil {
		return nil, false, fmt.Errorf("dbconn is nil")
	}

	// We use a different SELECT statement depending on whether version is provided or not.
	// Except for admin users, normal users are given the latest version of the object.
	var stmt *sqlite.Stmt
	if len(version) == 0 {
		const RetrieveTMFObjectNoVersionSQL = `SELECT * FROM tmfobject WHERE id = :id ORDER BY version DESC LIMIT 1;`
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

	// Even if we store it also in the db, the map representation of the object is always
	// built from the JSON representation in the content field.
	// The system ensures that this field is in synch with the in-memory fields of the struct.
	var content = make([]byte, stmt.GetLen("content"))
	stmt.GetBytes("content", content)

	updated := stmt.GetInt64("updated")

	var tmf *TMFObject
	err = json.Unmarshal(content, &tmf)
	if err != nil {
		return nil, false, err
	}

	tmf.Updated = updated

	// Complete the map representation with the relevant fields which are in our db but not in the DOME repo
	tmf.ContentAsMap["updated"] = updated
	tmf.ContentAsMap["organizationIdentifier"] = stmt.GetText("organizationIdentifier")
	tmf.ContentAsMap["organization"] = stmt.GetText("organization")

	return tmf, true, nil

}

func (po *TMFObject) LocalUpdateInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return fmt.Errorf("dbconn is nil")
	}

	// Calculate the hash, updating the ContentAsJSON at the same time
	hash := po.Hash()
	if hash == nil {
		return fmt.Errorf("hash is nil")
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
	updateStmt.SetText(":type", po.ResourceName)
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
		slog.Error("UpdateInStorage", "href", po.ID, "error", err)
		return fmt.Errorf("UpdateInStorage: %w", err)
	}

	return nil
}

func (po *TMFObject) LocalInsertInStorage(dbconn *sqlite.Conn) error {
	if dbconn == nil {
		return fmt.Errorf("dbconn is nil")
	}

	hash := po.Hash()
	if hash == nil {
		return fmt.Errorf("hash is nil")
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
	insertStmt.SetText(":type", po.ResourceName)
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
		slog.Error("InsertInStorage", "href", po.ID, "error", err)
		return err
	}

	return nil
}

func (po *TMFObject) LocalUpsertTMFObject(dbconn *sqlite.Conn, maxFreshness int) (err error) {
	if dbconn == nil {
		return fmt.Errorf("dbconn is nil")
	}

	// Start a SAVEPOINT and defer its Commit/Rollback
	release := sqlitex.Save(dbconn)
	defer release(&err)

	// Get the type of object from the ID
	resourceName, err := config.FromIdToResourceName(po.ID)
	if err != nil {
		return err
	}
	po.ResourceName = resourceName

	// Categories do not have an owner. Make sure we do not set the owner field to something else
	if po.ResourceName == "category" {
		po.OrganizationIdentifier = ""
		po.Organization = ""
	}

	// Check if the row already exists, with the same version
	hasRow, hash, freshness, err := LocalCheckIfExists(dbconn, po.ID, po.Version)
	if err != nil {
		return err
	}

	// The id and version are the same, but we have to check the hash to see if we have to update the record
	if hasRow {

		fresh := freshness < maxFreshness
		newHash := po.Hash()

		// Check if the data is recent enough and the hash of the content is the same
		if fresh && bytes.Equal(hash, newHash) {
			// The hash of the content is the same, so return immediately
			slog.Debug("Upsert: row exists and fresh", "id", po.ID)
			return nil
		}

		// The row has to be updated.
		// We do not have to update the id and version fields.
		err = po.LocalUpdateInStorage(dbconn)
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
	err = po.LocalInsertInStorage(dbconn)
	if err != nil {
		slog.Error("UpsertTMFObject", "href", po.ID, "version", po.Version, "error", err)
		return err
	}

	slog.Debug("Upsert: row inserted", "id", po.ID)

	return nil
}

func LocalCheckIfExists(
	dbconn *sqlite.Conn, id string, version string,
) (exists bool, hash []byte, freshness int, err error) {
	if dbconn == nil {
		return false, nil, 0, fmt.Errorf("dbconn is nil")
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
	hash = make([]byte, selectStmt.GetLen("hash"))
	selectStmt.GetBytes("hash", hash)

	updated := selectStmt.GetInt64("updated")
	now := time.Now().Unix()
	freshness = int(now - updated)

	return hasRow, hash, freshness, nil

}

func LocalRetrieveListTMFObject(dbconn *sqlite.Conn, tmfType string, queryValues url.Values) (pos []*TMFObject, found bool, err error) {
	if dbconn == nil {
		return nil, false, fmt.Errorf("dbconn is nil")
	}

	// Build the SQL SELECT based on the query passed on the HTTP request, as specified in TMForum
	sql, args := BuildSelectFromParms(tmfType, queryValues)

	var resultPOs []*TMFObject

	err = sqlitex.Execute(dbconn, sql, &sqlitex.ExecOptions{
		Args: args,

		// This function is called once for each record found in the database
		ResultFunc: func(stmt *sqlite.Stmt) error {

			// Even if we store it also in the db, the map representation of the object is always
			// built from the JSON representation in the content field.
			// The system ensures that this field is in synch with the in-memory fields of the struct.
			content := make([]byte, stmt.GetLen("content"))
			stmt.GetBytes("content", content)

			updated := stmt.GetInt64("updated")

			var tmf *TMFObject
			err = json.Unmarshal(content, &tmf)
			if err != nil {
				return err
			}

			tmf.Updated = updated

			// Complete the map representation with the relevant fields which are in our db but not in the DOME repo
			tmf.ContentAsMap["updated"] = updated
			tmf.ContentAsMap["organizationIdentifier"] = stmt.GetText("organizationIdentifier")
			tmf.ContentAsMap["organization"] = stmt.GetText("organization")

			resultPOs = append(resultPOs, tmf)

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
