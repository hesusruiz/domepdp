package pdp

import (
	"crypto/sha256"
	"fmt"

	"github.com/goccy/go-json"
)

type TMFObject struct {
	ID                     string         `json:"id"`
	Type                   string         `json:"type"`
	Name                   string         `json:"name"`
	Description            string         `json:"description"`
	LifecycleStatus        string         `json:"lifecycleStatus"`
	Version                string         `json:"version"`
	LastUpdate             string         `json:"lastUpdate"`
	ContentMap             map[string]any `json:"-"` // The content of the object as a map
	Content                []byte         `json:"-"` // The content of the object as a JSON byte array
	Organization           string         `json:"organization"`
	OrganizationIdentifier string         `json:"organizationIdentifier"`
	Updated                int64          `json:"updated"`
}

func NewTMFObject(oMap map[string]any, content []byte) (*TMFObject, error) {

	// Deduce the type of the object from the ID
	poType, err := TMFObjectIDtoType(oMap["id"].(string))
	if err != nil {
		return nil, err
	}

	// Canonicalize (if needed) the JSON object for the content field
	if content == nil {
		content, err = json.Marshal(oMap)
		if err != nil {
			return nil, err
		}
	}

	// Extract the fields from the map, if they exist
	name, _ := oMap["name"].(string)
	description, _ := oMap["description"].(string)
	lifecycleStatus, _ := oMap["lifecycleStatus"].(string)
	version, _ := oMap["version"].(string)
	lastUpdate, _ := oMap["lastUpdate"].(string)
	updated, _ := oMap["updated"].(int64)
	organizationIdentifier, _ := oMap["organizationIdentifier"].(string)
	organization, _ := oMap["organization"].(string)

	// Create a TMFObject struct from the map
	po := &TMFObject{
		ID:                     oMap["id"].(string),
		Type:                   poType,
		Name:                   name,
		Description:            description,
		LifecycleStatus:        lifecycleStatus,
		Version:                version,
		LastUpdate:             lastUpdate,
		ContentMap:             oMap,
		Content:                content,
		OrganizationIdentifier: organizationIdentifier,
		Organization:           organization,
		Updated:                updated,
	}

	return po, nil
}

func (po *TMFObject) String() string {
	return fmt.Sprintf("ID: %s\nType: %s\nName: %s\nLifecycleStatus: %s\nVersion: %s\nLastUpdate: %s\n", po.ID, po.Type, po.Name, po.LifecycleStatus, po.Version, po.LastUpdate)
}

func (po *TMFObject) SetOwner(organizationIdentifier string, organization string) (*TMFObject, error) {
	po.OrganizationIdentifier = organizationIdentifier
	po.ContentMap["organizationIdentifier"] = organizationIdentifier
	po.Organization = organization
	po.ContentMap["organization"] = organization

	// Update the content field
	poJSON, err := json.Marshal(po.ContentMap)
	if err != nil {
		return nil, err
	}

	po.Content = poJSON

	return po, nil
}

func (po *TMFObject) Hash() []byte {
	hasher := sha256.New()
	hasher.Write(po.Content)
	return hasher.Sum(nil)
}

func (po *TMFObject) ETag() string {
	etag := fmt.Sprintf(`"%x"`, po.Hash())
	return etag
}
