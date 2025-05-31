package pdp

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"

	"github.com/goccy/go-json"
	"github.com/hesusruiz/domeproxy/config"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	"gitlab.com/greyxor/slogor"
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

func NewTMFObject(oMap map[string]any, content []byte) (*TMFObject, error) {

	err := tmfObjectSanityCheck(oMap)
	if err != nil {
		slog.Error("invalid object", slogor.Err(err))
		out, err := json.MarshalIndent(oMap, "", "  ")
		if err != nil {
			return nil, err
		}
		fmt.Println(string(out))
		return nil, err
	}

	// Deduce the resource name of the object from the ID
	resourceName, err := config.FromIdToResourceName(oMap["id"].(string))
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
	po := &TMFObject{
		ID:                     id,
		ResourceName:           resourceName,
		Name:                   name,
		Description:            description,
		LifecycleStatus:        lifecycleStatus,
		Version:                version,
		LastUpdate:             lastUpdate,
		ContentAsMap:           oMap,
		ContentAsJSON:          content,
		OrganizationIdentifier: organizationIdentifier,
		Organization:           organization,
		Updated:                updated,
	}

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

			// TODO: be paranoic and check that the "name" field complies with the ELSI format completely,
			// including the organizationIdentifier part

			name, ok := rpMap["name"].(string)
			if !ok {
				slog.Error("invalid name type", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}
			if !strings.HasPrefix(name, "did:elsi:") {
				slog.Error("invalid name prefix", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}

			switch role {
			case "seller":
				po.Seller = name

			case "buyer":
				po.Buyer = name

			case "sellerOperator":
				po.SellerOperator = name

			case "buyerOperator":
				po.BuyerOperator = name
			}
		}
	}

	return po, nil
}

func (po *TMFObject) UnmarshalJSON(data []byte) error {
	var oMap map[string]any
	var err error

	err = json.Unmarshal(data, &oMap)
	if err != nil {
		return err
	}

	err = tmfObjectSanityCheck(oMap)
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

			// TODO: be paranoic and check that the "name" field complies with the ELSI format completely,
			// including the organizationIdentifier part

			name, ok := rpMap["name"].(string)
			if !ok {
				slog.Error("invalid name type", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}
			if !strings.HasPrefix(name, "did:elsi:") {
				slog.Error("invalid name prefix", "tmfObject", id)
				if out, err := json.MarshalIndent(rp, "", "  "); err == nil {
					fmt.Println(string(out))
				}
				break
			}

			switch role {
			case "seller":
				po.Seller = name

			case "buyer":
				po.Buyer = name

			case "sellerOperator":
				po.SellerOperator = name

			case "buyerOperator":
				po.BuyerOperator = name
			}
		}
	}

	return nil
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
