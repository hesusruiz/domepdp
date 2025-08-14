// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/hesusruiz/domeproxy/internal/errl"
	"github.com/hesusruiz/domeproxy/internal/sqlogger"
)

type Config struct {

	// DOME operator did
	// TODO: set the proper identification when the DOME foundation is created
	DOMEOperatorDid  string
	DOMEOperatorName string

	// Indicates the environment (SBX, DEV2, PRO, LCL) where the proxy is running.
	// It is used to determine the DOME host and the database name.
	// It is also used to determine the policy file name, which is used to load the policies from the DOME.
	Environment Environment

	// PolicyFileName is the name of the file where the policies are stored.
	// It can specify a local file or a remote URL.
	PolicyFileName string

	// PDPAddress is the address of the PDP server.
	PDPAddress string

	// Debug mode, more logs and less caching
	Debug bool

	// internalUpstreamPodHosts is a map of resource names to their internal pod hostnames.
	// It is used to access the TMForum APIs from inside the DOME instance.
	// The keys are the resource names (e.g. "productCatalogManagement") and the values are
	// the hostnames (e.g. "tm-forum-api-product-catalog:8080").
	// It is a sync.Map to allow concurrent access.
	internalUpstreamPodHosts *sync.Map
	internal                 bool
	usingBAEProxy            bool

	// fixMode enables "smart" automait fixing of objects so they comply with the DOME specs
	// There is no magic. however, and there are things that can not be done.
	fixMode bool

	resourceToPath *ResourceToExternalPathPrefix

	// BAEProxyDomain is the host of the DOME instance.
	// It is used to access the TMForum APIs from outside the DOME instance.
	BAEProxyDomain string

	// ExternalTMFDomain for TMF apis
	ExternalTMFDomain string

	// VerifierServer is the URL of the verifier server, which is used to verify the access tokens.
	VerifierServer string

	// Dbname is the name of the database file where the TMForum cahed data is stored
	// It is used to store the data in a local SQLite database, the best SQL database for this purpose.
	Dbname string

	// ClonePeriod is the period in which the proxy will clone the TMForum objects from the DOME instance,
	// to keep the local cache up to date.
	ClonePeriod time.Duration

	// LogHandler is the handler used to log messages.
	// It is a custom handler that uses the slog package to log messages both to the console and to a SQLite database.
	LogHandler *sqlogger.SQLogHandler

	// LogLevel is a slog.LevelVar that can be set to different log levels (e.g. Debug, Info, Warn, Error).
	LogLevel *slog.LevelVar
}

// TODO: These are here until the DOME foundation is created and the DOME operator did is set.
const (
	DOMEOperatorDid  = "did:elsi:VATES-11111111K"
	DOMEOperatorName = "DOME Foundation"
)

type Environment int

const DOME_PRO Environment = 0
const DOME_DEV2 Environment = 1
const DOME_SBX Environment = 2
const DOME_LCL Environment = 3
const ISBE Environment = 4

// As this PDP is designed for DOME and ISBE environments, many config data items are hardcoded.
// This avoids many configuration errors and simplifies deployment, at the expense of some flexibility.
// However, this flexibility is not really needed in practice, as the DOME environments are well defined and stable.
// Minimizing errors is here much more important than the ability to configure these parameters.

const PRO_dbname = "./tmf.db"
const DEV2_dbname = "./tmf-dev2.db"
const SBX_dbname = "./tmf-sbx.db"
const LCL_dbname = "./tmf-lcl.db"
const ISBE_dbname = "./tmf-isbe.db"

const DefaultClonePeriod = 10 * time.Minute

var proConfig = &Config{
	Environment:       DOME_PRO,
	PolicyFileName:    "auth_policies.star",
	BAEProxyDomain:    "dome-marketplace.eu",
	ExternalTMFDomain: "tmf.dome-marketplace.eu",
	VerifierServer:    "https://verifier.dome-marketplace.eu",
	Dbname:            PRO_dbname,
	ClonePeriod:       DefaultClonePeriod,
}

var dev2Config = &Config{
	Environment:       DOME_DEV2,
	PolicyFileName:    "auth_policies.star",
	BAEProxyDomain:    "dome-marketplace-dev2.org",
	ExternalTMFDomain: "tmf.dome-marketplace-dev2.org",
	VerifierServer:    "https://verifier.dome-marketplace-dev2.org",
	Dbname:            DEV2_dbname,
	ClonePeriod:       DefaultClonePeriod,
}

var sbxConfig = &Config{
	Environment:       DOME_SBX,
	PolicyFileName:    "auth_policies.star",
	BAEProxyDomain:    "dome-marketplace-sbx.org",
	ExternalTMFDomain: "tmf.dome-marketplace-sbx.org",
	VerifierServer:    "https://verifier.dome-marketplace-sbx.org",
	Dbname:            SBX_dbname,
	ClonePeriod:       DefaultClonePeriod,
}

var lclConfig = &Config{
	Environment:       DOME_LCL,
	PolicyFileName:    "auth_policies.star",
	BAEProxyDomain:    "dome-marketplace-lcl.org",
	ExternalTMFDomain: "tmf.dome-marketplace-lcl.org",
	VerifierServer:    "https://verifier.dome-marketplace-lcl.org",
	Dbname:            LCL_dbname,
	ClonePeriod:       DefaultClonePeriod,
}

var isbeConfig = &Config{
	Environment:       ISBE,
	PolicyFileName:    "auth_policies.star",
	BAEProxyDomain:    "tmf.mycredential.eu",
	ExternalTMFDomain: "tmf.mycredential.eu",
	VerifierServer:    "https://verifier.dome-marketplace-lcl.org",
	Dbname:            ISBE_dbname,
	ClonePeriod:       DefaultClonePeriod,
}

func DefaultConfig(where Environment, internal bool, usingBAEProxy bool) *Config {
	var conf *Config

	switch where {
	case DOME_PRO:
		conf = proConfig
	case DOME_DEV2:
		conf = dev2Config
	case DOME_SBX:
		conf = sbxConfig
	case DOME_LCL:
		conf = lclConfig
	case ISBE:
		conf = isbeConfig
	default:
		panic("unknown runtime environment")
	}

	conf.internal = internal
	conf.usingBAEProxy = usingBAEProxy
	conf.InitUpstreamHosts(defaultInternalUpstreamHosts)

	conf.resourceToPath = NewResourceToExternalPathPrefix(where)

	return conf
}

func SetLogger(debug bool, nocolor bool) *sqlogger.SQLogHandler {

	logLevel := new(slog.LevelVar)
	if debug {
		logLevel.Set(slog.LevelDebug)
	}

	mylogHandler, err := sqlogger.NewSQLogHandler(&sqlogger.Options{Level: logLevel, NoColor: nocolor})
	if err != nil {
		panic(err)
	}

	logger := slog.New(
		mylogHandler,
	)

	slog.SetDefault(logger)

	return mylogHandler
}

// LoadConfig initializes and returns a Config struct based on the provided parameters.
// It sets up logging, selects the appropriate environment, and applies configuration options.
//
// Parameters:
//   - envir:        The environment to use ("pro", "dev2", "sbx", "lcl").
//   - pdpAddress:   The address of the PDP service.
//   - internal:     Whether to use internal settings.
//   - usingBAEProxy: Whether to use the BAE proxy.
//   - debug:        Enables debug logging if true.
//   - nocolor:      Disables colored log output if true.
//
// Returns:
//   - *Config: The initialized configuration struct.
//   - error:   An error if configuration or logger setup fails.
func LoadConfig(
	envir string,
	pdpAddress string,
	internal bool,
	usingBAEProxy bool,
	debug bool,
	mylogHandler *sqlogger.SQLogHandler,
) (*Config, error) {
	var conf *Config

	var environment Environment

	switch envir {
	case "pro":
		environment = DOME_PRO
		slog.Info("Using the PRODUCTION environment")
	case "dev2":
		environment = DOME_DEV2
		slog.Info("Using the DEV2 environment")
	case "sbx":
		environment = DOME_SBX
		slog.Info("Using the SBX environment")
	case "lcl":
		environment = DOME_LCL
		slog.Info("Using the LCL environment")
	case "isbe":
		environment = ISBE
		slog.Info("Using the ISBE environment")
	default:
		environment = DOME_SBX
		slog.Info("Using the default (SBX) environment")
	}

	conf = DefaultConfig(environment, internal, usingBAEProxy)

	// Set the logger first, so we can log errors during configuration loading
	var logLevel *slog.LevelVar
	if mylogHandler == nil {

		logLevel = new(slog.LevelVar)
		if debug {
			logLevel.Set(slog.LevelDebug)
		}

		mylogHandler, err := sqlogger.NewSQLogHandler(&sqlogger.Options{Level: logLevel})
		if err != nil {
			return nil, errl.Error(err)
		}

		logger := slog.New(
			mylogHandler,
		)

		slog.SetDefault(logger)

	}

	conf.LogHandler = mylogHandler
	conf.LogLevel = logLevel

	conf.PDPAddress = pdpAddress
	conf.Debug = debug

	return conf, nil

}

func (c *Config) InitUpstreamHosts(hosts map[string]string) {
	if c.internalUpstreamPodHosts == nil {
		c.internalUpstreamPodHosts = &sync.Map{}
	} else {
		c.internalUpstreamPodHosts.Clear()
	}

	for resourceName, host := range hosts {
		c.internalUpstreamPodHosts.Store(resourceName, host)
	}
}

// SetUpstreamHost provides a typed method to set a host in the sync.Map
func (c *Config) SetUpstreamHost(resourceName string, host string) {
	c.internalUpstreamPodHosts.Store(resourceName, host)
}

// GetUpstreamHost provides a typed method for the upstream hosts map
func (c *Config) GetUpstreamHost(resourceName string) string {
	v, _ := c.internalUpstreamPodHosts.Load(resourceName)
	if v == nil {
		return ""
	}
	return v.(string)
}

// GetAllUpstreamHosts provides a typed method to retrieve all upstream hosts
func (c *Config) GetAllUpstreamHosts() map[string]string {
	hosts := make(map[string]string)
	c.internalUpstreamPodHosts.Range(func(key, value any) bool {
		hosts[key.(string)] = value.(string)
		return true
	})
	return hosts
}

// GetInternalPodHostFromId retrieves the upstream host for a given ID, depending on the resource type of the ID,
// when the proxy operates inside the DOME instance or not.
func (c *Config) GetInternalPodHostFromId(id string) (string, error) {
	resourceName, err := FromIdToResourceName(id)
	if err != nil {
		return "", errl.Error(err)
	}
	podHost := c.GetUpstreamHost(resourceName)
	if podHost == "" {
		return "", errl.Errorf("no internal pod host found for resource: %s", resourceName)
	}
	return podHost, nil
}

func (c *Config) GetHostAndPathFromResourcename(resourceName string) (string, error) {

	// Inside the DOME instance
	if c.internal {

		internalServiceDomainName := c.GetUpstreamHost(resourceName)
		if internalServiceDomainName == "" {
			return "", errl.Errorf("no internal pod host found for resource: %s", resourceName)
		}

		pathPrefix, ok := c.resourceToPath.GetPathPrefix(resourceName)
		if !ok {
			return "", errl.Errorf("unknown object type: %s", resourceName)
		}

		return "https://" + internalServiceDomainName + pathPrefix, nil

	}

	// Outside the DOME instance
	if c.usingBAEProxy {

		// Each type of object has a different path prefix
		// pathPrefix := defaultBAEResourceToPathPrefix[resourceName]
		pathPrefix := GeneratedDefaultResourceToBAEPathPrefix[resourceName]
		if pathPrefix == "" {
			err := errl.Errorf("unknown resource: %s", resourceName)
			slog.Error(err.Naked().Error())
			return "", err
		}
		// We are accessing the TMForum APIs using the BAE Proxy
		return "https://" + c.BAEProxyDomain + pathPrefix, nil

	} else {

		pathPrefix, ok := c.resourceToPath.GetPathPrefix(resourceName)
		if !ok {
			return "", errl.Errorf("unknown object type: %s", resourceName)
		}

		return "https://" + c.ExternalTMFDomain + pathPrefix, nil

	}

}

// GetHostAndPathFromId returns the TMForum base server path for a given ID.
// If the proxy operates inside the DOME instance, it uses the internal domain names of the pods.
// Otherwise, it uses the DOME host configured in the config (e.g dome-marketplace.eu).
// It returns the URL in the format "https://<domain-name>[:<port>]".
// func (c *Config) GetHostAndPathFromId(id string) (string, error) {

// 	resourceName, err := FromIdToResourceName(id)
// 	if err != nil {
// 		return "", errl.Error(err)
// 	}

// 	// Inside the DOME instance
// 	if c.internal {

// 		internalServiceDomainName := c.GetUpstreamHost(resourceName)
// 		if internalServiceDomainName == "" {
// 			return "", errl.Errorf("no internal pod host found for resource: %s", resourceName)
// 		}

// 		pathPrefix, ok := c.resourceToPath.GetPathPrefix(resourceName)
// 		if !ok {
// 			return "", errl.Errorf("unknown object type: %s", id)
// 		}

// 		return "https://" + internalServiceDomainName + pathPrefix, nil

// 	}

// 	// Outside the DOME instance
// 	if c.usingBAEProxy {

// 		// Each type of object has a different path prefix
// 		pathPrefix := defaultBAEResourceToPathPrefix[resourceName]
// 		if pathPrefix == "" {
// 			err := errl.Errorf("unknown object type: %s", resourceName)
// 			slog.Error(err.Naked().Error())
// 			return "", err
// 		}
// 		// We are accessing the TMForum APIs using the BAE Proxy
// 		return "https://" + c.BAEProxyDomain + pathPrefix, nil

// 	} else {

// 		pathPrefix, ok := c.resourceToPath.GetPathPrefix(id)
// 		if !ok {
// 			return "", errl.Errorf("unknown object type: %s", resourceName)
// 		}

// 		return "https://" + c.ExternalTMFDomain + pathPrefix, nil

// 	}

// }

var defaultBAEResourceToPathPrefix = map[string]string{
	"organization":          "/party/organization/",
	"category":              "/catalog/category/",
	"catalog":               "/catalog/catalog/",
	"productOffering":       "/catalog/productOffering/",
	"productSpecification":  "/catalog/productSpecification/",
	"productOfferingPrice":  "/catalog/productOfferingPrice/",
	"serviceSpecification":  "/service/serviceSpecification/",
	"resourceSpecification": "/resource/resourceSpecification/",
}

// FromIdToResourceName converts an ID in the format "urn:ngsi-ld:product-offering-price:32611feb-6f78-4ccd-a4a2-547cb01cf33d"
// to a resource name like "productOfferingPrice".
// It extracts the resource type from the ID and converts it to camelCase.
// This is the ID format used in DOME for the TMForum APIs.
// It returns an error if the ID format is invalid.
func FromIdToResourceName(id string) (string, error) {
	// id must be like "urn:ngsi-ld:product-offering-price:32611feb-6f78-4ccd-a4a2-547cb01cf33d"
	// We will convert from product-offering-price to productOfferingPrice

	// Extract the different components
	idParts := strings.Split(id, ":")
	if len(idParts) < 4 {
		return "", errl.Errorf("invalid ID format: %s", id)
	}

	if idParts[0] != "urn" || idParts[1] != "ngsi-ld" {
		return "", errl.Errorf("invalid ID format: %s", id)
	}

	words := strings.Split(idParts[2], "-")
	if len(words) == 0 || words[0] == "" {
		return "", errl.Errorf("invalid ID format: %s", id)
	}

	key := words[0]
	for _, part := range words[1:] {
		if len(part) == 0 {
			continue
		}

		rr := []byte(part)

		if 'a' <= rr[0] && rr[0] <= 'z' { // title case is upper case for ASCII
			rr[0] -= 'a' - 'A'
		}

		key += string(rr)

	}

	return key, nil
}

var defaultInternalUpstreamHosts = map[string]string{
	"productCatalogManagement":    "tm-forum-api-product-catalog:8080",
	"party":                       "tm-forum-api-party-catalog:8080",
	"customerBillManagement":      "tm-forum-api-customer-bill-management:8080",
	"customerManagement":          "tm-forum-api-customer-management:8080",
	"productInventory":            "tm-forum-api-product-inventory:8080",
	"productOrderingManagement":   "tm-forum-api-product-ordering-management:8080",
	"resourceCatalog":             "tm-forum-api-resource-catalog:8080",
	"resourceFunctionActivation":  "tm-forum-api-resource-function-activation:8080",
	"resourceInventoryManagement": "tm-forum-api-resource-inventory-management:8080",
	"serviceCatalogManagement":    "tm-forum-api-service-catalog:8080",
	"serviceInventory":            "tm-forum-api-service-inventory:8080",
	"accountManagement":           "tm-forum-api-account-management:8080",
	"agreementManagement":         "tm-forum-api-agreement-management:8080",
	"partyRoleManagement":         "tm-forum-api-party-role-management:8080",
	"usageManagement":             "tm-forum-api-usage-management:8080",
	"quote":                       "tm-forum-api-quote:8080",
}

var defaultResourceToPathPrefix = map[string]string{
	"agreement":                  "/tmf-api/agreementManagement",
	"agreementSpecification":     "/tmf-api/agreementManagement",
	"appliedCustomerBillingRate": "/tmf-api/customerBillManagement",
	"billFormat":                 "/tmf-api/accountManagement",
	"billPresentationMedia":      "/tmf-api/accountManagement",
	"billingAccount":             "/tmf-api/accountManagement",
	"billingCycleSpecification":  "/tmf-api/accountManagement",
	"cancelProductOrder":         "/tmf-api/productOrderingManagement",
	"catalog":                    "/tmf-api/productCatalogManagement",
	"category":                   "/tmf-api/productCatalogManagement",
	"customer":                   "/tmf-api/customerManagement",
	"customerBill":               "/tmf-api/customerBillManagement",
	"customerBillOnDemand":       "/tmf-api/customerBillManagement",
	"financialAccount":           "/tmf-api/accountManagement",
	"heal":                       "/tmf-api/resourceFunctionActivation",
	"individual":                 "/tmf-api/party",
	"migrate":                    "/tmf-api/resourceFunctionActivation",
	"monitor":                    "/tmf-api/resourceFunctionActivation",
	"organization":               "/tmf-api/party",
	"partyAccount":               "/tmf-api/accountManagement",
	"partyRole":                  "/tmf-api/partyRoleManagement",
	"product":                    "/tmf-api/productInventory",
	"productOffering":            "/tmf-api/productCatalogManagement",
	"productOfferingPrice":       "/tmf-api/productCatalogManagement",
	"productOrder":               "/tmf-api/productOrderingManagement",
	"productSpecification":       "/tmf-api/productCatalogManagement",
	"quote":                      "/tmf-api/quoteManagement",
	"resource":                   "/tmf-api/resourceInventoryManagement",
	"resourceCandidate":          "/tmf-api/resourceCatalog",
	"resourceCatalog":            "/tmf-api/resourceCatalog",
	"resourceCategory":           "/tmf-api/resourceCatalog",
	"resourceFunction":           "/tmf-api/resourceFunctionActivation",
	"resourceSpecification":      "/tmf-api/resourceCatalog",
	"scale":                      "/tmf-api/resourceFunctionActivation",
	"service":                    "/tmf-api/serviceInventory",
	"serviceCandidate":           "/tmf-api/serviceCatalogManagement",
	"serviceCatalog":             "/tmf-api/serviceCatalogManagement",
	"serviceCategory":            "/tmf-api/serviceCatalogManagement",
	"serviceSpecification":       "/tmf-api/serviceCatalogManagement",
	"settlementAccount":          "/tmf-api/accountManagement",
	"usage":                      "/tmf-api/usageManagement",
	"usageSpecification":         "/tmf-api/usageManagement",
}

// ResourceToExternalPathPrefix is a thread-safe structure that maps TMF resources to their external path prefixes.
// We use a sync.Map because of very frequent reads and seldom writes, so it is more efficient than a regular map with a mutex.
type ResourceToExternalPathPrefix struct {
	externalResourceMap sync.Map
	apiVersion          string // The API version for the TMF APIs, e.g. "v4".
}

func NewResourceToExternalPathPrefix(environment Environment) *ResourceToExternalPathPrefix {
	r := &ResourceToExternalPathPrefix{}
	if environment == ISBE {
		r.apiVersion = "v5" // ISBE uses v5 of the TMF APIs
	} else {
		r.apiVersion = "v4"
	}

	for resource, pathPrefix := range defaultResourceToPathPrefix {
		fullPathPrefix := pathPrefix + "/" + r.apiVersion + "/" + resource
		r.externalResourceMap.Store(resource, fullPathPrefix)
	}

	return r
}

func (r *ResourceToExternalPathPrefix) GetPathPrefix(resourceName string) (string, bool) {
	pathPrefix, ok := r.externalResourceMap.Load(resourceName)
	if !ok {
		return "", false
	}

	if pathPrefixStr, ok := pathPrefix.(string); ok {
		return pathPrefixStr, true
	}

	return "", false
}

func (r *ResourceToExternalPathPrefix) UpdateAllPathPrefixes(newPrefixes map[string]string) {
	// Delete all existing entries
	r.externalResourceMap.Clear()

	for resource, newPathPrefix := range newPrefixes {
		r.externalResourceMap.Store(resource, newPathPrefix)
	}
}

func (r *ResourceToExternalPathPrefix) GetAllPathPrefixes() map[string]string {
	allPrefixes := make(map[string]string)

	// Iterate over the resourceMap and collect all resource-pathPrefix pairs
	// Note: this is a simple iteration, not thread-safe, but it is expected to be called
	// in a context where no other goroutine is modifying the map.
	// This is ok, because only the administator can do this operation.
	r.externalResourceMap.Range(func(key, value any) bool {
		if resource, ok := key.(string); ok {
			if pathPrefix, ok := value.(string); ok {
				allPrefixes[resource] = pathPrefix
			}
		}
		return true // continue iteration
	})
	return allPrefixes
}

var StandardPrefixToBAEPrefix = map[string]string{
	"/tmf-api/productCatalogManagement/v4":    "catalog",
	"/tmf-api/productInventory/v4":            "inventory",
	"/tmf-api/productOrderingManagement/v4":   "ordering",
	"/tmf-api/accountManagement/v4":           "billing",
	"/tmf-api/usageManagement/v4":             "usage",
	"/tmf-api/party/v4":                       "party",
	"/tmf-api/customerManagement/v4":          "customer",
	"/tmf-api/resourceCatalog/v4":             "resources",
	"/tmf-api/serviceCatalogManagement/v4":    "services",
	"/tmf-api/resourceInventoryManagement/v4": "resourceInventory",
}

// The names of some special objects in the DOME ecosystem
const ProductOffering = "productOffering"
const ProductSpecification = "productSpecification"
const ProductOfferingPrice = "productOfferingPrice"
const ServiceSpecification = "serviceSpecification"
const ResourceSpecification = "resourceSpecification"
const Category = "category"
const Catalog = "catalog"
const Organization = "organization"
const Individual = "individual"

var RootBAEObjects = []string{
	"organization",
	"productOffering",
	"productSpecification",
	"productOfferingPrice",
	"individual",
	"category",
	"catalog",
	"serviceSpecification",
	"resourceSpecification",
	"productOrder",
	"product",
}

const SchemaLocationRelatedParty = "https://raw.githubusercontent.com/DOME-Marketplace/dome-odrl-profile/refs/heads/main/schemas/simplified/RelatedPartyRef.schema.json"
