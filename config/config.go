// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/hesusruiz/domeproxy/internal/sqlogger"
)

type Environment int

const DOME_PRO Environment = 0
const DOME_DEV2 Environment = 1
const DOME_SBX Environment = 2
const DOME_LCL Environment = 3

const PRO_dbname = "./tmf.db"
const DEV2_dbname = "./tmf-dev2.db"
const SBX_dbname = "./tmf-sbx.db"
const LCL_dbname = "./tmf-lcl.db"

const DefaultClonePeriod = 10 * time.Minute

type Config struct {
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
	default:
		panic("unknown DOME environment")
	}

	conf.internal = internal
	conf.usingBAEProxy = usingBAEProxy
	conf.InitUpstreamHosts(defaultInternalUpstreamHosts)

	conf.resourceToPath = NewResourceToExternalPathPrefix()

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

func LoadConfig(
	envir string,
	pdpAddress string,
	internal bool,
	usingBAEProxy bool,
	debug bool,
	nocolor bool,
) (*Config, error) {
	var conf *Config

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
	default:
		environment = DOME_SBX
		slog.Info("Using the default (SBX) environment")
	}

	conf = DefaultConfig(environment, internal, usingBAEProxy)
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
	return
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
	c.internalUpstreamPodHosts.Range(func(key, value interface{}) bool {
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
		return "", err
	}
	podHost := c.GetUpstreamHost(resourceName)
	if podHost == "" {
		return "", fmt.Errorf("no internal pod host found for resource: %s", resourceName)
	}
	return podHost, nil
}

func (c *Config) GetHostAndPathFromResourcename(resourceName string) (string, error) {

	// Inside the DOME instance
	if c.internal {

		internalServiceDomainName := c.GetUpstreamHost(resourceName)
		if internalServiceDomainName == "" {
			return "", fmt.Errorf("no internal pod host found for resource: %s", resourceName)
		}

		pathPrefix, ok := c.resourceToPath.GetPathPrefix(resourceName)
		if !ok {
			return "", fmt.Errorf("unknown object type: %s", resourceName)
		}

		return "https://" + internalServiceDomainName + pathPrefix, nil

	}

	// Outside the DOME instance
	if c.usingBAEProxy {

		// Each type of object has a different path prefix
		// pathPrefix := defaultBAEResourceToPathPrefix[resourceName]
		pathPrefix := GeneratedDefaultResourceToBAEPathPrefix[resourceName]
		if pathPrefix == "" {
			slog.Error("unknown object type", "type", resourceName)
			return "", fmt.Errorf("unknown object type: %s", resourceName)
		}
		// We are accessing the TMForum APIs using the BAE Proxy
		return "https://" + c.BAEProxyDomain + pathPrefix, nil

	} else {

		pathPrefix, ok := c.resourceToPath.GetPathPrefix(resourceName)
		if !ok {
			return "", fmt.Errorf("unknown object type: %s", resourceName)
		}

		return "https://" + c.ExternalTMFDomain + pathPrefix, nil

	}

}

// GetHostAndPathFromId returns the TMForum base server path for a given ID.
// If the proxy operates inside the DOME instance, it uses the internal domain names of the pods.
// Otherwise, it uses the DOME host configured in the config (e.g dome-marketplace.eu).
// It returns the URL in the format "https://<domain-name>[:<port>]".
func (c *Config) GetHostAndPathFromId(id string) (string, error) {

	resourceName, err := FromIdToResourceName(id)
	if err != nil {
		return "", err
	}

	// Inside the DOME instance
	if c.internal {

		internalServiceDomainName := c.GetUpstreamHost(resourceName)
		if internalServiceDomainName == "" {
			return "", fmt.Errorf("no internal pod host found for resource: %s", resourceName)
		}

		pathPrefix, ok := c.resourceToPath.GetPathPrefix(resourceName)
		if !ok {
			return "", fmt.Errorf("unknown object type: %s", id)
		}

		return "https://" + internalServiceDomainName + pathPrefix, nil

	}

	// Outside the DOME instance
	if c.usingBAEProxy {

		// Each type of object has a different path prefix
		pathPrefix := defaultBAEResourceToPathPrefix[resourceName]
		if pathPrefix == "" {
			slog.Error("unknown object type", "type", resourceName)
			return "", fmt.Errorf("unknown object type: %s", resourceName)
		}
		// We are accessing the TMForum APIs using the BAE Proxy
		return "https://" + c.BAEProxyDomain + pathPrefix, nil

	} else {

		pathPrefix, ok := c.resourceToPath.GetPathPrefix(id)
		if !ok {
			return "", fmt.Errorf("unknown object type: %s", resourceName)
		}

		return "https://" + c.ExternalTMFDomain + pathPrefix, nil

	}

}

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
		return "", fmt.Errorf("invalid ID format: %s", id)
	}

	if idParts[0] != "urn" || idParts[1] != "ngsi-ld" {
		return "", fmt.Errorf("invalid ID format: %s", id)
	}

	words := strings.Split(idParts[2], "-")
	if len(words) == 0 || words[0] == "" {
		return "", fmt.Errorf("invalid ID format: %s", id)
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
	"agreement":                  "/tmf-api/agreementManagement/v4/agreement",
	"agreementSpecification":     "/tmf-api/agreementManagement/v4/agreementSpecification",
	"appliedCustomerBillingRate": "/tmf-api/customerBillManagement/v4/appliedCustomerBillingRate",
	"billFormat":                 "/tmf-api/accountManagement/v4/billFormat",
	"billPresentationMedia":      "/tmf-api/accountManagement/v4/billPresentationMedia",
	"billingAccount":             "/tmf-api/accountManagement/v4/billingAccount",
	"billingCycleSpecification":  "/tmf-api/accountManagement/v4/billingCycleSpecification",
	"cancelProductOrder":         "/tmf-api/productOrderingManagement/v4/cancelProductOrder",
	"catalog":                    "/tmf-api/productCatalogManagement/v4/catalog",
	"category":                   "/tmf-api/productCatalogManagement/v4/category",
	"customer":                   "/tmf-api/customerManagement/v4/customer",
	"customerBill":               "/tmf-api/customerBillManagement/v4/customerBill",
	"customerBillOnDemand":       "/tmf-api/customerBillManagement/v4/customerBillOnDemand",
	"financialAccount":           "/tmf-api/accountManagement/v4/financialAccount",
	"heal":                       "/tmf-api/resourceFunctionActivation/v4/heal",
	"individual":                 "/tmf-api/party/v4/individual",
	"migrate":                    "/tmf-api/resourceFunctionActivation/v4/migrate",
	"monitor":                    "/tmf-api/resourceFunctionActivation/v4/monitor",
	"organization":               "/tmf-api/party/v4/organization",
	"partyAccount":               "/tmf-api/accountManagement/v4/partyAccount",
	"partyRole":                  "/tmf-api/partyRoleManagement/v4/partyRole",
	"product":                    "/tmf-api/productInventory/v4/product",
	"productOffering":            "/tmf-api/productCatalogManagement/v4/productOffering",
	"productOfferingPrice":       "/tmf-api/productCatalogManagement/v4/productOfferingPrice",
	"productOrder":               "/tmf-api/productOrderingManagement/v4/productOrder",
	"productSpecification":       "/tmf-api/productCatalogManagement/v4/productSpecification",
	"quote":                      "/tmf-api/quoteManagement/v4/quote",
	"resource":                   "/tmf-api/resourceInventoryManagement/v4/resource",
	"resourceCandidate":          "/tmf-api/resourceCatalog/v4/resourceCandidate",
	"resourceCatalog":            "/tmf-api/resourceCatalog/v4/resourceCatalog",
	"resourceCategory":           "/tmf-api/resourceCatalog/v4/resourceCategory",
	"resourceFunction":           "/tmf-api/resourceFunctionActivation/v4/resourceFunction",
	"resourceSpecification":      "/tmf-api/resourceCatalog/v4/resourceSpecification",
	"scale":                      "/tmf-api/resourceFunctionActivation/v4/scale",
	"service":                    "/tmf-api/serviceInventory/v4/service",
	"serviceCandidate":           "/tmf-api/serviceCatalogManagement/v4/serviceCandidate",
	"serviceCatalog":             "/tmf-api/serviceCatalogManagement/v4/serviceCatalog",
	"serviceCategory":            "/tmf-api/serviceCatalogManagement/v4/serviceCategory",
	"serviceSpecification":       "/tmf-api/serviceCatalogManagement/v4/serviceSpecification",
	"settlementAccount":          "/tmf-api/accountManagement/v4/settlementAccount",
	"usage":                      "/tmf-api/usageManagement/v4/usage",
	"usageSpecification":         "/tmf-api/usageManagement/v4/usageSpecification",
}

// ResourceToExternalPathPrefix is a thread-safe structure that maps TMF resources to their external path prefixes.
// We use a sync.Map because of very frequent reads and seldom writes, so it is more efficient than a regular map with a mutex.
type ResourceToExternalPathPrefix struct {
	externalResourceMap sync.Map
}

func NewResourceToExternalPathPrefix() *ResourceToExternalPathPrefix {
	r := &ResourceToExternalPathPrefix{}

	for resource, pathPrefix := range defaultResourceToPathPrefix {
		r.externalResourceMap.Store(resource, pathPrefix)
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
	// This is ok, because only the administator ca ndo this operation.
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
