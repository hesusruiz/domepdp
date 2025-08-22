// Copyright 2023 Jesus Ruiz. All rights reserved.
// Use of this source code is governed by an Apache 2.0
// license that can be found in the LICENSE file.

package config

var GeneratedISBEManagementToUpstream = map[string]string{
	"partyManagement":          "TODO: set upstream host and path like http://localhost:8620",
	"productCatalogManagement": "http://localhost:8620",
}

var GeneratedISBEResourceToManagement = map[string]string{
	"category":             "productCatalogManagement",
	"individual":           "partyManagement",
	"organization":         "partyManagement",
	"productCatalog":       "productCatalogManagement",
	"productOffering":      "productCatalogManagement",
	"productOfferingPrice": "productCatalogManagement",
	"productSpecification": "productCatalogManagement",
}

var GeneratedISBEResourceToPathPrefix = map[string]string{
	"category":             "/tmf-api/productCatalogManagement/v5/category",
	"individual":           "/tmf-api/partyManagement/v5/individual",
	"organization":         "/tmf-api/partyManagement/v5/organization",
	"productCatalog":       "/tmf-api/productCatalogManagement/v5/productCatalog",
	"productOffering":      "/tmf-api/productCatalogManagement/v5/productOffering",
	"productOfferingPrice": "/tmf-api/productCatalogManagement/v5/productOfferingPrice",
	"productSpecification": "/tmf-api/productCatalogManagement/v5/productSpecification",
}
