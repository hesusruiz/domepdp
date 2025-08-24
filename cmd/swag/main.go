package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/hesusruiz/domepdp/config"
	"github.com/hesusruiz/domepdp/internal/jpath"
	"golang.org/x/tools/imports"
)

// This is a simple tool to process the Swagger files in the "swagger" directory
// and extract the mapping of last path part to management system and the routes.
// It assumes the Swagger files are in the format used by the TMForum APIs.
// It will print the mapping and the routes to the standard output in JSON format.

//go:embed routes.hbs
var routesTemplate string

func main() {

	// Visit recursively the directories in the "swagger" directory
	// It assumes an "almost" flat structure with directories named after the management system
	// and one file inside each directory named "api.json" or similar.
	baseDir := "./swagger"
	directories, err := os.ReadDir(baseDir)
	if err != nil {
		panic(err)
	}

	mapping := map[string]string{}
	resourceToStdPath := map[string]string{}
	prefixes := map[string]string{}
	baeProxyRoutes := map[string]string{}

	for _, dir := range directories {
		if dir.IsDir() {
			// Visit the directory
			dirPath := baseDir + "/" + dir.Name()
			files, err := os.ReadDir(dirPath)
			if err != nil {
				panic(err)
			}
			for _, file := range files {
				if !file.IsDir() {
					// Process the file
					filePath := dirPath + "/" + file.Name()
					processOneFile(filePath, mapping, resourceToStdPath, baeProxyRoutes, prefixes)
				}
			}
		}
	}

	tmpl, err := template.New("routes").Parse(routesTemplate)
	if err != nil {
		panic(err)
	}

	var b bytes.Buffer
	err = tmpl.Execute(&b, map[string]any{
		"ResourceToStandardPath": resourceToStdPath,
		"BAEProxyRoutes":         baeProxyRoutes,
	})
	if err != nil {
		panic(err)
	}

	out, err := imports.Process("config/routes.go", b.Bytes(), nil)
	if err != nil {
		panic(err)
	}

	// opts := format.Options{
	// 	ModulePath: "github.com/hesusruiz/domepdp",
	// }
	// out, err = format.Source(out, opts)
	// if err != nil {
	// 	panic(err)
	// }

	err = os.WriteFile("./config/routes.go", out, 0644)
	if err != nil {
		panic(err)
	}

}

func processOneFile(filePath string, mapping map[string]string, resourceToStdPath map[string]string, baeProxyRoutes map[string]string, prefixes map[string]string) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	var oMap map[string]any
	err = json.Unmarshal(content, &oMap)
	if err != nil {
		panic(err)
	}

	// Get the base path as specified in the swagger file
	basePath, ok := oMap["basePath"].(string)
	if !ok {
		panic("basePath key not found or not a string")
	}

	description := jpath.GetString(oMap, "info.description")
	if len(description) == 0 {
		panic("description key not found or not a string")
	}

	basePathTrimmed := strings.TrimRight(basePath, "/")

	basePathParts := strings.Split(strings.TrimLeft(basePathTrimmed, "/"), "/")
	if len(basePathParts) != 3 {
		panic("basePath does not contain exactly 3 parts")
	}

	// apiPrefix := basePathParts[0]
	managementSystem := basePathParts[1]
	// version := basePathParts[2]

	// Get the "paths" key from the map
	paths, ok := oMap["paths"].(map[string]any)
	if !ok {
		panic("paths key not found or not a map")
	}

	localResourceNames := map[string]bool{}

	// Iterate over the keys in the "paths" map
	for path := range paths {
		// Check if the value is a map
		// methodsMap, ok := methods.(map[string]any)
		// if !ok {
		// 	panic("methods value is not a map")
		// }

		path = strings.Trim(path, "/")

		pathParts := strings.Split(path, "/")
		firstPart := pathParts[0]
		resourceName := pathParts[len(pathParts)-1]

		// Eliminate the placeholder, if the last part is a placeholder
		if strings.HasPrefix(resourceName, "{") && strings.HasSuffix(resourceName, "}") {
			// Set the lastPart to the previous part
			resourceName = pathParts[len(pathParts)-2]
		}

		if firstPart == "importJob" || firstPart == "exportJob" {
			// We do not implement these APIs
			continue
		}

		if firstPart == "hub" || firstPart == "listener" {
			// TODO: implement specia processing for these paths
			continue
		}

		localResourceNames[resourceName] = true
		mapping[resourceName] = managementSystem
		// resourceToStdPath[resourceName] = "/" + apiPrefix + "/" + managementSystem + "/" + version + "/" + resourceName
		resourceToStdPath[resourceName] = basePathTrimmed + "/" + resourceName

		baePref := config.StandardPrefixToBAEPrefix[basePathTrimmed]
		if baePref != "" {
			prefixes[resourceName] = baePref
			baeProxyRoutes[resourceName] = "/" + baePref + "/" + resourceName
		}

		// fmt.Printf("(%s) %s -> %s\n", firstPart, lastPart, managementSystem)
	}

	// fmt.Println(description)
	fmt.Printf("- **%s**: %s\n", managementSystem, baeManagementToDescription[managementSystem])

	for resourceName := range localResourceNames {
		fmt.Printf("  - %s\n", resourceName)
	}

	fmt.Println()

}

var baeManagementToDescription map[string]string = map[string]string{
	"accountManagement":           "TMF666 - Account Management",
	"agreementManagement":         "TMF651 - Agreement Management",
	"customerBillManagement":      "TMF678 - Customer Bill Management",
	"customerManagement":          "TMF629 - Customer Management",
	"party":                       "TMF632 - Party Management",
	"partyRoleManagement":         "TMF669 - Party Role Management",
	"productCatalogManagement":    "TMF620 - Product Catalog Management",
	"productInventory":            "TMF637 - Product Inventory Management",
	"productOrderingManagement":   "TMF622 - Product Ordering Management",
	"quoteManagement":             "TMF648 - Quote Management",
	"resourceCatalog":             "TMF634 - Resource Catalog Management",
	"resourceFunctionActivation":  "TMF664 - Resource Function Activation and Configuration",
	"resourceInventoryManagement": "TMF639 - Resource Inventory Management",
	"serviceCatalogManagement":    "TMF633 - Service Catalog Management",
	"serviceInventory":            "TMF638 - Service Inventory Management",
	"usageManagement":             "TMF635 - Usage Management",
	"resourceOrderingManagement":  "TMF652 - Resource Ordering Management",
	"serviceOrdering":             "TMF641 - Service Ordering Management",
}
