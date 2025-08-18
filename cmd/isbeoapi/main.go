package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/goccy/go-yaml"
	"github.com/hesusruiz/domeproxy/internal/jpath"
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
	baseDir := "./oapiv5"

	managementToUpstream := map[string]string{}
	resourceToManagement := map[string]string{}
	resourceToPath := map[string]string{}

	dirEntries, err := os.ReadDir(baseDir)
	if err != nil {
		panic(err)
	}
	for _, dirEntry := range dirEntries {
		if !dirEntry.IsDir() {
			// Process the file
			filePath := path.Join(baseDir, dirEntry.Name())
			if !strings.HasSuffix(filePath, ".yaml") {
				// Skip non-JSON files
				continue
			}
			processOneFile(filePath, managementToUpstream, resourceToManagement, resourceToPath)
		}
	}

	tmpl, err := template.New("routes").Parse(routesTemplate)
	if err != nil {
		panic(err)
	}

	var b bytes.Buffer
	err = tmpl.Execute(&b, map[string]any{
		"ResourceToManagement":   resourceToManagement,
		"ResourceToStandardPath": resourceToPath,
		"ManagementToUpstream":   managementToUpstream,
	})
	if err != nil {
		panic(err)
	}

	out, err := imports.Process("config/routes_isbe.go", b.Bytes(), nil)
	if err != nil {
		panic(err)
	}

	err = os.WriteFile("./config/routes_isbe.go", out, 0644)
	if err != nil {
		panic(err)
	}

}

func processOneFile(filePath string, managementToUpstream map[string]string, resourceToManagement map[string]string, resourceToPath map[string]string) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	var oMap map[string]any
	err = yaml.Unmarshal(content, &oMap)
	if err != nil {
		panic(err)
	}

	title := jpath.GetString(oMap, "info.title")
	if len(title) == 0 {
		panic("info.title key not found or empty")
	}

	serverURL := jpath.GetString(oMap, "servers.0.url")
	if len(serverURL) == 0 {
		panic("servers key not found or empty")
	}

	basePath := strings.TrimPrefix(serverURL, "https://serverRoot/")
	if len(basePath) == 0 {
		panic("server URL does not contain a valid API prefix")
	}

	basePathTrimmed := strings.TrimRight(basePath, "/")

	basePathParts := strings.Split(basePathTrimmed, "/")
	if len(basePathParts) < 2 {
		panic("basePath does not contain at least 2 parts")
	}

	managementSystem := basePathParts[0]
	fmt.Println("Management system", managementSystem)
	managementToUpstream[managementSystem] = "TODO: set upstream host and path like http://localhost:8062"

	// Get the "paths" key from the map
	paths := jpath.GetMap(oMap, "paths")

	localResourceNames := map[string]bool{}

	// Iterate over the keys in the "paths" map
	for thePath := range paths {
		// Check if the value is a map
		// methodsMap, ok := methods.(map[string]any)
		// if !ok {
		// 	panic("methods value is not a map")
		// }

		thePath = strings.Trim(thePath, "/")

		pathParts := strings.Split(thePath, "/")
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
		resourceToManagement[resourceName] = managementSystem
		resourceToPath[resourceName] = path.Join("/tmf-api", basePath, resourceName)

	}

	// fmt.Println(description)
	fmt.Printf("- **%s**\n", managementSystem)

	for resourceName := range localResourceNames {
		fmt.Printf("  - %s\n", resourceName)
	}

	fmt.Println()

}
