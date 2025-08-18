package tmfcache

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hesusruiz/domeproxy/internal/errl"
	"github.com/hesusruiz/domeproxy/internal/jpath"
	"gitlab.com/greyxor/slogor"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func (tmf *TMFCache) LocalProductOfferings(dbconn *sqlite.Conn, tmfResource string, visitedObjects map[string]bool, visitedStack Stack) (pos []TMFObject, s Stack, err error) {
	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, nil, errl.Error(fmt.Errorf("taking db connection: %w", err))
		}
		defer tmf.dbpool.Put(dbconn)
	}

	selectSQL := fmt.Sprintf("SELECT * from tmfobject where resource = '%s';", tmfResource)

	var resultPOs []TMFObject

	err = sqlitex.Execute(dbconn, selectSQL, &sqlitex.ExecOptions{

		// This function is called once for each record found in the database
		ResultFunc: func(stmt *sqlite.Stmt) error {

			dbObject, err := objectFromDbRecord(stmt)
			if err != nil {
				return errl.Error(err)
			}

			resultPOs = append(resultPOs, dbObject)

			return nil
		},
	})
	if err != nil {
		return nil, nil, errl.Error(err)
	}

	visitedStack = tmf.visitLocalMap(dbconn, resultPOs[0].GetContentAsMap(), 0, visitedObjects, visitedStack)

	return resultPOs, visitedStack, nil
}

func (tmf *TMFCache) visitLocalMap(
	dbconn *sqlite.Conn,
	currentObject map[string]any,
	indent int,
	visitedObjects map[string]bool,
	visitedStack Stack,
) Stack {

	// A map object can contain an 'href' field that points to another object.
	// In this case we retrieve and visit the object, if it was not retrieved before.
	// For other map objects we print the relevant fields if enabled by the tmf.Dump variable.
	href, _ := currentObject["href"].(string)
	if len(href) == 0 || visitedObjects[href] {
		return visitedStack
	}
	resource, _ := currentObject["resource"].(string)

	for k, v := range currentObject {
		switch v := v.(type) {

		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%s:\n", indentStr(indent), k)
			}
			visitedStack = tmf.visitLocalMap(dbconn, v, indent+3, visitedObjects, visitedStack)

		case []any:
			if tmf.Dump {
				fmt.Printf("%s%s: [\n", indentStr(indent), k)
			}
			visitedStack = tmf.visitLocalArray(dbconn, v, indent+3, visitedObjects, visitedStack)
			if tmf.Dump {
				fmt.Printf("%s]\n", indentStr(indent))
			}

		case string:
			if k == "href" {
				// if tmf.Dump {
				// 	fmt.Printf("%sRetrieving %s\n", indentStr(indent), v)
				// }
				// localObject, _, err := tmf.LocalRetrieveTMFObject(dbconn, href, "")
				// if err != nil {
				// 	slog.Error("retrieving local object", "href", href, slogor.Err(err))
				// 	continue
				// }
				// if tmf.Dump {
				// 	fmt.Printf("%sRetrieved %s: %s\n", indentStr(indent), v, localObject.GetID())
				// }

				// visitedStack = tmf.visitLocalMap(dbconn, localObject.GetContentAsMap(), indent+3, visitedObjects, visitedStack)
			}

		}
	}

	if tmf.Dump {
		fmt.Printf("%sRetrieving %s\n", indentStr(indent), href)
	}
	localObject, _, err := tmf.LocalRetrieveTMFObject(dbconn, href, resource, "")
	if err != nil {
		slog.Error("retrieving local object", "href", href, slogor.Err(err))
		return visitedStack
	}
	if tmf.Dump {
		fmt.Printf("%sRetrieved OK: %s\n", indentStr(indent), localObject.GetID())
	}

	visitedObjects[href] = true

	visitedStack = append(visitedStack, stackEntry{OrigHref: href, DestHref: href})
	visitedStack = tmf.visitLocalMap(dbconn, localObject.GetContentAsMap(), indent+3, visitedObjects, visitedStack)

	return visitedStack
}

func (tmf *TMFCache) visitLocalArray(
	dbconn *sqlite.Conn,
	arr []any,
	indent int,
	visitedObjects map[string]bool,
	visitedStack Stack,
) Stack {
	for i, v := range arr {
		switch v := v.(type) {
		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			visitedStack = tmf.visitLocalMap(dbconn, v, indent+3, visitedObjects, visitedStack)
		case []any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			visitedStack = tmf.visitLocalArray(dbconn, v, indent+3, visitedObjects, visitedStack)

		}
	}

	return visitedStack
}

func (tmf *TMFCache) VisitRemoteObject(
	dbconn *sqlite.Conn,
	id string,
	resource string,
	visitedObjects map[string]bool,
	visitedStack Stack,
) (object TMFObject, vs Stack, err error) {

	if dbconn == nil {
		var err error
		dbconn, err = tmf.dbpool.Take(context.Background())
		if err != nil {
			return nil, nil, errl.Error(fmt.Errorf("taking db connection: %w", err))
		}
		defer tmf.dbpool.Put(dbconn)
	}

	// With RetrieveOrUpdateObject, we go to the remote server only if the object is not in the local cache and
	// is not fresh enough
	tmfObject, _, err := tmf.RetrieveOrUpdateObject(dbconn, id, resource, "", "", "", LocalOrRemote)
	if err != nil {
		return nil, nil, errl.Error(err)
	}

	// Recursively retrieve and save the sub-objects of this ProductOffering.
	// We pass the owner information so those objects can include it with them.
	visitedStack = tmf.visitMapStack(dbconn, tmfObject.GetContentAsMap(), 3, visitedObjects, visitedStack)

	return tmfObject, visitedStack, nil

}

type stackEntry struct {
	OrigHref string
	DestHref string
}

type Stack []stackEntry

// visitMapStack visits recursively the descendants of an object (representaed as a map).
// It handles cicles to avoid infinite loops.
func (tmf *TMFCache) visitMapStack(
	dbconn *sqlite.Conn,
	currentObject map[string]any,
	indent int,
	visitedObjects map[string]bool,
	visitedStack Stack,
) Stack {

	// A map object can contain an 'href' field that points to another object.
	// In this case we retrieve and visit the object, if it was not retrieved before.
	// For other map objects we print the relevant fields if enabled by the tmf.Dump variable.
	href, _ := currentObject["href"].(string)
	if len(href) > 0 {
		if tmf.Dump {
			fmt.Printf("%shref: %v\n", indentStr(indent), href)
		}
		resource := jpath.GetString(currentObject, "resourceType")
		if !visitedObjects[href] {
			visitedObjects[href] = true
			remoteObj, _, err := tmf.RetrieveOrUpdateObject(dbconn, href, resource, "", "", "", LocalOrRemote)
			if err != nil {
				slog.Error(err.Error())
			} else {
				visitedStack = append(visitedStack, stackEntry{OrigHref: href, DestHref: remoteObj.GetID()})
				visitedStack = tmf.visitMapStack(dbconn, remoteObj.GetContentAsMap(), indent+3, visitedObjects, visitedStack)
			}
		}
	}

	for k, v := range currentObject {
		switch v := v.(type) {

		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%s:\n", indentStr(indent), k)
			}
			visitedStack = tmf.visitMapStack(dbconn, v, indent+3, visitedObjects, visitedStack)

		case []any:
			if tmf.Dump {
				fmt.Printf("%s%s: [\n", indentStr(indent), k)
			}
			visitedStack = tmf.visitArrayStack(dbconn, v, indent+3, visitedObjects, visitedStack)
			if tmf.Dump {
				fmt.Printf("%s]\n", indentStr(indent))
			}

		}
	}

	return visitedStack
}

// visitArray is the complement to visitMap for recursive traversal of a TMForum object graph
func (tmf *TMFCache) visitArrayStack(
	dbconn *sqlite.Conn,
	arr []any,
	indent int,
	visitedObjects map[string]bool,
	visitedStack Stack,
) Stack {
	for i, v := range arr {
		switch v := v.(type) {
		case map[string]any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			visitedStack = tmf.visitMapStack(dbconn, v, indent+3, visitedObjects, visitedStack)
		case []any:
			if tmf.Dump {
				fmt.Printf("%s%d:\n", indentStr(indent), i)
			}
			visitedStack = tmf.visitArrayStack(dbconn, v, indent+3, visitedObjects, visitedStack)

		}
	}

	return visitedStack
}
