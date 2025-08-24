# Gemini Agent Instructions for Go TM Forum REST API server implementation

This project is a RESTful API server built with Go. Follow these guidelines for all new and existing code:

- **Objective:** This repo includes an existing working implementation of the API server. What we are going to do is to write from scratch a new implementation, without modifying the existing one. That is, the repo will have both implementations, which will coexist until I am happy with the new version. That means that the directory structure for the new version must use new directories or subdirectories. If the existing code is good enough, we will reuse it and it can be imported from the new code. In some cases, the existing code could be extended or modified to support the new version of the server, without breaking the existing version of the server.

- **OpenAPI definitions:** The OpenAPI definition files are in the `oapiv5/` directory. There is one YAML file per each family of REST APIs. The server implements in a sinble binary all the APIs combined. 

- **Database:** We use SQLite (with the `mattn/go-sqlite3` driver) with the `sqlx` library. Avoid direct database calls from handlers.

- **Database Table design:** For storage of the TMForum objects, we will use just one table, with one JSON field to store the whole object, and other fields for metadata and particular fields of the JSON object which may be interesting to simplify the SQL queries. The inspiration for the table design can be the existing code in `tmfcache\tables.go`.

- **In-memory representation of the TMF Objects:** To enable flexibility in supporting many TMF object types with the same code base, and consistent with the one-table design, the in-memory representation of TMF objects must be around a map[string]any nested structure, with methods to query and manipulate the structure in a type safe way. The objective must be to be able to support most TMForum APIs with the same code base, because all the objects share most properties. In the small number of cases where the object behaviour is different, this should be implemented with methods.

- **Error Handling:** Use the standard Go `errors` package. When returning an error to the client, ensure it's a well-defined API error type from the `pkg/apierror` package, not a raw database error. Also, to include error location information, use the simple wrapper in existing code in `internal/errl/errl.go`.

- **Testing:** All new functionality requires unit tests in a `_test.go` file in the same directory. Use the `testify` suite for assertions.

- **Code Style:** Follow the standard Go formatting (`gofmt`). All functions should have clear, concise doc comments explaining their purpose, parameters, and return values.

- **File Structure:** Adhere to the following directory structure:
  - `cmd/tmfpdp/`: Main entry points for the application.
  - `tmfserver/`: Application code (handlers, repositories, services).
  - `pkg/`: Reusable, public packages (e.g., API error types, shared utilities).