# ISBEServer Architecture Documentation

This document outlines the architecture of the `isbeserver`, a Go-based REST API server designed to implement TM Forum APIs. It details the key components, design strategies, and relevant considerations from its development.

## 1. Overview and Purpose

The `isbeserver` is a new implementation of a TM Forum REST API server, designed to coexist with an existing implementation within the same repository. This strategy allows for a gradual transition and validation of the new version without disrupting the current operational server. The server aims to provide a flexible and robust platform for handling various TM Forum API specifications.

## 2. Core Components and Structure

The server's architecture is modular, separating concerns into distinct packages and layers:

*   **`cmd/isbeserver/main.go`**: The main entry point for the application, responsible for initializing and starting the server.
*   **`tmfserver/common/`**: This package houses shared utilities and common data structures used across different parts of the server.
    *   `callerinfo.go`: Likely handles extraction or processing of caller-related information.
    *   `common.go`: Contains general-purpose utilities and shared definitions.
    *   `jwt.go`: Manages JSON Web Token (JWT) related functionalities, including parsing and, as recently refactored, extracting tokens from authorization headers.
    *   `takedecision.go`: (Inferred) Likely contains logic related to decision-making processes, possibly for policy enforcement or routing.
*   **`tmfserver/handler/`**: This layer defines the API handlers, abstracting the underlying web framework.
    *   `tmfserver/handler/echo/`: Contains handlers implemented using the Echo web framework.
    *   `tmfserver/handler/fiber/`: Contains handlers implemented using the Fiber web framework.
    *   This dual-framework support demonstrates a pluggable handler design, allowing flexibility in choosing or switching web frameworks.
*   **`tmfserver/repository/`**: This layer is responsible for data persistence and interaction with the database.
    *   `tables.go`: Defines the database table structures.
    *   `tmfobject.go`: Provides methods for interacting with the database to store and retrieve TM Forum objects.
*   **`tmfserver/service/service.go`**: The service layer encapsulates the business logic. It orchestrates operations by interacting with the repository layer and providing an interface for the handlers. This separation ensures that business rules are independent of the web framework or database implementation details.
*   **`tmfserver/www/`**: This directory serves static assets, primarily for the Swagger UI, enabling interactive API documentation.
    *   `tmfserver/www/swagger/*.yaml`: These YAML files contain the OpenAPI definitions for the TM Forum APIs (e.g., Product Catalog Management, Party Management).

## 3. Key Architectural Strategies

### 3.1 Coexistence of Implementations

A fundamental strategy for this project is the ability for the new `isbeserver` implementation to coexist with an existing server within the same repository. This allows for:
*   **Phased Rollout**: New features or improvements can be deployed and tested alongside the stable version.
*   **Reduced Risk**: Changes can be validated in a production-like environment without immediately impacting existing services.
*   **Gradual Migration**: Users can be migrated to the new server incrementally.

### 3.2 Database Design: Single Table for TM Forum Objects

As per the project guidelines, TM Forum objects are stored in a single SQLite table. This design choice offers:
*   **Flexibility**: Accommodates various TM Forum object types without requiring a new table for each, simplifying schema management.
*   **JSON Storage**: The entire TM Forum object is stored as a JSON field, allowing for schema evolution without database migrations for every object change.
*   **Metadata Fields**: Additional fields are used for metadata and frequently queried attributes, optimizing common SQL queries.

### 3.3 In-Memory Representation: `map[string]any`

To support a wide range of TM Forum object types with a consistent codebase, the in-memory representation of these objects is based on a `map[string]any` nested structure. This approach provides:
*   **Genericity**: A single code path can handle most TM Forum APIs, as many objects share common properties.
*   **Type Safety (with methods)**: While the underlying structure is generic, specific methods are implemented to query and manipulate the map in a type-safe manner, ensuring data integrity and ease of use.

### 3.4 Error Handling

The server adheres to a structured error handling approach:
*   **Standard Go `errors` Package**: Used for internal error propagation.
*   **`pkg/apierror`**: For errors returned to the client, well-defined API error types are used, ensuring consistent and informative error responses.
*   **`internal/errl`**: A simple wrapper is used to include error location information, aiding in debugging and tracing issues.

### 3.5 Modular and Reusable Code

The architecture emphasizes modularity and code reusability:
*   **Separation of Concerns**: Clear boundaries between handlers, services, and repositories promote maintainability and testability.
*   **Common Utilities**: The `tmfserver/common` package centralizes shared logic, preventing code duplication. A recent example is the refactoring of JWT token extraction into a dedicated `ExtractJWTToken` function in `tmfserver/common/jwt.go`, which is now used by both Echo and Fiber handlers. This reduces redundancy and simplifies future modifications to the token extraction logic.
*   **Framework Agnostic Logic**: By abstracting core business logic into the service layer and common utilities, the server can easily integrate with different web frameworks.

## 4. Development Considerations

*   **OpenAPI Definitions**: The server integrates with OpenAPI definitions (located in `oapiv5/` and served via `tmfserver/www/swagger/`) to provide clear API contracts and enable automatic documentation generation.
*   **Testing**: All new functionality requires unit tests, typically placed in `_test.go` files within the same directory, utilizing the `testify` suite for assertions.
*   **Code Style**: Adherence to standard Go formatting (`gofmt`) and clear, concise doc comments for all functions are maintained to ensure code readability and consistency.
