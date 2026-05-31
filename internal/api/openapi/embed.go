// Package openapi embeds the generated OpenAPI 2.0 (Swagger) spec for the
// Ledger REST API so the server can serve interactive docs without any runtime
// filesystem dependency. The spec is generated from the proto sources via
// `make openapi` (buf + protoc-gen-openapiv2); do not edit api.swagger.json by hand.
package openapi

import _ "embed"

// SpecJSON is the generated OpenAPI 2.0 spec, served at /openapi.json and
// consumed by the Swagger UI mounted at /swagger/.
//
//go:embed api.swagger.json
var SpecJSON []byte
