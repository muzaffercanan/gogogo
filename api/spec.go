// Package apispec embeds the OpenAPI specification, so that the API can serve the contract it is
// generated from.
package apispec

import _ "embed" // for go:embed

// YAML is the content of api/openapi.yaml.
//
//go:embed openapi.yaml
var YAML string
