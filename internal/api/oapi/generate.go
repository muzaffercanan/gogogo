// Package oapi holds the types and the strict server interface generated from api/openapi.yaml.
// Do not edit oapi.gen.go by hand; change the spec and run `make generate`.
package oapi

//go:generate go tool oapi-codegen -config oapi-codegen.yaml ../../../api/openapi.yaml
