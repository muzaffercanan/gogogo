package oapi_test

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

// TestSpecIsValid fails when api/openapi.yaml is not a valid OpenAPI document or when one of its
// examples does not match its schema. The generator alone does not check examples.
func TestSpecIsValid(t *testing.T) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile("../../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("loading spec: %v", err)
	}
	if err := doc.Validate(loader.Context, openapi3.EnableExamplesValidation()); err != nil {
		t.Fatalf("invalid spec: %v", err)
	}
}
