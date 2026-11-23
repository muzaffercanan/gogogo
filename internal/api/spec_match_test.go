package api

import (
	"slices"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"

	"github.com/muzaffercanan/gogogo/internal/domain"
)

// TestErrorsMatchSpec fails when the field error codes or problem types of the code and of
// api/openapi.yaml drift apart.
func TestErrorsMatchSpec(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("loading spec: %v", err)
	}

	var specCodes []string
	for _, v := range doc.Components.Schemas["FieldError"].Value.Properties["code"].Value.Enum {
		specCodes = append(specCodes, v.(string))
	}
	codes := []string{}
	for _, c := range []domain.FieldErrorCode{
		domain.CodeRequired, domain.CodeInvalidValue, domain.CodeInvalidFormat, domain.CodeTooShort,
		domain.CodeTooLong, domain.CodeTooManyItems, domain.CodeNotInFuture, domain.CodeTooFarInFuture,
	} {
		codes = append(codes, string(c))
	}
	slices.Sort(specCodes)
	slices.Sort(codes)
	if !slices.Equal(codes, specCodes) {
		t.Errorf("field error codes = %v, spec lists %v", codes, specCodes)
	}

	// Every problem response in the spec has an example whose type is one of the problem types.
	var specTypes []string
	for name, resp := range doc.Components.Responses {
		media := resp.Value.Content.Get("application/problem+json")
		if media == nil {
			t.Errorf("response %s has no application/problem+json content", name)
			continue
		}
		typ, _ := media.Example.(map[string]any)["type"].(string)
		specTypes = append(specTypes, typ)
	}
	types := []string{
		problemValidation, problemNotFound, problemNotCancellable,
		problemPayloadTooLarge, problemIdempotencyKeyReused, problemInternal,
	}
	slices.Sort(specTypes)
	slices.Sort(types)
	if !slices.Equal(types, specTypes) {
		t.Errorf("problem types = %v, spec examples use %v", types, specTypes)
	}
}
