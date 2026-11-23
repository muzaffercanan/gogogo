# 0005. Spec-first API with OpenAPI and oapi-codegen

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

The API is a public contract and must ship with an OpenAPI specification. If the spec is generated from
comments or maintained by hand next to the code, it tends to drift from the actual behaviour.

## Decision

- `api/openapi.yaml` (OpenAPI 3.0) is the source of truth for request/response shapes, status codes and
  errors.
- `oapi-codegen` generates the types and a strict server interface for the standard library `net/http`
  router; handlers implement that interface.
- Domain validation that a schema cannot express (per-channel recipient formats, SMS encoding and segment
  limits, template variables) lives in Go, in `internal/domain`.
- The spec is served at `/docs` for interactive exploration.

## Alternatives considered

- **Code-first with `swag` annotations.** Fast to start, but the stable release emits Swagger 2.0, and
  annotations are comments the compiler never checks.
- **Code-first with Huma.** Emits OpenAPI 3.1 from Go types without drift, but couples the service to a framework.

## Consequences

- Spec/implementation drift becomes a compile error.
- A code-generation step is added to the build (`make generate`, checked in CI).
- Limited to the OpenAPI 3.0 features well supported by the generator.
