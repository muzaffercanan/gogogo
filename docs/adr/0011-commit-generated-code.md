# 0011. Generated code is committed; generator versions are pinned

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

The API types and the strict server interface are generated from `api/openapi.yaml` by `oapi-codegen`
(ADR 0005), and query code will be generated from SQL by `sqlc` (ADR 0006). Generated code can either be
committed or produced as a build step. `sqlc` needs cgo, so on machines without a C toolchain it has to run
in a container.

## Decision

- Generated Go code is committed: `internal/api/oapi/oapi.gen.go` (from the spec) and
  `internal/repository/sqlc/*.gen.go` (from `internal/repository/queries/*.sql` and the migrations).
- `make generate` regenerates everything. CI regenerates and fails if the working tree changes, so a spec
  change without regenerated code cannot be merged.
- `oapi-codegen` is pinned with a `tool` directive in `go.mod` and invoked as `go tool oapi-codegen` from a
  `//go:generate` line, so every machine and CI run the same version without a global install.
- `sqlc` needs cgo, so it runs from a pinned Docker image (`SQLC_IMAGE` in the Makefile) instead of as a
  Go tool. Regenerating therefore needs Docker; building and testing do not.
- `*.gen.go` files are marked `linguist-generated`, so GitHub collapses them in diffs.
- A unit test validates the spec, including its examples, because the generator does not check examples.

## Alternatives considered

- **Generate during the build, keep the output out of git.** No generated code in the history, but a fresh
  clone does not compile, test or lint until the generator has run; the Dockerfile, every CI job and editors
  each need the extra step, and with `sqlc` every build would depend on Docker.
- **`go run <module>@<version>` from the Makefile.** Keeps `go.mod` free of tool dependencies, but the tool
  version is not verified through `go.sum`, and editors and CI must repeat the version.

## Consequences

- `go build`, `go test` and `golangci-lint` work on a fresh clone with only the Go toolchain.
- Commits that change the spec also carry the regenerated code.
- The tool's dependencies join the module graph: `go.sum` grows, and minimum version selection can raise the
  version of modules the services share with the tool (for example `golang.org/x/sys`), which then ship in
  the binaries.
