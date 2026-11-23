# CLAUDE.md

Guidance for AI coding assistants working in this repository.

## Project

Event-driven notification service written in Go. It accepts notification requests over HTTP,
queues them, and delivers them through SMS, Email and Push providers with rate limiting,
priorities, retries and status tracking.

Status: early scaffolding. Architecture decisions are recorded in `docs/adr/` as they are made.

## Ground rules

- Design and architecture decisions belong to the maintainer. Present the options with their
  trade-offs and a recommendation; never pick one silently.
- Record every accepted decision as an ADR: `docs/adr/NNNN-short-title.md`
  (Context / Decision / Consequences), short and technical.
- Small, focused commits using Conventional Commits (`feat:`, `fix:`, `refactor:`, `test:`,
  `docs:`, `chore:`, `ci:`, `build:`).
- Never commit secrets or real provider endpoints. Use `.env` (gitignored) and keep
  `.env.example` up to date with placeholders.
- Do not push without explicit approval from the maintainer.

## Environment notes

- The primary dev machine runs Windows; CI and reviewers use Linux/macOS.
- All text files are LF (`.gitattributes`). Shell scripts must be POSIX `sh`.
- The race detector needs cgo, so run race tests in Docker/CI rather than natively on Windows.
- Makefile recipes use POSIX `sh` syntax; on Windows, run `make` from Git Bash.

## Commands

To be filled in once the skeleton exists (build, run, test, lint, migrate).

## Code conventions

- `gofmt`/`goimports` clean; `golangci-lint run` must pass.
- `context.Context` is the first parameter of anything that does I/O.
- Wrap errors with context (`fmt.Errorf("doing x: %w", err)`); no panics outside `main`.
- No mutable package-level state; dependencies are passed in explicitly.
- Table-driven tests; every bug fix comes with a test that reproduces it.
