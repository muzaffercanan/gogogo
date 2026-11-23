# Recipes use POSIX sh. On Windows, run make from Git Bash.
SHELL := /bin/sh
# Stop Git Bash from rewriting container paths such as /src.
export MSYS_NO_PATHCONV := 1

GO_IMAGE ?= golang:1.27
COMPOSE  ?= docker compose

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

.PHONY: up
up: ## Build and start the whole stack in the background
	$(COMPOSE) up --build -d

.PHONY: down
down: ## Stop the stack (keeps the database volume)
	$(COMPOSE) down

.PHONY: logs
logs: ## Follow logs of the application services
	$(COMPOSE) logs -f api relay worker

.PHONY: build
build: ## Build all binaries into ./bin
	go build -o bin/ ./cmd/...

.PHONY: test
test: ## Run unit tests
	go test -count=1 ./...

.PHONY: test-race
test-race: ## Run unit tests with the race detector inside a Linux container
	docker run --rm -v "$(CURDIR)":/src -v gogogo-gomod:/go/pkg/mod -w /src $(GO_IMAGE) go test -race -count=1 ./...

.PHONY: generate
generate: ## Regenerate code from api/openapi.yaml
	go generate ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	go mod tidy
