# TradeWatch developer entry points.
#
# Requirements: GNU make and Docker with the compose plugin. Go and Node are
# not needed on the host; toolchain targets run inside containers.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
MAKEFLAGS += --no-print-directory

COMPOSE ?= docker compose
TOOLBOX := $(COMPOSE) run --rm --no-deps toolbox
INFRA := kafka postgres redis

BUF_VERSION := v1.73.0
PROTOC_GEN_GO_VERSION := v1.36.12
STATICCHECK_VERSION := v0.8.2

##@ Stack

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^##@/ {printf "\n%s\n", substr($$0, 5)} /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: env
env: ## Create or complete .env with generated local secrets
	@sh scripts/init-env.sh

.PHONY: infra
infra: env ## Start Kafka, Postgres and Redis and wait until they are healthy
	$(COMPOSE) up -d --wait $(INFRA)

.PHONY: up
up: env ## Build and start the stack and wait until every service is healthy
	$(COMPOSE) up -d --build --wait

.PHONY: down
down: ## Stop the stack, keeping data volumes
	$(COMPOSE) --profile tools down --remove-orphans

.PHONY: clean
clean: ## Stop the stack and delete all volumes
	$(COMPOSE) --profile tools down --volumes --remove-orphans

.PHONY: ps
ps: ## Show service status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Follow service logs
	$(COMPOSE) logs -f --tail=100

##@ Development

.PHONY: test
test: infra ## Run unit, race and integration tests (in Docker)
	$(TOOLBOX) go test -race -tags=integration -count=1 ./...

.PHONY: test-unit
test-unit: ## Run unit tests with the local Go toolchain
	go test ./...

.PHONY: lint
lint: env ## Run gofmt, go vet and staticcheck (in Docker)
	$(TOOLBOX) sh -c 'test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }; go vet ./... && go vet -tags=integration ./... && go run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...'

.PHONY: bench-micro
bench-micro: env ## Run Go micro-benchmarks and save results to docs/results/microbench
	$(TOOLBOX) bash scripts/bench-micro.sh orderbook ./internal/orderbook

.PHONY: mutation-test
mutation-test: env ## Check that the order book tests catch hand-injected engine bugs
	$(TOOLBOX) bash scripts/mutation-orderbook.sh

.PHONY: proto
proto: env ## Lint the protobuf schemas and regenerate Go code (in Docker)
	$(TOOLBOX) sh -c 'go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) && go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION) && buf lint && buf format --diff --exit-code && buf generate'
