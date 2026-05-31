.PHONY: build test lint proto openapi migrate-up migrate-down dev-up dev-down dev-token clean conformance docker-build

# Binaries
SERVER_BIN   := bin/server
WORKER_BIN   := bin/worker
CTL_BIN      := bin/ledgerctl
DEVTOKEN_BIN := bin/devtoken

# Go
GO          := go
GOFLAGS     :=

# Docker Compose file defining the local dev stack.
COMPOSE     := compose.yml

# ---- Build ----

build: $(SERVER_BIN) $(WORKER_BIN) $(CTL_BIN) $(DEVTOKEN_BIN)

$(SERVER_BIN):
	$(GO) build $(GOFLAGS) -o $@ ./cmd/server

$(WORKER_BIN):
	$(GO) build $(GOFLAGS) -o $@ ./cmd/worker

$(CTL_BIN):
	$(GO) build $(GOFLAGS) -o $@ ./cmd/ledgerctl

$(DEVTOKEN_BIN):
	$(GO) build $(GOFLAGS) -o $@ ./cmd/devtoken

# ---- Test ----

test:
	$(GO) test ./...

conformance:
	$(GO) test -v -tags=conformance ./test/conformance/...

# ---- Lint ----

lint:
	golangci-lint run ./...

# ---- Proto ----

proto:
	protoc \
		--proto_path=proto \
		--proto_path=proto/third_party \
		--go_out=pkg/proto --go_opt=paths=source_relative \
		--go-grpc_out=pkg/proto --go-grpc_opt=paths=source_relative \
		proto/ledger/v1/ir.proto \
		proto/ledger/v1/events.proto \
		proto/ledger/v1/stream.proto \
		proto/ledger/v1/api.proto
	protoc \
		--proto_path=proto \
		--proto_path=proto/third_party \
		--grpc-gateway_out=pkg/proto --grpc-gateway_opt=paths=source_relative \
		proto/ledger/v1/api.proto

# Generate the embedded OpenAPI/Swagger spec from the same proto sources, using buf's
# remote openapiv2 plugin (no local protoc-gen-openapiv2 install needed). Output is
# committed at internal/api/openapi/api.swagger.json and served by the server.
openapi:
	cd proto && buf generate --template buf.gen.openapi.yaml

# ---- Migrations ----

LEDGER_POSTGRES_DSN ?= postgres://localhost:5432/ledger?sslmode=disable

migrate-up:
	$(GO) run ./cmd/ledgerctl migrate-up

migrate-down:
	$(GO) run ./cmd/ledgerctl migrate-down

# ---- Docker ----

docker-build:
	docker compose -f $(COMPOSE) build

dev-up:
	docker compose -f $(COMPOSE) up -d --build

dev-down:
	docker compose -f $(COMPOSE) down

# dev-token prints a bearer token for local use (e.g. ledgerctl --token, grpcurl).
# Requires the dev stack (and the devtoken service) to be running via 'make dev-up'.
dev-token:
	@curl -fsS 'http://localhost:8081/token?sub=dev&ledgers=*'

# ---- Clean ----

clean:
	rm -rf bin/
