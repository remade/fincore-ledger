.PHONY: build test lint proto migrate-up migrate-down dev-up dev-down clean conformance

# Binaries
SERVER_BIN  := bin/server
WORKER_BIN  := bin/worker
CTL_BIN     := bin/ledgerctl

# Go
GO          := go
GOFLAGS     :=

# ---- Build ----

build: $(SERVER_BIN) $(WORKER_BIN) $(CTL_BIN)

$(SERVER_BIN):
	$(GO) build $(GOFLAGS) -o $@ ./cmd/server

$(WORKER_BIN):
	$(GO) build $(GOFLAGS) -o $@ ./cmd/worker

$(CTL_BIN):
	$(GO) build $(GOFLAGS) -o $@ ./cmd/ledgerctl

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

# ---- Migrations ----

LEDGER_POSTGRES_DSN ?= postgres://localhost:5432/ledger?sslmode=disable

migrate-up:
	$(GO) run ./cmd/ledgerctl migrate-up

migrate-down:
	$(GO) run ./cmd/ledgerctl migrate-down

# ---- Docker ----

dev-up:
	docker compose -f deploy/docker-compose/docker-compose.yml up -d

dev-down:
	docker compose -f deploy/docker-compose/docker-compose.yml down

# ---- Clean ----

clean:
	rm -rf bin/
