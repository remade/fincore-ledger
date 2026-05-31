# ---- Build stage ----
FROM golang:1.24-alpine AS build

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/worker ./cmd/worker
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/devtoken ./cmd/devtoken

# ---- Server ----
FROM gcr.io/distroless/static-debian12 AS server

COPY --from=build /bin/server /server

EXPOSE 9090 8080

ENTRYPOINT ["/server", "--env-file="]

# ---- Worker ----
FROM gcr.io/distroless/static-debian12 AS worker

COPY --from=build /bin/worker /worker

ENTRYPOINT ["/worker", "--env-file="]

# ---- Devtoken (development-only JWKS server + token minter) ----
FROM gcr.io/distroless/static-debian12 AS devtoken

COPY --from=build /bin/devtoken /devtoken

EXPOSE 8081

ENTRYPOINT ["/devtoken", "serve"]
