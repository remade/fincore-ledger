# ---- Build stage ----
FROM golang:1.24-alpine AS build

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /bin/worker ./cmd/worker

# ---- Server ----
FROM gcr.io/distroless/static-debian12 AS server

COPY --from=build /bin/server /server

EXPOSE 9090 8080

ENTRYPOINT ["/server", "--env-file="]

# ---- Worker ----
FROM gcr.io/distroless/static-debian12 AS worker

COPY --from=build /bin/worker /worker

ENTRYPOINT ["/worker", "--env-file="]
