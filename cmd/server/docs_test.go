package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestRegisterDocs_OpenAPISpec verifies the embedded spec is served at /openapi.json
// with a JSON content type and a well-formed OpenAPI 2.0 body.
func TestRegisterDocs_OpenAPISpec(t *testing.T) {
	mux := http.NewServeMux()
	registerDocs(mux, zap.NewNop())

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/openapi.json")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var spec struct {
		Swagger string                    `json:"swagger"`
		Paths   map[string]map[string]any `json:"paths"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&spec))
	assert.Equal(t, "2.0", spec.Swagger)
	assert.Contains(t, spec.Paths, "/v1/ledgers/{intent.ledgerId}/submit",
		"universal submit endpoint must be documented")
}

// TestRegisterDocs_SwaggerUI verifies the interactive UI is mounted at /swagger/ and
// serves its assets from the embedded http-swagger module.
func TestRegisterDocs_SwaggerUI(t *testing.T) {
	mux := http.NewServeMux()
	registerDocs(mux, zap.NewNop())

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/swagger/index.html")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
}
