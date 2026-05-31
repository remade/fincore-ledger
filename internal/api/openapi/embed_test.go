package openapi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSpecJSON_BakedIn verifies the generated OpenAPI spec is embedded, parses as
// OpenAPI 2.0, declares the Bearer security scheme, and carries baked-in examples for
// the key write operations. This guards against a stale or missing `make openapi` run.
func TestSpecJSON_BakedIn(t *testing.T) {
	require.NotEmpty(t, SpecJSON, "spec must be embedded; run `make openapi`")

	var spec struct {
		Swagger             string         `json:"swagger"`
		SecurityDefinitions map[string]any `json:"securityDefinitions"`
		Definitions         map[string]struct {
			Example json.RawMessage `json:"example"`
		} `json:"definitions"`
	}
	require.NoError(t, json.Unmarshal(SpecJSON, &spec))

	assert.Equal(t, "2.0", spec.Swagger)
	assert.Contains(t, spec.SecurityDefinitions, "Bearer", "JWT security scheme must be declared")

	// Each key write operation's request/response schema must carry an example.
	for _, def := range []string{
		"LedgerServiceSubmitBody",
		"v1CreateLedgerRequest",
		"LedgerServiceInsertSchemaBody",
		"v1SubmitResponse",
	} {
		assert.NotEmpty(t, spec.Definitions[def].Example, "definition %q must have a baked-in example", def)
	}
}
