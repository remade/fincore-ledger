package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/status"

	"github.com/remade/ledger/pkg/sdk"
)

var client *sdk.Client

// requireClient returns a PersistentPreRunE that establishes the SDK connection.
// The server always requires authentication, so a bearer token must be supplied
// via --token or LEDGER_TOKEN.
func requireClient() func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if authToken == "" {
			return fmt.Errorf("a bearer token is required: set --token or LEDGER_TOKEN (get one via 'make dev-token')")
		}
		c, err := sdk.New(serverAddr, sdk.WithBearerToken(authToken))
		if err != nil {
			return fmt.Errorf("connecting to %s: %w", serverAddr, err)
		}
		client = c
		return nil
	}
}

// closeClient returns a PersistentPostRun that closes the SDK connection.
func closeClient() func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if client != nil {
			client.Close()
		}
	}
}

// grpcErr converts a gRPC status error into a clean CLI error message.
func grpcErr(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok {
		return fmt.Errorf("%s: %s", s.Code(), s.Message())
	}
	return err
}
