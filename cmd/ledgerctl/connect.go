package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/status"

	"github.com/remade/ledger/pkg/sdk"
)

var client *sdk.Client

// requireClient returns a PersistentPreRunE that establishes the SDK connection.
func requireClient() func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		c, err := sdk.New(serverAddr)
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
