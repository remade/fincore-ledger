package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	outputFormat string
	serverAddr   string
	postgresDSN  string
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "ledgerctl",
		Short:         "CLI for the Ledger double-entry accounting system",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.PersistentFlags().StringVarP(&outputFormat, "output", "o", "table", "Output format: table or json")
	cmd.PersistentFlags().StringVar(&serverAddr, "server-addr", "localhost:9090", "gRPC server address")
	cmd.PersistentFlags().StringVar(&postgresDSN, "postgres-dsn", "", "PostgreSQL DSN (for migrate commands; env: LEDGER_POSTGRES_DSN)")

	cmd.AddCommand(newMigrateUpCmd(), newMigrateDownCmd())
	cmd.AddCommand(newSubmitCmd())
	cmd.AddCommand(newBalanceCmd())
	cmd.AddCommand(newLogCmd())

	return cmd
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
