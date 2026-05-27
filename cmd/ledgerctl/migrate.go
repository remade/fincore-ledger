package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/remade/ledger/internal/storage/postgres"
)

func resolveDSN() string {
	if postgresDSN != "" {
		return postgresDSN
	}
	if v := os.Getenv("LEDGER_POSTGRES_DSN"); v != "" {
		return v
	}
	return "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"
}

func newMigrateUpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate-up",
		Short: "Run database migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := postgres.NewStandalone(resolveDSN())
			if err != nil {
				return err
			}
			defer db.Close()

			if err := db.MigrateUp(cmd.Context()); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "Migrations applied successfully.")
			return nil
		},
	}
}

func newMigrateDownCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate-down",
		Short: "Reverse database migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := postgres.NewStandalone(resolveDSN())
			if err != nil {
				return err
			}
			defer db.Close()

			if err := db.MigrateDown(cmd.Context()); err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, "Migrations rolled back successfully.")
			return nil
		},
	}
}
