package main

import (
	"github.com/spf13/cobra"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

func newBalanceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "balance",
		Short:             "Query account balance",
		PersistentPreRunE: requireClient(),
		PersistentPostRun: closeClient(),
	}
	cmd.AddCommand(newBalanceGetCmd())
	return cmd
}

func newBalanceGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get balance for an account and asset",
		RunE: func(cmd *cobra.Command, args []string) error {
			ledgerID, _ := cmd.Flags().GetString("ledger")
			account, _ := cmd.Flags().GetString("account")
			asset, _ := cmd.Flags().GetString("asset")
			includeHolds, _ := cmd.Flags().GetBool("include-holds")

			resp, err := client.GRPC().GetBalance(cmd.Context(), &pb.GetBalanceRequest{
				LedgerId:     ledgerID,
				Account:      account,
				Asset:        asset,
				IncludeHolds: includeHolds,
			})
			if err != nil {
				return grpcErr(err)
			}

			p := newPrinter()
			return p.print(resp, func(p *printer) {
				p.printTable(
					[]string{"ASSET", "POSTED", "AVAILABLE"},
					[][]string{{resp.Asset, resp.PostedBalance, resp.AvailableBalance}},
				)
			})
		},
	}
	cmd.Flags().String("ledger", "", "Ledger ID (required)")
	cmd.Flags().String("account", "", "Account address (required)")
	cmd.Flags().String("asset", "", "Asset (required)")
	cmd.Flags().Bool("include-holds", false, "Include hold impact in available balance")
	must(cmd.MarkFlagRequired("ledger"))
	must(cmd.MarkFlagRequired("account"))
	must(cmd.MarkFlagRequired("asset"))
	return cmd
}
