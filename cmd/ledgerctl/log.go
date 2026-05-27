package main

import (
	"github.com/spf13/cobra"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

func newLogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "log",
		Short:             "View log events",
		PersistentPreRunE: requireClient(),
		PersistentPostRun: closeClient(),
	}
	cmd.AddCommand(newLogListCmd(), newLogGetCmd())
	return cmd
}

func newLogListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List log events for a ledger",
		RunE: func(cmd *cobra.Command, args []string) error {
			ledgerID, _ := cmd.Flags().GetString("ledger")
			pageSize, _ := cmd.Flags().GetInt32("page-size")

			resp, err := client.GRPC().ListLogEvents(cmd.Context(), &pb.ListLogEventsRequest{
				LedgerId: ledgerID,
				PageSize: pageSize,
			})
			if err != nil {
				return grpcErr(err)
			}

			p := newPrinter()
			return p.print(resp, func(p *printer) {
				rows := make([][]string, 0, len(resp.Events))
				for _, e := range resp.Events {
					ts := ""
					if e.SystemTime != nil {
						ts = e.SystemTime.AsTime().Format("2006-01-02T15:04:05Z")
					}
					rows = append(rows, []string{e.EventId, e.Type.String(), ts})
				}
				p.printTable([]string{"EVENT ID", "TYPE", "SYSTEM TIME"}, rows)
			})
		},
	}
	cmd.Flags().String("ledger", "", "Ledger ID (required)")
	cmd.Flags().Int32("page-size", 20, "Number of events to return")
	must(cmd.MarkFlagRequired("ledger"))
	return cmd
}

func newLogGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get a specific log event",
		RunE: func(cmd *cobra.Command, args []string) error {
			ledgerID, _ := cmd.Flags().GetString("ledger")
			eventID, _ := cmd.Flags().GetString("event-id")

			resp, err := client.GRPC().GetLogEvent(cmd.Context(), &pb.GetLogEventRequest{
				LedgerId: ledgerID,
				EventId:  eventID,
			})
			if err != nil {
				return grpcErr(err)
			}

			p := newPrinter()
			return p.print(resp, func(p *printer) {
				ts := ""
				if resp.SystemTime != nil {
					ts = resp.SystemTime.AsTime().Format("2006-01-02T15:04:05Z")
				}
				vt := ""
				if resp.ValidTime != nil {
					vt = resp.ValidTime.AsTime().Format("2006-01-02T15:04:05Z")
				}
				p.printSingle([][2]string{
					{"Event ID", resp.EventId},
					{"Type", resp.Type.String()},
					{"Ledger", resp.LedgerId},
					{"System Time", ts},
					{"Valid Time", vt},
					{"Idempotency Key", resp.IdempotencyKey},
					{"Batch ID", resp.BatchId},
				})
			})
		},
	}
	cmd.Flags().String("ledger", "", "Ledger ID (required)")
	cmd.Flags().String("event-id", "", "Event ID (required)")
	must(cmd.MarkFlagRequired("ledger"))
	must(cmd.MarkFlagRequired("event-id"))
	return cmd
}
