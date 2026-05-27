package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

func newSubmitCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "submit",
		Short:             "Submit an intent to the ledger",
		PersistentPreRunE: requireClient(),
		PersistentPostRun: closeClient(),
	}

	cmd.PersistentFlags().String("ledger", "", "Ledger ID (required)")
	cmd.PersistentFlags().String("ik", "", "Idempotency key")
	cmd.PersistentFlags().String("reference", "", "Transaction reference")
	cmd.PersistentFlags().Bool("dry-run", false, "Validate without committing")
	must(cmd.MarkPersistentFlagRequired("ledger"))

	cmd.AddCommand(
		newSubmitPostCmd(),
		newSubmitAuthorizeCmd(),
		newSubmitCaptureCmd(),
		newSubmitVoidCmd(),
		newSubmitRevertCmd(),
		newSubmitAmendCmd(),
		newSubmitConvertCmd(),
		newSubmitSetMetadataCmd(),
		newSubmitDeleteMetadataCmd(),
	)
	return cmd
}

// submitIntent sends the intent and prints the response.
func submitIntent(cmd *cobra.Command, intent *pb.Intent) error {
	// Apply shared flags.
	intent.LedgerId, _ = cmd.Flags().GetString("ledger")
	intent.IdempotencyKey, _ = cmd.Flags().GetString("ik")
	intent.Reference, _ = cmd.Flags().GetString("reference")
	intent.DryRun, _ = cmd.Flags().GetBool("dry-run")

	resp, err := client.GRPC().Submit(cmd.Context(), &pb.SubmitRequest{Intent: intent})
	if err != nil {
		return grpcErr(err)
	}

	p := newPrinter()
	return p.print(resp, func(p *printer) {
		pairs := [][2]string{
			{"Event ID", resp.EventId},
			{"Idempotent Hit", fmt.Sprintf("%v", resp.IdempotentHit)},
		}
		p.printSingle(pairs)
	})
}

// parsePostings parses "source:destination:amount:asset" strings.
func parsePostings(raw []string) ([]*pb.Posting, error) {
	postings := make([]*pb.Posting, 0, len(raw))
	for _, s := range raw {
		parts := strings.SplitN(s, ":", 4)
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid posting format %q: expected source:destination:amount:asset", s)
		}
		postings = append(postings, &pb.Posting{
			Source:      parts[0],
			Destination: parts[1],
			Amount:      parts[2],
			Asset:       parts[3],
		})
	}
	return postings, nil
}

// parseMetadataFlags parses "key=value" strings into protobuf metadata.
func parseMetadataFlags(raw []string) (map[string]*pb.MetadataValue, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	m := make(map[string]*pb.MetadataValue, len(raw))
	for _, s := range raw {
		k, v, ok := strings.Cut(s, "=")
		if !ok {
			return nil, fmt.Errorf("invalid metadata format %q: expected key=value", s)
		}
		m[k] = &pb.MetadataValue{Value: &pb.MetadataValue_StringValue{StringValue: v}}
	}
	return m, nil
}

// --- submit post ---

func newSubmitPostCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "post",
		Short: "Post a transaction with one or more postings",
		RunE: func(cmd *cobra.Command, args []string) error {
			raw, _ := cmd.Flags().GetStringSlice("posting")
			if len(raw) == 0 {
				return fmt.Errorf("at least one --posting is required")
			}
			postings, err := parsePostings(raw)
			if err != nil {
				return err
			}
			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_Post{Post: &pb.PostOperation{Postings: postings}},
			})
		},
	}
	cmd.Flags().StringSlice("posting", nil, "Posting in source:destination:amount:asset format (repeatable)")
	return cmd
}

// --- submit authorize ---

func newSubmitAuthorizeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "authorize",
		Short: "Authorize a hold on an account",
		RunE: func(cmd *cobra.Command, args []string) error {
			source, _ := cmd.Flags().GetString("source")
			amount, _ := cmd.Flags().GetString("amount")
			asset, _ := cmd.Flags().GetString("asset")
			destHint, _ := cmd.Flags().GetString("destination-hint")
			expiresStr, _ := cmd.Flags().GetString("expires-at")

			op := &pb.AuthorizeOperation{
				Source:          source,
				DestinationHint: destHint,
				Amount:          amount,
				Asset:           asset,
			}
			if expiresStr != "" {
				t, err := time.Parse(time.RFC3339, expiresStr)
				if err != nil {
					return fmt.Errorf("invalid --expires-at: %w", err)
				}
				op.ExpiresAt = timestamppb.New(t)
			}
			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_Authorize{Authorize: op},
			})
		},
	}
	cmd.Flags().String("source", "", "Source account (required)")
	cmd.Flags().String("amount", "", "Hold amount (required)")
	cmd.Flags().String("asset", "", "Asset (required)")
	cmd.Flags().String("destination-hint", "", "Suggested destination account")
	cmd.Flags().String("expires-at", "", "Expiry time (RFC3339)")
	must(cmd.MarkFlagRequired("source"))
	must(cmd.MarkFlagRequired("amount"))
	must(cmd.MarkFlagRequired("asset"))
	return cmd
}

// --- submit capture ---

func newSubmitCaptureCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "capture",
		Short: "Capture (settle) an authorized hold",
		RunE: func(cmd *cobra.Command, args []string) error {
			holdID, _ := cmd.Flags().GetString("hold-id")
			amount, _ := cmd.Flags().GetString("amount")
			dest, _ := cmd.Flags().GetString("destination")

			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_Capture{Capture: &pb.CaptureOperation{
					HoldId:      holdID,
					Amount:      amount,
					Destination: dest,
				}},
			})
		},
	}
	cmd.Flags().String("hold-id", "", "Hold ID to capture (required)")
	cmd.Flags().String("amount", "", "Amount to capture (required)")
	cmd.Flags().String("destination", "", "Destination account override")
	must(cmd.MarkFlagRequired("hold-id"))
	must(cmd.MarkFlagRequired("amount"))
	return cmd
}

// --- submit void ---

func newSubmitVoidCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "void",
		Short: "Void an authorized hold",
		RunE: func(cmd *cobra.Command, args []string) error {
			holdID, _ := cmd.Flags().GetString("hold-id")
			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_Void{Void: &pb.VoidOperation{HoldId: holdID}},
			})
		},
	}
	cmd.Flags().String("hold-id", "", "Hold ID to void (required)")
	must(cmd.MarkFlagRequired("hold-id"))
	return cmd
}

// --- submit revert ---

func newSubmitRevertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revert",
		Short: "Revert a transaction",
		RunE: func(cmd *cobra.Command, args []string) error {
			txID, _ := cmd.Flags().GetString("tx-id")
			force, _ := cmd.Flags().GetBool("force")
			atEffective, _ := cmd.Flags().GetBool("at-effective-date")
			reason, _ := cmd.Flags().GetString("reason")

			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_Revert{Revert: &pb.RevertOperation{
					OriginalTransactionId: txID,
					Force:                 force,
					AtEffectiveDate:       atEffective,
					Reason:                reason,
				}},
			})
		},
	}
	cmd.Flags().String("tx-id", "", "Transaction ID to revert (required)")
	cmd.Flags().Bool("force", false, "Force revert even if accounts would go negative")
	cmd.Flags().Bool("at-effective-date", false, "Revert at original valid_time")
	cmd.Flags().String("reason", "", "Reason for reversal")
	must(cmd.MarkFlagRequired("tx-id"))
	return cmd
}

// --- submit amend ---

func newSubmitAmendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "amend",
		Short: "Amend transaction metadata",
		RunE: func(cmd *cobra.Command, args []string) error {
			txID, _ := cmd.Flags().GetString("tx-id")
			raw, _ := cmd.Flags().GetStringSlice("metadata")
			meta, err := parseMetadataFlags(raw)
			if err != nil {
				return err
			}
			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_Amend{Amend: &pb.AmendOperation{
					OriginalTransactionId: txID,
					MetadataChanges:       meta,
				}},
			})
		},
	}
	cmd.Flags().String("tx-id", "", "Transaction ID to amend (required)")
	cmd.Flags().StringSlice("metadata", nil, "Metadata in key=value format (repeatable)")
	must(cmd.MarkFlagRequired("tx-id"))
	must(cmd.MarkFlagRequired("metadata"))
	return cmd
}

// --- submit convert ---

func newSubmitConvertCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "convert",
		Short: "Record a currency conversion",
		RunE: func(cmd *cobra.Command, args []string) error {
			source, _ := cmd.Flags().GetString("source")
			dest, _ := cmd.Flags().GetString("destination")
			srcAmount, _ := cmd.Flags().GetString("src-amount")
			srcAsset, _ := cmd.Flags().GetString("src-asset")
			dstAmount, _ := cmd.Flags().GetString("dst-amount")
			dstAsset, _ := cmd.Flags().GetString("dst-asset")
			rate, _ := cmd.Flags().GetString("rate")
			rateSource, _ := cmd.Flags().GetString("rate-source")

			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_Convert{Convert: &pb.ConvertOperation{
					Source:            source,
					Destination:       dest,
					SourceAmount:      srcAmount,
					SourceAsset:       srcAsset,
					DestinationAmount: dstAmount,
					DestinationAsset:  dstAsset,
					Rate:              rate,
					RateSource:        rateSource,
				}},
			})
		},
	}
	cmd.Flags().String("source", "", "Source account (required)")
	cmd.Flags().String("destination", "", "Destination account (required)")
	cmd.Flags().String("src-amount", "", "Source amount (required)")
	cmd.Flags().String("src-asset", "", "Source asset (required)")
	cmd.Flags().String("dst-amount", "", "Destination amount (required)")
	cmd.Flags().String("dst-asset", "", "Destination asset (required)")
	cmd.Flags().String("rate", "", "Exchange rate (required)")
	cmd.Flags().String("rate-source", "", "Rate source/provider")
	must(cmd.MarkFlagRequired("source"))
	must(cmd.MarkFlagRequired("destination"))
	must(cmd.MarkFlagRequired("src-amount"))
	must(cmd.MarkFlagRequired("src-asset"))
	must(cmd.MarkFlagRequired("dst-amount"))
	must(cmd.MarkFlagRequired("dst-asset"))
	must(cmd.MarkFlagRequired("rate"))
	return cmd
}

// --- submit set-metadata ---

func newSubmitSetMetadataCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-metadata",
		Short: "Set metadata on an account or transaction",
		RunE: func(cmd *cobra.Command, args []string) error {
			targetType, _ := cmd.Flags().GetString("target-type")
			targetID, _ := cmd.Flags().GetString("target-id")
			raw, _ := cmd.Flags().GetStringSlice("metadata")

			meta, err := parseMetadataFlags(raw)
			if err != nil {
				return err
			}

			var tt pb.SetMetadataOperation_TargetType
			switch targetType {
			case "account":
				tt = pb.SetMetadataOperation_ACCOUNT
			case "transaction":
				tt = pb.SetMetadataOperation_TRANSACTION
			default:
				return fmt.Errorf("--target-type must be 'account' or 'transaction'")
			}

			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_SetMetadata{SetMetadata: &pb.SetMetadataOperation{
					TargetType: tt,
					TargetId:   targetID,
					Metadata:   meta,
				}},
			})
		},
	}
	cmd.Flags().String("target-type", "", "Target type: account or transaction (required)")
	cmd.Flags().String("target-id", "", "Target ID (account address or transaction ID) (required)")
	cmd.Flags().StringSlice("metadata", nil, "Metadata in key=value format (repeatable)")
	must(cmd.MarkFlagRequired("target-type"))
	must(cmd.MarkFlagRequired("target-id"))
	must(cmd.MarkFlagRequired("metadata"))
	return cmd
}

// --- submit delete-metadata ---

func newSubmitDeleteMetadataCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-metadata",
		Short: "Delete a metadata key from an account or transaction",
		RunE: func(cmd *cobra.Command, args []string) error {
			targetType, _ := cmd.Flags().GetString("target-type")
			targetID, _ := cmd.Flags().GetString("target-id")
			key, _ := cmd.Flags().GetString("key")

			var tt pb.DeleteMetadataOperation_TargetType
			switch targetType {
			case "account":
				tt = pb.DeleteMetadataOperation_ACCOUNT
			case "transaction":
				tt = pb.DeleteMetadataOperation_TRANSACTION
			default:
				return fmt.Errorf("--target-type must be 'account' or 'transaction'")
			}

			return submitIntent(cmd, &pb.Intent{
				Operation: &pb.Intent_DeleteMetadata{DeleteMetadata: &pb.DeleteMetadataOperation{
					TargetType: tt,
					TargetId:   targetID,
					Key:        key,
				}},
			})
		},
	}
	cmd.Flags().String("target-type", "", "Target type: account or transaction (required)")
	cmd.Flags().String("target-id", "", "Target ID (account address or transaction ID) (required)")
	cmd.Flags().String("key", "", "Metadata key to delete (required)")
	must(cmd.MarkFlagRequired("target-type"))
	must(cmd.MarkFlagRequired("target-id"))
	must(cmd.MarkFlagRequired("key"))
	return cmd
}
