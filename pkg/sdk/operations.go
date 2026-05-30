package sdk

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// --- Holds ---

// Authorize creates a hold (authorization) on funds.
func (c *Client) Authorize(ctx context.Context, ledgerID, source, asset, amount string, expiresAt time.Time, opts ...SubmitOption) (*pb.SubmitResponse, error) {
	if err := validateAccount("source", source); err != nil {
		return nil, err
	}
	if err := validateAsset("asset", asset); err != nil {
		return nil, err
	}
	if err := validateAmount("amount", amount); err != nil {
		return nil, err
	}
	intent := &pb.Intent{
		LedgerId: ledgerID,
		Operation: &pb.Intent_Authorize{
			Authorize: &pb.AuthorizeOperation{
				Source:    source,
				Asset:     asset,
				Amount:    amount,
				ExpiresAt: timestamppb.New(expiresAt),
			},
		},
	}
	applySubmitOptions(intent, opts)
	return c.ledger.Submit(ctx, &pb.SubmitRequest{Intent: intent})
}

// Capture captures funds from a hold.
func (c *Client) Capture(ctx context.Context, ledgerID, holdID, amount, destination string, opts ...SubmitOption) (*pb.SubmitResponse, error) {
	if holdID == "" {
		return nil, fmt.Errorf("hold_id is required")
	}
	if err := validateAmount("amount", amount); err != nil {
		return nil, err
	}
	// destination is optional (the server falls back to the hold's destination hint).
	if destination != "" {
		if err := validateAccount("destination", destination); err != nil {
			return nil, err
		}
	}
	intent := &pb.Intent{
		LedgerId: ledgerID,
		Operation: &pb.Intent_Capture{
			Capture: &pb.CaptureOperation{
				HoldId:      holdID,
				Amount:      amount,
				Destination: destination,
			},
		},
	}
	applySubmitOptions(intent, opts)
	return c.ledger.Submit(ctx, &pb.SubmitRequest{Intent: intent})
}

// Void cancels a hold, releasing the authorized funds.
func (c *Client) Void(ctx context.Context, ledgerID, holdID string, opts ...SubmitOption) (*pb.SubmitResponse, error) {
	if holdID == "" {
		return nil, fmt.Errorf("hold_id is required")
	}
	intent := &pb.Intent{
		LedgerId: ledgerID,
		Operation: &pb.Intent_Void{
			Void: &pb.VoidOperation{HoldId: holdID},
		},
	}
	applySubmitOptions(intent, opts)
	return c.ledger.Submit(ctx, &pb.SubmitRequest{Intent: intent})
}

// GetHold returns a hold by ID.
func (c *Client) GetHold(ctx context.Context, ledgerID, holdID string) (*pb.Hold, error) {
	return c.ledger.GetHold(ctx, &pb.GetHoldRequest{LedgerId: ledgerID, HoldId: holdID})
}

// ListHolds lists holds for a ledger, optionally filtered by account.
func (c *Client) ListHolds(ctx context.Context, ledgerID string, account string) ([]*pb.Hold, error) {
	var all []*pb.Hold
	var pageToken string
	for {
		resp, err := c.ledger.ListHolds(ctx, &pb.ListHoldsRequest{
			LedgerId:  ledgerID,
			PageSize:  defaultListPageSize,
			PageToken: pageToken,
			Account:   account,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, resp.Holds...)
		// Stop when the server reports no more pages, or fails to advance the
		// cursor (defensive guard against a non-progressing token).
		if resp.NextPageToken == "" || resp.NextPageToken == pageToken {
			break
		}
		pageToken = resp.NextPageToken
	}
	return all, nil
}

// --- Reversals ---

// Revert reverses a transaction.
func (c *Client) Revert(ctx context.Context, ledgerID, txID string, force bool, opts ...SubmitOption) (*pb.SubmitResponse, error) {
	intent := &pb.Intent{
		LedgerId: ledgerID,
		Operation: &pb.Intent_Revert{
			Revert: &pb.RevertOperation{
				OriginalTransactionId: txID,
				Force:                 force,
			},
		},
	}
	applySubmitOptions(intent, opts)
	return c.ledger.Submit(ctx, &pb.SubmitRequest{Intent: intent})
}

// Amend updates metadata on a transaction.
func (c *Client) Amend(ctx context.Context, ledgerID, txID string, metadata map[string]*pb.MetadataValue, opts ...SubmitOption) (*pb.SubmitResponse, error) {
	intent := &pb.Intent{
		LedgerId: ledgerID,
		Operation: &pb.Intent_Amend{
			Amend: &pb.AmendOperation{
				OriginalTransactionId: txID,
				MetadataChanges:       metadata,
			},
		},
	}
	applySubmitOptions(intent, opts)
	return c.ledger.Submit(ctx, &pb.SubmitRequest{Intent: intent})
}

// --- Conversions ---

// ConvertParams holds parameters for a currency conversion.
type ConvertParams struct {
	Source          string
	Destination     string
	SourceAmount    string
	SourceAsset     string
	DestAmount      string
	DestAsset       string
	Rate            string
	RateSource      string
	SlippageAccount string
	SlippageAmount  string
}

// Convert performs a currency conversion.
func (c *Client) Convert(ctx context.Context, ledgerID string, params ConvertParams, opts ...SubmitOption) (*pb.SubmitResponse, error) {
	if err := validateAccount("source", params.Source); err != nil {
		return nil, err
	}
	if err := validateAccount("destination", params.Destination); err != nil {
		return nil, err
	}
	if err := validateAsset("source_asset", params.SourceAsset); err != nil {
		return nil, err
	}
	if err := validateAsset("dest_asset", params.DestAsset); err != nil {
		return nil, err
	}
	if err := validateAmount("source_amount", params.SourceAmount); err != nil {
		return nil, err
	}
	if err := validateAmount("dest_amount", params.DestAmount); err != nil {
		return nil, err
	}
	// Slippage is optional; validate the pair only when an amount is supplied.
	if params.SlippageAmount != "" {
		if err := validateAccount("slippage_account", params.SlippageAccount); err != nil {
			return nil, err
		}
		if err := validateAmount("slippage_amount", params.SlippageAmount); err != nil {
			return nil, err
		}
	}
	intent := &pb.Intent{
		LedgerId: ledgerID,
		Operation: &pb.Intent_Convert{
			Convert: &pb.ConvertOperation{
				Source:            params.Source,
				Destination:       params.Destination,
				SourceAmount:      params.SourceAmount,
				SourceAsset:       params.SourceAsset,
				DestinationAmount: params.DestAmount,
				DestinationAsset:  params.DestAsset,
				Rate:              params.Rate,
				RateSource:        params.RateSource,
				SlippageAccount:   params.SlippageAccount,
				SlippageAmount:    params.SlippageAmount,
			},
		},
	}
	applySubmitOptions(intent, opts)
	return c.ledger.Submit(ctx, &pb.SubmitRequest{Intent: intent})
}

// --- Relationships ---

// GetRelationships returns relationships for a transaction.
func (c *Client) GetRelationships(ctx context.Context, ledgerID, txID string, depth int) ([]*pb.Relationship, error) {
	resp, err := c.ledger.GetRelationships(ctx, &pb.GetRelationshipsRequest{
		LedgerId:      ledgerID,
		TransactionId: txID,
		Depth:         int32(depth),
	})
	if err != nil {
		return nil, err
	}
	return resp.Relationships, nil
}

// --- Submit options ---

// SubmitOption configures an intent before submission.
type SubmitOption func(*pb.Intent)

// WithIK sets the idempotency key.
func WithIK(key string) SubmitOption {
	return func(i *pb.Intent) { i.IdempotencyKey = key }
}

// WithDestinationHint sets the destination hint on an authorize operation.
func WithDestinationHint(hint string) SubmitOption {
	return func(i *pb.Intent) {
		if auth, ok := i.Operation.(*pb.Intent_Authorize); ok {
			auth.Authorize.DestinationHint = hint
		}
	}
}

func applySubmitOptions(intent *pb.Intent, opts []SubmitOption) {
	for _, opt := range opts {
		opt(intent)
	}
}
