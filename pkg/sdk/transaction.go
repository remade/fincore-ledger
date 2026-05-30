package sdk

import (
	"context"
	"fmt"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// TransactionBuilder provides a fluent API for building and submitting transactions.
//
// Usage:
//
//	result, err := client.NewTransaction("my-ledger").
//	    WithReference("payment-123").
//	    Post("_world", "users:42:wallet", "10000", "USD/2").
//	    Post("users:42:wallet", "fees:platform", "100", "USD/2").
//	    Submit(ctx)
type TransactionBuilder struct {
	client    *Client
	ledgerID  string
	postings  []*pb.Posting
	reference string
	metadata  map[string]*pb.MetadataValue
	ik        string
	dryRun    bool
}

// NewTransaction starts building a transaction for the given ledger.
func (c *Client) NewTransaction(ledgerID string) *TransactionBuilder {
	return &TransactionBuilder{
		client:   c,
		ledgerID: ledgerID,
	}
}

// Post adds a posting to the transaction.
func (b *TransactionBuilder) Post(source, destination, amount, asset string) *TransactionBuilder {
	b.postings = append(b.postings, &pb.Posting{
		Source:      source,
		Destination: destination,
		Amount:      amount,
		Asset:       asset,
	})
	return b
}

// WithReference sets a unique reference for the transaction.
func (b *TransactionBuilder) WithReference(ref string) *TransactionBuilder {
	b.reference = ref
	return b
}

// WithIdempotencyKey sets the idempotency key for the transaction.
func (b *TransactionBuilder) WithIdempotencyKey(key string) *TransactionBuilder {
	b.ik = key
	return b
}

// WithMetadata adds a string metadata key-value pair.
func (b *TransactionBuilder) WithMetadata(key, value string) *TransactionBuilder {
	if b.metadata == nil {
		b.metadata = make(map[string]*pb.MetadataValue)
	}
	b.metadata[key] = &pb.MetadataValue{
		Value: &pb.MetadataValue_StringValue{StringValue: value},
	}
	return b
}

// DryRun sets the transaction to dry-run mode (validates but doesn't commit).
func (b *TransactionBuilder) DryRun() *TransactionBuilder {
	b.dryRun = true
	return b
}

// Submit sends the transaction to the server.
func (b *TransactionBuilder) Submit(ctx context.Context) (*pb.SubmitResponse, error) {
	if len(b.postings) == 0 {
		return nil, fmt.Errorf("at least one posting is required")
	}
	for i, pst := range b.postings {
		if err := validatePosting(pst.Source, pst.Destination, pst.Amount, pst.Asset); err != nil {
			return nil, fmt.Errorf("posting %d: %w", i, err)
		}
	}

	intent := &pb.Intent{
		LedgerId:       b.ledgerID,
		IdempotencyKey: b.ik,
		Reference:      b.reference,
		Metadata:       b.metadata,
		DryRun:         b.dryRun,
		Operation: &pb.Intent_Post{
			Post: &pb.PostOperation{
				Postings: b.postings,
			},
		},
	}

	return b.client.ledger.Submit(ctx, &pb.SubmitRequest{Intent: intent})
}
