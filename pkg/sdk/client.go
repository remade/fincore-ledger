package sdk

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// Client wraps the gRPC LedgerService with a fluent Go API.
type Client struct {
	conn   *grpc.ClientConn
	ledger pb.LedgerServiceClient
}

// New creates a new SDK client connected to the given address.
func New(addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to ledger at %s: %w", addr, err)
	}
	return &Client{
		conn:   conn,
		ledger: pb.NewLedgerServiceClient(conn),
	}, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// CreateLedger creates a new ledger.
func (c *Client) CreateLedger(ctx context.Context, id string, opts ...CreateLedgerOption) (*pb.Ledger, error) {
	req := &pb.CreateLedgerRequest{Id: id, BucketId: "_default"}
	for _, opt := range opts {
		opt(req)
	}
	return c.ledger.CreateLedger(ctx, req)
}

// CreateLedgerOption customizes ledger creation.
type CreateLedgerOption func(*pb.CreateLedgerRequest)

// WithBucket sets the bucket for the ledger.
func WithBucket(bucket string) CreateLedgerOption {
	return func(r *pb.CreateLedgerRequest) { r.BucketId = bucket }
}

// WithLedgerMetadata sets metadata on the ledger.
func WithLedgerMetadata(meta map[string]string) CreateLedgerOption {
	return func(r *pb.CreateLedgerRequest) { r.Metadata = meta }
}

// GetLedger returns a ledger by ID.
func (c *Client) GetLedger(ctx context.Context, id string) (*pb.Ledger, error) {
	return c.ledger.GetLedger(ctx, &pb.GetLedgerRequest{Id: id})
}

// defaultListPageSize is the server-side page size requested by the SDK's
// auto-paginating list helpers.
const defaultListPageSize = 1000

// ListLedgers lists all ledgers, transparently following pagination so callers
// receive every ledger rather than only the first page.
func (c *Client) ListLedgers(ctx context.Context) ([]*pb.Ledger, error) {
	var all []*pb.Ledger
	var pageToken string
	for {
		resp, err := c.ledger.ListLedgers(ctx, &pb.ListLedgersRequest{
			PageSize:  defaultListPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, resp.Ledgers...)
		if resp.NextPageToken == "" || resp.NextPageToken == pageToken {
			break
		}
		pageToken = resp.NextPageToken
	}
	return all, nil
}

// SealLedger seals a ledger, preventing further writes.
func (c *Client) SealLedger(ctx context.Context, id string) (*pb.Ledger, error) {
	return c.ledger.SealLedger(ctx, &pb.SealLedgerRequest{Id: id})
}

// GetTransaction returns a transaction by ID.
func (c *Client) GetTransaction(ctx context.Context, ledgerID, txID string) (*pb.Transaction, error) {
	return c.ledger.GetTransaction(ctx, &pb.GetTransactionRequest{
		LedgerId:      ledgerID,
		TransactionId: txID,
	})
}

// GetAccount returns an account by address.
func (c *Client) GetAccount(ctx context.Context, ledgerID, address string) (*pb.Account, error) {
	return c.ledger.GetAccount(ctx, &pb.GetAccountRequest{
		LedgerId: ledgerID,
		Address:  address,
	})
}

// GetBalance returns the balance for an account+asset pair.
func (c *Client) GetBalance(ctx context.Context, ledgerID, account, asset string) (*pb.GetBalanceResponse, error) {
	return c.ledger.GetBalance(ctx, &pb.GetBalanceRequest{
		LedgerId: ledgerID,
		Account:  account,
		Asset:    asset,
	})
}

// GRPC returns the raw gRPC client for advanced usage.
func (c *Client) GRPC() pb.LedgerServiceClient {
	return c.ledger
}
