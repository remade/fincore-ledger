package sdk

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// Client wraps the gRPC LedgerService with a fluent Go API.
type Client struct {
	conn   *grpc.ClientConn
	ledger pb.LedgerServiceClient
}

// options collects the settings applied by Option values before New dials.
type options struct {
	bearerToken string
	dialOpts    []grpc.DialOption
}

// Option configures a Client created by New.
type Option func(*options)

// WithBearerToken attaches the given JWT as an "authorization: Bearer <token>"
// header on every unary and streaming RPC. The server always requires
// authentication, so callers must supply a token.
func WithBearerToken(token string) Option {
	return func(o *options) { o.bearerToken = token }
}

// WithDialOptions appends raw gRPC dial options (e.g. custom transport
// credentials). They are applied after the defaults, so a transport-credentials
// option here overrides the default insecure credentials.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, opts...) }
}

// New creates a new SDK client connected to the given address.
func New(addr string, opts ...Option) (*Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// Default to insecure transport; a caller's WithDialOptions credentials,
	// appended last, override it.
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if o.bearerToken != "" {
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(bearerUnaryInterceptor(o.bearerToken)),
			grpc.WithStreamInterceptor(bearerStreamInterceptor(o.bearerToken)),
		)
	}
	dialOpts = append(dialOpts, o.dialOpts...)

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to ledger at %s: %w", addr, err)
	}
	return &Client{
		conn:   conn,
		ledger: pb.NewLedgerServiceClient(conn),
	}, nil
}

// withBearer adds the bearer token to the outgoing gRPC metadata.
func withBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// bearerUnaryInterceptor injects the bearer token on every unary call. The req
// and reply parameters are typed any to satisfy the grpc.UnaryClientInterceptor
// signature.
func bearerUnaryInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(withBearer(ctx, token), method, req, reply, cc, opts...)
	}
}

// bearerStreamInterceptor injects the bearer token on every streaming call.
func bearerStreamInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(withBearer(ctx, token), desc, cc, method, opts...)
	}
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
