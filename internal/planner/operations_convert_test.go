package planner

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

func convertParams(src, dst string, srcAmt int64, srcAsset string, dstAmt int64, dstAsset string) ConvertParams {
	return ConvertParams{
		Source: src, Destination: dst,
		SourceAmount: big.NewInt(srcAmt), SourceAsset: srcAsset,
		DestAmount: big.NewInt(dstAmt), DestAsset: dstAsset,
		Rate: "1.0",
	}
}

func TestSubmitConvert_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "alice", "USD", 1000, 0) // alice holds 1000 USD
	p := newPostTestPlanner(fs)

	res, err := p.SubmitConvert(context.Background(), "L1",
		convertParams("alice", "market", 100, "USD", 90, "EUR"), "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.NotEmpty(t, res.ConversionID)
	assert.Len(t, fs.events, 1)
	// alice: -100 USD output, +90 EUR input.
	assert.Equal(t, int64(100), fs.getBalance("L1", "alice", "USD").output.Int64())
	assert.Equal(t, int64(90), fs.getBalance("L1", "alice", "EUR").input.Int64())
	// market: +100 USD input, -90 EUR output.
	assert.Equal(t, int64(100), fs.getBalance("L1", "market", "USD").input.Int64())
	assert.Equal(t, int64(90), fs.getBalance("L1", "market", "EUR").output.Int64())
}

func TestSubmitConvert_Validation(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "alice", "USD", 1000, 0)
	p := newPostTestPlanner(fs)

	tests := map[string]ConvertParams{
		"same asset":         convertParams("alice", "market", 100, "USD", 90, "USD"),
		"bad source":         convertParams("bad acct!", "market", 100, "USD", 90, "EUR"),
		"bad source asset":   convertParams("alice", "market", 100, "lower!", 90, "EUR"),
		"zero source amount": {Source: "alice", Destination: "market", SourceAmount: big.NewInt(0), SourceAsset: "USD", DestAmount: big.NewInt(90), DestAsset: "EUR"},
		"nil dest amount":    {Source: "alice", Destination: "market", SourceAmount: big.NewInt(100), SourceAsset: "USD", DestAmount: nil, DestAsset: "EUR"},
		"invalid rate":       {Source: "alice", Destination: "market", SourceAmount: big.NewInt(100), SourceAsset: "USD", DestAmount: big.NewInt(90), DestAsset: "EUR", Rate: "not-a-number"},
	}
	for name, params := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := p.SubmitConvert(context.Background(), "L1", params, "")
			require.Error(t, err)
			assert.Empty(t, fs.events)
		})
	}
}

func TestSubmitConvert_InsufficientFunds(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	// alice has only 50 USD but tries to convert 100.
	fs.setBalance("L1", "alice", "USD", 50, 0)
	p := newPostTestPlanner(fs)

	_, err := p.SubmitConvert(context.Background(), "L1",
		convertParams("alice", "market", 100, "USD", 90, "EUR"), "")
	require.ErrorIs(t, err, storage.ErrInsufficientFunds)
	assert.Empty(t, fs.events)
}

func TestSubmitConvert_SlippageIncludedInBalanceCheck(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "alice", "USD", 105, 0) // 100 + 5 slippage = 105 needed; exactly enough
	p := newPostTestPlanner(fs)

	params := convertParams("alice", "market", 100, "USD", 90, "EUR")
	params.SlippageAccount = "fees"
	params.SlippageAmount = big.NewInt(5)
	res, err := p.SubmitConvert(context.Background(), "L1", params, "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.Equal(t, int64(5), fs.getBalance("L1", "fees", "USD").input.Int64())
}

func TestSubmitConvert_IssuerSourceBypassesBalance(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitConvert(context.Background(), "L1",
		convertParams("_world", "market", 100, "USD", 90, "EUR"), "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
}

// Batch ALL_OR_NOTHING convert now runs validation (previously skipped entirely).
func TestSubmitBatch_Convert_NowValidates(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	intents := []BatchIntent{
		{Type: "convert", ConvertParams: ptr(convertParams("_world", "market", 100, "USD", 90, "USD"))}, // same asset -> invalid
	}
	res, err := p.SubmitBatch(context.Background(), "L1", intents, "ALL_OR_NOTHING")
	require.Error(t, err)
	assert.True(t, res.Failed)
	assert.Empty(t, fs.events, "invalid convert rolls back the batch")
}

func ptr[T any](v T) *T { return &v }
