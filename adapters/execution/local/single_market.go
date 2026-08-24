package local

import (
	"context"
	"fmt"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// SingleMarketSource adds the smallest executable allocation to an exact
// local quote. It is valid only for a route containing one concrete pool.
type SingleMarketSource struct {
	Market market.MarketID
	Source quoteport.Source
}

func (s SingleMarketSource) QuoteExecutable(ctx context.Context, input quoteport.Input) (ExecutableQuote, error) {
	if s.Market == "" || s.Source == nil || input.Snapshot.Metadata().Market != s.Market {
		return ExecutableQuote{}, fmt.Errorf("single-market executable source is incomplete")
	}
	quote, err := s.Source.Quote(ctx, input)
	if err != nil {
		return ExecutableQuote{}, err
	}
	return ExecutableQuote{Quote: quote, Allocation: execution.RouteAllocation{
		Input: quote.AmountIn, ExpectedOutput: quote.AmountOut,
		Groups: []execution.RouteGroup{{ID: "local", InputToken: quote.AmountIn.Token(),
			OutputToken: quote.AmountOut.Token(), Branches: []execution.RouteBranch{{
				Market: s.Market, PlannedInput: quote.AmountIn.Units(), ExpectedOutput: quote.AmountOut.Units(),
			}},
		}},
	}}, nil
}

var _ ExecutableSource = SingleMarketSource{}
