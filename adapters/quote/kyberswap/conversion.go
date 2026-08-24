package kyberswap

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type ConversionConfig struct {
	ID        market.SourceID
	Chain     string
	Origin    string
	Addresses map[market.TokenID]string
	Source    *Source
}

type ConversionSource struct{ config ConversionConfig }

func NewConversionSource(config ConversionConfig) (*ConversionSource, error) {
	if config.ID == "" || strings.TrimSpace(config.Chain) == "" || config.Source == nil || len(config.Addresses) < 2 {
		return nil, fmt.Errorf("KyberSwap conversion source is incomplete")
	}
	return &ConversionSource{config: config}, nil
}

func (s *ConversionSource) ID() market.SourceID { return s.config.ID }

func (s *ConversionSource) QuoteConversion(ctx context.Context,
	request quoteport.ConversionRequest) (market.TokenAmount, error) {
	in := s.config.Addresses[request.Input.Token()]
	out := s.config.Addresses[request.OutputToken]
	if !commonAddress(in) || !commonAddress(out) || in == out || request.Input.IsZero() {
		return market.TokenAmount{}, fmt.Errorf("KyberSwap conversion direction is unavailable")
	}
	result, err := s.config.Source.Route(ctx, RouteRequest{Chain: s.config.Chain,
		TokenIn: in, TokenOut: out, AmountIn: request.Input.Units().String(), Origin: s.config.Origin})
	if err != nil {
		return market.TokenAmount{}, err
	}
	units, ok := new(big.Int).SetString(result.AmountOut, 10)
	if !ok || units.Sign() <= 0 {
		return market.TokenAmount{}, fmt.Errorf("KyberSwap conversion output is invalid")
	}
	return market.NewTokenAmount(request.OutputToken, units)
}

var _ quoteport.ConversionProvider = (*ConversionSource)(nil)
