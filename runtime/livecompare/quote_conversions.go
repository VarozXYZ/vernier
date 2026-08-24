package livecompare

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/core/costing"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

// StartQuoteConversions creates two independent workers for every configured
// stable-token pair. Provider calls happen only in these workers; evaluation
// reads QuoteConversionBook from memory.
func StartQuoteConversions(ctx context.Context, config configuration.ParsedConfig,
	lookup configuration.LookupEnv, origins map[string]string, logger *slog.Logger) (*costing.QuoteConversionBook, error) {
	if len(config.QuoteConversions) == 0 {
		return nil, nil
	}
	workers := make([]*costing.QuoteConversionWorker, 0, len(config.QuoteConversions)*2)
	aliases := make([]costing.QuoteConversionAlias, 0, len(config.QuoteConversions)*2)
	for _, conversion := range config.QuoteConversions {
		profile, ok := config.QuoteSources[conversion.Source]
		if !ok || profile.Kind != "kyberswap" {
			return nil, fmt.Errorf("quote conversion %q requires KyberSwap", conversion.ID)
		}
		clientID, ok := lookup(profile.ClientIDEnv)
		if !ok || strings.TrimSpace(clientID) == "" {
			return nil, fmt.Errorf("KyberSwap client ID environment %q is unset", profile.ClientIDEnv)
		}
		client, err := kyberswap.New(kyberswap.Config{BaseURL: profile.BaseURL, ClientID: clientID})
		if err != nil {
			return nil, err
		}
		origin := strings.TrimSpace(origins[conversion.Chain])
		if origin == "" && profile.TakerEnv != "" {
			origin, _ = lookup(profile.TakerEnv)
		}
		provider, err := kyberswap.NewConversionSource(kyberswap.ConversionConfig{
			ID: market.SourceID(conversion.ID), Chain: profile.ChainSlug, Origin: origin,
			Addresses: map[market.TokenID]string{
				conversion.TokenA.Token.ID: conversion.TokenA.AddressText,
				conversion.TokenB.Token.ID: conversion.TokenB.AddressText,
			}, Source: client,
		})
		if err != nil {
			return nil, err
		}
		for directionIndex, direction := range []struct{ input, output configuration.ResolvedToken }{
			{conversion.TokenA, conversion.TokenB}, {conversion.TokenB, conversion.TokenA},
		} {
			quantity, quantityErr := market.NewAssetQuantity(direction.input.Token.Asset, conversion.Amount)
			if quantityErr != nil {
				return nil, quantityErr
			}
			input, quantityErr := quantity.ToTokenAmount(direction.input.Token)
			if quantityErr != nil {
				return nil, quantityErr
			}
			inputID, outputID := direction.input.Token.ID, direction.output.Token.ID
			var readyOnce sync.Once
			worker, workerErr := costing.NewQuoteConversionWorker(costing.QuoteConversionWorkerConfig{
				Provider: provider, Input: input, OutputToken: direction.output.Token.ID,
				RefreshInterval: conversion.RefreshInterval, TTL: conversion.TTL,
				InitialDelay: time.Duration(directionIndex) * conversion.RefreshInterval / 2,
				OnError: func(refreshErr error) {
					if errors.Is(refreshErr, context.Canceled) {
						return
					}
					if logger != nil {
						logger.Warn("quote conversion refresh failed", "conversion", conversion.ID,
							"input", inputID, "output", outputID, "error", refreshErr)
					}
				},
				OnSuccess: func(snapshot market.QuoteConversionSnapshot) {
					readyOnce.Do(func() {
						if logger != nil {
							logger.Info("quote conversion cache ready", "conversion", conversion.ID,
								"input", inputID, "output", outputID, "expires_at", snapshot.ExpiresAt)
						}
					})
				},
			})
			if workerErr != nil {
				return nil, workerErr
			}
			workers = append(workers, worker)
		}
		if conversion.BridgeToken != nil {
			bridge := *conversion.BridgeToken
			operational := conversion.TokenA
			if operational.Token.ID == bridge.Token.ID {
				operational = conversion.TokenB
			}
			peer, peerOK := quoteConversionPeer(config, conversion, bridge)
			if !peerOK {
				return nil, fmt.Errorf("quote conversion %q has no unique cross-chain quote peer", conversion.ID)
			}
			aliases = append(aliases,
				costing.QuoteConversionAlias{Input: operational.Token, Output: peer.Token,
					CanonicalInput: operational.Token, CanonicalOutput: bridge.Token},
				costing.QuoteConversionAlias{Input: peer.Token, Output: operational.Token,
					CanonicalInput: bridge.Token, CanonicalOutput: operational.Token},
			)
		}
	}
	book, err := costing.NewQuoteConversionBookWithAliases(workers, aliases)
	if err != nil {
		return nil, err
	}
	if logger != nil {
		logger.Info("quote conversion workers started", "workers", len(workers), "cross_chain_aliases", len(aliases))
	}
	for _, worker := range workers {
		worker := worker
		go func() { _ = worker.Run(ctx) }()
	}
	return book, nil
}

func quoteConversionPeer(config configuration.ParsedConfig,
	conversion configuration.ResolvedQuoteConversion,
	bridge configuration.ResolvedToken) (configuration.ResolvedToken, bool) {
	var peer configuration.ResolvedToken
	found := false
	for _, configured := range config.Markets {
		candidate := configured.Quote
		if configured.Chain == conversion.Chain || candidate.Token.Asset != bridge.Token.Asset {
			continue
		}
		if found && candidate.Token.ID != peer.Token.ID {
			return configuration.ResolvedToken{}, false
		}
		peer, found = candidate, true
	}
	return peer, found
}
