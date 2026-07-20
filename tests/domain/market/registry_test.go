package market_test

import (
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/domain/market"
)

func TestRegistryValidatesAndCopiesCatalog(t *testing.T) {
	catalog := validCatalog()
	registry, err := market.NewRegistry(catalog)
	if err != nil {
		t.Fatal(err)
	}

	catalog.Pools[0].Tokens[0] = "mutated"
	pool, ok := registry.Pool("pool-a")
	if !ok || pool.Tokens[0] != "base-a" {
		t.Fatalf("registry retained caller-owned pool slice: %+v", pool)
	}
	pool.Tokens[0] = "mutated-again"
	pool, _ = registry.Pool("pool-a")
	if pool.Tokens[0] != "base-a" {
		t.Fatal("registry exposed its internal pool slice")
	}
}

func TestRegistryRejectsInvalidRelationships(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*market.Catalog)
		message string
	}{
		{
			name: "duplicate asset",
			mutate: func(c *market.Catalog) {
				c.Assets = append(c.Assets, c.Assets[0])
			},
			message: "duplicate asset",
		},
		{
			name: "unknown token asset",
			mutate: func(c *market.Catalog) {
				c.Tokens[0].Asset = "missing"
			},
			message: "unknown asset",
		},
		{
			name: "same pair assets",
			mutate: func(c *market.Catalog) {
				c.Pairs[0].QuoteAsset = c.Pairs[0].BaseAsset
			},
			message: "distinct assets",
		},
		{
			name: "pool token on another chain",
			mutate: func(c *market.Catalog) {
				c.Pools[0].Tokens[0] = "base-b"
			},
			message: "belongs to chain",
		},
		{
			name: "discontinuous path",
			mutate: func(c *market.Catalog) {
				c.Paths[0].Hops = append(c.Paths[0].Hops, market.Hop{Pool: "pool-a", TokenIn: "base-a", TokenOut: "quote-a"})
			},
			message: "discontinuous",
		},
		{
			name: "market endpoint mismatch",
			mutate: func(c *market.Catalog) {
				c.Markets[0].BaseToken = "quote-a"
			},
			message: "endpoint assets",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := validCatalog()
			test.mutate(&catalog)
			_, err := market.NewRegistry(catalog)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected error containing %q, got %v", test.message, err)
			}
		})
	}
}

func validCatalog() market.Catalog {
	return market.Catalog{
		Chains: []market.Chain{{ID: "chain-a"}, {ID: "chain-b"}},
		Assets: []market.Asset{{ID: "base", Symbol: "BASE"}, {ID: "quote", Symbol: "QUOTE"}},
		Tokens: []market.Token{
			{ID: "base-a", Asset: "base", Chain: "chain-a", Decimals: 18, Symbol: "BASE"},
			{ID: "quote-a", Asset: "quote", Chain: "chain-a", Decimals: 6, Symbol: "QUOTE"},
			{ID: "base-b", Asset: "base", Chain: "chain-b", Decimals: 8, Symbol: "BASE"},
			{ID: "quote-b", Asset: "quote", Chain: "chain-b", Decimals: 6, Symbol: "QUOTE"},
		},
		Venues: []market.Venue{{ID: "venue"}},
		Pairs:  []market.Pair{{ID: "base-quote", BaseAsset: "base", QuoteAsset: "quote"}},
		Pools: []market.Pool{
			{ID: "pool-a", Venue: "venue", Chain: "chain-a", Tokens: []market.TokenID{"base-a", "quote-a"}, Adapter: "constant_product"},
			{ID: "pool-b", Venue: "venue", Chain: "chain-b", Tokens: []market.TokenID{"base-b", "quote-b"}, Adapter: "constant_product"},
		},
		Paths: []market.Path{
			{ID: "path-a", Chain: "chain-a", Hops: []market.Hop{{Pool: "pool-a", TokenIn: "base-a", TokenOut: "quote-a"}}},
			{ID: "path-b", Chain: "chain-b", Hops: []market.Hop{{Pool: "pool-b", TokenIn: "base-b", TokenOut: "quote-b"}}},
		},
		Markets: []market.Market{
			{ID: "market-a", Pair: "base-quote", Chain: "chain-a", Path: "path-a", BaseToken: "base-a", QuoteToken: "quote-a"},
			{ID: "market-b", Pair: "base-quote", Chain: "chain-b", Path: "path-b", BaseToken: "base-b", QuoteToken: "quote-b"},
		},
	}
}
