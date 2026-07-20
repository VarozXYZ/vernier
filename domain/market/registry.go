package market

import "fmt"

// Registry is an immutable, validated catalog of market identities.
type Registry struct {
	chains  map[ChainID]Chain
	assets  map[AssetID]Asset
	tokens  map[TokenID]Token
	venues  map[VenueID]Venue
	pairs   map[PairID]Pair
	pools   map[PoolID]Pool
	paths   map[PathID]Path
	markets map[MarketID]Market
}

func NewRegistry(catalog Catalog) (*Registry, error) {
	r := &Registry{
		chains:  make(map[ChainID]Chain),
		assets:  make(map[AssetID]Asset),
		tokens:  make(map[TokenID]Token),
		venues:  make(map[VenueID]Venue),
		pairs:   make(map[PairID]Pair),
		pools:   make(map[PoolID]Pool),
		paths:   make(map[PathID]Path),
		markets: make(map[MarketID]Market),
	}

	for _, chain := range catalog.Chains {
		if chain.ID == "" {
			return nil, fmt.Errorf("chain ID is required")
		}
		if _, exists := r.chains[chain.ID]; exists {
			return nil, fmt.Errorf("duplicate chain %q", chain.ID)
		}
		r.chains[chain.ID] = chain
	}
	for _, asset := range catalog.Assets {
		if asset.ID == "" || asset.Symbol == "" {
			return nil, fmt.Errorf("asset ID and symbol are required")
		}
		if _, exists := r.assets[asset.ID]; exists {
			return nil, fmt.Errorf("duplicate asset %q", asset.ID)
		}
		r.assets[asset.ID] = asset
	}
	for _, token := range catalog.Tokens {
		if token.ID == "" || token.Symbol == "" {
			return nil, fmt.Errorf("token ID and symbol are required")
		}
		if _, exists := r.tokens[token.ID]; exists {
			return nil, fmt.Errorf("duplicate token %q", token.ID)
		}
		if _, exists := r.assets[token.Asset]; !exists {
			return nil, fmt.Errorf("token %q references unknown asset %q", token.ID, token.Asset)
		}
		if _, exists := r.chains[token.Chain]; !exists {
			return nil, fmt.Errorf("token %q references unknown chain %q", token.ID, token.Chain)
		}
		r.tokens[token.ID] = token
	}
	for _, venue := range catalog.Venues {
		if venue.ID == "" {
			return nil, fmt.Errorf("venue ID is required")
		}
		if _, exists := r.venues[venue.ID]; exists {
			return nil, fmt.Errorf("duplicate venue %q", venue.ID)
		}
		r.venues[venue.ID] = venue
	}
	for _, pair := range catalog.Pairs {
		if pair.ID == "" {
			return nil, fmt.Errorf("pair ID is required")
		}
		if _, exists := r.pairs[pair.ID]; exists {
			return nil, fmt.Errorf("duplicate pair %q", pair.ID)
		}
		if pair.BaseAsset == pair.QuoteAsset {
			return nil, fmt.Errorf("pair %q must use distinct assets", pair.ID)
		}
		if _, exists := r.assets[pair.BaseAsset]; !exists {
			return nil, fmt.Errorf("pair %q references unknown base asset %q", pair.ID, pair.BaseAsset)
		}
		if _, exists := r.assets[pair.QuoteAsset]; !exists {
			return nil, fmt.Errorf("pair %q references unknown quote asset %q", pair.ID, pair.QuoteAsset)
		}
		r.pairs[pair.ID] = pair
	}
	for _, pool := range catalog.Pools {
		if err := r.addPool(pool); err != nil {
			return nil, err
		}
	}
	for _, path := range catalog.Paths {
		if err := r.addPath(path); err != nil {
			return nil, err
		}
	}
	for _, market := range catalog.Markets {
		if err := r.addMarket(market); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Registry) addPool(pool Pool) error {
	if pool.ID == "" || pool.Adapter == "" {
		return fmt.Errorf("pool ID and adapter are required")
	}
	if _, exists := r.pools[pool.ID]; exists {
		return fmt.Errorf("duplicate pool %q", pool.ID)
	}
	if _, exists := r.venues[pool.Venue]; !exists {
		return fmt.Errorf("pool %q references unknown venue %q", pool.ID, pool.Venue)
	}
	if _, exists := r.chains[pool.Chain]; !exists {
		return fmt.Errorf("pool %q references unknown chain %q", pool.ID, pool.Chain)
	}
	if len(pool.Tokens) < 2 {
		return fmt.Errorf("pool %q requires at least two tokens", pool.ID)
	}
	seen := make(map[TokenID]struct{}, len(pool.Tokens))
	for _, tokenID := range pool.Tokens {
		token, exists := r.tokens[tokenID]
		if !exists {
			return fmt.Errorf("pool %q references unknown token %q", pool.ID, tokenID)
		}
		if token.Chain != pool.Chain {
			return fmt.Errorf("pool %q token %q belongs to chain %q", pool.ID, tokenID, token.Chain)
		}
		if _, duplicate := seen[tokenID]; duplicate {
			return fmt.Errorf("pool %q repeats token %q", pool.ID, tokenID)
		}
		seen[tokenID] = struct{}{}
	}
	pool.Tokens = append([]TokenID(nil), pool.Tokens...)
	r.pools[pool.ID] = pool
	return nil
}

func (r *Registry) addPath(path Path) error {
	if path.ID == "" {
		return fmt.Errorf("path ID is required")
	}
	if _, exists := r.paths[path.ID]; exists {
		return fmt.Errorf("duplicate path %q", path.ID)
	}
	if _, exists := r.chains[path.Chain]; !exists {
		return fmt.Errorf("path %q references unknown chain %q", path.ID, path.Chain)
	}
	if len(path.Hops) == 0 {
		return fmt.Errorf("path %q requires at least one hop", path.ID)
	}
	for index, hop := range path.Hops {
		pool, exists := r.pools[hop.Pool]
		if !exists {
			return fmt.Errorf("path %q references unknown pool %q", path.ID, hop.Pool)
		}
		if pool.Chain != path.Chain {
			return fmt.Errorf("path %q pool %q belongs to chain %q", path.ID, pool.ID, pool.Chain)
		}
		if hop.TokenIn == hop.TokenOut || !poolContains(pool, hop.TokenIn) || !poolContains(pool, hop.TokenOut) {
			return fmt.Errorf("path %q has invalid hop %d", path.ID, index)
		}
		if index > 0 && path.Hops[index-1].TokenOut != hop.TokenIn {
			return fmt.Errorf("path %q is discontinuous at hop %d", path.ID, index)
		}
	}
	path.Hops = append([]Hop(nil), path.Hops...)
	r.paths[path.ID] = path
	return nil
}

func (r *Registry) addMarket(candidate Market) error {
	if candidate.ID == "" {
		return fmt.Errorf("market ID is required")
	}
	if _, exists := r.markets[candidate.ID]; exists {
		return fmt.Errorf("duplicate market %q", candidate.ID)
	}
	pair, exists := r.pairs[candidate.Pair]
	if !exists {
		return fmt.Errorf("market %q references unknown pair %q", candidate.ID, candidate.Pair)
	}
	path, exists := r.paths[candidate.Path]
	if !exists {
		return fmt.Errorf("market %q references unknown path %q", candidate.ID, candidate.Path)
	}
	if candidate.Chain != path.Chain {
		return fmt.Errorf("market %q and path %q use different chains", candidate.ID, path.ID)
	}
	base, baseExists := r.tokens[candidate.BaseToken]
	quote, quoteExists := r.tokens[candidate.QuoteToken]
	if !baseExists || !quoteExists {
		return fmt.Errorf("market %q references unknown endpoint tokens", candidate.ID)
	}
	if base.Chain != candidate.Chain || quote.Chain != candidate.Chain {
		return fmt.Errorf("market %q endpoint tokens must belong to chain %q", candidate.ID, candidate.Chain)
	}
	if base.Asset != pair.BaseAsset || quote.Asset != pair.QuoteAsset {
		return fmt.Errorf("market %q endpoint assets do not match pair %q", candidate.ID, pair.ID)
	}
	if path.Hops[0].TokenIn != candidate.BaseToken || path.Hops[len(path.Hops)-1].TokenOut != candidate.QuoteToken {
		return fmt.Errorf("market %q path endpoints do not match base and quote tokens", candidate.ID)
	}
	r.markets[candidate.ID] = candidate
	return nil
}

func poolContains(pool Pool, token TokenID) bool {
	for _, candidate := range pool.Tokens {
		if candidate == token {
			return true
		}
	}
	return false
}

func (r *Registry) Chain(id ChainID) (Chain, bool)    { value, ok := r.chains[id]; return value, ok }
func (r *Registry) Asset(id AssetID) (Asset, bool)    { value, ok := r.assets[id]; return value, ok }
func (r *Registry) Token(id TokenID) (Token, bool)    { value, ok := r.tokens[id]; return value, ok }
func (r *Registry) Venue(id VenueID) (Venue, bool)    { value, ok := r.venues[id]; return value, ok }
func (r *Registry) Pair(id PairID) (Pair, bool)       { value, ok := r.pairs[id]; return value, ok }
func (r *Registry) Market(id MarketID) (Market, bool) { value, ok := r.markets[id]; return value, ok }

func (r *Registry) Pool(id PoolID) (Pool, bool) {
	value, ok := r.pools[id]
	value.Tokens = append([]TokenID(nil), value.Tokens...)
	return value, ok
}

func (r *Registry) Path(id PathID) (Path, bool) {
	value, ok := r.paths[id]
	value.Hops = append([]Hop(nil), value.Hops...)
	return value, ok
}
