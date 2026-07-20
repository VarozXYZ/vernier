package market

type Asset struct {
	ID     AssetID
	Symbol string
}

type Chain struct {
	ID ChainID
}

type Token struct {
	ID       TokenID
	Asset    AssetID
	Chain    ChainID
	Decimals uint8
	Symbol   string
}

type Venue struct {
	ID VenueID
}

type Pair struct {
	ID         PairID
	BaseAsset  AssetID
	QuoteAsset AssetID
}

type Pool struct {
	ID      PoolID
	Venue   VenueID
	Chain   ChainID
	Tokens  []TokenID
	Adapter string
}

type Hop struct {
	Pool     PoolID
	TokenIn  TokenID
	TokenOut TokenID
}

type Path struct {
	ID    PathID
	Chain ChainID
	Hops  []Hop
}

type Market struct {
	ID         MarketID
	Pair       PairID
	Chain      ChainID
	Path       PathID
	BaseToken  TokenID
	QuoteToken TokenID
}

type Catalog struct {
	Chains  []Chain
	Assets  []Asset
	Tokens  []Token
	Venues  []Venue
	Pairs   []Pair
	Pools   []Pool
	Paths   []Path
	Markets []Market
}
