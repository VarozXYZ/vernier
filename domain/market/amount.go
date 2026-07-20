package market

import (
	"fmt"
	"math/big"
)

// TokenAmount is an immutable, non-negative amount in a token's raw units.
type TokenAmount struct {
	token TokenID
	units *big.Int
}

func NewTokenAmount(token TokenID, units *big.Int) (TokenAmount, error) {
	if token == "" {
		return TokenAmount{}, fmt.Errorf("token is required")
	}
	if units == nil {
		return TokenAmount{}, fmt.Errorf("units are required")
	}
	if units.Sign() < 0 {
		return TokenAmount{}, fmt.Errorf("token amount cannot be negative")
	}
	return TokenAmount{token: token, units: new(big.Int).Set(units)}, nil
}

func ParseTokenAmount(token TokenID, units string) (TokenAmount, error) {
	parsed, ok := new(big.Int).SetString(units, 10)
	if !ok {
		return TokenAmount{}, fmt.Errorf("invalid integer units %q", units)
	}
	return NewTokenAmount(token, parsed)
}

func (a TokenAmount) Token() TokenID { return a.token }

func (a TokenAmount) Units() *big.Int {
	if a.units == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(a.units)
}

func (a TokenAmount) String() string { return a.Units().String() }

func (a TokenAmount) IsZero() bool { return a.Units().Sign() == 0 }

func (a TokenAmount) ToAssetQuantity(token Token) (AssetQuantity, error) {
	if token.ID != a.token {
		return AssetQuantity{}, fmt.Errorf("amount token %q does not match token %q", a.token, token.ID)
	}
	value := new(big.Rat).SetFrac(a.Units(), decimalFactor(token.Decimals))
	return NewAssetQuantity(token.Asset, value)
}

// AssetQuantity is an immutable exact quantity of an economic Asset.
// It may be negative when representing PnL or a delta.
type AssetQuantity struct {
	asset AssetID
	value *big.Rat
}

func NewAssetQuantity(asset AssetID, value *big.Rat) (AssetQuantity, error) {
	if asset == "" {
		return AssetQuantity{}, fmt.Errorf("asset is required")
	}
	if value == nil {
		return AssetQuantity{}, fmt.Errorf("value is required")
	}
	return AssetQuantity{asset: asset, value: new(big.Rat).Set(value)}, nil
}

func ParseAssetQuantity(asset AssetID, value string) (AssetQuantity, error) {
	parsed, ok := new(big.Rat).SetString(value)
	if !ok {
		return AssetQuantity{}, fmt.Errorf("invalid rational quantity %q", value)
	}
	return NewAssetQuantity(asset, parsed)
}

func (q AssetQuantity) Asset() AssetID { return q.asset }

func (q AssetQuantity) Rat() *big.Rat {
	if q.value == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(q.value)
}

func (q AssetQuantity) String() string { return q.Rat().RatString() }

func (q AssetQuantity) Decimal(places int) string { return q.Rat().FloatString(places) }

func (q AssetQuantity) Sign() int { return q.Rat().Sign() }

func (q AssetQuantity) Add(other AssetQuantity) (AssetQuantity, error) {
	if q.asset != other.asset {
		return AssetQuantity{}, fmt.Errorf("cannot add assets %q and %q", q.asset, other.asset)
	}
	return NewAssetQuantity(q.asset, new(big.Rat).Add(q.Rat(), other.Rat()))
}

func (q AssetQuantity) Sub(other AssetQuantity) (AssetQuantity, error) {
	if q.asset != other.asset {
		return AssetQuantity{}, fmt.Errorf("cannot subtract assets %q and %q", q.asset, other.asset)
	}
	return NewAssetQuantity(q.asset, new(big.Rat).Sub(q.Rat(), other.Rat()))
}

func (q AssetQuantity) Cmp(other AssetQuantity) (int, error) {
	if q.asset != other.asset {
		return 0, fmt.Errorf("cannot compare assets %q and %q", q.asset, other.asset)
	}
	return q.Rat().Cmp(other.Rat()), nil
}

// ToTokenAmount converts to raw token units and rounds toward negative infinity.
// Negative asset quantities cannot be represented as TokenAmount.
func (q AssetQuantity) ToTokenAmount(token Token) (TokenAmount, error) {
	if token.Asset != q.asset {
		return TokenAmount{}, fmt.Errorf("quantity asset %q does not match token asset %q", q.asset, token.Asset)
	}
	if q.Sign() < 0 {
		return TokenAmount{}, fmt.Errorf("negative quantity cannot become a token amount")
	}
	scaled := new(big.Rat).Mul(q.Rat(), new(big.Rat).SetInt(decimalFactor(token.Decimals)))
	units := new(big.Int).Quo(scaled.Num(), scaled.Denom())
	return NewTokenAmount(token.ID, units)
}

func decimalFactor(decimals uint8) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
}
