// Package wormholewtt implements the EVM intent boundary for Wormhole's
// legacy Wrapped Token Transfers (Token Bridge). It is intentionally separate
// from NTT because custody, payloads, and redemption contracts differ.
package wormholewtt

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

const MessageDecimals uint8 = 8

const tokenBridgeABI = `[
 {"inputs":[{"name":"token","type":"address"},{"name":"amount","type":"uint256"},{"name":"recipientChain","type":"uint16"},{"name":"recipient","type":"bytes32"},{"name":"arbiterFee","type":"uint256"},{"name":"nonce","type":"uint32"}],"name":"transferTokens","outputs":[{"name":"sequence","type":"uint64"}],"stateMutability":"payable","type":"function"},
 {"inputs":[{"name":"encodedVm","type":"bytes"}],"name":"completeTransfer","outputs":[],"stateMutability":"nonpayable","type":"function"},
 {"inputs":[{"name":"hash","type":"bytes32"}],"name":"isTransferCompleted","outputs":[{"name":"completed","type":"bool"}],"stateMutability":"view","type":"function"}
]`

type EVMAdapter struct {
	bridge common.Address
	abi    abi.ABI
}

func NewEVMAdapter(bridge common.Address) (*EVMAdapter, error) {
	if bridge == (common.Address{}) {
		return nil, fmt.Errorf("wormhole Token Bridge address is required")
	}
	parsed, err := abi.JSON(strings.NewReader(tokenBridgeABI))
	if err != nil {
		return nil, err
	}
	return &EVMAdapter{bridge: bridge, abi: parsed}, nil
}

func (a *EVMAdapter) Bridge() common.Address { return a.bridge }

func (a *EVMAdapter) BuildTransfer(token common.Address, amount *big.Int, destinationChain uint16,
	recipient [32]byte, nonce uint32) ([]byte, error) {
	if token == (common.Address{}) || amount == nil || amount.Sign() <= 0 ||
		destinationChain == 0 || recipient == ([32]byte{}) {
		return nil, fmt.Errorf("WTT transfer intent is incomplete")
	}
	return a.abi.Pack("transferTokens", token, amount, destinationChain, recipient, new(big.Int), nonce)
}

func (a *EVMAdapter) BuildRedeem(vaa []byte) ([]byte, error) {
	if len(vaa) == 0 {
		return nil, fmt.Errorf("WTT redeem requires a signed VAA")
	}
	return a.abi.Pack("completeTransfer", append([]byte(nil), vaa...))
}

func (a *EVMAdapter) BuildCompletionCheck(vaa []byte) ([]byte, [32]byte, error) {
	hash, err := VAAHash(vaa)
	if err != nil {
		return nil, [32]byte{}, err
	}
	payload, err := a.abi.Pack("isTransferCompleted", hash)
	return payload, hash, err
}

// VAAHash returns the canonical Wormhole VM hash. Guardian signatures are an
// envelope and are not part of the hash consumed by Token Bridge.
func VAAHash(vaa []byte) ([32]byte, error) {
	if len(vaa) < 6 || vaa[0] != 1 {
		return [32]byte{}, fmt.Errorf("WTT completion check requires a version-1 signed VAA")
	}
	bodyOffset := 6 + int(vaa[5])*66
	if bodyOffset >= len(vaa) {
		return [32]byte{}, fmt.Errorf("WTT signed VAA envelope is truncated")
	}
	var hash [32]byte
	copy(hash[:], crypto.Keccak256(vaa[bodyOffset:]))
	return hash, nil
}

// TrimTransferAmount preserves source-wallet dust that the Token Bridge would
// otherwise truncate when normalizing a transfer to at most eight decimals.
func TrimTransferAmount(amount *big.Int, sourceDecimals uint8) (transferable, dust *big.Int, err error) {
	if amount == nil || amount.Sign() <= 0 || sourceDecimals == 0 {
		return nil, nil, fmt.Errorf("WTT transfer amount and decimals must be positive")
	}
	if sourceDecimals <= MessageDecimals {
		return new(big.Int).Set(amount), new(big.Int), nil
	}
	quantum := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(sourceDecimals-MessageDecimals)), nil)
	dust = new(big.Int).Mod(new(big.Int).Set(amount), quantum)
	transferable = new(big.Int).Sub(new(big.Int).Set(amount), dust)
	if transferable.Sign() <= 0 {
		return nil, nil, fmt.Errorf("WTT transfer amount is below transferable precision")
	}
	return transferable, dust, nil
}
