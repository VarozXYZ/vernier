package solana

import (
	"crypto/sha256"
	"fmt"
	"math/big"

	"filippo.io/edwards25519"
)

const pdaMarker = "ProgramDerivedAddress"

const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// DecodePublicKey decodes a Solana base58 public key without introducing a
// protocol-specific key type into the domain layer.
func DecodePublicKey(value string) ([32]byte, error) {
	decoded, err := decodeBase58(value)
	if err != nil {
		return [32]byte{}, err
	}
	if len(decoded) != 32 {
		return [32]byte{}, fmt.Errorf("public key must decode to 32 bytes")
	}
	var result [32]byte
	copy(result[:], decoded)
	return result, nil
}

func EncodePublicKey(value [32]byte) string { return encodeBase58(value[:]) }

// FindProgramAddress implements the Solana PDA derivation used by read-only
// market bootstraps. It deliberately returns no signing capability.
func FindProgramAddress(seeds [][]byte, programID [32]byte) ([32]byte, byte, error) {
	for bump := 255; bump >= 0; bump-- {
		input := make([]byte, 0, len(pdaMarker)+32+1)
		input = append(input, pdaMarker...)
		for _, seed := range seeds {
			if len(seed) > 32 {
				return [32]byte{}, 0, fmt.Errorf("PDA seed exceeds 32 bytes")
			}
			input = append(input, seed...)
		}
		input = append(input, byte(bump))
		input = append(input, programID[:]...)
		hash := sha256.Sum256(input)
		if _, err := new(edwards25519.Point).SetBytes(hash[:]); err == nil {
			continue
		}
		return hash, byte(bump), nil
	}
	return [32]byte{}, 0, fmt.Errorf("unable to find program address")
}

func decodeBase58(value string) ([]byte, error) {
	if value == "" {
		return nil, fmt.Errorf("base58 value is required")
	}
	zeros := 0
	for zeros < len(value) && value[zeros] == base58Alphabet[0] {
		zeros++
	}
	result := make([]byte, 0, len(value))
	for _, character := range value {
		index := -1
		for i := 0; i < len(base58Alphabet); i++ {
			if rune(base58Alphabet[i]) == character {
				index = i
				break
			}
		}
		if index < 0 {
			return nil, fmt.Errorf("invalid base58 character")
		}
		carry := index
		for i := len(result) - 1; i >= 0; i-- {
			carry += int(result[i]) * 58
			result[i] = byte(carry)
			carry /= 256
		}
		for carry > 0 {
			result = append([]byte{byte(carry)}, result...)
			carry /= 256
		}
	}
	decoded := make([]byte, zeros+len(result))
	copy(decoded[zeros:], result)
	return decoded, nil
}

func encodeBase58(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	number := new(big.Int).SetBytes(value)
	base := big.NewInt(58)
	zero := big.NewInt(0)
	result := ""
	for number.Cmp(zero) > 0 {
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(number, base, remainder)
		result = string(base58Alphabet[remainder.Int64()]) + result
		number = quotient
	}
	for _, value := range value {
		if value != 0 {
			break
		}
		result = string(base58Alphabet[0]) + result
	}
	return result
}
