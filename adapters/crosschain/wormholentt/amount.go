package wormholentt

import (
	"fmt"
	"math/big"
)

// MessageDecimals is the maximum precision encoded by an NTT transfer
// message. A peer with fewer decimals lowers the effective precision further.
const MessageDecimals uint8 = 8

// TrimTransferAmount returns the largest source-chain amount that NTT can
// represent without destroying dust, plus the residual retained by the source
// wallet. EVM NTT contracts reject untrimmed amounts with
// TransferAmountHasDust.
func TrimTransferAmount(
	amount *big.Int,
	sourceDecimals uint8,
	destinationDecimals uint8,
) (transferable *big.Int, dust *big.Int, trimmedDecimals uint8, err error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, nil, 0, fmt.Errorf("NTT transfer amount must be positive")
	}
	if sourceDecimals == 0 || destinationDecimals == 0 {
		return nil, nil, 0, fmt.Errorf("NTT token decimals must be positive")
	}
	trimmedDecimals = MessageDecimals
	if sourceDecimals < trimmedDecimals {
		trimmedDecimals = sourceDecimals
	}
	if destinationDecimals < trimmedDecimals {
		trimmedDecimals = destinationDecimals
	}
	quantum := new(big.Int).Exp(
		big.NewInt(10),
		big.NewInt(int64(sourceDecimals-trimmedDecimals)),
		nil,
	)
	dust = new(big.Int).Mod(new(big.Int).Set(amount), quantum)
	transferable = new(big.Int).Sub(new(big.Int).Set(amount), dust)
	if transferable.Sign() <= 0 {
		return nil, nil, 0, fmt.Errorf(
			"NTT transfer amount is below its transferable precision",
		)
	}
	return transferable, dust, trimmedDecimals, nil
}
