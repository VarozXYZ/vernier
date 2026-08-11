package livecanary

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type RefuelNetwork interface {
	NativeBalance(context.Context) (*big.Int, error)
	AwaitRefuel(
		context.Context,
		execution.TransactionIdentity,
		*big.Int,
	) (after, received, fee *big.Int, err error)
	ReconcileRefuel(
		context.Context,
		execution.TransactionIdentity,
		*big.Int,
	) (settled bool, failed bool, after, received, fee *big.Int, err error)
}

type SwapRefuelExecutorConfig struct {
	Chain              market.ChainID
	Market             market.MarketID
	Account            execution.AccountID
	QuoteToken         market.Token
	NativeToken        market.Token
	NativeAsset        market.AssetID
	Binding            SwapBinding
	Network            RefuelNetwork
	Prices             *CostValuator
	Clock              func() time.Time
	ConfirmTimeout     time.Duration
	LocalNativeBalance func() (*big.Int, error)
	OnSettled          func(executionport.RefuelRecord) error
}

type SwapRefuelExecutor struct {
	config SwapRefuelExecutorConfig
}

func NewSwapRefuelExecutor(
	config SwapRefuelExecutorConfig,
) (*SwapRefuelExecutor, error) {
	if config.Chain == "" || config.Market == "" || config.Account == "" ||
		config.QuoteToken.ID == "" || config.NativeToken.ID == "" ||
		config.NativeAsset == "" || config.Binding.Validator == nil ||
		config.Binding.TxManager == nil || config.Network == nil ||
		config.Prices == nil {
		return nil, fmt.Errorf("swap refuel executor configuration is incomplete")
	}
	if config.Binding.RefuelValidator != nil {
		config.Binding.Validator = config.Binding.RefuelValidator
	}
	if _, ok := config.Binding.TxManager.(chainport.PreparedTransactionSimulator); !ok {
		return nil, fmt.Errorf("refuel TxManager has no simulator")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.ConfirmTimeout <= 0 {
		config.ConfirmTimeout = 2 * time.Minute
	}
	return &SwapRefuelExecutor{config: config}, nil
}

func (e *SwapRefuelExecutor) Chain() market.ChainID {
	return e.config.Chain
}

func (e *SwapRefuelExecutor) Balance(
	ctx context.Context,
) (executionport.RefuelBalance, error) {
	units, err := e.nativeBalance(ctx)
	if err != nil {
		return executionport.RefuelBalance{}, err
	}
	native, err := market.NewAssetQuantity(
		e.config.NativeAsset,
		new(big.Rat).SetFrac(
			units,
			decimalScale(e.config.NativeToken.Decimals),
		),
	)
	if err != nil {
		return executionport.RefuelBalance{}, err
	}
	price, ok := e.config.Prices.Price(e.config.NativeAsset)
	if !ok {
		return executionport.RefuelBalance{}, fmt.Errorf(
			"cached native price for %s is unavailable",
			e.config.NativeAsset,
		)
	}
	value, err := market.NewAssetQuantity(
		e.config.QuoteToken.Asset,
		new(big.Rat).Mul(native.Rat(), price.Value),
	)
	if err != nil {
		return executionport.RefuelBalance{}, err
	}
	return executionport.RefuelBalance{
		Chain: e.config.Chain, Native: native, QuoteValue: value,
		ObservedAt: e.config.Clock().UTC(),
	}, nil
}

func (e *SwapRefuelExecutor) Execute(
	ctx context.Context,
	spend market.AssetQuantity,
	journal executionport.RefuelJournal,
) (executionport.RefuelRecord, error) {
	if journal == nil {
		return executionport.RefuelRecord{},
			fmt.Errorf("refuel journal is unavailable")
	}
	record, prepared, beforeUnits, err := e.prepare(ctx, spend)
	if err != nil {
		// Preparation happens before durable persistence and broadcast. Its
		// failure is therefore definitive and must never be promoted to an
		// uncertain on-chain outcome by the generic error classifier.
		return record, executionport.NewRecoveryError(
			executionport.RecoveryFailureTemporary,
			err,
		)
	}
	if err := journal.CreateRefuel(
		context.WithoutCancel(ctx),
		record,
	); err != nil {
		// No broadcast is permitted before the prepared record is durable.
		return record, executionport.NewRecoveryError(
			executionport.RecoveryFailureTemporary,
			err,
		)
	}
	broadcast, err := e.config.Binding.TxManager.Broadcast(ctx, prepared)
	if err != nil || !broadcast.Accepted {
		record.LastError = errorText(err, "refuel broadcaster did not accept")
		if broadcast.Disposition == chainport.BroadcastRejected {
			record.State = executionport.RefuelFailed
			_ = journal.FinishRefuel(context.WithoutCancel(ctx), record)
			return record, executionport.NewRecoveryError(
				executionport.RecoveryFailureTemporary,
				fmt.Errorf("%s", record.LastError),
			)
		}
		record.State = executionport.RefuelOutcomeUnknown
		_ = journal.FinishRefuel(context.WithoutCancel(ctx), record)
		return record, executionport.NewRecoveryError(
			executionport.RecoveryFailureUncertain,
			fmt.Errorf("%s", record.LastError),
		)
	}
	if err := journal.MarkRefuelBroadcast(
		context.WithoutCancel(ctx),
		record.ID,
		prepared.Identity,
	); err != nil {
		return record, executionport.NewRecoveryError(
			executionport.RecoveryFailureUncertain,
			err,
		)
	}
	waitCtx, cancel := context.WithTimeout(ctx, e.config.ConfirmTimeout)
	afterUnits, receivedUnits, feeUnits, err :=
		e.config.Network.AwaitRefuel(
			waitCtx,
			prepared.Identity,
			beforeUnits,
		)
	cancel()
	if err != nil {
		record.State = executionport.RefuelOutcomeUnknown
		record.LastError = err.Error()
		_ = journal.FinishRefuel(context.WithoutCancel(ctx), record)
		return record, executionport.NewRecoveryError(
			executionport.RecoveryFailureUncertain,
			err,
		)
	}
	return e.finishRecord(
		ctx,
		record,
		afterUnits,
		receivedUnits,
		feeUnits,
		journal,
		true,
	)
}

func (e *SwapRefuelExecutor) Preview(
	ctx context.Context,
	spend market.AssetQuantity,
) (executionport.RefuelRecord, error) {
	record, _, _, err := e.prepare(ctx, spend)
	return record, err
}

func (e *SwapRefuelExecutor) prepare(
	ctx context.Context,
	spend market.AssetQuantity,
) (executionport.RefuelRecord, chainport.PreparedTransaction, *big.Int, error) {
	if spend.Asset() != e.config.QuoteToken.Asset || spend.Sign() <= 0 {
		return executionport.RefuelRecord{},
			chainport.PreparedTransaction{}, nil,
			fmt.Errorf("refuel spend is invalid")
	}
	inputUnits := new(big.Int).Quo(
		new(big.Int).Mul(
			spend.Rat().Num(),
			decimalScale(e.config.QuoteToken.Decimals),
		),
		spend.Rat().Denom(),
	)
	if inputUnits.Sign() <= 0 {
		return executionport.RefuelRecord{},
			chainport.PreparedTransaction{}, nil,
			fmt.Errorf("refuel spend rounds to zero")
	}
	input, err := market.NewTokenAmount(
		e.config.QuoteToken.ID,
		inputUnits,
	)
	if err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, err
	}
	if e.config.Binding.SpendableBalance != nil {
		available, balanceErr := e.config.Binding.SpendableBalance.SpendableBalance(
			ctx,
			input.Token(),
		)
		if balanceErr != nil {
			return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, balanceErr
		}
		if available == nil || available.Cmp(input.Units()) < 0 {
			return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil,
				fmt.Errorf("local quote balance is insufficient for refuel")
		}
	}
	beforeUnits, err := e.nativeBalance(ctx)
	if err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, err
	}
	before, err := market.NewAssetQuantity(
		e.config.NativeAsset,
		new(big.Rat).SetFrac(
			beforeUnits,
			decimalScale(e.config.NativeToken.Decimals),
		),
	)
	if err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, err
	}
	id, err := newRefuelID()
	if err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, err
	}
	stage := execution.SequentialStagePlan{
		Ordinal: 1, Stage: execution.StageBuy,
		SourceChain: e.config.Chain,
		InputToken:  input.Token(), OutputToken: e.config.NativeToken.ID,
		Market: e.config.Market,
	}
	request := execution.SequentialStageRequest{
		Operation: execution.OperationID(id),
		Plan:      execution.PlanID("gas-refuel"),
		Stage:     stage,
		Input:     input,
	}
	validation, err := swapValidationRequest(
		request,
		e.config.Account,
		e.config.Clock().UTC(),
	)
	if err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, err
	}
	artifact, err := e.config.Binding.Validator.Validate(ctx, validation)
	if err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, fmt.Errorf(
			"build refuel swap: %w",
			err,
		)
	}
	var prepared chainport.PreparedTransaction
	compactRebuilds := 0
	for {
		artifact.Leg.ExpectedOutput = artifact.ValidatedQuote.AmountOut
		prepared, err = e.config.Binding.TxManager.Prepare(ctx, artifact)
		if err == nil {
			break
		}
		var oversized *executionport.ArtifactTooLargeError
		compact, supportsCompact :=
			e.config.Binding.Validator.(executionport.CompactValidator)
		if !errors.As(err, &oversized) ||
			!supportsCompact || compactRebuilds >= 3 {
			return executionport.RefuelRecord{},
				chainport.PreparedTransaction{}, nil, err
		}
		validation.RequestedAt = e.config.Clock().UTC()
		artifact, err = compact.ValidateCompact(
			ctx,
			validation,
			artifact,
		)
		if err != nil {
			return executionport.RefuelRecord{},
				chainport.PreparedTransaction{}, nil,
				fmt.Errorf(
					"compact refuel artifact after %d-byte transaction: %w",
					oversized.ActualBytes,
					err,
				)
		}
		compactRebuilds++
	}
	simulator := e.config.Binding.TxManager.(chainport.PreparedTransactionSimulator)
	if err := simulator.SimulatePrepared(ctx, prepared); err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, fmt.Errorf(
			"simulate refuel swap: %w",
			err,
		)
	}
	record := executionport.RefuelRecord{
		ID: id, Chain: e.config.Chain,
		State: executionport.RefuelPrepared,
		Input: input, NativeAsset: e.config.NativeAsset,
		BalanceBefore: before, Identity: prepared.Identity,
		CreatedAt: e.config.Clock().UTC(),
		UpdatedAt: e.config.Clock().UTC(),
	}
	record.NativeReceived, err = quantityFromUnits(
		e.config.NativeAsset,
		artifact.ValidatedQuote.AmountOut.Units(),
		e.config.NativeToken.Decimals,
	)
	if err != nil {
		return executionport.RefuelRecord{}, chainport.PreparedTransaction{}, nil, err
	}
	return record, prepared, beforeUnits, nil
}

func (e *SwapRefuelExecutor) Reconcile(
	ctx context.Context,
	record executionport.RefuelRecord,
	journal executionport.RefuelJournal,
) (executionport.RefuelRecord, error) {
	beforeUnits := assetUnits(
		record.BalanceBefore,
		e.config.NativeToken.Decimals,
	)
	settled, failed, after, received, fee, err :=
		e.config.Network.ReconcileRefuel(
			ctx,
			record.Identity,
			beforeUnits,
		)
	if err != nil {
		return record, err
	}
	if !settled {
		return record, executionport.NewRecoveryError(
			executionport.RecoveryFailureUncertain,
			fmt.Errorf("refuel transaction remains uncertain"),
		)
	}
	if failed {
		record.State = executionport.RefuelFailed
		record.LastError = "refuel transaction reverted"
		if err := journal.FinishRefuel(
			context.WithoutCancel(ctx),
			record,
		); err != nil {
			return record, err
		}
		return record, errors.New(record.LastError)
	}
	return e.finishRecord(ctx, record, after, received, fee, journal, false)
}

func (e *SwapRefuelExecutor) finishRecord(
	ctx context.Context,
	record executionport.RefuelRecord,
	afterUnits, receivedUnits, feeUnits *big.Int,
	journal executionport.RefuelJournal,
	applyLocal bool,
) (executionport.RefuelRecord, error) {
	var err error
	record.BalanceAfter, err = quantityFromUnits(
		e.config.NativeAsset,
		afterUnits,
		e.config.NativeToken.Decimals,
	)
	if err != nil {
		return record, err
	}
	record.NativeReceived, err = quantityFromUnits(
		e.config.NativeAsset,
		receivedUnits,
		e.config.NativeToken.Decimals,
	)
	if err != nil {
		return record, err
	}
	record.Fee, err = quantityFromUnits(
		e.config.NativeAsset,
		feeUnits,
		e.config.NativeToken.Decimals,
	)
	if err != nil {
		return record, err
	}
	record.State = executionport.RefuelCompleted
	record.UpdatedAt = e.config.Clock().UTC()
	if err := journal.FinishRefuel(
		context.WithoutCancel(ctx),
		record,
	); err != nil {
		return record, err
	}
	if applyLocal && e.config.OnSettled != nil {
		if err := e.config.OnSettled(record); err != nil {
			return record, fmt.Errorf("apply local refuel balance: %w", err)
		}
	}
	return record, nil
}

func (e *SwapRefuelExecutor) nativeBalance(ctx context.Context) (*big.Int, error) {
	if e.config.LocalNativeBalance != nil {
		return e.config.LocalNativeBalance()
	}
	return e.config.Network.NativeBalance(ctx)
}

func quantityFromUnits(
	asset market.AssetID,
	units *big.Int,
	decimals uint8,
) (market.AssetQuantity, error) {
	if units == nil || units.Sign() < 0 {
		return market.AssetQuantity{}, fmt.Errorf(
			"native amount units are invalid",
		)
	}
	return market.NewAssetQuantity(
		asset,
		new(big.Rat).SetFrac(units, decimalScale(decimals)),
	)
}

func assetUnits(value market.AssetQuantity, decimals uint8) *big.Int {
	if value.Asset() == "" {
		return new(big.Int)
	}
	return new(big.Int).Quo(
		new(big.Int).Mul(value.Rat().Num(), decimalScale(decimals)),
		value.Rat().Denom(),
	)
}

func newRefuelID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return "refuel-" + hex.EncodeToString(entropy[:]), nil
}

func errorText(err error, fallback string) string {
	if err != nil {
		return err.Error()
	}
	return fallback
}
