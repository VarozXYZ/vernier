package livecanary

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

// ConversionAwareTransfer keeps the operational quote token separate from
// the token transported by a bridge. On the conversion chain it swaps before
// an outbound bridge and after an inbound bridge. Every conversion identity
// is persisted before primary-only broadcast and is recoverable independently
// from the bridge transaction.
type ConversionAwareTransfer struct {
	Bridge           crosschainport.RecoverableLiveTransferService
	ConversionChain  market.ChainID
	OperationalToken market.TokenID
	TransitToken     market.TokenID
	Market           market.MarketID
	Binding          SwapBinding
	SlippageBPS      uint16
	Clock            func() time.Time
	PollInterval     time.Duration
	Timeout          time.Duration
}

func NewConversionAwareTransfer(config ConversionAwareTransfer) (*ConversionAwareTransfer, error) {
	if config.Bridge == nil || config.ConversionChain == "" || config.OperationalToken == "" ||
		config.TransitToken == "" || config.OperationalToken == config.TransitToken || config.Market == "" ||
		config.Binding.ConversionValidator == nil || config.Binding.ConversionEstimator == nil ||
		config.Binding.TxManager == nil || config.Binding.Account == "" || config.SlippageBPS > 10_000 {
		return nil, fmt.Errorf("conversion-aware transfer configuration is incomplete")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 500 * time.Millisecond
	}
	if config.Timeout <= 0 {
		config.Timeout = 20 * time.Minute
	}
	return &config, nil
}

func (s *ConversionAwareTransfer) Transfer(ctx context.Context, request execution.SequentialStageRequest,
	journal executionport.SequentialJournal) (crosschainport.LiveTransferResult, error) {
	return s.transfer(ctx, request, nil, journal)
}

func (s *ConversionAwareTransfer) RecoverTransfer(ctx context.Context, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	return s.transfer(ctx, request, records, journal)
}

func (s *ConversionAwareTransfer) transfer(ctx context.Context, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	if err := request.Validate(); err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	if journal == nil {
		return crosschainport.LiveTransferResult{}, fmt.Errorf("conversion-aware transfer journal is unavailable")
	}
	switch {
	case request.Stage.SourceChain == s.ConversionChain && request.Input.Token() == s.OperationalToken:
		converted, err := s.convert(ctx, request, request.Input, s.TransitToken, "quote_convert_source", records, journal)
		if err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		bridgeRequest := request
		bridgeRequest.Input = converted.output
		bridgeRequest.Stage.InputToken = s.TransitToken
		bridged, err := s.bridge(ctx, bridgeRequest, records, journal)
		if err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		bridged.ActualInput = request.Input
		bridged.SourceIdentity = converted.identity
		bridged.Costs = append(converted.costs, bridged.Costs...)
		bridged.Evidence = "quote_conversion_then_" + bridged.Evidence
		return bridged, nil

	case request.Stage.DestinationChain == s.ConversionChain && request.Stage.OutputToken == s.OperationalToken:
		bridgeRequest := request
		bridgeRequest.Stage.OutputToken = s.TransitToken
		bridged, err := s.bridge(ctx, bridgeRequest, records, journal)
		if err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		converted, err := s.convert(ctx, request, bridged.ActualOutput, s.OperationalToken,
			"quote_convert_destination", records, journal)
		if err != nil {
			return crosschainport.LiveTransferResult{}, err
		}
		bridged.ActualInput = request.Input
		bridged.ActualOutput = converted.output
		bridged.DestinationIdentity = converted.identity
		bridged.DestinationBalanceBefore = nil
		bridged.DestinationBalanceAfter = nil
		bridged.Costs = append(bridged.Costs, converted.costs...)
		bridged.Evidence += "_then_quote_conversion"
		return bridged, nil
	default:
		return crosschainport.LiveTransferResult{}, fmt.Errorf("quote restoration does not match operational/transit token topology")
	}
}

func (s *ConversionAwareTransfer) bridge(ctx context.Context, request execution.SequentialStageRequest,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	if len(records) == 0 {
		return s.Bridge.Transfer(ctx, request, journal)
	}
	return s.Bridge.RecoverTransfer(ctx, request, records, journal)
}

type conversionSettlement struct {
	input, output market.TokenAmount
	identity      execution.TransactionIdentity
	costs         []execution.CostComponent
}

func (s *ConversionAwareTransfer) convert(ctx context.Context, request execution.SequentialStageRequest,
	input market.TokenAmount, outputToken market.TokenID, phase string,
	records []executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (conversionSettlement, error) {
	for _, record := range records {
		if record.Ordinal == request.Stage.Ordinal && record.Phase == phase {
			return s.reconcileConversion(ctx, request, input, outputToken, phase, record, journal)
		}
	}
	expected, err := s.Binding.ConversionEstimator.QuoteExactInput(ctx, input, outputToken)
	if err != nil {
		return conversionSettlement{}, fmt.Errorf("quote restoration conversion: %w", err)
	}
	now := s.Clock().UTC()
	seed := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s", request.Operation, phase, input.Units())))
	discovery, err := market.NewQuote(market.Quote{Source: "quote-conversion", Market: s.Market,
		SnapshotVersion: 1, SnapshotHash: seed, ResponseHash: seed,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		AmountIn: input, AmountOut: expected, QuotedAt: now})
	if err != nil {
		return conversionSettlement{}, err
	}
	leg := execution.Leg{ID: execution.StepID(fmt.Sprintf("%s/%d/%s", request.Operation, request.Stage.Ordinal, phase)),
		Side: execution.LegSell, Chain: s.ConversionChain, Account: s.Binding.Account,
		Market: s.Market, Input: input, ExpectedOutput: expected}
	artifact, err := s.Binding.ConversionValidator.Validate(ctx, executionport.ValidationRequest{
		Operation: request.Operation, Leg: leg, Discovery: discovery,
		Slippage: &executionport.SlippageConstraint{BPS: s.SlippageBPS}, RequestedAt: now})
	if err != nil {
		return conversionSettlement{}, fmt.Errorf("build and simulate quote restoration conversion: %w", err)
	}
	prepared, err := s.Binding.TxManager.Prepare(ctx, artifact)
	if err != nil {
		return conversionSettlement{}, err
	}
	if err := journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{Operation: request.Operation,
		Ordinal: request.Stage.Ordinal, Phase: phase, Identity: prepared.Identity, PreparedAt: prepared.PreparedAt,
		SimulatedInput: artifact.ValidatedQuote.AmountIn, SimulatedOutput: artifact.ValidatedQuote.AmountOut,
		SimulationEvidence: "provider_build_eth_call"}); err != nil {
		return conversionSettlement{}, err
	}
	broadcast, err := chainport.BroadcastPrimary(ctx, s.Binding.TxManager, prepared)
	if err != nil || !broadcast.Accepted {
		state, disposition := "outcome_unknown", executionport.DispositionPossible
		if broadcast.Disposition == chainport.BroadcastRejected {
			state, disposition = "rejected", executionport.DispositionRejected
		}
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, state)
		if err == nil {
			err = fmt.Errorf("quote restoration conversion broadcast was not accepted")
		}
		return conversionSettlement{}, executionport.NewStageError(disposition, err)
	}
	if err := journal.MarkTransaction(ctx, request.Operation, request.Stage.Ordinal, phase, "broadcast"); err != nil {
		return conversionSettlement{}, executionport.NewStageError(executionport.DispositionPossible, err)
	}
	record := executionport.SequentialTransactionRecord{Operation: request.Operation, Ordinal: request.Stage.Ordinal,
		Phase: phase, Identity: prepared.Identity, Status: "broadcast", PreparedAt: prepared.PreparedAt}
	return s.reconcileConversion(ctx, request, input, outputToken, phase, record, journal)
}

func (s *ConversionAwareTransfer) reconcileConversion(ctx context.Context, request execution.SequentialStageRequest,
	input market.TokenAmount, outputToken market.TokenID, phase string,
	record executionport.SequentialTransactionRecord, journal executionport.SequentialJournal,
) (conversionSettlement, error) {
	expected, err := market.NewTokenAmount(outputToken, big.NewInt(1))
	if err != nil {
		return conversionSettlement{}, err
	}
	step := execution.OperationStep{Operation: request.Operation, Identity: record.Identity,
		Leg: execution.Leg{ID: execution.StepID(fmt.Sprintf("%s/%d/%s", request.Operation, request.Stage.Ordinal, phase)),
			Side: execution.LegSell, Chain: s.ConversionChain, Account: s.Binding.Account,
			Market: s.Market, Input: input, ExpectedOutput: expected},
		Technical: execution.StateBroadcastPossible, Economic: execution.EconomicReserved}
	deadline := s.Clock().Add(s.Timeout)
	for {
		settlement, reconcileErr := s.Binding.TxManager.Reconcile(ctx, step)
		if reconcileErr == nil {
			switch settlement.Technical {
			case execution.StateConfirmedSuccess:
				if settlement.Economic != execution.EconomicEffectVerified || settlement.ActualIn.IsZero() || settlement.ActualOut.IsZero() {
					return conversionSettlement{}, executionport.NewStageError(executionport.DispositionPossible,
						fmt.Errorf("confirmed quote conversion lacks economic evidence"))
				}
				if err := journal.MarkTransaction(ctx, request.Operation, request.Stage.Ordinal, phase, "confirmed"); err != nil {
					return conversionSettlement{}, executionport.NewStageError(executionport.DispositionPossible, err)
				}
				return conversionSettlement{input: settlement.ActualIn, output: settlement.ActualOut,
					identity: record.Identity, costs: settlement.Costs}, nil
			case execution.StateConfirmedRevert:
				_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, "confirmed_revert")
				return conversionSettlement{}, executionport.NewStageErrorWithCosts(executionport.DispositionConfirmedFailure,
					settlement.Costs, fmt.Errorf("quote restoration conversion reverted"))
			}
		}
		if s.Clock().After(deadline) {
			_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, "outcome_unknown")
			if reconcileErr == nil {
				reconcileErr = fmt.Errorf("quote restoration conversion receipt is unavailable")
			}
			return conversionSettlement{}, executionport.NewStageError(executionport.DispositionPossible, reconcileErr)
		}
		timer := time.NewTimer(s.PollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return conversionSettlement{}, executionport.NewStageError(executionport.DispositionPossible, ctx.Err())
		case <-timer.C:
		}
	}
}

var _ crosschainport.RecoverableLiveTransferService = (*ConversionAwareTransfer)(nil)
