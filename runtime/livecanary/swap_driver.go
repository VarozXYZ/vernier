package livecanary

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type SwapBinding struct {
	Account          execution.AccountID
	Validator        executionport.Validator
	RefuelValidator  executionport.Validator
	Estimator        SwapQuoteEstimator
	TxManager        chainport.TxManager
	Confirmation     chainport.ConfirmationSource
	NonceCoordinator chainport.EVMNonceCoordinator
	SpendableBalance SpendableBalanceReader
	BalanceSnapshot  func(market.TokenID) (*big.Int, uint64, error)
	Allowance        AllowanceReader
	RefuelNetwork    RefuelNetwork
	NativeToken      market.Token
}

type AllowanceReader interface {
	Allowance(
		context.Context,
		market.TokenID,
		string,
	) (*big.Int, error)
}

type AllowanceReaderFunc func(
	context.Context,
	market.TokenID,
	string,
) (*big.Int, error)

func (f AllowanceReaderFunc) Allowance(
	ctx context.Context,
	token market.TokenID,
	spender string,
) (*big.Int, error) {
	return f(ctx, token, spender)
}

type SpendableBalanceReader interface {
	SpendableBalance(
		context.Context,
		market.TokenID,
	) (*big.Int, error)
}

type SpendableBalanceReaderFunc func(
	context.Context,
	market.TokenID,
) (*big.Int, error)

func (f SpendableBalanceReaderFunc) SpendableBalance(
	ctx context.Context,
	token market.TokenID,
) (*big.Int, error) {
	return f(ctx, token)
}

type SwapQuoteEstimator interface {
	QuoteExactInput(
		context.Context,
		market.TokenAmount,
		market.TokenID,
	) (market.TokenAmount, error)
}

type SwapQuoteEstimatorFunc func(
	context.Context,
	market.TokenAmount,
	market.TokenID,
) (market.TokenAmount, error)

func (f SwapQuoteEstimatorFunc) QuoteExactInput(
	ctx context.Context,
	input market.TokenAmount,
	output market.TokenID,
) (market.TokenAmount, error) {
	return f(ctx, input, output)
}

type SellPreflightResult struct {
	Artifact executionport.Artifact
	Identity string
}

type SellPreflight interface {
	ValidateAndSimulate(
		context.Context,
		execution.SequentialStageRequest,
	) (SellPreflightResult, error)
}

type SellPreflightFunc struct {
	Identity string
	Run      func(
		context.Context,
		execution.SequentialStageRequest,
	) (executionport.Artifact, error)
}

func (f SellPreflightFunc) ValidateAndSimulate(
	ctx context.Context,
	request execution.SequentialStageRequest,
) (SellPreflightResult, error) {
	if f.Run == nil {
		return SellPreflightResult{}, fmt.Errorf(
			"sell preflight function is unavailable",
		)
	}
	artifact, err := f.Run(ctx, request)
	if err != nil {
		return SellPreflightResult{}, err
	}
	return SellPreflightResult{
		Artifact: artifact,
		Identity: f.Identity,
	}, nil
}

type ExitCostSource interface {
	ExitCost(
		arbitrage.Direction,
		execution.SequentialExitRoute,
		time.Time,
	) (market.AssetQuantity, bool)
}

type PrefundedExitCostSource interface {
	PrefundedExitCost(
		arbitrage.Direction,
		execution.SequentialExitRoute,
		time.Time,
	) (market.AssetQuantity, bool)
}

type SwapDriver struct {
	Bindings                 map[market.MarketID]SwapBinding
	SellPreflights           map[market.MarketID]SellPreflight
	TokenDecimals            map[market.TokenID]uint8
	BridgePrecision          uint8
	QuoteAsset               market.AssetID
	BaseAsset                market.AssetID
	MinimumNet               *big.Rat
	ReturnMargin             *big.Rat
	ExitCosts                ExitCostSource
	DynamicSlippage          DynamicSlippagePolicy
	ExitValidationAttempts   int
	ExitValidationRetryDelay time.Duration
	Clock                    func() time.Time
	FallbackAfter            time.Duration
	ArtifactMaxAge           time.Duration
	Output                   io.Writer
	Costs                    executionport.CostValuator

	preflightMu   sync.Mutex
	outputMu      sync.Mutex
	preflightBuys map[execution.OperationID]preparedSwap
	exitSells     map[execution.OperationID]preparedSwap
	swapAttempts  map[execution.OperationID]map[int]int
}

type preparedSwap struct {
	artifact        executionport.Artifact
	prepared        chainport.PreparedTransaction
	validationTime  time.Duration
	compactRebuilds int
	simulation      chainport.EconomicSimulationResult
}

type exitReturnQuote struct {
	output market.TokenAmount
	err    error
}

type prefundedRecoveryCandidate struct {
	route    execution.SequentialExitRoute
	bundle   preparedSwap
	output   market.TokenAmount
	recovery market.AssetQuantity
	costOK   bool
	attempts int
	err      error
}

func (d *SwapDriver) ExecuteStage(
	ctx context.Context,
	request execution.SequentialStageRequest,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	fallback := d.FallbackAfter
	if fallback <= 0 {
		fallback = 2 * time.Second
	}
	binding, err := d.binding(request)
	if err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	bundle, cached := d.takePreparedSwap(request)
	if cached && d.artifactExpired(bundle.artifact, clock()) {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf(
					"simulated buy artifact expired before durable persistence",
				),
			)
	}
	if !cached {
		bundle, err = d.prepareAndSimulate(ctx, request, binding, nil)
		if err != nil {
			return execution.SequentialStageSettlement{}, executionport.NewStageError(
				executionport.DispositionRejected, err,
			)
		}
	}
	artifact, prepared := bundle.artifact, bundle.prepared
	d.logPrepared(request, bundle, cached)
	prepareStarted := clock()
	transactionPhase := d.nextSwapTransactionPhase(
		request.Operation,
		request.Stage.Ordinal,
	)
	if err := journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{
		Operation: request.Operation, Ordinal: request.Stage.Ordinal,
		Phase:      transactionPhase,
		Identity:   prepared.Identity,
		PreparedAt: prepared.PreparedAt,
	}); err != nil {
		return execution.SequentialStageSettlement{}, executionport.NewStageError(
			executionport.DispositionRejected, err,
		)
	}
	if prepared.Identity.Nonce != nil {
		d.write(
			"live_stage operation=%s stage=%d/%s phase=durable tx=%s nonce=%d latency=%s\n",
			request.Operation, request.Stage.Ordinal, request.Stage.Stage,
			prepared.Identity.Hash, *prepared.Identity.Nonce,
			clock().Sub(prepareStarted).Round(10*time.Microsecond),
		)
	} else {
		d.write(
			"live_stage operation=%s stage=%d/%s phase=durable tx=%s latency=%s\n",
			request.Operation, request.Stage.Ordinal, request.Stage.Stage,
			prepared.Identity.Hash,
			clock().Sub(prepareStarted).Round(10*time.Microsecond),
		)
	}
	broadcastStarted := clock()
	broadcast, err := binding.TxManager.Broadcast(ctx, prepared)
	if err != nil {
		disposition := executionport.DispositionPossible
		if broadcast.Disposition == chainport.BroadcastRejected {
			disposition = executionport.DispositionRejected
			_ = journal.MarkTransaction(
				context.WithoutCancel(ctx), request.Operation,
				request.Stage.Ordinal, transactionPhase, "rejected",
			)
		} else {
			_ = journal.MarkTransaction(
				context.WithoutCancel(ctx), request.Operation,
				request.Stage.Ordinal, transactionPhase, "outcome_unknown",
			)
		}
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(disposition, err)
	}
	if !broadcast.Accepted {
		err := fmt.Errorf("swap broadcaster did not accept the prepared identity")
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx), request.Operation,
			request.Stage.Ordinal, transactionPhase, "outcome_unknown",
		)
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if err := journal.MarkTransaction(
		ctx, request.Operation, request.Stage.Ordinal, transactionPhase, "broadcast",
	); err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	d.write(
		"live_stage operation=%s stage=%d/%s phase=broadcast tx=%s endpoint=%s latency=%s\n",
		request.Operation, request.Stage.Ordinal, request.Stage.Stage,
		prepared.Identity.Hash, broadcast.Endpoint,
		clock().Sub(broadcastStarted).Round(10*time.Microsecond),
	)
	step := execution.OperationStep{
		Operation: request.Operation, Leg: artifact.Leg,
		Identity: prepared.Identity, Technical: execution.StateBroadcastPossible,
		Economic: execution.EconomicReserved,
	}
	confirmationStarted := clock()
	settlement, err := d.confirm(ctx, binding, step, fallback)
	if err != nil {
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx), request.Operation,
			request.Stage.Ordinal, transactionPhase, "outcome_unknown",
		)
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if settlement.Technical != execution.StateConfirmedSuccess ||
		settlement.Economic != execution.EconomicEffectVerified ||
		settlement.ActualIn.IsZero() || settlement.ActualOut.IsZero() {
		err := fmt.Errorf(
			"swap settlement is not a confirmed economic success: technical=%s economic=%s",
			settlement.Technical, settlement.Economic,
		)
		disposition := executionport.DispositionPossible
		status := "outcome_unknown"
		var failureCosts []execution.CostComponent
		if settlement.Technical == execution.StateConfirmedRevert {
			disposition = executionport.DispositionConfirmedFailure
			status = "confirmed_revert"
			failureCosts, err = valueCosts(d.Costs, settlement.Costs)
			if err != nil {
				return execution.SequentialStageSettlement{},
					executionport.NewStageError(
						executionport.DispositionPossible,
						fmt.Errorf("value confirmed revert costs: %w", err),
					)
			}
		}
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx), request.Operation,
			request.Stage.Ordinal, transactionPhase, status,
		)
		return execution.SequentialStageSettlement{},
			executionport.NewStageErrorWithCosts(
				disposition,
				failureCosts,
				err,
			)
	}
	valuedCosts, err := valueCosts(d.Costs, settlement.Costs)
	if err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if err := journal.MarkTransaction(
		ctx, request.Operation, request.Stage.Ordinal, transactionPhase, "confirmed",
	); err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	d.write(
		"live_stage operation=%s stage=%d/%s phase=settled actual_input_units=%s actual_output_units=%s evidence=%s latency=%s\n",
		request.Operation, request.Stage.Ordinal, request.Stage.Stage,
		settlement.ActualIn, settlement.ActualOut, settlement.Evidence,
		clock().Sub(confirmationStarted).Round(10*time.Microsecond),
	)
	return execution.SequentialStageSettlement{
		Request: request, ActualInput: settlement.ActualIn,
		ActualOutput: settlement.ActualOut, Costs: valuedCosts,
		SourceIdentity: prepared.Identity,
		ObservedAt:     settlement.ObservedAt, Evidence: settlement.Evidence,
	}, nil
}

func (d *SwapDriver) RecoverStage(
	ctx context.Context,
	request execution.SequentialStageRequest,
	transactions []executionport.SequentialTransactionRecord,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	if err := request.Validate(); err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	binding, err := d.binding(request)
	if err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	var candidate *executionport.SequentialTransactionRecord
	for index := range transactions {
		record := &transactions[index]
		if record.Ordinal != request.Stage.Ordinal ||
			record.Identity.Chain != request.Stage.SourceChain {
			continue
		}
		if candidate == nil ||
			record.PreparedAt.After(candidate.PreparedAt) {
			candidate = record
		}
	}
	if candidate == nil ||
		candidate.Status == "rejected" ||
		candidate.Status == "confirmed_revert" {
		d.primeSwapAttempts(request.Operation, request.Stage.Ordinal, len(transactions))
		return d.executeRecoverySwap(ctx, request, binding, journal)
	}
	expected, expectedErr := market.NewTokenAmount(
		request.Stage.OutputToken,
		big.NewInt(1),
	)
	if expectedErr != nil {
		return execution.SequentialStageSettlement{}, expectedErr
	}
	side := execution.LegSell
	if request.Stage.Stage == execution.StageBuy {
		side = execution.LegBuy
	}
	step := execution.OperationStep{
		Operation: request.Operation,
		Leg: execution.Leg{
			ID: execution.StepID(fmt.Sprintf(
				"%s/%d/%s",
				request.Operation,
				request.Stage.Ordinal,
				candidate.Phase,
			)),
			Side: side, Chain: request.Stage.SourceChain,
			Account: binding.Account, Market: request.Stage.Market,
			Input: request.Input, ExpectedOutput: expected,
		},
		Identity:  candidate.Identity,
		Technical: execution.StateBroadcastPossible,
		Economic:  execution.EconomicReserved,
	}
	settlement, err := binding.TxManager.Reconcile(ctx, step)
	if err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewRecoveryError(
				executionport.RecoveryFailureTemporary,
				fmt.Errorf("reconcile swap %s: %w", candidate.Identity.Hash, err),
			)
	}
	switch settlement.Technical {
	case execution.StateConfirmedSuccess:
		if settlement.Economic != execution.EconomicEffectVerified ||
			settlement.ActualIn.IsZero() || settlement.ActualOut.IsZero() {
			return execution.SequentialStageSettlement{},
				executionport.NewRecoveryError(
					executionport.RecoveryFailureUncertain,
					fmt.Errorf(
						"confirmed swap %s has incomplete economic evidence",
						candidate.Identity.Hash,
					),
				)
		}
		valuedCosts, valueErr := valueCosts(d.Costs, settlement.Costs)
		if valueErr != nil {
			return execution.SequentialStageSettlement{},
				executionport.NewRecoveryError(
					executionport.RecoveryFailureTemporary,
					valueErr,
				)
		}
		if markErr := journal.MarkTransaction(
			ctx,
			request.Operation,
			request.Stage.Ordinal,
			candidate.Phase,
			"confirmed",
		); markErr != nil {
			return execution.SequentialStageSettlement{}, markErr
		}
		return execution.SequentialStageSettlement{
			Request: request, ActualInput: settlement.ActualIn,
			ActualOutput: settlement.ActualOut, Costs: valuedCosts,
			SourceIdentity: candidate.Identity,
			ObservedAt:     settlement.ObservedAt,
			Evidence:       settlement.Evidence + "+startup_reconciliation",
		}, nil
	case execution.StateConfirmedRevert:
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx),
			request.Operation,
			request.Stage.Ordinal,
			candidate.Phase,
			"confirmed_revert",
		)
		valuedCosts, valueErr := valueCosts(d.Costs, settlement.Costs)
		if valueErr != nil {
			return execution.SequentialStageSettlement{}, valueErr
		}
		return execution.SequentialStageSettlement{},
			executionport.NewStageErrorWithCosts(
				executionport.DispositionConfirmedFailure,
				valuedCosts,
				fmt.Errorf("reconciled swap reverted"),
			)
	case execution.StateBroadcastRejected:
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx),
			request.Operation,
			request.Stage.Ordinal,
			candidate.Phase,
			"rejected",
		)
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf("reconciled swap was never accepted"),
			)
	default:
		return execution.SequentialStageSettlement{},
			executionport.NewRecoveryError(
				executionport.RecoveryFailureUncertain,
				fmt.Errorf(
					"swap %s remains uncertain: %s",
					candidate.Identity.Hash,
					settlement.Evidence,
				),
			)
	}
}

func (d *SwapDriver) executeRecoverySwap(
	ctx context.Context,
	request execution.SequentialStageRequest,
	binding SwapBinding,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	executionRequest := request
	if request.Stage.Stage == execution.StageSell &&
		binding.SpendableBalance != nil {
		available, err := binding.SpendableBalance.SpendableBalance(
			ctx,
			request.Stage.InputToken,
		)
		if err != nil {
			return execution.SequentialStageSettlement{},
				executionport.NewRecoveryError(
					executionport.RecoveryFailureTemporary,
					fmt.Errorf("read spendable sell balance: %w", err),
				)
		}
		if available == nil || available.Sign() <= 0 {
			return execution.SequentialStageSettlement{},
				executionport.NewRecoveryError(
					executionport.RecoveryFailureBalanceMismatch,
					fmt.Errorf("sell inventory is not yet visible"),
				)
		}
		if available.Cmp(request.Input.Units()) < 0 {
			minimumHistorical := new(big.Int).Mul(
				request.Input.Units(),
				big.NewInt(99),
			)
			minimumHistorical.Quo(minimumHistorical, big.NewInt(100))
			if available.Cmp(minimumHistorical) < 0 {
				return execution.SequentialStageSettlement{},
					executionport.NewRecoveryError(
						executionport.RecoveryFailureBalanceMismatch,
						fmt.Errorf(
							"spendable sell balance %s is materially below attributable inventory %s",
							available,
							request.Input.Units(),
						),
					)
			}
			resized, resizeErr := market.NewTokenAmount(
				request.Stage.InputToken,
				available,
			)
			if resizeErr != nil {
				return execution.SequentialStageSettlement{}, resizeErr
			}
			executionRequest.Input = resized
		}
	}
	settlement, err := d.ExecuteStage(ctx, executionRequest, journal)
	if err != nil {
		return execution.SequentialStageSettlement{},
			classifyRecoverySwapError(
				ctx,
				binding,
				executionRequest.Input,
				err,
			)
	}
	if executionRequest.Input.Units().Cmp(request.Input.Units()) != 0 {
		settlement.Request = request
		settlement.Evidence += "+recovery_balance_resized"
	}
	return settlement, nil
}

func classifyRecoverySwapError(
	ctx context.Context,
	binding SwapBinding,
	input market.TokenAmount,
	err error,
) error {
	var allowanceFailure *executionport.AllowanceRequiredError
	if errors.As(err, &allowanceFailure) &&
		binding.Allowance != nil &&
		binding.SpendableBalance != nil {
		balance, balanceErr := binding.SpendableBalance.SpendableBalance(
			ctx,
			input.Token(),
		)
		if balanceErr != nil {
			return executionport.NewRecoveryError(
				executionport.RecoveryFailureTemporary,
				fmt.Errorf("inspect token balance: %w", balanceErr),
			)
		}
		allowance, allowanceErr := binding.Allowance.Allowance(
			ctx,
			input.Token(),
			allowanceFailure.Spender,
		)
		if allowanceErr != nil {
			return executionport.NewRecoveryError(
				executionport.RecoveryFailureTemporary,
				fmt.Errorf("inspect token allowance: %w", allowanceErr),
			)
		}
		if balance.Cmp(input.Units()) < 0 {
			return executionport.NewRecoveryError(
				executionport.RecoveryFailureBalanceMismatch,
				fmt.Errorf(
					"spendable balance %s is below required %s",
					balance,
					input.Units(),
				),
			)
		}
		if allowance.Cmp(input.Units()) < 0 {
			return executionport.NewRecoveryError(
				executionport.RecoveryFailureAllowance,
				fmt.Errorf(
					"allowance to %s is %s, required %s",
					allowanceFailure.Spender,
					allowance,
					input.Units(),
				),
			)
		}
		return executionport.NewRecoveryError(
			executionport.RecoveryFailureStaleArtifact,
			err,
		)
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "return amount is not enough"),
		strings.Contains(text, "too little received"),
		strings.Contains(text, "slippage"):
		return executionport.NewRecoveryError(
			executionport.RecoveryFailureStaleArtifact,
			err,
		)
	case strings.Contains(text, "insufficient funds for gas"),
		strings.Contains(text, "insufficient lamports"),
		strings.Contains(text, "insufficient native"):
		return executionport.NewRecoveryError(
			executionport.RecoveryFailureInsufficientNative,
			err,
		)
	case strings.Contains(text, "fee cap"),
		strings.Contains(text, "priority fee") &&
			strings.Contains(text, "maximum"):
		return executionport.NewRecoveryError(
			executionport.RecoveryFailureFeeCap,
			err,
		)
	default:
		return err
	}
}

func (d *SwapDriver) Preflight(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) error {
	if plan.EffectivePolicy() == execution.PolicyPrefundedParallel {
		return d.preflightParallel(ctx, operation, plan)
	}
	sellIndex := 2
	if plan.EffectivePolicy() == execution.PolicyPrefundedSequential {
		sellIndex = 1
	}
	if operation == "" || len(plan.Stages) != 4 ||
		plan.Stages[0].Stage != execution.StageBuy ||
		plan.Stages[sellIndex].Stage != execution.StageSell {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("swap preflight plan is incomplete"),
		)
	}
	started := time.Now()
	buyRequest := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID,
		Stage: plan.Stages[0], Input: plan.InitialInput,
	}
	buyBinding, err := d.binding(buyRequest)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected, err,
		)
	}
	buySlippage, err := d.dynamicBuySlippage(plan)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("buy dynamic slippage: %w", err),
		)
	}
	buy, err := d.prepareSwap(
		ctx,
		buyRequest,
		buyBinding,
		buySlippage,
	)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("buy preflight: %w", err),
		)
	}
	sellInput, err := d.bridgeDestinationAmount(
		buy.artifact.ValidatedQuote.AmountOut,
		plan.Stages[sellIndex].InputToken,
	)
	if plan.EffectivePolicy() == execution.PolicyPrefundedSequential {
		sellInput, err = d.convertAmount(
			buy.artifact.ValidatedQuote.AmountOut,
			plan.Stages[sellIndex].InputToken,
		)
	}
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("derive preflight sell input: %w", err),
		)
	}
	sellRequest := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID,
		Stage: plan.Stages[sellIndex], Input: sellInput,
	}
	sellBinding, err := d.binding(sellRequest)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected, err,
		)
	}
	if plan.EffectivePolicy() == execution.PolicyPrefundedSequential &&
		sellBinding.SpendableBalance != nil {
		available, balanceErr :=
			sellBinding.SpendableBalance.SpendableBalance(
				ctx,
				sellInput.Token(),
			)
		if balanceErr != nil {
			return executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf(
					"read prefunded destination inventory: %w",
					balanceErr,
				),
			)
		}
		if available == nil || available.Cmp(sellInput.Units()) < 0 {
			availableText := "0"
			if available != nil {
				availableText = available.String()
			}
			return executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf(
					"prefunded destination inventory is insufficient: available_units=%s required_units=%s",
					availableText,
					sellInput.Units(),
				),
			)
		}
	}
	sellPreflight := d.SellPreflights[sellRequest.Stage.Market]
	type sellResult struct {
		artifact executionport.Artifact
		identity string
		err      error
	}
	buySimulation := make(chan error, 1)
	sellPrepared := make(chan sellResult, 1)
	go func() {
		buySimulation <- d.simulate(ctx, buyBinding, buy.prepared)
	}()
	go func() {
		if sellPreflight != nil {
			result, preflightErr := sellPreflight.ValidateAndSimulate(
				ctx,
				sellRequest,
			)
			sellPrepared <- sellResult{
				artifact: result.Artifact,
				identity: result.Identity,
				err:      preflightErr,
			}
			return
		}
		// Initial preflight runs before the operation and its first settlement
		// exist. A failed destination simulation must therefore abort and let
		// the scheduler perform one fresh evaluation; the multi-attempt exit
		// policy is reserved for recovery of inventory already at risk.
		bundle, prepareErr := d.prepareAndSimulate(
			ctx, sellRequest, sellBinding, nil,
		)
		sellPrepared <- sellResult{
			artifact: bundle.artifact,
			identity: string(sellBinding.Account),
			err:      prepareErr,
		}
	}()
	buySimulationErr := <-buySimulation
	sellResultValue := <-sellPrepared
	if buySimulationErr != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("buy preflight: %w", buySimulationErr),
		)
	}
	if sellResultValue.err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("sell preflight: %w", sellResultValue.err),
		)
	}
	roundTrip, err := d.convertAmount(
		sellResultValue.artifact.ValidatedQuote.AmountOut,
		plan.InitialInput.Token(),
	)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("value preflight round trip: %w", err),
		)
	}
	forcedCanary := isForcedCanaryOpportunity(plan.Opportunity)
	if roundTrip.Units().Cmp(plan.InitialInput.Units()) <= 0 &&
		!forcedCanary {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf(
				"fresh simulated round trip is no longer gross profitable: input=%s output=%s",
				plan.InitialInput,
				roundTrip,
			),
		)
	}
	d.preflightMu.Lock()
	if d.preflightBuys == nil {
		d.preflightBuys = make(map[execution.OperationID]preparedSwap)
	}
	d.preflightBuys[operation] = buy
	d.preflightMu.Unlock()
	d.write(
		"live_preflight operation=%s status=accepted forced_canary=%t buy_input_units=%s buy_output_units=%s sell_input_units=%s sell_output_units=%s sell_preflight_reference=%s round_trip_units=%s latency=%s\n",
		operation,
		forcedCanary,
		plan.InitialInput,
		buy.artifact.ValidatedQuote.AmountOut,
		sellInput,
		sellResultValue.artifact.ValidatedQuote.AmountOut,
		sellResultValue.identity,
		roundTrip,
		time.Since(started).Round(10*time.Microsecond),
	)
	return nil
}

func (d *SwapDriver) DiscardPreflight(operation execution.OperationID) {
	d.preflightMu.Lock()
	delete(d.preflightBuys, operation)
	delete(d.exitSells, operation)
	delete(d.swapAttempts, operation)
	d.preflightMu.Unlock()
}

func (d *SwapDriver) nextSwapTransactionPhase(
	operation execution.OperationID,
	ordinal int,
) string {
	d.preflightMu.Lock()
	defer d.preflightMu.Unlock()
	if d.swapAttempts == nil {
		d.swapAttempts = make(map[execution.OperationID]map[int]int)
	}
	byOrdinal := d.swapAttempts[operation]
	if byOrdinal == nil {
		byOrdinal = make(map[int]int)
		d.swapAttempts[operation] = byOrdinal
	}
	attempt := byOrdinal[ordinal]
	byOrdinal[ordinal] = attempt + 1
	if attempt == 0 {
		return "swap"
	}
	return fmt.Sprintf("swap_recovery_%d", attempt)
}

func (d *SwapDriver) primeSwapAttempts(
	operation execution.OperationID,
	ordinal, attempts int,
) {
	d.preflightMu.Lock()
	defer d.preflightMu.Unlock()
	if d.swapAttempts == nil {
		d.swapAttempts = make(map[execution.OperationID]map[int]int)
	}
	if d.swapAttempts[operation] == nil {
		d.swapAttempts[operation] = make(map[int]int)
	}
	if d.swapAttempts[operation][ordinal] < attempts {
		d.swapAttempts[operation][ordinal] = attempts
	}
}

func (d *SwapDriver) SelectExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
	incurred []execution.CostComponent,
) (execution.SequentialExitDecision, error) {
	return d.selectExit(ctx, operation, plan, bridged, incurred, false)
}

func (d *SwapDriver) SelectRecoveryExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
	incurred []execution.CostComponent,
) (execution.SequentialExitDecision, error) {
	return d.selectExit(ctx, operation, plan, bridged, incurred, true)
}

// SelectPrefundedExit always prepares the destination sale first with the
// validator's configured fixed slippage. A preparation failure may open
// recovery only after its retries prove that no destination transaction was
// committed.
func (d *SwapDriver) SelectPrefundedExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bought market.TokenAmount,
	incurred []execution.CostComponent,
) (execution.SequentialExitDecision, error) {
	if (plan.EffectivePolicy() != execution.PolicyPrefundedSequential &&
		plan.EffectivePolicy() != execution.PolicyPrefundedParallel) ||
		len(plan.Stages) != 4 || len(plan.CircuitBreaker) != 1 ||
		bought.IsZero() {
		return execution.SequentialExitDecision{},
			fmt.Errorf("prefunded exit input is incomplete")
	}
	input, err := d.convertAmount(bought, plan.Stages[1].InputToken)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	request := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID,
		Stage: plan.Stages[1], Input: input,
	}
	binding, err := d.binding(request)
	if err != nil {
		return d.SelectPrefundedRecoveryExit(
			ctx, operation, plan, bought, incurred, err,
		)
	}
	bundle, attempts, err := d.prepareExitWithRetry(
		ctx, operation, request, binding, nil,
	)
	if err != nil {
		return d.SelectPrefundedRecoveryExit(
			ctx, operation, plan, bought, incurred,
			fmt.Errorf("destination preparation failed after %d attempt(s): %w", attempts, err),
		)
	}
	quoteAsset := d.planQuoteAsset(plan)
	recovery, err := d.recoveryValue(
		bundle.artifact.ValidatedQuote.AmountOut,
		plan.Stages[0].InputToken,
		quoteAsset,
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	zero, _ := market.NewAssetQuantity(quoteAsset, new(big.Rat))
	decision := execution.SequentialExitDecision{
		Operation: operation, Route: execution.ExitSellAtDestination,
		DestinationOutput:   bundle.artifact.ValidatedQuote.AmountOut,
		DestinationRecovery: recovery, SafetyMargin: zero,
		DestinationQualified: true, CostEvidenceAvailable: true,
		DecidedAt: d.now(),
		Evidence:  "prefunded_destination_first+fresh_build+simulation+fixed_slippage",
	}
	d.storeExitSell(operation, bundle)
	d.logExitDecision(decision, 0)
	return decision, nil
}

// SelectPrefundedRecoveryExit rebuilds and simulates both sales independently.
// It is only called before any destination effect or after non-execution has
// been proved. A failed preparation is retried with a fresh quote/build and
// makes that branch unavailable only after the retry budget is exhausted.
func (d *SwapDriver) SelectPrefundedRecoveryExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bought market.TokenAmount,
	incurred []execution.CostComponent,
	cause error,
) (execution.SequentialExitDecision, error) {
	if (plan.EffectivePolicy() != execution.PolicyPrefundedSequential &&
		plan.EffectivePolicy() != execution.PolicyPrefundedParallel) ||
		len(plan.Stages) != 4 || len(plan.CircuitBreaker) != 1 || bought.IsZero() {
		return execution.SequentialExitDecision{},
			fmt.Errorf("prefunded recovery input is incomplete")
	}
	quoteAsset := d.planQuoteAsset(plan)
	if quoteAsset == "" {
		return execution.SequentialExitDecision{}, fmt.Errorf("prefunded recovery quote asset is unavailable")
	}
	results := make(chan prefundedRecoveryCandidate, 2)
	for _, candidate := range []struct {
		route execution.SequentialExitRoute
		stage execution.SequentialStagePlan
		log   string
	}{
		{execution.ExitSellAtDestination, plan.Stages[1], "live_prefunded_recovery_destination"},
		{execution.ExitSellAtOrigin, plan.CircuitBreaker[0], "live_prefunded_recovery_origin"},
	} {
		candidate := candidate
		go func() {
			results <- d.preparePrefundedRecoveryCandidate(
				ctx, operation, plan, bought, quoteAsset,
				candidate.route, candidate.stage, candidate.log,
			)
		}()
	}
	first, second := <-results, <-results
	var destination, origin prefundedRecoveryCandidate
	for _, result := range []prefundedRecoveryCandidate{first, second} {
		if result.route == execution.ExitSellAtDestination {
			destination = result
		} else {
			origin = result
		}
	}
	if destination.err != nil && origin.err != nil {
		return execution.SequentialExitDecision{}, errors.Join(
			fmt.Errorf("destination recovery unavailable after %d attempt(s): %w", destination.attempts, destination.err),
			fmt.Errorf("origin recovery unavailable after %d attempt(s): %w", origin.attempts, origin.err),
		)
	}
	zero, _ := market.NewAssetQuantity(quoteAsset, new(big.Rat))
	evidence := "prefunded_recovery_comparison+fresh_quote_build_simulation"
	if cause != nil {
		var threshold *executionport.SlippageThresholdError
		if errors.As(cause, &threshold) {
			evidence += "+sell_slippage_failure"
		} else {
			evidence += "+destination_safe_failure"
		}
	}
	decision := execution.SequentialExitDecision{
		Operation: operation, DestinationRecovery: zero,
		ReturnRecovery: zero, SafetyMargin: zero, DecidedAt: d.now(),
		Evidence: evidence,
	}
	if destination.err == nil {
		decision.DestinationOutput = destination.output
		decision.DestinationRecovery = destination.recovery
		decision.DestinationQualified = true
	} else {
		decision.Evidence += "+destination_unavailable_after_retries"
	}
	if origin.err == nil {
		decision.ReturnOutput = origin.output
		decision.ReturnRecovery = origin.recovery
	} else {
		decision.Evidence += "+origin_unavailable_after_retries"
	}
	decision.CostEvidenceAvailable =
		(destination.err != nil || destination.costOK) &&
			(origin.err != nil || origin.costOK)
	selected := destination
	switch {
	case destination.err != nil:
		decision.Route = execution.ExitSellAtOrigin
		selected = origin
		decision.Evidence += "+only_origin_executable"
	case origin.err != nil:
		decision.Route = execution.ExitSellAtDestination
		decision.Evidence += "+only_destination_executable"
	case origin.recovery.Rat().Cmp(destination.recovery.Rat()) > 0:
		decision.Route = execution.ExitSellAtOrigin
		selected = origin
		decision.Evidence += "+origin_net_advantage"
	default:
		decision.Route = execution.ExitSellAtDestination
		decision.Evidence += "+destination_net_advantage"
	}
	d.storeExitSell(operation, selected.bundle)
	d.logExitDecision(decision, 0)
	return decision, nil
}

func (d *SwapDriver) preparePrefundedRecoveryCandidate(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bought market.TokenAmount,
	quoteAsset market.AssetID,
	route execution.SequentialExitRoute,
	stage execution.SequentialStagePlan,
	logEvent string,
) prefundedRecoveryCandidate {
	result := prefundedRecoveryCandidate{route: route}
	input, err := d.convertAmount(bought, stage.InputToken)
	if err != nil {
		result.err = err
		return result
	}
	request := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID, Stage: stage, Input: input,
	}
	binding, err := d.binding(request)
	if err != nil {
		result.err = err
		return result
	}
	result.bundle, result.attempts, result.err = d.prepareSwapWithRetry(
		ctx, operation, request, binding, nil, logEvent,
	)
	if result.err != nil {
		return result
	}
	result.output = result.bundle.artifact.ValidatedQuote.AmountOut
	result.recovery, result.err = d.recoveryValue(
		result.output, stage.OutputToken, quoteAsset,
	)
	if result.err != nil {
		return result
	}
	if d.ExitCosts != nil {
		var (
			cost market.AssetQuantity
			ok   bool
		)
		if source, prefunded := d.ExitCosts.(PrefundedExitCostSource); prefunded {
			cost, ok = source.PrefundedExitCost(
				plan.Opportunity.Direction, route, d.now(),
			)
		} else {
			cost, ok = d.ExitCosts.ExitCost(
				plan.Opportunity.Direction, route, d.now(),
			)
		}
		if ok {
			result.recovery, result.err = result.recovery.Sub(cost)
			result.costOK = result.err == nil
		}
	}
	return result
}

func (d *SwapDriver) planQuoteAsset(plan execution.SequentialPlan) market.AssetID {
	if d.QuoteAsset != "" {
		return d.QuoteAsset
	}
	if plan.Opportunity.SelectedIndex >= 0 &&
		plan.Opportunity.SelectedIndex < len(plan.Opportunity.Candidates) {
		return plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex].Input.Asset()
	}
	return ""
}

func (d *SwapDriver) now() time.Time {
	if d.Clock != nil {
		return d.Clock().UTC()
	}
	return time.Now().UTC()
}

func (d *SwapDriver) ConvertStageInput(
	stage execution.SequentialStagePlan,
	source market.TokenAmount,
) (market.TokenAmount, error) {
	if source.Token() == stage.InputToken {
		return source, nil
	}
	return d.convertAmount(source, stage.InputToken)
}

func (d *SwapDriver) selectExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
	incurred []execution.CostComponent,
	forceComparison bool,
) (execution.SequentialExitDecision, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	if operation == "" || len(plan.Stages) != 4 || bridged.IsZero() {
		return execution.SequentialExitDecision{},
			fmt.Errorf("post-bridge exit input is incomplete")
	}
	quoteAsset := d.QuoteAsset
	if quoteAsset == "" &&
		plan.Opportunity.SelectedIndex >= 0 &&
		plan.Opportunity.SelectedIndex < len(plan.Opportunity.Candidates) {
		quoteAsset = plan.Opportunity.
			Candidates[plan.Opportunity.SelectedIndex].
			Input.Asset()
	}
	if quoteAsset == "" {
		return execution.SequentialExitDecision{},
			fmt.Errorf("post-bridge exit quote asset is unavailable")
	}
	margin, err := market.NewAssetQuantity(
		quoteAsset,
		cloneRatOrZero(d.ReturnMargin),
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	forcedCanary := isForcedCanaryOpportunity(plan.Opportunity)
	compareReturn := !forcedCanary || forceComparison
	destinationRequest := execution.SequentialStageRequest{
		Operation: operation,
		Plan:      plan.ID,
		Stage:     plan.Stages[2],
		Input:     bridged,
	}
	destinationBinding, err := d.binding(destinationRequest)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	var returnResult chan exitReturnQuote
	if compareReturn {
		returnCtx, cancelReturn := context.WithCancel(ctx)
		defer cancelReturn()
		returnResult = make(chan exitReturnQuote, 1)
		go func() {
			returnResult <- d.quoteReturnExit(
				returnCtx,
				plan,
				bridged,
			)
		}()
	}
	started := clock()
	now := clock().UTC()
	destinationCost, destinationCostsOK := market.AssetQuantity{}, false
	if d.ExitCosts != nil {
		destinationCost, destinationCostsOK = d.ExitCosts.ExitCost(
			plan.Opportunity.Direction,
			execution.ExitSellAtDestination,
			now,
		)
	}
	destination, destinationAttempts, err := d.prepareExitWithRetry(
		ctx,
		operation,
		destinationRequest,
		destinationBinding,
		nil,
	)
	if err != nil {
		d.write(
			"live_exit_destination operation=%s status=unavailable attempts=%d error=%q\n",
			operation,
			destinationAttempts,
			err,
		)
		// The parallel return quote may be several seconds old after the
		// destination exhausted its validation retries. Never bridge back
		// using that stale comparison.
		quotedReturn := d.quoteReturnExit(ctx, plan, bridged)
		return d.selectForcedReturn(
			operation,
			plan,
			quoteAsset,
			margin,
			started,
			quotedReturn,
			fmt.Errorf("destination liquidation unavailable: %w", err),
		)
	}
	destinationOutput := destination.artifact.ValidatedQuote.AmountOut
	destinationRecovery, err := d.recoveryValue(
		destinationOutput,
		plan.Stages[0].InputToken,
		quoteAsset,
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	if destinationCostsOK {
		destinationRecovery, err = destinationRecovery.Sub(destinationCost)
		if err != nil {
			return execution.SequentialExitDecision{}, err
		}
	}
	destinationQualified := false
	if destinationCostsOK {
		destinationQualified, err = d.exitStillQualified(
			plan,
			destinationRecovery,
			incurred,
			quoteAsset,
		)
		if err != nil {
			return execution.SequentialExitDecision{}, err
		}
	}
	decision := execution.SequentialExitDecision{
		Operation:             operation,
		Route:                 execution.ExitSellAtDestination,
		DestinationOutput:     destinationOutput,
		DestinationRecovery:   destinationRecovery,
		SafetyMargin:          margin,
		DestinationQualified:  destinationQualified,
		CostEvidenceAvailable: destinationCostsOK,
		DecidedAt:             now,
		Evidence:              "fresh_destination_build+simulation",
	}
	if forceComparison {
		decision.Evidence += "+automatic_recovery_comparison"
	}
	if forcedCanary && !forceComparison {
		decision.Evidence += "+forced_canary_destination"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	if destinationQualified && !forceComparison {
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	if !destinationCostsOK && !forceComparison {
		decision.Evidence += "+destination_cost_cache_unavailable"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}

	var quotedReturn exitReturnQuote
	if destinationAttempts > 1 {
		quotedReturn = d.quoteReturnExit(ctx, plan, bridged)
		decision.Evidence += "+refreshed_return_after_destination_retry"
	} else {
		quotedReturn = <-returnResult
	}
	if quotedReturn.err != nil {
		decision.Evidence += "+return_quote_unavailable"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	decision.ReturnOutput = quotedReturn.output
	returnRecovery, err := d.recoveryValue(
		decision.ReturnOutput,
		plan.Stages[0].InputToken,
		quoteAsset,
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	returnCost, returnCostsOK := d.ExitCosts.ExitCost(
		plan.Opportunity.Direction,
		execution.ExitReturnToOrigin,
		now,
	)
	if returnCostsOK {
		returnRecovery, err = returnRecovery.Sub(returnCost)
		if err != nil {
			return execution.SequentialExitDecision{}, err
		}
	}
	decision.ReturnRecovery = returnRecovery
	decision.CostEvidenceAvailable =
		decision.CostEvidenceAvailable && returnCostsOK
	decision.Evidence += "+fresh_origin_quote"
	if !returnCostsOK {
		decision.Evidence += "+return_cost_cache_unavailable"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	returnThreshold, err := destinationRecovery.Add(margin)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	if returnRecovery.Rat().Cmp(returnThreshold.Rat()) > 0 {
		decision.Route = execution.ExitReturnToOrigin
		decision.Evidence += "+return_advantage"
	} else {
		decision.Evidence += "+destination_advantage"
		d.storeExitSell(operation, destination)
	}
	d.logExitDecision(decision, clock().Sub(started))
	return decision, nil
}

func (d *SwapDriver) selectForcedReturn(
	operation execution.OperationID,
	plan execution.SequentialPlan,
	quoteAsset market.AssetID,
	margin market.AssetQuantity,
	started time.Time,
	quotedReturn exitReturnQuote,
	destinationErr error,
) (execution.SequentialExitDecision, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	if quotedReturn.err != nil {
		return execution.SequentialExitDecision{},
			errors.Join(destinationErr, fmt.Errorf(
				"return liquidation quote unavailable: %w",
				quotedReturn.err,
			))
	}
	returnRecovery, err := d.recoveryValue(
		quotedReturn.output,
		plan.Stages[0].InputToken,
		quoteAsset,
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	costEvidence := false
	if d.ExitCosts != nil {
		if cost, ok := d.ExitCosts.ExitCost(
			plan.Opportunity.Direction,
			execution.ExitReturnToOrigin,
			clock().UTC(),
		); ok {
			returnRecovery, err = returnRecovery.Sub(cost)
			if err != nil {
				return execution.SequentialExitDecision{}, err
			}
			costEvidence = true
		}
	}
	zero, _ := market.NewAssetQuantity(quoteAsset, new(big.Rat))
	decision := execution.SequentialExitDecision{
		Operation:             operation,
		Route:                 execution.ExitReturnToOrigin,
		DestinationRecovery:   zero,
		ReturnOutput:          quotedReturn.output,
		ReturnRecovery:        returnRecovery,
		SafetyMargin:          margin,
		CostEvidenceAvailable: costEvidence,
		DecidedAt:             clock().UTC(),
		Evidence:              "destination_unavailable+fresh_origin_quote",
	}
	d.logExitDecision(decision, clock().Sub(started))
	return decision, nil
}

func (d *SwapDriver) quoteReturnExit(
	ctx context.Context,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
) exitReturnQuote {
	returnStages, err := plan.ReturnExitStages()
	if err != nil {
		return exitReturnQuote{err: err}
	}
	returnInput, err := d.bridgeDestinationAmount(
		bridged,
		returnStages[1].InputToken,
	)
	if err != nil {
		return exitReturnQuote{err: err}
	}
	binding, ok := d.Bindings[returnStages[1].Market]
	if !ok || binding.Estimator == nil {
		return exitReturnQuote{
			err: fmt.Errorf("origin liquidation quote estimator is unavailable"),
		}
	}
	output, err := binding.Estimator.QuoteExactInput(
		ctx,
		returnInput,
		returnStages[1].OutputToken,
	)
	if err != nil {
		return exitReturnQuote{err: err}
	}
	return exitReturnQuote{output: output}
}

func (d *SwapDriver) storeExitSell(
	operation execution.OperationID,
	bundle preparedSwap,
) {
	d.preflightMu.Lock()
	if d.exitSells == nil {
		d.exitSells = make(map[execution.OperationID]preparedSwap)
	}
	d.exitSells[operation] = bundle
	d.preflightMu.Unlock()
}

func (d *SwapDriver) recoveryValue(
	output market.TokenAmount,
	terminalToken market.TokenID,
	asset market.AssetID,
) (market.AssetQuantity, error) {
	terminal, err := d.convertAmount(output, terminalToken)
	if err != nil {
		return market.AssetQuantity{}, err
	}
	decimals, ok := d.TokenDecimals[terminalToken]
	if !ok {
		return market.AssetQuantity{},
			fmt.Errorf("terminal quote-token decimals are unavailable")
	}
	return market.NewAssetQuantity(
		asset,
		new(big.Rat).SetFrac(
			terminal.Units(),
			decimalScale(decimals),
		),
	)
}

func (d *SwapDriver) exitStillQualified(
	plan execution.SequentialPlan,
	recovery market.AssetQuantity,
	incurred []execution.CostComponent,
	asset market.AssetID,
) (bool, error) {
	initial, err := d.recoveryValue(
		plan.InitialInput,
		plan.Stages[0].InputToken,
		asset,
	)
	if err != nil {
		return false, err
	}
	net, err := recovery.Sub(initial)
	if err != nil {
		return false, err
	}
	for _, component := range incurred {
		if component.IncludedInOutput {
			continue
		}
		if component.QuoteValue.Asset() != asset {
			return false, fmt.Errorf(
				"incurred exit cost is not valued in %s",
				asset,
			)
		}
		net, err = net.Sub(component.QuoteValue)
		if err != nil {
			return false, err
		}
	}
	return net.Rat().Cmp(cloneRatOrZero(d.MinimumNet)) >= 0, nil
}

func cloneRatOrZero(value *big.Rat) *big.Rat {
	if value == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(value)
}

func (d *SwapDriver) logExitDecision(
	decision execution.SequentialExitDecision,
	elapsed time.Duration,
) {
	returnOutput, returnRecovery := "", ""
	if !decision.ReturnOutput.IsZero() {
		returnOutput = decision.ReturnOutput.String()
	}
	if decision.ReturnRecovery.Asset() != "" {
		returnRecovery = decision.ReturnRecovery.Decimal(8)
	}
	d.write(
		"live_exit operation=%s route=%s destination_output_units=%s destination_recovery=%s return_output_units=%s return_recovery=%s safety_margin=%s destination_qualified=%t cost_evidence=%t evidence=%s latency=%s\n",
		decision.Operation,
		decision.Route,
		tokenUnitsOrEmpty(decision.DestinationOutput),
		decision.DestinationRecovery.Decimal(8),
		returnOutput,
		returnRecovery,
		decision.SafetyMargin.Decimal(8),
		decision.DestinationQualified,
		decision.CostEvidenceAvailable,
		decision.Evidence,
		elapsed.Round(10*time.Microsecond),
	)
}

func tokenUnitsOrEmpty(amount market.TokenAmount) string {
	if amount.IsZero() {
		return ""
	}
	return amount.String()
}

func (d *SwapDriver) binding(
	request execution.SequentialStageRequest,
) (SwapBinding, error) {
	if err := request.Validate(); err != nil {
		return SwapBinding{}, err
	}
	if request.Stage.Stage != execution.StageBuy &&
		request.Stage.Stage != execution.StageSell {
		return SwapBinding{}, fmt.Errorf("swap driver received bridge stage")
	}
	binding, ok := d.Bindings[request.Stage.Market]
	if !ok || binding.Account == "" || binding.Validator == nil ||
		binding.TxManager == nil {
		return SwapBinding{}, fmt.Errorf(
			"swap binding for market %q is unavailable", request.Stage.Market,
		)
	}
	if _, ok := binding.TxManager.(chainport.PreparedTransactionSimulator); !ok {
		return SwapBinding{}, fmt.Errorf(
			"swap binding for market %q has no transaction simulator",
			request.Stage.Market,
		)
	}
	return binding, nil
}

func (d *SwapDriver) prepareAndSimulate(
	ctx context.Context,
	request execution.SequentialStageRequest,
	binding SwapBinding,
	slippage *executionport.SlippageConstraint,
) (preparedSwap, error) {
	bundle, err := d.prepareSwap(ctx, request, binding, slippage)
	if err != nil {
		return preparedSwap{}, fmt.Errorf("quote/build preparation: %w", err)
	}
	if err := d.simulate(ctx, binding, bundle.prepared); err != nil {
		return preparedSwap{}, fmt.Errorf("transaction simulation: %w", err)
	}
	return bundle, nil
}

func (d *SwapDriver) prepareExitWithRetry(
	ctx context.Context,
	operation execution.OperationID,
	request execution.SequentialStageRequest,
	binding SwapBinding,
	slippage *executionport.SlippageConstraint,
) (preparedSwap, int, error) {
	return d.prepareSwapWithRetry(
		ctx,
		operation,
		request,
		binding,
		slippage,
		"live_exit_destination",
	)
}

func (d *SwapDriver) prepareSwapWithRetry(
	ctx context.Context,
	operation execution.OperationID,
	request execution.SequentialStageRequest,
	binding SwapBinding,
	slippage *executionport.SlippageConstraint,
	logEvent string,
) (preparedSwap, int, error) {
	attempts := d.ExitValidationAttempts
	if attempts <= 0 {
		attempts = 15
	}
	retryDelay := d.ExitValidationRetryDelay
	if retryDelay <= 0 {
		retryDelay = 100 * time.Millisecond
	}
	failures := make([]error, 0, attempts)
	for attempt := 1; attempt <= attempts; attempt++ {
		bundle, err := d.prepareAndSimulate(
			ctx,
			request,
			binding,
			slippage,
		)
		if err == nil {
			if attempt > 1 {
				d.write(
					"%s operation=%s status=ready attempt=%d/%d\n",
					logEvent,
					operation,
					attempt,
					attempts,
				)
			}
			return bundle, attempt, nil
		}
		attemptErr := fmt.Errorf(
			"attempt %d/%d: %w",
			attempt,
			attempts,
			err,
		)
		failures = append(failures, attemptErr)
		d.write(
			"%s operation=%s status=failed attempt=%d/%d error=%q\n",
			logEvent,
			operation,
			attempt,
			attempts,
			err,
		)
		var threshold *executionport.SlippageThresholdError
		if errors.As(err, &threshold) {
			return preparedSwap{}, attempt, errors.Join(failures...)
		}
		if attempt < attempts {
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return preparedSwap{}, attempt, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return preparedSwap{},
		attempts,
		errors.Join(failures...)
}

func (d *SwapDriver) prepareSwap(
	ctx context.Context,
	request execution.SequentialStageRequest,
	binding SwapBinding,
	slippage *executionport.SlippageConstraint,
) (preparedSwap, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	now := clock().UTC()
	validationRequest, err := swapValidationRequest(
		request,
		binding.Account,
		now,
	)
	if err != nil {
		return preparedSwap{}, err
	}
	validationRequest.Slippage = slippage
	started := clock()
	artifact, err := binding.Validator.Validate(ctx, validationRequest)
	if err != nil {
		return preparedSwap{}, err
	}
	if d.artifactExpired(artifact, clock()) {
		validationRequest.RequestedAt = clock().UTC()
		artifact, err = binding.Validator.Validate(ctx, validationRequest)
		if err != nil {
			return preparedSwap{}, err
		}
		if d.artifactExpired(artifact, clock()) {
			return preparedSwap{}, fmt.Errorf(
				"rebuilt swap artifact exceeded the pre-commit age limit",
			)
		}
	}
	var prepared chainport.PreparedTransaction
	compactRebuilds := 0
	for {
		artifact.Leg.ExpectedOutput = artifact.ValidatedQuote.AmountOut
		prepared, err = binding.TxManager.Prepare(ctx, artifact)
		if err == nil {
			break
		}
		var oversized *executionport.ArtifactTooLargeError
		compact, supportsCompact := binding.Validator.(executionport.CompactValidator)
		if !errors.As(err, &oversized) ||
			!supportsCompact ||
			compactRebuilds >= 3 {
			return preparedSwap{}, err
		}
		validationRequest.RequestedAt = clock().UTC()
		artifact, err = compact.ValidateCompact(
			ctx,
			validationRequest,
			artifact,
		)
		if err != nil {
			return preparedSwap{}, fmt.Errorf(
				"compact swap artifact after %d-byte transaction: %w",
				oversized.ActualBytes,
				err,
			)
		}
		compactRebuilds++
		if d.artifactExpired(artifact, clock()) {
			return preparedSwap{}, fmt.Errorf(
				"compact swap artifact exceeded the pre-commit age limit",
			)
		}
	}
	return preparedSwap{
		artifact: artifact, prepared: prepared,
		validationTime:  clock().Sub(started),
		compactRebuilds: compactRebuilds,
	}, nil
}

func swapValidationRequest(
	request execution.SequentialStageRequest,
	account execution.AccountID,
	now time.Time,
) (executionport.ValidationRequest, error) {
	if account == "" {
		return executionport.ValidationRequest{},
			fmt.Errorf("swap validation account is unavailable")
	}
	placeholder, err := market.NewTokenAmount(
		request.Stage.OutputToken,
		big.NewInt(1),
	)
	if err != nil {
		return executionport.ValidationRequest{}, err
	}
	side := execution.LegBuy
	if request.Stage.Stage == execution.StageSell {
		side = execution.LegSell
	}
	leg := execution.Leg{
		ID: execution.StepID(fmt.Sprintf(
			"%02d-%s",
			request.Stage.Ordinal,
			request.Stage.Stage,
		)),
		Side: side, Chain: request.Stage.SourceChain,
		Account: account, Market: request.Stage.Market,
		Input: request.Input, ExpectedOutput: placeholder,
	}
	discovery, err := market.NewQuote(market.Quote{
		Source: "live", Market: request.Stage.Market,
		SnapshotVersion: 1, Purpose: market.QuotePurposeLiveDiscovery,
		Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Input, AmountOut: placeholder, QuotedAt: now,
	})
	if err != nil {
		return executionport.ValidationRequest{}, err
	}
	return executionport.ValidationRequest{
		Operation: request.Operation, Leg: leg, Discovery: discovery,
		RequestedAt: now,
	}, nil
}

func (d *SwapDriver) simulate(
	ctx context.Context,
	binding SwapBinding,
	prepared chainport.PreparedTransaction,
) error {
	simulator := binding.TxManager.(chainport.PreparedTransactionSimulator)
	return simulator.SimulatePrepared(ctx, prepared)
}

func (d *SwapDriver) takePreparedSwap(
	request execution.SequentialStageRequest,
) (preparedSwap, bool) {
	if request.Stage.Ordinal != 1 &&
		request.Stage.Ordinal != 2 &&
		request.Stage.Ordinal != 3 &&
		request.Stage.Ordinal != 5 {
		return preparedSwap{}, false
	}
	d.preflightMu.Lock()
	var bundle preparedSwap
	var ok bool
	if request.Stage.Ordinal == 1 &&
		request.Stage.Stage == execution.StageBuy {
		bundle, ok = d.preflightBuys[request.Operation]
		delete(d.preflightBuys, request.Operation)
	} else if request.Stage.Stage == execution.StageSell {
		bundle, ok = d.exitSells[request.Operation]
		delete(d.exitSells, request.Operation)
	}
	d.preflightMu.Unlock()
	if !ok ||
		bundle.artifact.Leg.Market != request.Stage.Market ||
		bundle.artifact.Leg.Input.Token() != request.Input.Token() ||
		bundle.artifact.Leg.Input.Units().Cmp(request.Input.Units()) != 0 {
		return preparedSwap{}, false
	}
	return bundle, true
}

func (d *SwapDriver) artifactExpired(
	artifact executionport.Artifact,
	now time.Time,
) bool {
	return d.ArtifactMaxAge > 0 &&
		now.UTC().Sub(artifact.BuiltAt) > d.ArtifactMaxAge
}

func (d *SwapDriver) logPrepared(
	request execution.SequentialStageRequest,
	bundle preparedSwap,
	preflightReused bool,
) {
	if bundle.compactRebuilds > 0 {
		d.write(
			"live_stage operation=%s stage=%d/%s phase=artifact_compacted rebuilds=%d max_accounts=%s serialized_bytes=%d\n",
			request.Operation,
			request.Stage.Ordinal,
			request.Stage.Stage,
			bundle.compactRebuilds,
			bundle.artifact.Metadata["max_accounts"],
			len(bundle.prepared.SignedPayload),
		)
	}
	attempts := bundle.artifact.Metadata["build_attempts"]
	if reason := bundle.artifact.Metadata["slippage_reason"]; reason != "" {
		d.write(
			"live_stage operation=%s stage=%d/%s phase=dynamic_slippage reason=%s bps=%s expected_output_units=%s minimum_output_units=%s required_final_units=%s budget_units=%s\n",
			request.Operation,
			request.Stage.Ordinal,
			request.Stage.Stage,
			reason,
			bundle.artifact.Metadata["slippage_bps"],
			bundle.artifact.ValidatedQuote.AmountOut,
			bundle.artifact.Metadata["minimum_output_units"],
			bundle.artifact.Metadata["slippage_required_final_units"],
			dynamicBudgetMetadata(bundle.artifact.Metadata),
		)
	}
	d.write(
		"live_stage operation=%s stage=%d/%s phase=artifact_ready input_units=%s output_units=%s build_attempts=%s preflight_reused=%t latency=%s\n",
		request.Operation,
		request.Stage.Ordinal,
		request.Stage.Stage,
		request.Input,
		bundle.artifact.ValidatedQuote.AmountOut,
		attempts,
		preflightReused,
		bundle.validationTime.Round(10*time.Microsecond),
	)
	d.write(
		"live_stage operation=%s stage=%d/%s phase=simulation_ready tx=%s\n",
		request.Operation,
		request.Stage.Ordinal,
		request.Stage.Stage,
		bundle.prepared.Identity.Hash,
	)
}

func dynamicBudgetMetadata(metadata map[string]string) string {
	if value := metadata["slippage_dynamic_budget_units"]; value != "" {
		return value
	}
	return metadata["slippage_remaining_budget_units"]
}

func (d *SwapDriver) bridgeDestinationAmount(
	source market.TokenAmount,
	destinationToken market.TokenID,
) (market.TokenAmount, error) {
	sourceDecimals, sourceOK := d.TokenDecimals[source.Token()]
	destinationDecimals, destinationOK := d.TokenDecimals[destinationToken]
	if !sourceOK || !destinationOK {
		return market.TokenAmount{}, fmt.Errorf(
			"bridge preflight token decimals are unavailable",
		)
	}
	precision := d.BridgePrecision
	if precision == 0 {
		precision = 8
	}
	if sourceDecimals < precision {
		precision = sourceDecimals
	}
	if destinationDecimals < precision {
		precision = destinationDecimals
	}
	sourceScale := decimalScale(sourceDecimals - precision)
	messageUnits := new(big.Int).Quo(source.Units(), sourceScale)
	if messageUnits.Sign() <= 0 {
		return market.TokenAmount{}, fmt.Errorf(
			"preflight buy output is below bridge precision",
		)
	}
	destinationUnits := new(big.Int).Mul(
		messageUnits,
		decimalScale(destinationDecimals-precision),
	)
	return market.NewTokenAmount(destinationToken, destinationUnits)
}

func (d *SwapDriver) convertAmount(
	source market.TokenAmount,
	destinationToken market.TokenID,
) (market.TokenAmount, error) {
	sourceDecimals, sourceOK := d.TokenDecimals[source.Token()]
	destinationDecimals, destinationOK := d.TokenDecimals[destinationToken]
	if !sourceOK || !destinationOK {
		return market.TokenAmount{}, fmt.Errorf(
			"preflight comparison token decimals are unavailable",
		)
	}
	units := source.Units()
	switch {
	case sourceDecimals > destinationDecimals:
		units.Quo(units, decimalScale(sourceDecimals-destinationDecimals))
	case sourceDecimals < destinationDecimals:
		units.Mul(units, decimalScale(destinationDecimals-sourceDecimals))
	}
	return market.NewTokenAmount(destinationToken, units)
}

func decimalScale(decimals uint8) *big.Int {
	return new(big.Int).Exp(
		big.NewInt(10),
		big.NewInt(int64(decimals)),
		nil,
	)
}

func (d *SwapDriver) write(format string, arguments ...any) {
	if d.Output != nil {
		d.outputMu.Lock()
		defer d.outputMu.Unlock()
		_, _ = fmt.Fprintf(d.Output, format, arguments...)
	}
}

func (d *SwapDriver) confirm(
	ctx context.Context,
	binding SwapBinding,
	step execution.OperationStep,
	fallbackAfter time.Duration,
) (execution.Settlement, error) {
	if binding.Confirmation == nil {
		return pollSwapSettlement(ctx, binding.TxManager, step)
	}
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type confirmationResult struct {
		settlement execution.Settlement
		err        error
	}
	websocketResult := make(chan confirmationResult, 1)
	rpcResult := make(chan confirmationResult, 1)
	go func() {
		websocketCtx, websocketCancel := context.WithTimeout(
			raceCtx,
			fallbackAfter,
		)
		defer websocketCancel()
		settlement, err := binding.Confirmation.Await(websocketCtx, step)
		websocketResult <- confirmationResult{settlement: settlement, err: err}
	}()
	go func() {
		settlement, err := pollSwapSettlement(
			raceCtx,
			binding.TxManager,
			step,
		)
		rpcResult <- confirmationResult{settlement: settlement, err: err}
	}()
	websocket := (<-chan confirmationResult)(websocketResult)
	for {
		select {
		case result := <-websocket:
			websocket = nil
			if result.err == nil &&
				result.settlement.Technical ==
					execution.StateConfirmedSuccess &&
				result.settlement.Economic ==
					execution.EconomicEffectVerified {
				return result.settlement, nil
			}
			// Inclusion-only evidence deliberately does not settle the swap.
			// Receipt reconciliation is already running concurrently.
		case result := <-rpcResult:
			return result.settlement, result.err
		case <-ctx.Done():
			return execution.Settlement{}, ctx.Err()
		}
	}
}

func pollSwapSettlement(
	ctx context.Context,
	manager chainport.TxManager,
	step execution.OperationStep,
) (execution.Settlement, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		settlement, err := manager.Reconcile(ctx, step)
		if err == nil {
			switch settlement.Technical {
			case execution.StateConfirmedSuccess,
				execution.StateConfirmedRevert:
				return settlement, nil
			}
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return execution.Settlement{}, err
			}
			return execution.Settlement{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

var _ executionport.SequentialStageDriver = (*SwapDriver)(nil)
var _ executionport.SequentialPreflight = (*SwapDriver)(nil)
var _ executionport.SequentialExitSelector = (*SwapDriver)(nil)
var _ executionport.SequentialRecoveryExitSelector = (*SwapDriver)(nil)
var _ executionport.SequentialRecoveryDriver = (*SwapDriver)(nil)
var _ executionport.SequentialPrefundedExitSelector = (*SwapDriver)(nil)
var _ executionport.SequentialInputConverter = (*SwapDriver)(nil)
