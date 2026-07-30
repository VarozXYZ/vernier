package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func (s *SequentialLiveStore) CreateRecoverableSequentialOperation(
	ctx context.Context,
	operation domainexecution.SequentialOperation,
	plan domainexecution.SequentialPlan,
) error {
	if operation.ID == "" || operation.Plan == "" ||
		operation.Plan != plan.ID || len(plan.Stages) != 4 ||
		operation.CurrentAmount.IsZero() ||
		plan.Opportunity.SelectedIndex < 0 ||
		plan.Opportunity.SelectedIndex >= len(plan.Opportunity.Candidates) {
		return fmt.Errorf("recoverable sequential operation is incomplete")
	}
	candidate := plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex]
	if candidate.Input.Asset() == "" ||
		candidate.BuyQuote.AmountOut.IsZero() ||
		candidate.SellQuote.AmountIn.IsZero() ||
		candidate.SellQuote.AmountOut.IsZero() {
		return fmt.Errorf("recoverable sequential economic snapshot is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	started := operation.StartedAt.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO sequential_live_operations (
		operation_id, plan_id, opportunity_id, config_hash, state,
		current_stage, current_token, current_units, started_at, updated_at
	) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)`,
		operation.ID, operation.Plan, operation.OpportunityID,
		operation.ConfigHash, operation.State, operation.CurrentAmount.Token(),
		operation.CurrentAmount.Units().String(), started, started,
	); err != nil {
		return fmt.Errorf("create recoverable sequential operation: %w", err)
	}
	forced := 0
	for _, reason := range plan.Opportunity.Reasons {
		if reason == "forced_canary" {
			forced = 1
			break
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sequential_live_plan_snapshots (
		operation_id, plan_id, evaluation_id, config_hash,
		buy_market, sell_market, initial_token, initial_units,
		discovery_token, discovery_units, input_asset, input_value,
		buy_output_token, buy_output_units, sell_input_token,
		sell_input_units, sell_output_token, sell_output_units,
		forced_canary, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		operation.ID, plan.ID, plan.Opportunity.Evaluation,
		plan.Opportunity.ConfigHash, plan.Opportunity.Direction.BuyMarket,
		plan.Opportunity.Direction.SellMarket, plan.InitialInput.Token(),
		plan.InitialInput.Units().String(), plan.DiscoveryAmount.Token(),
		plan.DiscoveryAmount.Units().String(), candidate.Input.Asset(),
		candidate.Input.String(), candidate.BuyQuote.AmountOut.Token(),
		candidate.BuyQuote.AmountOut.Units().String(),
		candidate.SellQuote.AmountIn.Token(),
		candidate.SellQuote.AmountIn.Units().String(),
		candidate.SellQuote.AmountOut.Token(),
		candidate.SellQuote.AmountOut.Units().String(), forced,
		plan.CreatedAt.UTC().Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("persist sequential plan snapshot: %w", err)
	}
	for _, stage := range plan.Stages {
		if err := stage.Validate(); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO sequential_live_plan_stages (
			operation_id, ordinal, stage, source_chain, destination_chain,
			input_token, output_token, market_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			operation.ID, stage.Ordinal, stage.Stage, stage.SourceChain,
			stage.DestinationChain, stage.InputToken, stage.OutputToken,
			stage.Market,
		); err != nil {
			return fmt.Errorf("persist sequential plan stage: %w", err)
		}
	}
	return tx.Commit()
}

func (s *SequentialLiveStore) LoadSequentialRecovery(
	ctx context.Context,
	operationID domainexecution.OperationID,
) (executionport.SequentialRecoverySnapshot, error) {
	operation, found, err := s.loadSequentialOperation(ctx, operationID)
	if err != nil {
		return executionport.SequentialRecoverySnapshot{}, err
	}
	if !found {
		return executionport.SequentialRecoverySnapshot{},
			fmt.Errorf("sequential operation %s was not found", operationID)
	}
	plan, err := s.loadSequentialPlan(ctx, operation)
	if err != nil {
		return executionport.SequentialRecoverySnapshot{}, err
	}
	transactions, err := s.loadSequentialTransactions(ctx, operationID)
	if err != nil {
		return executionport.SequentialRecoverySnapshot{}, err
	}
	settlements, costs, err := s.loadSequentialSettlements(
		ctx,
		operation,
		plan,
		transactions,
	)
	if err != nil {
		return executionport.SequentialRecoverySnapshot{}, err
	}
	exit, err := s.loadSequentialExitDecision(ctx, operationID)
	if err != nil {
		return executionport.SequentialRecoverySnapshot{}, err
	}
	return executionport.SequentialRecoverySnapshot{
		Operation: operation, Plan: plan, Transactions: transactions,
		Settlements: settlements, Costs: costs, ExitDecision: exit,
	}, nil
}

func (s *SequentialLiveStore) SetSequentialRecoveryState(
	ctx context.Context,
	operationID domainexecution.OperationID,
	state domainexecution.SequentialOperationState,
	cause error,
) error {
	switch state {
	case domainexecution.SequentialRecovering,
		domainexecution.SequentialRecoveryBlocked,
		domainexecution.SequentialAborted:
	default:
		return fmt.Errorf("invalid sequential recovery state %q", state)
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_operations
		SET state=?, last_error=?, updated_at=?
		WHERE operation_id=? AND state IN (
			'running', 'recovering', 'manual_intervention_required'
		)`,
		state, message, time.Now().UTC().Format(time.RFC3339Nano),
		operationID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("active sequential operation was not found")
	}
	return nil
}

func (s *SequentialLiveStore) RecordSequentialRecoveryAttempt(
	ctx context.Context,
	attempt executionport.SequentialRecoveryAttempt,
) error {
	if attempt.Operation == "" || attempt.Ordinal < 1 ||
		attempt.Action == "" || attempt.Reason == "" ||
		attempt.Attempt < 1 || attempt.CreatedAt.IsZero() {
		return fmt.Errorf("sequential recovery attempt is incomplete")
	}
	retryAt := ""
	if !attempt.RetryAt.IsZero() {
		retryAt = attempt.RetryAt.UTC().Format(time.RFC3339Nano)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO
		sequential_live_recovery_attempts (
			operation_id, attempt_index, ordinal, action, reason,
			detail, attempt, created_at, retry_at
		)
		SELECT ?, COALESCE(MAX(attempt_index), 0)+1, ?, ?, ?, ?, ?, ?, ?
		FROM sequential_live_recovery_attempts
		WHERE operation_id=?`,
		attempt.Operation, attempt.Ordinal, attempt.Action, attempt.Reason,
		attempt.Detail, attempt.Attempt,
		attempt.CreatedAt.UTC().Format(time.RFC3339Nano), retryAt,
		attempt.Operation,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sequential_live_transactions
		SET recovery_reason=?, recovery_attempts=?, next_recovery_attempt=?,
			last_error=?, updated_at=?
		WHERE operation_id=? AND ordinal=? AND phase=(
			SELECT phase FROM sequential_live_transactions
			WHERE operation_id=? AND ordinal=?
			ORDER BY prepared_at DESC LIMIT 1
		)`,
		attempt.Reason, attempt.Attempt, retryAt, attempt.Detail,
		attempt.CreatedAt.UTC().Format(time.RFC3339Nano),
		attempt.Operation, attempt.Ordinal,
		attempt.Operation, attempt.Ordinal,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SequentialLiveStore) loadSequentialOperation(
	ctx context.Context,
	operationID domainexecution.OperationID,
) (domainexecution.SequentialOperation, bool, error) {
	var operation domainexecution.SequentialOperation
	var token, units, started, updated string
	err := s.db.QueryRowContext(ctx, `SELECT operation_id, plan_id,
		opportunity_id, config_hash, state, current_stage, current_token,
		current_units, last_error, started_at, updated_at
		FROM sequential_live_operations WHERE operation_id=?`,
		operationID,
	).Scan(
		&operation.ID, &operation.Plan, &operation.OpportunityID,
		&operation.ConfigHash, &operation.State, &operation.CurrentStage,
		&token, &units, &operation.LastError, &started, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domainexecution.SequentialOperation{}, false, nil
	}
	if err != nil {
		return domainexecution.SequentialOperation{}, false, err
	}
	operation.CurrentAmount, err = market.ParseTokenAmount(
		market.TokenID(token),
		units,
	)
	if err != nil {
		return domainexecution.SequentialOperation{}, false, err
	}
	operation.StartedAt, err = time.Parse(time.RFC3339Nano, started)
	if err != nil {
		return domainexecution.SequentialOperation{}, false, err
	}
	operation.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	return operation, true, err
}

func (s *SequentialLiveStore) loadSequentialPlan(
	ctx context.Context,
	operation domainexecution.SequentialOperation,
) (domainexecution.SequentialPlan, error) {
	var (
		planID, evaluation, configHash, buyMarket, sellMarket      string
		initialToken, initialUnits, discoveryToken, discoveryUnits string
		inputAsset, inputValue                                     string
		buyOutputToken, buyOutputUnits                             string
		sellInputToken, sellInputUnits                             string
		sellOutputToken, sellOutputUnits, created                  string
		forced                                                     int
	)
	err := s.db.QueryRowContext(ctx, `SELECT plan_id, evaluation_id,
		config_hash, buy_market, sell_market, initial_token, initial_units,
		discovery_token, discovery_units, input_asset, input_value,
		buy_output_token, buy_output_units, sell_input_token,
		sell_input_units, sell_output_token, sell_output_units,
		forced_canary, created_at
		FROM sequential_live_plan_snapshots WHERE operation_id=?`,
		operation.ID,
	).Scan(
		&planID, &evaluation, &configHash, &buyMarket, &sellMarket,
		&initialToken, &initialUnits, &discoveryToken, &discoveryUnits,
		&inputAsset, &inputValue, &buyOutputToken, &buyOutputUnits,
		&sellInputToken, &sellInputUnits, &sellOutputToken,
		&sellOutputUnits, &forced, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domainexecution.SequentialPlan{}, fmt.Errorf(
			"operation %s has no durable recovery plan",
			operation.ID,
		)
	}
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	initial, err := market.ParseTokenAmount(
		market.TokenID(initialToken),
		initialUnits,
	)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	discovery, err := market.ParseTokenAmount(
		market.TokenID(discoveryToken),
		discoveryUnits,
	)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	input, err := market.ParseAssetQuantity(
		market.AssetID(inputAsset),
		inputValue,
	)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	buyOutput, err := market.ParseTokenAmount(
		market.TokenID(buyOutputToken),
		buyOutputUnits,
	)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	sellInput, err := market.ParseTokenAmount(
		market.TokenID(sellInputToken),
		sellInputUnits,
	)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	sellOutput, err := market.ParseTokenAmount(
		market.TokenID(sellOutputToken),
		sellOutputUnits,
	)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ordinal, stage,
		source_chain, destination_chain, input_token, output_token, market_id
		FROM sequential_live_plan_stages
		WHERE operation_id=? ORDER BY ordinal`,
		operation.ID,
	)
	if err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	defer rows.Close()
	var stages []domainexecution.SequentialStagePlan
	for rows.Next() {
		var stage domainexecution.SequentialStagePlan
		if err := rows.Scan(
			&stage.Ordinal, &stage.Stage, &stage.SourceChain,
			&stage.DestinationChain, &stage.InputToken, &stage.OutputToken,
			&stage.Market,
		); err != nil {
			return domainexecution.SequentialPlan{}, err
		}
		stages = append(stages, stage)
	}
	if err := rows.Err(); err != nil {
		return domainexecution.SequentialPlan{}, err
	}
	if len(stages) != 4 {
		return domainexecution.SequentialPlan{},
			fmt.Errorf("durable sequential plan has %d stages", len(stages))
	}
	reasons := []string(nil)
	if forced != 0 {
		reasons = []string{"forced_canary"}
	}
	candidate := arbitrage.Candidate{
		Input: input,
		BuyQuote: market.Quote{
			Market:   market.MarketID(buyMarket),
			AmountIn: initial, AmountOut: buyOutput,
		},
		SellQuote: market.Quote{
			Market:   market.MarketID(sellMarket),
			AmountIn: sellInput, AmountOut: sellOutput,
		},
	}
	return domainexecution.SequentialPlan{
		ID: domainexecution.PlanID(planID),
		Opportunity: arbitrage.Opportunity{
			Evaluation: arbitrage.EvaluationID(evaluation),
			ConfigHash: configHash,
			Direction: arbitrage.Direction{
				BuyMarket:  market.MarketID(buyMarket),
				SellMarket: market.MarketID(sellMarket),
			},
			Classification: arbitrage.ClassificationPolicyQualified,
			Candidates:     []arbitrage.Candidate{candidate},
			SelectedIndex:  0,
			Reasons:        reasons,
		},
		InitialInput: initial, DiscoveryAmount: discovery,
		Stages: stages, CreatedAt: createdAt.UTC(),
	}, nil
}

func (s *SequentialLiveStore) loadSequentialTransactions(
	ctx context.Context,
	operationID domainexecution.OperationID,
) ([]executionport.SequentialTransactionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT ordinal, phase, chain_name,
		account_id, identity, nonce, blockhash, last_valid_block_height,
		status, last_error, prepared_at, updated_at, first_uncertain_at,
		recovery_reason, recovery_attempts, next_recovery_attempt
		FROM sequential_live_transactions
		WHERE operation_id=? ORDER BY ordinal, prepared_at`,
		operationID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []executionport.SequentialTransactionRecord
	for rows.Next() {
		var record executionport.SequentialTransactionRecord
		var chain, account, hash, nonce, blockhash string
		var prepared, updated, uncertain, retry string
		var lastValid uint64
		record.Operation = operationID
		if err := rows.Scan(
			&record.Ordinal, &record.Phase, &chain, &account, &hash,
			&nonce, &blockhash, &lastValid, &record.Status,
			&record.LastError, &prepared, &updated, &uncertain,
			&record.RecoveryReason, &record.RecoveryAttempts, &retry,
		); err != nil {
			return nil, err
		}
		record.Identity = domainexecution.TransactionIdentity{
			Chain:   market.ChainID(chain),
			Account: domainexecution.AccountID(account),
			Hash:    hash, Blockhash: blockhash,
			LastValidBlockHeight: lastValid,
		}
		if nonce != "" {
			value, parseErr := strconv.ParseUint(nonce, 10, 64)
			if parseErr != nil {
				return nil, parseErr
			}
			record.Identity.Nonce = &value
		}
		record.PreparedAt, err = time.Parse(time.RFC3339Nano, prepared)
		if err != nil {
			return nil, err
		}
		record.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
		if err != nil {
			return nil, err
		}
		if uncertain != "" {
			record.FirstUncertainAt, err =
				time.Parse(time.RFC3339Nano, uncertain)
			if err != nil {
				return nil, err
			}
		}
		if retry != "" {
			record.NextRecoveryAttempt, err =
				time.Parse(time.RFC3339Nano, retry)
			if err != nil {
				return nil, err
			}
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *SequentialLiveStore) loadSequentialSettlements(
	ctx context.Context,
	operation domainexecution.SequentialOperation,
	plan domainexecution.SequentialPlan,
	transactions []executionport.SequentialTransactionRecord,
) ([]domainexecution.SequentialStageSettlement, []domainexecution.CostComponent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT ordinal, stage,
		input_token, input_units, output_token, output_units,
		source_identity, destination_identity, destination_balance_before,
		destination_balance_after, evidence, observed_at
		FROM sequential_live_settlements
		WHERE operation_id=? ORDER BY ordinal`,
		operation.ID,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var settlements []domainexecution.SequentialStageSettlement
	for rows.Next() {
		var ordinal int
		var stageName domainexecution.SequentialStage
		var inputToken, inputUnits, outputToken, outputUnits string
		var sourceHash, destinationHash, balanceBefore, balanceAfter string
		var evidence, observed string
		if err := rows.Scan(
			&ordinal, &stageName, &inputToken, &inputUnits, &outputToken,
			&outputUnits, &sourceHash, &destinationHash,
			&balanceBefore, &balanceAfter, &evidence, &observed,
		); err != nil {
			return nil, nil, err
		}
		if ordinal < 1 || ordinal > len(plan.Stages) {
			return nil, nil, fmt.Errorf("settlement ordinal is invalid")
		}
		input, err := market.ParseTokenAmount(
			market.TokenID(inputToken),
			inputUnits,
		)
		if err != nil {
			return nil, nil, err
		}
		output, err := market.ParseTokenAmount(
			market.TokenID(outputToken),
			outputUnits,
		)
		if err != nil {
			return nil, nil, err
		}
		observedAt, err := time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			return nil, nil, err
		}
		source := recoveryIdentity(sourceHash, transactions)
		var destination *domainexecution.TransactionIdentity
		if destinationHash != "" {
			value := recoveryIdentity(destinationHash, transactions)
			destination = &value
		}
		var destinationBalanceBefore, destinationBalanceAfter *big.Int
		if balanceBefore != "" || balanceAfter != "" {
			var beforeOK, afterOK bool
			destinationBalanceBefore, beforeOK =
				new(big.Int).SetString(balanceBefore, 10)
			destinationBalanceAfter, afterOK =
				new(big.Int).SetString(balanceAfter, 10)
			if !beforeOK || !afterOK {
				return nil, nil, fmt.Errorf(
					"stored destination balance evidence is invalid",
				)
			}
		}
		settlements = append(settlements, domainexecution.SequentialStageSettlement{
			Request: domainexecution.SequentialStageRequest{
				Operation: operation.ID, Plan: plan.ID,
				Stage: plan.Stages[ordinal-1], Input: input,
			},
			ActualInput: input, ActualOutput: output,
			SourceIdentity: source, DestinationIdentity: destination,
			DestinationBalanceBefore: destinationBalanceBefore,
			DestinationBalanceAfter:  destinationBalanceAfter,
			ObservedAt:               observedAt.UTC(), Evidence: evidence,
		})
		_ = stageName
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	costs, err := s.loadSequentialCosts(ctx, operation.ID)
	if err != nil {
		return nil, nil, err
	}
	return settlements, costs, nil
}

func (s *SequentialLiveStore) loadSequentialCosts(
	ctx context.Context,
	operationID domainexecution.OperationID,
) ([]domainexecution.CostComponent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT kind, chain_name, asset,
		amount_value, quote_asset, quote_value, included_in_output, evidence
		FROM sequential_live_costs
		WHERE operation_id=? ORDER BY ordinal, component_index`,
		operationID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var costs []domainexecution.CostComponent
	for rows.Next() {
		var kind, chain, asset, amount, quoteAsset, quoteValue, evidence string
		var included int
		if err := rows.Scan(
			&kind, &chain, &asset, &amount, &quoteAsset, &quoteValue,
			&included, &evidence,
		); err != nil {
			return nil, err
		}
		quantity, err := market.ParseAssetQuantity(
			market.AssetID(asset),
			amount,
		)
		if err != nil {
			return nil, err
		}
		component := domainexecution.CostComponent{
			Kind: kind, Chain: market.ChainID(chain), Amount: quantity,
			IncludedInOutput: included != 0, Evidence: evidence,
		}
		if quoteAsset != "" {
			component.QuoteValue, err = market.ParseAssetQuantity(
				market.AssetID(quoteAsset),
				quoteValue,
			)
			if err != nil {
				return nil, err
			}
		}
		costs = append(costs, component)
	}
	return costs, rows.Err()
}

func (s *SequentialLiveStore) loadSequentialExitDecision(
	ctx context.Context,
	operationID domainexecution.OperationID,
) (*domainexecution.SequentialExitDecision, error) {
	var route string
	var destinationToken, destinationUnits, returnToken, returnUnits string
	var asset, destinationRecovery, returnRecovery, margin, evidence, decided string
	var qualified, costsAvailable int
	err := s.db.QueryRowContext(ctx, `SELECT route,
		destination_output_token, destination_output_units,
		return_output_token, return_output_units, recovery_asset,
		destination_recovery, return_recovery, safety_margin,
		destination_qualified, cost_evidence_available, evidence, decided_at
		FROM sequential_live_exit_decisions WHERE operation_id=?`,
		operationID,
	).Scan(
		&route, &destinationToken, &destinationUnits, &returnToken,
		&returnUnits, &asset, &destinationRecovery, &returnRecovery,
		&margin, &qualified, &costsAvailable, &evidence, &decided,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	decision := &domainexecution.SequentialExitDecision{
		Operation:             operationID,
		Route:                 domainexecution.SequentialExitRoute(route),
		DestinationQualified:  qualified != 0,
		CostEvidenceAvailable: costsAvailable != 0,
		Evidence:              evidence,
	}
	if destinationToken != "" {
		decision.DestinationOutput, err = market.ParseTokenAmount(
			market.TokenID(destinationToken),
			destinationUnits,
		)
		if err != nil {
			return nil, err
		}
	}
	if returnToken != "" {
		decision.ReturnOutput, err = market.ParseTokenAmount(
			market.TokenID(returnToken),
			returnUnits,
		)
		if err != nil {
			return nil, err
		}
	}
	decision.DestinationRecovery, err = market.ParseAssetQuantity(
		market.AssetID(asset),
		destinationRecovery,
	)
	if err != nil {
		return nil, err
	}
	if returnRecovery != "" {
		decision.ReturnRecovery, err = market.ParseAssetQuantity(
			market.AssetID(asset),
			returnRecovery,
		)
		if err != nil {
			return nil, err
		}
	}
	decision.SafetyMargin, err = market.ParseAssetQuantity(
		market.AssetID(asset),
		margin,
	)
	if err != nil {
		return nil, err
	}
	decision.DecidedAt, err = time.Parse(time.RFC3339Nano, decided)
	return decision, err
}

func recoveryIdentity(
	hash string,
	transactions []executionport.SequentialTransactionRecord,
) domainexecution.TransactionIdentity {
	for _, transaction := range transactions {
		if transaction.Identity.Hash == hash {
			return transaction.Identity
		}
	}
	return domainexecution.TransactionIdentity{Hash: strings.TrimSpace(hash)}
}

var _ executionport.SequentialRecoveryJournal = (*SequentialLiveStore)(nil)
