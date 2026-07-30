package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func (s *SequentialLiveStore) CreateRefuel(
	ctx context.Context,
	record executionport.RefuelRecord,
) error {
	if record.ID == "" || record.Chain == "" ||
		record.State != executionport.RefuelPrepared ||
		record.Input.IsZero() || record.NativeAsset == "" ||
		record.BalanceBefore.Asset() != record.NativeAsset ||
		record.CreatedAt.IsZero() {
		return fmt.Errorf("refuel record is incomplete")
	}
	if err := record.Identity.Validate(); err != nil {
		return err
	}
	nonce := ""
	if record.Identity.Nonce != nil {
		nonce = strconv.FormatUint(*record.Identity.Nonce, 10)
	}
	at := record.CreatedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT INTO live_refuel_operations (
		refuel_id, chain_name, state, input_token, input_units,
		native_asset, balance_before, tx_chain, tx_account, tx_identity,
		tx_nonce, tx_blockhash, tx_last_valid_height, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.Chain, record.State, record.Input.Token(),
		record.Input.Units().String(), record.NativeAsset,
		record.BalanceBefore.String(), record.Identity.Chain,
		record.Identity.Account, record.Identity.Hash, nonce,
		record.Identity.Blockhash, record.Identity.LastValidBlockHeight,
		at, at,
	)
	if err != nil {
		return fmt.Errorf("create durable refuel: %w", err)
	}
	return nil
}

func (s *SequentialLiveStore) MarkRefuelBroadcast(
	ctx context.Context,
	id string,
	identity domainexecution.TransactionIdentity,
) error {
	if id == "" {
		return fmt.Errorf("refuel ID is required")
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE live_refuel_operations
		SET state=?, updated_at=?
		WHERE refuel_id=? AND state='prepared'`,
		executionport.RefuelBroadcast,
		time.Now().UTC().Format(time.RFC3339Nano), id,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("prepared refuel was not found")
	}
	return nil
}

func (s *SequentialLiveStore) FinishRefuel(
	ctx context.Context,
	record executionport.RefuelRecord,
) error {
	switch record.State {
	case executionport.RefuelCompleted,
		executionport.RefuelFailed,
		executionport.RefuelOutcomeUnknown:
	default:
		return fmt.Errorf("invalid terminal refuel state %q", record.State)
	}
	balanceAfter, received, fee := "", "", ""
	if record.BalanceAfter.Asset() != "" {
		balanceAfter = record.BalanceAfter.String()
	}
	if record.NativeReceived.Asset() != "" {
		received = record.NativeReceived.String()
	}
	if record.Fee.Asset() != "" {
		fee = record.Fee.String()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE live_refuel_operations
		SET state=?, balance_after=?, native_received=?, fee_value=?,
			last_error=?, updated_at=?
		WHERE refuel_id=? AND state IN (
			'prepared', 'broadcast', 'outcome_unknown'
		)`,
		record.State, balanceAfter, received, fee, record.LastError,
		time.Now().UTC().Format(time.RFC3339Nano), record.ID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("active refuel was not found")
	}
	return nil
}

func (s *SequentialLiveStore) ActiveRefuel(
	ctx context.Context,
) (executionport.RefuelRecord, bool, error) {
	return s.loadRefuel(ctx, `WHERE state IN (
		'prepared', 'broadcast', 'outcome_unknown'
	) ORDER BY created_at LIMIT 1`)
}

func (s *SequentialLiveStore) LastCompletedRefuel(
	ctx context.Context,
	chain market.ChainID,
) (executionport.RefuelRecord, bool, error) {
	if chain == "" {
		return executionport.RefuelRecord{}, false,
			fmt.Errorf("refuel chain is required")
	}
	return s.loadRefuel(
		ctx,
		`WHERE state='completed' AND chain_name=?
			ORDER BY updated_at DESC LIMIT 1`,
		chain,
	)
}

func (s *SequentialLiveStore) loadRefuel(
	ctx context.Context,
	clause string,
	args ...any,
) (executionport.RefuelRecord, bool, error) {
	var record executionport.RefuelRecord
	var inputToken, inputUnits, nativeAsset, balanceBefore string
	var balanceAfter, received, fee string
	var txChain, txAccount, txIdentity, nonce, blockhash string
	var lastValid uint64
	var created, updated string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT refuel_id, chain_name, state, input_token, input_units,
			native_asset, balance_before, balance_after, native_received,
			fee_value, tx_chain, tx_account, tx_identity, tx_nonce,
			tx_blockhash, tx_last_valid_height, last_error,
			created_at, updated_at
			FROM live_refuel_operations `+clause,
		args...,
	).Scan(
		&record.ID, &record.Chain, &record.State,
		&inputToken, &inputUnits, &nativeAsset, &balanceBefore,
		&balanceAfter, &received, &fee, &txChain, &txAccount,
		&txIdentity, &nonce, &blockhash, &lastValid,
		&record.LastError, &created, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return executionport.RefuelRecord{}, false, nil
	}
	if err != nil {
		return executionport.RefuelRecord{}, false, err
	}
	record.Input, err = market.ParseTokenAmount(
		market.TokenID(inputToken),
		inputUnits,
	)
	if err != nil {
		return executionport.RefuelRecord{}, false, err
	}
	record.NativeAsset = market.AssetID(nativeAsset)
	record.BalanceBefore, err = market.ParseAssetQuantity(
		record.NativeAsset,
		balanceBefore,
	)
	if err != nil {
		return executionport.RefuelRecord{}, false, err
	}
	if balanceAfter != "" {
		record.BalanceAfter, err = market.ParseAssetQuantity(
			record.NativeAsset,
			balanceAfter,
		)
		if err != nil {
			return executionport.RefuelRecord{}, false, err
		}
	}
	if received != "" {
		record.NativeReceived, err = market.ParseAssetQuantity(
			record.NativeAsset,
			received,
		)
		if err != nil {
			return executionport.RefuelRecord{}, false, err
		}
	}
	if fee != "" {
		record.Fee, err = market.ParseAssetQuantity(
			record.NativeAsset,
			fee,
		)
		if err != nil {
			return executionport.RefuelRecord{}, false, err
		}
	}
	if txIdentity != "" {
		record.Identity = domainexecution.TransactionIdentity{
			Chain:   market.ChainID(txChain),
			Account: domainexecution.AccountID(txAccount),
			Hash:    txIdentity, Blockhash: blockhash,
			LastValidBlockHeight: lastValid,
		}
		if nonce != "" {
			value, parseErr := strconv.ParseUint(nonce, 10, 64)
			if parseErr != nil {
				return executionport.RefuelRecord{}, false, parseErr
			}
			record.Identity.Nonce = &value
		}
	}
	record.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return executionport.RefuelRecord{}, false, err
	}
	record.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return executionport.RefuelRecord{}, false, err
	}
	return record, true, nil
}

var _ executionport.RefuelJournal = (*SequentialLiveStore)(nil)
