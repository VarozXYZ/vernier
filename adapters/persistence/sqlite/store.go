// Package sqlite persists only Research opportunity-window lifecycles.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	persistence "github.com/VarozXYZ/vernier/ports/persistence"
	_ "modernc.org/sqlite"
)

const schemaVersion = 9

type Store struct {
	db   *sql.DB
	path string
	once sync.Once
	err  error
}

func Open(path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("SQLite opportunity store path is required")
	}
	directory := filepath.Dir(path)
	if directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create opportunity store directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open opportunity store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, path: path}
	if err := store.configure(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) configure() error {
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("configure SQLite (%s): %w", statement, err)
		}
	}
	return nil
}

func (s *Store) RecordExecutableValidationRound(ctx context.Context, round *arbitrage.ExecutableValidationRound) error {
	if round == nil {
		return fmt.Errorf("executable validation round is required")
	}
	if err := round.Validate(); err != nil {
		return err
	}
	started := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO executable_validation_rounds (
		round_id, window_id, point_sequence, buy_market, sell_market, status,
		failure_stage, failure_class, error, requested_at, route_finished_at,
		build_finished_at, simulation_finished_at, local_recaptured_at,
		recalculated_at, persisted_at, discovery_output_asset, discovery_output_value,
		build_output_asset, build_output_value, discovery_net_asset, discovery_net_value,
		final_net_asset, final_net_value, threshold_asset, threshold_value,
		remote_market, local_market, initial_local_snapshot, final_local_snapshot,
		route_hash, build_hash, route_http_status, build_http_status,
		route_duration_nanos, build_duration_nanos, build_attempts
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		round.ID, string(round.WindowID), round.PointSequence, string(round.Direction.BuyMarket), string(round.Direction.SellMarket), string(round.Status),
		round.FailureStage, round.FailureClass, round.Error, formatTime(round.RequestedAt), formatOptionalTime(round.RouteFinishedAt),
		formatOptionalTime(round.BuildFinishedAt), formatOptionalTime(round.SimulationFinishedAt), formatOptionalTime(round.LocalRecapturedAt),
		formatOptionalTime(round.RecalculatedAt), formatTime(started), string(round.DiscoveryOutput.Asset()), round.DiscoveryOutput.String(),
		string(round.BuildOutput.Asset()), quantityString(round.BuildOutput), string(round.DiscoveryNet.Asset()), round.DiscoveryNet.String(),
		string(round.FinalNet.Asset()), quantityString(round.FinalNet), string(round.Threshold.Asset()), round.Threshold.String(),
		string(round.RemoteMarket), string(round.LocalMarket), trackingSnapshot(round.InitialLocalSnapshot), trackingSnapshot(round.FinalLocalSnapshot),
		round.RouteHash, round.BuildHash, round.RouteHTTPStatus, round.BuildHTTPStatus, round.RouteDuration.Nanoseconds(), round.BuildDuration.Nanoseconds(), round.BuildAttempts,
	)
	if err != nil {
		return fmt.Errorf("record executable validation round: %w", err)
	}
	round.PersistedAt = time.Now().UTC()
	return nil
}

func quantityString(value market.AssetQuantity) string {
	if value.Asset() == "" {
		return "0"
	}
	return value.String()
}

func trackingSnapshot(value market.SnapshotMetadata) string {
	if value.Market == "" {
		return ""
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func (s *Store) OpenTrackingWindow(ctx context.Context, window *arbitrage.TrackingWindow) error {
	if window == nil {
		return fmt.Errorf("tracking window is required")
	}
	if window.ID == "" || window.Run == "" || window.Strategy == "" || window.ConfigHash == "" ||
		window.Direction.BuyMarket == "" || window.Direction.SellMarket == "" || window.OpenedAt.IsZero() {
		return fmt.Errorf("tracking window is incomplete")
	}
	if err := window.OpeningTrigger.Validate(); err != nil {
		return err
	}
	buyToken := string(window.Opening.BuyQuote.AmountOut.Token())
	buyUnits := "0"
	if window.Opening.BuyQuote.AmountOut.Token() != "" {
		buyUnits = window.Opening.BuyQuote.AmountOut.Units().String()
	}
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO opportunity_tracking_windows (
		window_id, run_id, strategy_id, config_hash, buy_market, sell_market,
		input_asset, input_value, fixed_threshold_value, percentage_threshold_value,
		effective_threshold_value, cost_value, opening_trigger, opened_at,
		discovery_started_at, discovery_finished_at, opening_persisted_at,
		opening_buy_token, opening_buy_units, opening_buy_asset, opening_buy_value, opening_sell_value, opening_net_value,
		opening_snapshots, discovery_trace
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(window.ID), string(window.Run), string(window.Strategy), window.ConfigHash,
		string(window.Direction.BuyMarket), string(window.Direction.SellMarket),
		string(window.Input.Asset()), window.Input.String(), window.FixedThreshold.String(),
		window.PercentageThreshold.String(), window.EffectiveThreshold.String(), window.Cost.String(),
		trackingTrigger(window.OpeningTrigger), formatTime(window.OpenedAt),
		formatTime(window.DiscoveryStartedAt), formatTime(window.DiscoveryFinishedAt),
		formatOptionalTime(window.OpeningPersistedAt), buyToken, buyUnits, string(window.OpeningBuyOutput.Asset()), window.OpeningBuyOutput.String(), window.Opening.Output.String(),
		window.Opening.NetPnL.String(), trackingSnapshots(window.OpeningSnapshots), trackingDiscoveryTrace(window.DiscoveryTrace),
	)
	if err != nil {
		return fmt.Errorf("open tracking window: %w", err)
	}
	window.OpeningPersistedAt = time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `UPDATE opportunity_tracking_windows SET opening_persisted_at = ? WHERE window_id = ?`, formatTime(window.OpeningPersistedAt), string(window.ID)); err != nil {
		return fmt.Errorf("record tracking window persistence timing: %w", err)
	}
	return nil
}

func (s *Store) RecordTrackingPoint(ctx context.Context, point *arbitrage.TrackingPoint) error {
	if point == nil {
		return fmt.Errorf("tracking point is required")
	}
	if err := point.Validate(); err != nil {
		return err
	}
	durations := point.Timestamps.Durations()
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO opportunity_tracking_points (
		point_id, window_id, sequence, evaluation_id, trigger,
		trigger_received_at, snapshot_captured_at, queued_at, evaluation_started_at,
		buy_started_at, buy_finished_at, conversion_started_at, conversion_finished_at,
		sell_started_at, sell_finished_at, pnl_started_at, pnl_finished_at,
		persistence_started_at, persistence_finished_at, notification_enqueued_at,
		evaluation_finished_at, snapshots, input_value, buy_output_asset, buy_output_value,
		sell_output_value, gross_pnl_value, net_pnl_value, fixed_threshold_value,
		percentage_threshold_value, effective_threshold_value, delta_opening_value,
		delta_previous_value, classification, reason, economic_change,
		snapshot_capture_nanos, queue_nanos, buy_quote_nanos, conversion_nanos,
		sell_quote_nanos, pnl_nanos, local_calculation_nanos, persistence_nanos,
		event_to_evaluation_nanos, event_to_persisted_nanos, notification_enqueue_nanos,
		interval_previous_nanos, since_opening_nanos
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		point.ID, string(point.WindowID), point.Sequence, string(point.Evaluation), trackingTrigger(point.Trigger),
		formatTime(point.Timestamps.TriggerReceivedAt), formatTime(point.Timestamps.SnapshotCapturedAt),
		formatTime(point.Timestamps.QueuedAt), formatTime(point.Timestamps.EvaluationStartedAt),
		formatOptionalTime(point.Timestamps.BuyStartedAt), formatOptionalTime(point.Timestamps.BuyFinishedAt),
		formatOptionalTime(point.Timestamps.ConversionStartedAt), formatOptionalTime(point.Timestamps.ConversionFinishedAt),
		formatOptionalTime(point.Timestamps.SellStartedAt), formatOptionalTime(point.Timestamps.SellFinishedAt),
		formatOptionalTime(point.Timestamps.PnLStartedAt), formatOptionalTime(point.Timestamps.PnLFinishedAt),
		formatTime(point.Timestamps.PersistenceStartedAt), formatOptionalTime(point.Timestamps.PersistenceFinishedAt),
		formatOptionalTime(point.Timestamps.NotificationEnqueuedAt), formatOptionalTime(point.Timestamps.EvaluationFinishedAt),
		trackingSnapshots(point.Snapshots), point.Input.String(), string(point.BuyOutput.Asset()), point.BuyOutput.String(),
		point.SellOutput.String(), point.GrossPnL.String(), point.NetPnL.String(), point.FixedThreshold.String(),
		point.PercentageThreshold.String(), point.EffectiveThreshold.String(), point.DeltaFromOpening.String(),
		point.DeltaFromPrevious.String(), string(point.Classification), point.Reason, boolInt(point.EconomicChange),
		durations.SnapshotCapture.Nanoseconds(), durations.Queue.Nanoseconds(), durations.BuyQuote.Nanoseconds(),
		durations.DecimalConversion.Nanoseconds(), durations.SellQuote.Nanoseconds(), durations.PnLCalculation.Nanoseconds(),
		durations.LocalCalculation.Nanoseconds(), durations.Persistence.Nanoseconds(), durations.EventToEvaluation.Nanoseconds(),
		durations.EventToPersistedPoint.Nanoseconds(), durations.NotificationEnqueue.Nanoseconds(),
		point.IntervalFromPrevious.Nanoseconds(), point.SinceOpening.Nanoseconds(),
	)
	if err != nil {
		return fmt.Errorf("record tracking point: %w", err)
	}
	point.Timestamps.PersistenceFinishedAt = time.Now().UTC()
	durations = point.Timestamps.Durations()
	if _, err := s.db.ExecContext(ctx, `UPDATE opportunity_tracking_points SET
		persistence_finished_at = ?, persistence_nanos = ?, event_to_persisted_nanos = ?
		WHERE point_id = ?`, formatTime(point.Timestamps.PersistenceFinishedAt),
		durations.Persistence.Nanoseconds(), durations.EventToPersistedPoint.Nanoseconds(), point.ID); err != nil {
		return fmt.Errorf("record tracking point persistence timing: %w", err)
	}
	return nil
}

func (s *Store) MarkTrackingNotificationEnqueued(ctx context.Context, windowID arbitrage.WindowID, sequence uint64, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE opportunity_tracking_points
		SET notification_enqueued_at = ?, evaluation_finished_at = ?,
		event_to_evaluation_nanos = CASE
			WHEN trigger_received_at = '' THEN 0
			ELSE MAX(0, CAST((julianday(?) - julianday(trigger_received_at)) * 86400000000000 AS INTEGER))
		END,
		notification_enqueue_nanos = CASE
			WHEN persistence_finished_at = '' THEN 0
			ELSE MAX(0, CAST((julianday(?) - julianday(persistence_finished_at)) * 86400000000000 AS INTEGER))
		END WHERE window_id = ? AND sequence = ?`,
		formatTime(at), formatTime(at), formatTime(at), formatTime(at), string(windowID), sequence)
	if err != nil {
		return fmt.Errorf("mark tracking notification enqueued: %w", err)
	}
	return nil
}

func (s *Store) RecordSimulationRound(ctx context.Context, round *arbitrage.SimulationRound) error {
	if round == nil {
		return fmt.Errorf("simulation round is required")
	}
	if err := round.Validate(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin simulation round: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO simulation_rounds (
		round_id, window_id, point_sequence, requested_at, started_at, finished_at,
		status, failure_class, error, local_qualified, local_net_asset,
		local_net_value, local_threshold_asset, local_threshold_value,
		simulated_net_asset, simulated_net_value
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		round.ID, string(round.WindowID), round.PointSequence, formatTime(round.RequestedAt),
		formatTime(round.StartedAt), formatOptionalTime(round.FinishedAt), string(round.Status),
		string(round.FailureClass), round.Error, boolInt(round.LocalQualified),
		string(round.LocalNetPnL.Asset()), round.LocalNetPnL.String(),
		string(round.LocalThreshold.Asset()), round.LocalThreshold.String(),
		string(round.SimulatedNetPnL.Asset()), round.SimulatedNetPnL.String(),
	)
	if err != nil {
		return fmt.Errorf("record simulation round: %w", err)
	}
	for legName, leg := range map[string]arbitrage.SimulationLeg{"buy": round.Buy, "sell": round.Sell} {
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO simulation_legs (
			round_id, leg, chain, market, input_token, input_units, local_output_token,
			local_output_units, simulated_output_token, simulated_output_units,
			status, failure_class, error, snapshot_version, snapshot_hash, context,
			context_position, gas_or_compute_units, started_at, finished_at, duration_nanos
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			round.ID, legName, leg.Chain, string(leg.Market), string(leg.Input.Token()), leg.Input.Units().String(),
			string(leg.LocalOutput.Token()), leg.LocalOutput.Units().String(),
			string(leg.SimulatedOutput.Token()), leg.SimulatedOutput.Units().String(),
			string(leg.Status), string(leg.FailureClass), leg.Error, leg.SnapshotVersion,
			hex.EncodeToString(leg.SnapshotHash[:]), leg.Context, leg.ContextPosition,
			leg.GasOrComputeUnits, formatOptionalTime(leg.StartedAt), formatOptionalTime(leg.FinishedAt),
			leg.Duration().Nanoseconds(),
		)
		if err != nil {
			return fmt.Errorf("record simulation %s leg: %w", legName, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit simulation round: %w", err)
	}
	return nil
}

func (s *Store) CloseTrackingWindow(ctx context.Context, closing arbitrage.TrackingWindowClosing) error {
	if closing.WindowID == "" || closing.ClosedAt.IsZero() || closing.Reason == "" {
		return fmt.Errorf("tracking window closing is incomplete")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE opportunity_tracking_windows SET
		status = ?, close_reason = ?, closed_at = ?, economic_duration_nanos = ?,
		observed_duration_nanos = ?, cumulative_calculation_nanos = ?,
		cumulative_queue_nanos = ?, maximum_queue_nanos = ?, events = ?,
		economic_changes = ?, initial_pnl_value = ?, final_pnl_value = ?,
		best_pnl_value = ?, worst_pnl_value = ?, latency_min_nanos = ?, latency_mean_nanos = ?,
		latency_p50_nanos = ?, latency_p95_nanos = ?, latency_p99_nanos = ?, latency_max_nanos = ?
		WHERE window_id = ? AND status = 'open'`,
		string(closing.Status), closing.Reason, formatTime(closing.ClosedAt), closing.EconomicDuration.Nanoseconds(),
		closing.ObservedDuration.Nanoseconds(), closing.CumulativeCalculation.Nanoseconds(),
		closing.CumulativeQueue.Nanoseconds(), closing.MaximumQueue.Nanoseconds(), closing.Events,
		closing.EconomicChanges, closing.InitialPnL.String(), closing.FinalPnL.String(), closing.BestPnL.String(),
		closing.WorstPnL.String(), closing.LatencyMinimum.Nanoseconds(), closing.LatencyMean.Nanoseconds(),
		closing.LatencyP50.Nanoseconds(), closing.LatencyP95.Nanoseconds(), closing.LatencyP99.Nanoseconds(),
		closing.LatencyMaximum.Nanoseconds(), string(closing.WindowID),
	)
	if err != nil {
		return fmt.Errorf("close tracking window: %w", err)
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return fmt.Errorf("tracking window %q is not open", closing.WindowID)
	}
	return nil
}

func (s *Store) SetTrackingMessage(ctx context.Context, windowID arbitrage.WindowID, messageID int64) error {
	if windowID == "" || messageID <= 0 {
		return fmt.Errorf("tracking message identity is invalid")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE opportunity_tracking_windows SET telegram_message_id = ? WHERE window_id = ?`, messageID, string(windowID))
	return err
}

func (s *Store) TrackingMessage(ctx context.Context, windowID arbitrage.WindowID) (int64, bool, error) {
	var messageID int64
	err := s.db.QueryRowContext(ctx, `SELECT telegram_message_id FROM opportunity_tracking_windows WHERE window_id = ?`, string(windowID)).Scan(&messageID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return messageID, messageID > 0, nil
}

func (s *Store) FinalizeDanglingTracking(ctx context.Context, at time.Time) ([]arbitrage.DanglingTrackingWindow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT window_id, telegram_message_id, buy_market, sell_market,
		input_asset, input_value, opening_buy_asset, opening_buy_value, opening_sell_value,
		opening_net_value, effective_threshold_value, opened_at, discovery_started_at, discovery_finished_at
		FROM opportunity_tracking_windows WHERE status = 'open' ORDER BY opened_at`)
	if err != nil {
		return nil, fmt.Errorf("list dangling tracking windows: %w", err)
	}
	type rawWindow struct {
		id, buy, sell, inputAsset, inputValue, buyAsset, buyValue string
		sellValue, netValue, thresholdValue, openedAt             string
		discoveryStartedAt, discoveryFinishedAt                   string
		messageID                                                 int64
	}
	var pending []rawWindow
	for rows.Next() {
		var item rawWindow
		if err := rows.Scan(&item.id, &item.messageID, &item.buy, &item.sell, &item.inputAsset, &item.inputValue,
			&item.buyAsset, &item.buyValue, &item.sellValue, &item.netValue, &item.thresholdValue, &item.openedAt,
			&item.discoveryStartedAt, &item.discoveryFinishedAt); err != nil {
			rows.Close()
			return nil, err
		}
		pending = append(pending, item)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	result := make([]arbitrage.DanglingTrackingWindow, 0, len(pending))
	for _, item := range pending {
		if item.buyAsset == "" {
			item.buyAsset = "base_unknown"
		}
		input, err := market.ParseAssetQuantity(market.AssetID(item.inputAsset), item.inputValue)
		if err != nil {
			return nil, err
		}
		buyOutput, err := market.ParseAssetQuantity(market.AssetID(item.buyAsset), item.buyValue)
		if err != nil {
			return nil, err
		}
		sellOutput, err := market.ParseAssetQuantity(market.AssetID(item.inputAsset), item.sellValue)
		if err != nil {
			return nil, err
		}
		netPnL, err := market.ParseAssetQuantity(market.AssetID(item.inputAsset), item.netValue)
		if err != nil {
			return nil, err
		}
		threshold, err := market.ParseAssetQuantity(market.AssetID(item.inputAsset), item.thresholdValue)
		if err != nil {
			return nil, err
		}
		openedAt, err := parseTime(item.openedAt)
		if err != nil {
			return nil, err
		}
		best, worst := netPnL, netPnL
		lastTrigger, points, changes := openedAt, uint64(1), uint64(0)
		discoveryStartedAt, err := parseTime(item.discoveryStartedAt)
		if err != nil {
			return nil, err
		}
		discoveryFinishedAt, err := parseTime(item.discoveryFinishedAt)
		if err != nil {
			return nil, err
		}
		cumulativeCalculation := nonNegativeNanos(discoveryFinishedAt.Sub(discoveryStartedAt))
		var cumulativeQueue, maximumQueue int64
		var latencies []int64
		pointRows, err := s.db.QueryContext(ctx, `SELECT sequence, trigger_received_at, buy_output_asset,
			buy_output_value, sell_output_value, net_pnl_value, economic_change,
			local_calculation_nanos, queue_nanos, event_to_evaluation_nanos
			FROM opportunity_tracking_points WHERE window_id = ? ORDER BY sequence`, item.id)
		if err != nil {
			return nil, err
		}
		for pointRows.Next() {
			var sequence uint64
			var triggerAt, pointBuyAsset, pointBuyValue, pointSellValue, pointNetValue string
			var changed int
			var calculationNanos, queueNanos, latencyNanos int64
			if err := pointRows.Scan(&sequence, &triggerAt, &pointBuyAsset, &pointBuyValue, &pointSellValue, &pointNetValue, &changed, &calculationNanos, &queueNanos, &latencyNanos); err != nil {
				pointRows.Close()
				return nil, err
			}
			candidateNet, parseErr := market.ParseAssetQuantity(market.AssetID(item.inputAsset), pointNetValue)
			if parseErr != nil {
				pointRows.Close()
				return nil, parseErr
			}
			if quantityCmp(candidateNet, best) > 0 {
				best = candidateNet
			}
			if quantityCmp(candidateNet, worst) < 0 {
				worst = candidateNet
			}
			buyOutput, _ = market.ParseAssetQuantity(market.AssetID(pointBuyAsset), pointBuyValue)
			sellOutput, _ = market.ParseAssetQuantity(market.AssetID(item.inputAsset), pointSellValue)
			netPnL = candidateNet
			lastTrigger, _ = parseTime(triggerAt)
			points = sequence
			if changed != 0 {
				changes++
			}
			cumulativeCalculation += calculationNanos
			cumulativeQueue += queueNanos
			if queueNanos > maximumQueue {
				maximumQueue = queueNanos
			}
			latencies = append(latencies, latencyNanos)
		}
		if err := pointRows.Close(); err != nil {
			return nil, err
		}
		latencyMin, latencyMean, latencyP50, latencyP95, latencyP99, latencyMax := trackingLatencyStats(latencies)
		if _, err := s.db.ExecContext(ctx, `UPDATE opportunity_tracking_windows SET status = 'failed',
			close_reason = 'process_interrupted', closed_at = ?, economic_duration_nanos = ?,
			observed_duration_nanos = ?, cumulative_calculation_nanos = ?, cumulative_queue_nanos = ?,
			maximum_queue_nanos = ?, events = ?, economic_changes = ?, initial_pnl_value = ?,
			final_pnl_value = ?, best_pnl_value = ?, worst_pnl_value = ?, latency_min_nanos = ?,
			latency_mean_nanos = ?, latency_p50_nanos = ?, latency_p95_nanos = ?, latency_p99_nanos = ?,
			latency_max_nanos = ? WHERE window_id = ? AND status = 'open'`,
			formatTime(at), nonNegativeNanos(lastTrigger.Sub(openedAt)), nonNegativeNanos(at.Sub(openedAt)),
			cumulativeCalculation, cumulativeQueue, maximumQueue, points, changes,
			item.netValue, netPnL.String(), best.String(), worst.String(), latencyMin, latencyMean,
			latencyP50, latencyP95, latencyP99, latencyMax, item.id); err != nil {
			return nil, err
		}
		result = append(result, arbitrage.DanglingTrackingWindow{
			WindowID: arbitrage.WindowID(item.id), MessageID: item.messageID,
			Direction: arbitrage.Direction{BuyMarket: market.MarketID(item.buy), SellMarket: market.MarketID(item.sell)},
			Input:     input, BuyOutput: buyOutput, SellOutput: sellOutput, NetPnL: netPnL, Threshold: threshold,
			BestPnL: best, WorstPnL: worst, OpenedAt: openedAt, ClosedAt: at.UTC(), LastTriggerAt: lastTrigger,
			Points: points, EconomicChanges: changes,
			CumulativeCalculation: time.Duration(cumulativeCalculation), CumulativeQueue: time.Duration(cumulativeQueue),
			MaximumQueue: time.Duration(maximumQueue),
		})
	}
	return result, nil
}

func quantityCmp(left, right market.AssetQuantity) int {
	value, err := left.Cmp(right)
	if err != nil {
		return 0
	}
	return value
}

func nonNegativeNanos(value time.Duration) int64 {
	if value < 0 {
		return 0
	}
	return value.Nanoseconds()
}

func trackingLatencyStats(values []int64) (minimum, mean, p50, p95, p99, maximum int64) {
	if len(values) == 0 {
		return 0, 0, 0, 0, 0, 0
	}
	ordered := append([]int64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var total int64
	for _, value := range ordered {
		total += value
	}
	at := func(percentile int) int64 {
		index := (percentile*len(ordered) + 99) / 100
		if index < 1 {
			index = 1
		}
		return ordered[index-1]
	}
	return ordered[0], total / int64(len(ordered)), at(50), at(95), at(99), ordered[len(ordered)-1]
}

func (s *Store) migrate() error {
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite schema version: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("unsupported SQLite schema version %d", version)
	}
	if version == schemaVersion {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin SQLite migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS opportunity_windows (
			window_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			strategy_id TEXT NOT NULL,
			config_hash TEXT NOT NULL,
			buy_market TEXT NOT NULL,
			sell_market TEXT NOT NULL,
			trigger_market TEXT NOT NULL DEFAULT '',
			trigger_source TEXT NOT NULL DEFAULT '',
			trigger_position_kind TEXT NOT NULL DEFAULT '',
			trigger_position_value TEXT NOT NULL DEFAULT '0',
			trigger_reference_kind TEXT NOT NULL DEFAULT '',
			trigger_reference_value TEXT NOT NULL DEFAULT '',
			trigger_at TEXT NOT NULL DEFAULT '',
			has_trigger INTEGER NOT NULL CHECK (has_trigger IN (0, 1)),
			opened_at TEXT NOT NULL,
			first_profitable_at TEXT NOT NULL,
			last_profitable_at TEXT NOT NULL,
			closed_at TEXT NOT NULL DEFAULT '',
			best_size_asset TEXT NOT NULL,
			best_size_value TEXT NOT NULL,
			best_gross_asset TEXT NOT NULL,
			best_gross_value TEXT NOT NULL,
			best_net_asset TEXT NOT NULL,
			best_net_value TEXT NOT NULL,
			best_cost_asset TEXT NOT NULL,
			best_cost_value TEXT NOT NULL,
			threshold_asset TEXT NOT NULL,
			threshold_value TEXT NOT NULL,
			classification TEXT NOT NULL,
			status TEXT NOT NULL,
			close_reason TEXT NOT NULL DEFAULT '',
			degraded INTEGER NOT NULL CHECK (degraded IN (0, 1)),
			duration_nanos INTEGER NOT NULL DEFAULT 0,
			identity_hash TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS opportunity_windows_open_idx ON opportunity_windows(status, opened_at)`,
		`CREATE TABLE IF NOT EXISTS opportunity_window_observations (
			observation_id TEXT PRIMARY KEY,
			window_id TEXT NOT NULL REFERENCES opportunity_windows(window_id) ON DELETE CASCADE,
			evaluation_id TEXT NOT NULL,
			observed_at TEXT NOT NULL,
			classification TEXT NOT NULL,
			has_candidate INTEGER NOT NULL CHECK (has_candidate IN (0, 1)),
			best INTEGER NOT NULL CHECK (best IN (0, 1)),
			size_asset TEXT NOT NULL DEFAULT '',
			size_value TEXT NOT NULL DEFAULT '',
			gross_asset TEXT NOT NULL DEFAULT '',
			gross_value TEXT NOT NULL DEFAULT '',
			net_asset TEXT NOT NULL DEFAULT '',
			net_value TEXT NOT NULL DEFAULT '',
			cost_asset TEXT NOT NULL DEFAULT '',
			cost_value TEXT NOT NULL DEFAULT '',
			observation_fingerprint TEXT NOT NULL,
			UNIQUE(window_id, evaluation_id)
		)`,
		`CREATE TABLE IF NOT EXISTS opportunity_tracking_windows (
			window_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			strategy_id TEXT NOT NULL,
			config_hash TEXT NOT NULL,
			buy_market TEXT NOT NULL,
			sell_market TEXT NOT NULL,
			input_asset TEXT NOT NULL,
			input_value TEXT NOT NULL,
			fixed_threshold_value TEXT NOT NULL,
			percentage_threshold_value TEXT NOT NULL,
			effective_threshold_value TEXT NOT NULL,
			cost_value TEXT NOT NULL,
			opening_trigger TEXT NOT NULL,
			opened_at TEXT NOT NULL,
			discovery_started_at TEXT NOT NULL,
			discovery_finished_at TEXT NOT NULL,
			opening_persisted_at TEXT NOT NULL,
			opening_buy_token TEXT NOT NULL,
			opening_buy_units TEXT NOT NULL,
			opening_buy_asset TEXT NOT NULL DEFAULT '',
			opening_buy_value TEXT NOT NULL DEFAULT '0',
			opening_sell_value TEXT NOT NULL,
			opening_net_value TEXT NOT NULL,
			opening_snapshots TEXT NOT NULL,
			discovery_trace TEXT NOT NULL DEFAULT '',
			telegram_message_id INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'open',
			close_reason TEXT NOT NULL DEFAULT '',
			closed_at TEXT NOT NULL DEFAULT '',
			economic_duration_nanos INTEGER NOT NULL DEFAULT 0,
			observed_duration_nanos INTEGER NOT NULL DEFAULT 0,
			cumulative_calculation_nanos INTEGER NOT NULL DEFAULT 0,
			cumulative_queue_nanos INTEGER NOT NULL DEFAULT 0,
			maximum_queue_nanos INTEGER NOT NULL DEFAULT 0,
			events INTEGER NOT NULL DEFAULT 0,
			economic_changes INTEGER NOT NULL DEFAULT 0,
			initial_pnl_value TEXT NOT NULL DEFAULT '0',
			final_pnl_value TEXT NOT NULL DEFAULT '0',
			best_pnl_value TEXT NOT NULL DEFAULT '0',
			worst_pnl_value TEXT NOT NULL DEFAULT '0',
			latency_min_nanos INTEGER NOT NULL DEFAULT 0,
			latency_mean_nanos INTEGER NOT NULL DEFAULT 0,
			latency_p50_nanos INTEGER NOT NULL DEFAULT 0,
			latency_p95_nanos INTEGER NOT NULL DEFAULT 0,
			latency_p99_nanos INTEGER NOT NULL DEFAULT 0,
			latency_max_nanos INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS opportunity_tracking_points (
			point_id TEXT PRIMARY KEY,
			window_id TEXT NOT NULL REFERENCES opportunity_tracking_windows(window_id) ON DELETE CASCADE,
			sequence INTEGER NOT NULL,
			evaluation_id TEXT NOT NULL,
			trigger TEXT NOT NULL,
			trigger_received_at TEXT NOT NULL,
			snapshot_captured_at TEXT NOT NULL,
			queued_at TEXT NOT NULL,
			evaluation_started_at TEXT NOT NULL,
			buy_started_at TEXT NOT NULL,
			buy_finished_at TEXT NOT NULL,
			conversion_started_at TEXT NOT NULL,
			conversion_finished_at TEXT NOT NULL,
			sell_started_at TEXT NOT NULL,
			sell_finished_at TEXT NOT NULL,
			pnl_started_at TEXT NOT NULL,
			pnl_finished_at TEXT NOT NULL,
			persistence_started_at TEXT NOT NULL,
			persistence_finished_at TEXT NOT NULL,
			notification_enqueued_at TEXT NOT NULL,
			evaluation_finished_at TEXT NOT NULL,
			snapshots TEXT NOT NULL,
			input_value TEXT NOT NULL,
			buy_output_asset TEXT NOT NULL,
			buy_output_value TEXT NOT NULL,
			sell_output_value TEXT NOT NULL,
			gross_pnl_value TEXT NOT NULL,
			net_pnl_value TEXT NOT NULL,
			fixed_threshold_value TEXT NOT NULL,
			percentage_threshold_value TEXT NOT NULL,
			effective_threshold_value TEXT NOT NULL,
			delta_opening_value TEXT NOT NULL,
			delta_previous_value TEXT NOT NULL,
			classification TEXT NOT NULL,
			reason TEXT NOT NULL,
			economic_change INTEGER NOT NULL CHECK (economic_change IN (0, 1)),
			snapshot_capture_nanos INTEGER NOT NULL,
			queue_nanos INTEGER NOT NULL,
			buy_quote_nanos INTEGER NOT NULL,
			conversion_nanos INTEGER NOT NULL,
			sell_quote_nanos INTEGER NOT NULL,
			pnl_nanos INTEGER NOT NULL,
			local_calculation_nanos INTEGER NOT NULL,
			persistence_nanos INTEGER NOT NULL,
			event_to_evaluation_nanos INTEGER NOT NULL,
			event_to_persisted_nanos INTEGER NOT NULL,
			notification_enqueue_nanos INTEGER NOT NULL,
			interval_previous_nanos INTEGER NOT NULL DEFAULT 0,
			since_opening_nanos INTEGER NOT NULL DEFAULT 0,
			UNIQUE(window_id, sequence)
		)`,
		`CREATE INDEX IF NOT EXISTS opportunity_tracking_points_window_idx ON opportunity_tracking_points(window_id, sequence)`,
		`CREATE TABLE IF NOT EXISTS simulation_rounds (
			round_id TEXT PRIMARY KEY,
			window_id TEXT NOT NULL,
			point_sequence INTEGER NOT NULL,
			requested_at TEXT NOT NULL,
			started_at TEXT NOT NULL,
			finished_at TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			failure_class TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT '',
			local_qualified INTEGER NOT NULL CHECK (local_qualified IN (0, 1)),
			local_net_asset TEXT NOT NULL DEFAULT '',
			local_net_value TEXT NOT NULL DEFAULT '0',
			local_threshold_asset TEXT NOT NULL DEFAULT '',
			local_threshold_value TEXT NOT NULL DEFAULT '0',
			simulated_net_asset TEXT NOT NULL DEFAULT '',
			simulated_net_value TEXT NOT NULL DEFAULT '0'
		)`,
		`CREATE INDEX IF NOT EXISTS simulation_rounds_window_idx ON simulation_rounds(window_id, point_sequence)`,
		`CREATE TABLE IF NOT EXISTS simulation_legs (
			round_id TEXT NOT NULL REFERENCES simulation_rounds(round_id) ON DELETE CASCADE,
			leg TEXT NOT NULL,
			chain TEXT NOT NULL,
			market TEXT NOT NULL,
			input_token TEXT NOT NULL,
			input_units TEXT NOT NULL,
			local_output_token TEXT NOT NULL,
			local_output_units TEXT NOT NULL,
			simulated_output_token TEXT NOT NULL,
			simulated_output_units TEXT NOT NULL,
			status TEXT NOT NULL,
			failure_class TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT '',
			snapshot_version INTEGER NOT NULL DEFAULT 0,
			snapshot_hash TEXT NOT NULL DEFAULT '',
			context TEXT NOT NULL DEFAULT '',
			context_position INTEGER NOT NULL DEFAULT 0,
			gas_or_compute_units INTEGER NOT NULL DEFAULT 0,
			started_at TEXT NOT NULL DEFAULT '',
			finished_at TEXT NOT NULL DEFAULT '',
			duration_nanos INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(round_id, leg)
		)`,
		`CREATE TABLE IF NOT EXISTS executable_validation_rounds (
			round_id TEXT PRIMARY KEY,
			window_id TEXT NOT NULL,
			point_sequence INTEGER NOT NULL,
			buy_market TEXT NOT NULL,
			sell_market TEXT NOT NULL,
			status TEXT NOT NULL,
			failure_stage TEXT NOT NULL DEFAULT '',
			failure_class TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT '',
			requested_at TEXT NOT NULL,
			route_finished_at TEXT NOT NULL DEFAULT '',
			build_finished_at TEXT NOT NULL DEFAULT '',
			simulation_finished_at TEXT NOT NULL DEFAULT '',
			local_recaptured_at TEXT NOT NULL DEFAULT '',
			recalculated_at TEXT NOT NULL DEFAULT '',
			persisted_at TEXT NOT NULL,
			discovery_output_asset TEXT NOT NULL,
			discovery_output_value TEXT NOT NULL,
			build_output_asset TEXT NOT NULL DEFAULT '',
			build_output_value TEXT NOT NULL DEFAULT '0',
			discovery_net_asset TEXT NOT NULL,
			discovery_net_value TEXT NOT NULL,
			final_net_asset TEXT NOT NULL DEFAULT '',
			final_net_value TEXT NOT NULL DEFAULT '0',
			threshold_asset TEXT NOT NULL,
			threshold_value TEXT NOT NULL,
			remote_market TEXT NOT NULL,
			local_market TEXT NOT NULL,
			initial_local_snapshot TEXT NOT NULL,
			final_local_snapshot TEXT NOT NULL DEFAULT '',
			route_hash TEXT NOT NULL DEFAULT '',
			build_hash TEXT NOT NULL DEFAULT '',
			route_http_status INTEGER NOT NULL DEFAULT 0,
			build_http_status INTEGER NOT NULL DEFAULT 0,
			route_duration_nanos INTEGER NOT NULL DEFAULT 0,
			build_duration_nanos INTEGER NOT NULL DEFAULT 0,
			build_attempts INTEGER NOT NULL DEFAULT 0,
			UNIQUE(window_id, point_sequence)
		)`,
		`CREATE INDEX IF NOT EXISTS executable_validation_window_idx ON executable_validation_rounds(window_id, point_sequence)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("apply SQLite migration: %w", err)
		}
	}
	if version == 1 {
		for _, statement := range []string{
			`ALTER TABLE opportunity_windows ADD COLUMN threshold_asset TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE opportunity_windows ADD COLUMN threshold_value TEXT NOT NULL DEFAULT '0'`,
			`UPDATE opportunity_windows SET threshold_asset = best_net_asset WHERE threshold_asset = ''`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return fmt.Errorf("apply SQLite threshold migration: %w", err)
			}
		}
	}
	if version == 3 {
		if _, err := tx.Exec(`ALTER TABLE opportunity_tracking_windows ADD COLUMN discovery_trace TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("apply SQLite discovery trace migration: %w", err)
		}
	}
	if version == 3 || version == 4 {
		for _, statement := range []string{
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN opening_buy_asset TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN opening_buy_value TEXT NOT NULL DEFAULT '0'`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return fmt.Errorf("apply SQLite opening output migration: %w", err)
			}
		}
	}
	if version >= 3 && version <= 5 {
		for _, statement := range []string{
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN latency_min_nanos INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN latency_mean_nanos INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN latency_p50_nanos INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN latency_p95_nanos INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN latency_p99_nanos INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE opportunity_tracking_windows ADD COLUMN latency_max_nanos INTEGER NOT NULL DEFAULT 0`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return fmt.Errorf("apply SQLite latency summary migration: %w", err)
			}
		}
	}
	if version >= 3 && version <= 6 {
		for _, statement := range []string{
			`ALTER TABLE opportunity_tracking_points ADD COLUMN interval_previous_nanos INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE opportunity_tracking_points ADD COLUMN since_opening_nanos INTEGER NOT NULL DEFAULT 0`,
		} {
			if _, err := tx.Exec(statement); err != nil {
				return fmt.Errorf("apply SQLite point interval migration: %w", err)
			}
		}
	}
	if _, err := tx.Exec("PRAGMA user_version = 9"); err != nil {
		return fmt.Errorf("set SQLite schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite migration: %w", err)
	}
	return nil
}

func (s *Store) OpenWindow(ctx context.Context, opening arbitrage.WindowOpening) error {
	window := opening.Window
	if err := window.Validate(); err != nil {
		return fmt.Errorf("validate opportunity window: %w", err)
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin open window: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existingHash string
	err = tx.QueryRowContext(ctx, "SELECT identity_hash FROM opportunity_windows WHERE window_id = ?", string(window.ID)).Scan(&existingHash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO opportunity_windows (
			window_id, run_id, strategy_id, config_hash, buy_market, sell_market,
			trigger_market, trigger_source, trigger_position_kind, trigger_position_value,
			trigger_reference_kind, trigger_reference_value, trigger_at, has_trigger,
			opened_at, first_profitable_at, last_profitable_at, closed_at,
			best_size_asset, best_size_value, best_gross_asset, best_gross_value,
			best_net_asset, best_net_value, best_cost_asset, best_cost_value,
			threshold_asset, threshold_value, classification, status, close_reason, degraded, duration_nanos, identity_hash,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(window.ID), string(window.Run), string(window.Strategy), window.ConfigHash,
			string(window.Direction.BuyMarket), string(window.Direction.SellMarket),
			triggerMarket(window), triggerSource(window), triggerPositionKind(window), triggerPositionValue(window),
			triggerReferenceKind(window), triggerReferenceValue(window), triggerAt(window), boolInt(window.HasTrigger),
			formatTime(window.OpenedAt), formatTime(window.FirstProfitableAt), formatTime(window.LastProfitableAt), "",
			string(window.Best.Size.Asset()), window.Best.Size.String(), string(window.Best.GrossPnL.Asset()), window.Best.GrossPnL.String(),
			string(window.Best.NetPnL.Asset()), window.Best.NetPnL.String(), string(window.Best.Cost.Asset()), window.Best.Cost.String(),
			string(window.Threshold.Asset()), window.Threshold.String(), string(window.Classification), string(window.Status), "", 0, 0,
			arbitrage.WindowFingerprint(window), formatTime(now), formatTime(now),
		)
	case err == nil:
		if existingHash != arbitrage.WindowFingerprint(window) {
			return fmt.Errorf("window %q already exists with different data", window.ID)
		}
		return nil
	default:
		return fmt.Errorf("check existing window: %w", err)
	}
	if err != nil {
		return fmt.Errorf("insert opportunity window: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit opportunity window: %w", err)
	}
	return nil
}

func (s *Store) RecordImprovement(ctx context.Context, observation arbitrage.WindowObservation) error {
	if err := observation.Validate(); err != nil {
		return fmt.Errorf("validate window observation: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin window observation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existingFingerprint string
	err = tx.QueryRowContext(ctx, `SELECT observation_fingerprint FROM opportunity_window_observations WHERE observation_id = ?`, observation.ID).Scan(&existingFingerprint)
	if err == nil {
		if existingFingerprint != observationFingerprint(observation) {
			return fmt.Errorf("observation %q already exists with different data", observation.ID)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check existing observation: %w", err)
	}
	var status string
	var lastText string
	err = tx.QueryRowContext(ctx, "SELECT status, last_profitable_at FROM opportunity_windows WHERE window_id = ?", string(observation.WindowID)).Scan(&status, &lastText)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("window %q does not exist", observation.WindowID)
	}
	if err != nil {
		return fmt.Errorf("read window for observation: %w", err)
	}
	if status != string(arbitrage.WindowStatusOpen) {
		return fmt.Errorf("window %q is not open", observation.WindowID)
	}
	last, err := parseTime(lastText)
	if err != nil {
		return err
	}
	if observation.ObservedAt.Before(last) {
		return fmt.Errorf("window observation precedes last profitable timestamp")
	}
	update := `UPDATE opportunity_windows SET last_profitable_at = ?, classification = ?, updated_at = ? WHERE window_id = ?`
	args := []any{formatTime(observation.ObservedAt), string(observation.Classification), formatTime(time.Now().UTC()), string(observation.WindowID)}
	if observation.Best {
		update = `UPDATE opportunity_windows SET last_profitable_at = ?, classification = ?,
			best_size_asset = ?, best_size_value = ?, best_gross_asset = ?, best_gross_value = ?,
			best_net_asset = ?, best_net_value = ?, best_cost_asset = ?, best_cost_value = ?, updated_at = ?
			WHERE window_id = ?`
		args = []any{formatTime(observation.ObservedAt), string(observation.Classification),
			string(observation.Candidate.Size.Asset()), observation.Candidate.Size.String(),
			string(observation.Candidate.GrossPnL.Asset()), observation.Candidate.GrossPnL.String(),
			string(observation.Candidate.NetPnL.Asset()), observation.Candidate.NetPnL.String(),
			string(observation.Candidate.Cost.Asset()), observation.Candidate.Cost.String(), formatTime(time.Now().UTC()), string(observation.WindowID)}
	}
	if _, err := tx.ExecContext(ctx, update, args...); err != nil {
		return fmt.Errorf("update opportunity window: %w", err)
	}
	if observation.Best {
		if _, err := tx.ExecContext(ctx, `INSERT INTO opportunity_window_observations (
			observation_id, window_id, evaluation_id, observed_at, classification, has_candidate, best,
			size_asset, size_value, gross_asset, gross_value, net_asset, net_value, cost_asset, cost_value,
			observation_fingerprint
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			observation.ID, string(observation.WindowID), string(observation.Evaluation), formatTime(observation.ObservedAt), string(observation.Classification),
			boolInt(observation.HasCandidate), boolInt(observation.Best), string(observation.Candidate.Size.Asset()), observation.Candidate.Size.String(),
			string(observation.Candidate.GrossPnL.Asset()), observation.Candidate.GrossPnL.String(), string(observation.Candidate.NetPnL.Asset()), observation.Candidate.NetPnL.String(),
			string(observation.Candidate.Cost.Asset()), observation.Candidate.Cost.String(), observationFingerprint(observation),
		); err != nil {
			return fmt.Errorf("insert window observation: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit window observation: %w", err)
	}
	return nil
}

func (s *Store) CloseWindow(ctx context.Context, closing arbitrage.WindowClosing) error {
	return s.finishWindow(ctx, closing.WindowID, closing.ClosedAt, closing.LastProfitableAt, string(arbitrage.WindowStatusClosed), closing.Reason, closing.Degraded)
}

func (s *Store) FailWindow(ctx context.Context, failure arbitrage.WindowFailure) error {
	return s.finishWindow(ctx, failure.WindowID, failure.ClosedAt, failure.LastProfitableAt, string(arbitrage.WindowStatusFailed), failure.Reason, true)
}

func (s *Store) finishWindow(ctx context.Context, id arbitrage.WindowID, closedAt, lastProfitableAt time.Time, status, reason string, degraded bool) error {
	if id == "" || closedAt.IsZero() || lastProfitableAt.IsZero() || reason == "" {
		return fmt.Errorf("window close identity, timestamps, and reason are required")
	}
	closedAt, lastProfitableAt = closedAt.UTC(), lastProfitableAt.UTC()
	if closedAt.Before(lastProfitableAt) {
		return fmt.Errorf("window closes before last profitable timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin close window: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var currentStatus, openedText, currentReason, currentClosedText, currentLastText string
	var currentDegraded int64
	err = tx.QueryRowContext(ctx, `SELECT status, opened_at, close_reason, degraded, closed_at, last_profitable_at FROM opportunity_windows WHERE window_id = ?`, string(id)).Scan(&currentStatus, &openedText, &currentReason, &currentDegraded, &currentClosedText, &currentLastText)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("window %q does not exist", id)
	}
	if err != nil {
		return fmt.Errorf("read window for close: %w", err)
	}
	if currentStatus != string(arbitrage.WindowStatusOpen) {
		if currentStatus == status && currentReason == reason && currentDegraded == int64(boolInt(degraded)) && currentClosedText == formatTime(closedAt) && currentLastText == formatTime(lastProfitableAt) {
			return nil
		}
		return fmt.Errorf("window %q is already finalized", id)
	}
	openedAt, err := parseTime(openedText)
	if err != nil {
		return err
	}
	if closedAt.Before(openedAt) {
		return fmt.Errorf("window closes before opening")
	}
	duration := closedAt.Sub(openedAt)
	if _, err := tx.ExecContext(ctx, `UPDATE opportunity_windows SET last_profitable_at = ?, closed_at = ?, status = ?, close_reason = ?, degraded = ?, duration_nanos = ?, updated_at = ? WHERE window_id = ?`,
		formatTime(lastProfitableAt), formatTime(closedAt), status, reason, boolInt(degraded), duration.Nanoseconds(), formatTime(time.Now().UTC()), string(id)); err != nil {
		return fmt.Errorf("finalize opportunity window: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalized opportunity window: %w", err)
	}
	return nil
}

func (s *Store) FinalizeDangling(ctx context.Context, observedAt time.Time) error {
	if observedAt.IsZero() {
		return fmt.Errorf("dangling-window timestamp is required")
	}
	observedAt = observedAt.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin dangling-window cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE opportunity_windows SET closed_at = ?, status = ?, close_reason = ?, degraded = 1,
		duration_nanos = CASE WHEN julianday(?) >= julianday(opened_at) THEN CAST((julianday(?) - julianday(opened_at)) * 86400000000000 AS INTEGER) ELSE 0 END,
		updated_at = ? WHERE status = ?`,
		formatTime(observedAt), string(arbitrage.WindowStatusFailed), "process_interrupted", formatTime(observedAt), formatTime(observedAt), formatTime(observedAt), string(arbitrage.WindowStatusOpen)); err != nil {
		return fmt.Errorf("finalize dangling windows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit dangling-window cleanup: %w", err)
	}
	return nil
}

func (s *Store) ListWindows(ctx context.Context, query arbitrage.WindowQuery) ([]arbitrage.WindowRecord, error) {
	limit := query.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	statement := windowSelect + " FROM opportunity_windows"
	args := make([]any, 0, 3)
	conditions := make([]string, 0, 3)
	if query.Run != "" {
		conditions = append(conditions, "run_id = ?")
		args = append(args, string(query.Run))
	}
	if query.Strategy != "" {
		conditions = append(conditions, "strategy_id = ?")
		args = append(args, string(query.Strategy))
	}
	if query.Status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, string(query.Status))
	}
	if len(conditions) != 0 {
		statement += " WHERE " + strings.Join(conditions, " AND ")
	}
	statement += " ORDER BY opened_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list opportunity windows: %w", err)
	}
	defer rows.Close()
	windows := make([]arbitrage.OpportunityWindow, 0)
	for rows.Next() {
		window, err := scanWindow(rows)
		if err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate opportunity windows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close opportunity window rows: %w", err)
	}
	result := make([]arbitrage.WindowRecord, 0, len(windows))
	for _, window := range windows {
		observations, err := s.listObservations(ctx, window.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, arbitrage.WindowRecord{Window: window, Observations: observations})
	}
	return result, nil
}

func (s *Store) listObservations(ctx context.Context, id arbitrage.WindowID) ([]arbitrage.WindowObservation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT observation_id, window_id, evaluation_id, observed_at, classification,
		has_candidate, best, size_asset, size_value, gross_asset, gross_value, net_asset, net_value, cost_asset, cost_value
		FROM opportunity_window_observations WHERE window_id = ? ORDER BY observed_at ASC`, string(id))
	if err != nil {
		return nil, fmt.Errorf("list window observations: %w", err)
	}
	defer rows.Close()
	result := make([]arbitrage.WindowObservation, 0)
	for rows.Next() {
		var observation arbitrage.WindowObservation
		var windowID, evaluation, observed, classification string
		var hasCandidate, best int64
		var sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue string
		if err := rows.Scan(&observation.ID, &windowID, &evaluation, &observed, &classification, &hasCandidate, &best, &sizeAsset, &sizeValue, &grossAsset, &grossValue, &netAsset, &netValue, &costAsset, &costValue); err != nil {
			return nil, fmt.Errorf("scan window observation: %w", err)
		}
		observation.WindowID = arbitrage.WindowID(windowID)
		observation.Evaluation = arbitrage.EvaluationID(evaluation)
		observation.ObservedAt, err = parseTime(observed)
		if err != nil {
			return nil, err
		}
		observation.Classification = arbitrage.Classification(classification)
		observation.HasCandidate, observation.Best = hasCandidate == 1, best == 1
		if observation.HasCandidate {
			observation.Candidate, err = parseCandidate(sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue)
			if err != nil {
				return nil, err
			}
		}
		if err := observation.Validate(); err != nil {
			return nil, fmt.Errorf("validate stored observation: %w", err)
		}
		result = append(result, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate window observations: %w", err)
	}
	return result, nil
}

func (s *Store) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}

var _ persistence.OpportunityStore = (*Store)(nil)

const windowSelect = `SELECT window_id, run_id, strategy_id, config_hash, buy_market, sell_market,
	trigger_market, trigger_source, trigger_position_kind, trigger_position_value,
	trigger_reference_kind, trigger_reference_value, trigger_at, has_trigger,
	opened_at, first_profitable_at, last_profitable_at, closed_at,
	best_size_asset, best_size_value, best_gross_asset, best_gross_value,
	best_net_asset, best_net_value, best_cost_asset, best_cost_value,
	threshold_asset, threshold_value, classification, status, close_reason, degraded`

type scanner interface{ Scan(...any) error }

func scanWindow(row scanner) (arbitrage.OpportunityWindow, error) {
	var window arbitrage.OpportunityWindow
	var id, run, strategy, configHash, buyMarket, sellMarket string
	var triggerMarket, triggerSource, positionKind, positionValue, referenceKind, referenceValue, triggerAt string
	var hasTrigger int64
	var openedAt, firstAt, lastAt, closedAt string
	var sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue string
	var thresholdAsset, thresholdValue string
	var classification, status, closeReason string
	var degraded int64
	if err := row.Scan(&id, &run, &strategy, &configHash, &buyMarket, &sellMarket,
		&triggerMarket, &triggerSource, &positionKind, &positionValue, &referenceKind, &referenceValue, &triggerAt, &hasTrigger,
		&openedAt, &firstAt, &lastAt, &closedAt, &sizeAsset, &sizeValue, &grossAsset, &grossValue, &netAsset, &netValue, &costAsset, &costValue,
		&thresholdAsset, &thresholdValue,
		&classification, &status, &closeReason, &degraded); err != nil {
		return arbitrage.OpportunityWindow{}, fmt.Errorf("scan opportunity window: %w", err)
	}
	window.ID, window.Run, window.Strategy, window.ConfigHash = arbitrage.WindowID(id), arbitrage.ResearchRunID(run), arbitrage.StrategyID(strategy), configHash
	window.Direction = arbitrage.Direction{BuyMarket: market.MarketID(buyMarket), SellMarket: market.MarketID(sellMarket)}
	var err error
	window.OpenedAt, err = parseTime(openedAt)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	window.FirstProfitableAt, err = parseTime(firstAt)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	window.LastProfitableAt, err = parseTime(lastAt)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	if closedAt != "" {
		window.ClosedAt, err = parseTime(closedAt)
		if err != nil {
			return arbitrage.OpportunityWindow{}, err
		}
	}
	window.Best, err = parseCandidate(sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	window.HasBest = true
	window.Threshold, err = market.ParseAssetQuantity(market.AssetID(thresholdAsset), thresholdValue)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	window.Classification, window.Status, window.CloseReason, window.Degraded = arbitrage.Classification(classification), arbitrage.WindowStatus(status), closeReason, degraded == 1
	window.HasTrigger = hasTrigger == 1
	if window.HasTrigger {
		window.Trigger.Market, window.Trigger.Source = market.MarketID(triggerMarket), market.SourceID(triggerSource)
		window.Trigger.Position = market.SourcePosition{Kind: market.SourcePositionKind(positionKind), Value: parseUint(positionValue)}
		window.Trigger.Reference = market.SourceReference{Kind: market.SourceReferenceKind(referenceKind), Value: referenceValue}
		window.Trigger.At, err = parseTime(triggerAt)
		if err != nil {
			return arbitrage.OpportunityWindow{}, err
		}
	}
	if err := window.Validate(); err != nil {
		return arbitrage.OpportunityWindow{}, fmt.Errorf("validate stored window: %w", err)
	}
	return window, nil
}

func parseCandidate(sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue string) (arbitrage.WindowCandidate, error) {
	size, err := market.ParseAssetQuantity(market.AssetID(sizeAsset), sizeValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	gross, err := market.ParseAssetQuantity(market.AssetID(grossAsset), grossValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	net, err := market.ParseAssetQuantity(market.AssetID(netAsset), netValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	cost, err := market.ParseAssetQuantity(market.AssetID(costAsset), costValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	return arbitrage.WindowCandidate{Size: size, GrossPnL: gross, NetPnL: net, Cost: cost}, nil
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored timestamp %q: %w", value, err)
	}
	return parsed.UTC(), nil
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return formatTime(value)
}

func trackingTrigger(trigger arbitrage.TriggerMetadata) string {
	return fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s", trigger.Market, trigger.Source,
		trigger.Position.Kind, trigger.Position.Value, trigger.Reference.Kind,
		trigger.Reference.Value, formatTime(trigger.At))
}

func trackingSnapshots(snapshots []arbitrage.TrackingSnapshot) string {
	parts := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		parts = append(parts, fmt.Sprintf("%s:%d:%s", snapshot.Market, snapshot.Version, hex.EncodeToString(snapshot.StateHash[:])))
	}
	return strings.Join(parts, ",")
}

func trackingDiscoveryTrace(directions []arbitrage.TrackingDiscoveryDirection) string {
	type quote struct {
		Leg      string `json:"leg"`
		Input    string `json:"input"`
		Output   string `json:"output"`
		Duration int64  `json:"duration_nanos"`
		Cached   bool   `json:"cached"`
		Error    string `json:"error,omitempty"`
	}
	type direction struct {
		BuyMarket  string  `json:"buy_market"`
		SellMarket string  `json:"sell_market"`
		Duration   int64   `json:"duration_nanos"`
		Quotes     []quote `json:"quotes"`
	}
	encoded := make([]direction, 0, len(directions))
	for _, candidate := range directions {
		item := direction{BuyMarket: string(candidate.Direction.BuyMarket), SellMarket: string(candidate.Direction.SellMarket), Duration: candidate.Duration.Nanoseconds()}
		for _, candidateQuote := range candidate.Quotes {
			item.Quotes = append(item.Quotes, quote{Leg: candidateQuote.Leg, Input: formatTrackingQuantity(candidateQuote.Input), Output: formatTrackingQuantity(candidateQuote.Output), Duration: candidateQuote.Duration.Nanoseconds(), Cached: candidateQuote.Cached, Error: candidateQuote.Error})
		}
		encoded = append(encoded, item)
	}
	value, _ := json.Marshal(encoded)
	return string(value)
}

func formatTrackingQuantity(quantity market.AssetQuantity) string {
	if quantity.Asset() == "" {
		return ""
	}
	return string(quantity.Asset()) + ":" + quantity.String()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func parseUint(value string) uint64 { parsed, _ := strconv.ParseUint(value, 10, 64); return parsed }

func triggerMarket(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Market)
	}
	return ""
}
func triggerSource(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Source)
	}
	return ""
}
func triggerPositionKind(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Position.Kind)
	}
	return ""
}
func triggerPositionValue(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return strconv.FormatUint(window.Trigger.Position.Value, 10)
	}
	return "0"
}
func triggerReferenceKind(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Reference.Kind)
	}
	return ""
}
func triggerReferenceValue(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return window.Trigger.Reference.Value
	}
	return ""
}
func triggerAt(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return formatTime(window.Trigger.At)
	}
	return ""
}

func observationFingerprint(observation arbitrage.WindowObservation) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%t|%t|%s|%s|%s|%s|%s|%s|%s|%s",
		observation.ID, observation.WindowID, observation.Evaluation, formatTime(observation.ObservedAt), observation.Classification,
		observation.HasCandidate, observation.Best, observation.Candidate.Size.String(), observation.Candidate.GrossPnL.String(), observation.Candidate.NetPnL.String(), observation.Candidate.Cost.String(),
		observation.Candidate.Size.Asset(), observation.Candidate.GrossPnL.Asset(), observation.Candidate.NetPnL.Asset(), observation.Candidate.Cost.Asset())
}
