package research

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

func WriteJSON(writer io.Writer, report Report) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	return encoder.Encode(newReportDTO(report))
}

func WriteText(writer io.Writer, report Report) error {
	if _, err := fmt.Fprintf(writer, "Vernier deterministic Research\nrun: %s\nstatus: %s\nconfig: %s\nevaluations: %d\nopportunities: %d\nignored_events: %d\nfeed_incidents: %d\n",
		report.RunID, report.Status, report.ConfigHash, report.Evaluations, len(report.Opportunities),
		len(report.IgnoredEvents), len(report.FeedIncidents)); err != nil {
		return err
	}
	for _, opportunity := range report.Opportunities {
		net := "n/a"
		if opportunity.SelectedIndex >= 0 {
			net = opportunity.Candidates[opportunity.SelectedIndex].NetPnL.Decimal(6)
		}
		if _, err := fmt.Fprintf(writer, "%s %s->%s %s net=%s\n",
			opportunity.Strategy, opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket,
			opportunity.Classification, net); err != nil {
			return err
		}
	}
	return nil
}

type reportDTO struct {
	SchemaVersion int               `json:"schema_version"`
	RunID         string            `json:"run_id"`
	ConfigHash    string            `json:"config_hash"`
	Status        Status            `json:"status"`
	Evaluations   int               `json:"evaluations"`
	Opportunities []opportunityDTO  `json:"opportunities"`
	IgnoredEvents []ignoredEventDTO `json:"ignored_events"`
	FeedIncidents []feedIncidentDTO `json:"feed_incidents"`
}

type opportunityDTO struct {
	Evaluation     string         `json:"evaluation"`
	RunID          string         `json:"run_id"`
	ConfigHash     string         `json:"config_hash"`
	Strategy       string         `json:"strategy"`
	BuyMarket      string         `json:"buy_market"`
	SellMarket     string         `json:"sell_market"`
	Classification string         `json:"classification"`
	SelectedIndex  int            `json:"selected_index"`
	Threshold      quantityDTO    `json:"threshold"`
	Reasons        []string       `json:"reasons"`
	TriggeredAt    string         `json:"triggered_at"`
	StartedAt      string         `json:"started_at"`
	FinishedAt     string         `json:"finished_at"`
	Snapshots      []snapshotDTO  `json:"snapshots"`
	Candidates     []candidateDTO `json:"candidates"`
}

type snapshotDTO struct {
	Market          string             `json:"market"`
	Source          string             `json:"source"`
	Version         uint64             `json:"version"`
	SourcePosition  *sourcePositionDTO `json:"source_position,omitempty"`
	Finality        string             `json:"finality"`
	SourceTime      string             `json:"source_time,omitempty"`
	SourceTimeKnown bool               `json:"source_time_known"`
	ReceivedAt      string             `json:"received_at"`
	AppliedAt       string             `json:"applied_at"`
	Health          string             `json:"health"`
	HealthReason    string             `json:"health_reason,omitempty"`
	HealthChangedAt string             `json:"health_changed_at"`
	StateHash       string             `json:"state_hash"`
}

type sourcePositionDTO struct {
	Kind  string `json:"kind"`
	Value uint64 `json:"value"`
}

type candidateDTO struct {
	Input     quantityDTO `json:"input"`
	Output    quantityDTO `json:"output"`
	GrossPnL  quantityDTO `json:"gross_pnl"`
	Cost      costDTO     `json:"cost"`
	NetPnL    quantityDTO `json:"net_pnl"`
	BuyQuote  quoteDTO    `json:"buy_quote"`
	SellQuote quoteDTO    `json:"sell_quote"`
}

type costDTO struct {
	ID         string      `json:"id"`
	Amount     quantityDTO `json:"amount"`
	CapturedAt string      `json:"captured_at"`
}

type quantityDTO struct {
	Asset   string `json:"asset"`
	Exact   string `json:"exact"`
	Decimal string `json:"decimal"`
}

type quoteDTO struct {
	Source          string `json:"source"`
	Market          string `json:"market"`
	SnapshotVersion uint64 `json:"snapshot_version"`
	SnapshotHash    string `json:"snapshot_hash"`
	Purpose         string `json:"purpose"`
	TokenIn         string `json:"token_in"`
	AmountIn        string `json:"amount_in"`
	TokenOut        string `json:"token_out"`
	AmountOut       string `json:"amount_out"`
	Fee             string `json:"fee"`
	QuotedAt        string `json:"quoted_at"`
}

type ignoredEventDTO struct {
	Market          string             `json:"market"`
	Reason          string             `json:"reason"`
	SourcePosition  *sourcePositionDTO `json:"source_position,omitempty"`
	CurrentPosition *sourcePositionDTO `json:"current_position,omitempty"`
	ReceivedAt      string             `json:"received_at"`
}

type feedIncidentDTO struct {
	Market     string `json:"market"`
	Health     string `json:"health"`
	Reason     string `json:"reason"`
	ObservedAt string `json:"observed_at"`
}

func newReportDTO(report Report) reportDTO {
	dto := reportDTO{
		SchemaVersion: 1, RunID: string(report.RunID), ConfigHash: report.ConfigHash,
		Status: report.Status, Evaluations: report.Evaluations,
		Opportunities: make([]opportunityDTO, 0, len(report.Opportunities)),
		IgnoredEvents: make([]ignoredEventDTO, 0, len(report.IgnoredEvents)),
		FeedIncidents: make([]feedIncidentDTO, 0, len(report.FeedIncidents)),
	}
	for _, opportunity := range report.Opportunities {
		dto.Opportunities = append(dto.Opportunities, newOpportunityDTO(opportunity))
	}
	for _, ignored := range report.IgnoredEvents {
		dto.IgnoredEvents = append(dto.IgnoredEvents, ignoredEventDTO{
			Market: string(ignored.Market), Reason: ignored.Reason,
			SourcePosition: sourcePosition(ignored.Position), CurrentPosition: sourcePosition(ignored.CurrentPosition),
			ReceivedAt: formatTime(ignored.ReceivedAt),
		})
	}
	for _, incident := range report.FeedIncidents {
		dto.FeedIncidents = append(dto.FeedIncidents, feedIncidentDTO{
			Market: string(incident.Market), Health: string(incident.Health),
			Reason: incident.Reason, ObservedAt: formatTime(incident.ObservedAt),
		})
	}
	return dto
}

func newOpportunityDTO(opportunity arbitrage.Opportunity) opportunityDTO {
	dto := opportunityDTO{
		Evaluation: string(opportunity.Evaluation), RunID: string(opportunity.Run), ConfigHash: opportunity.ConfigHash,
		Strategy:  string(opportunity.Strategy),
		BuyMarket: string(opportunity.Direction.BuyMarket), SellMarket: string(opportunity.Direction.SellMarket),
		Classification: string(opportunity.Classification), SelectedIndex: opportunity.SelectedIndex,
		Threshold: quantity(opportunity.Threshold), Reasons: append([]string(nil), opportunity.Reasons...),
		TriggeredAt: formatTime(opportunity.TriggeredAt), StartedAt: formatTime(opportunity.StartedAt),
		FinishedAt: formatTime(opportunity.FinishedAt), Snapshots: make([]snapshotDTO, 0, len(opportunity.Snapshots)),
		Candidates: make([]candidateDTO, 0, len(opportunity.Candidates)),
	}
	for _, metadata := range opportunity.Snapshots {
		dto.Snapshots = append(dto.Snapshots, snapshot(metadata))
	}
	for _, candidate := range opportunity.Candidates {
		dto.Candidates = append(dto.Candidates, candidateDTO{
			Input: quantity(candidate.Input), Output: quantity(candidate.Output), GrossPnL: quantity(candidate.GrossPnL),
			Cost:     costDTO{ID: candidate.Cost.ID, Amount: quantity(candidate.Cost.Amount), CapturedAt: formatTime(candidate.Cost.CapturedAt)},
			NetPnL:   quantity(candidate.NetPnL),
			BuyQuote: quote(candidate.BuyQuote), SellQuote: quote(candidate.SellQuote),
		})
	}
	return dto
}

func snapshot(metadata market.SnapshotMetadata) snapshotDTO {
	dto := snapshotDTO{
		Market: string(metadata.Market), Source: string(metadata.Source), Version: metadata.Version,
		SourcePosition: sourcePosition(metadata.EventPosition), Finality: string(metadata.Finality),
		SourceTimeKnown: metadata.SourceTimeKnown, ReceivedAt: formatTime(metadata.ReceivedAt),
		AppliedAt: formatTime(metadata.AppliedAt), Health: string(metadata.Health), HealthReason: metadata.HealthReason,
		HealthChangedAt: formatTime(metadata.HealthChangedAt),
		StateHash:       hex.EncodeToString(metadata.StateHash[:]),
	}
	if metadata.SourceTimeKnown {
		dto.SourceTime = formatTime(metadata.SourceTime)
	}
	return dto
}

func sourcePosition(position market.SourcePosition) *sourcePositionDTO {
	if !position.Known() {
		return nil
	}
	return &sourcePositionDTO{Kind: string(position.Kind), Value: position.Value}
}

func quantity(value market.AssetQuantity) quantityDTO {
	return quantityDTO{Asset: string(value.Asset()), Exact: value.String(), Decimal: trimDecimal(value.Decimal(12))}
}

func quote(value market.Quote) quoteDTO {
	return quoteDTO{
		Source: string(value.Source), Market: string(value.Market), SnapshotVersion: value.SnapshotVersion,
		SnapshotHash: hex.EncodeToString(value.SnapshotHash[:]), Purpose: string(value.Purpose),
		TokenIn: string(value.AmountIn.Token()), AmountIn: value.AmountIn.String(),
		TokenOut: string(value.AmountOut.Token()), AmountOut: value.AmountOut.String(), Fee: value.Fee.String(),
		QuotedAt: formatTime(value.QuotedAt),
	}
}

func trimDecimal(value string) string {
	if !strings.Contains(value, ".") {
		return value
	}
	value = strings.TrimRight(value, "0")
	return strings.TrimRight(value, ".")
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }
