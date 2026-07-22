package research

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
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
	LocalTiming   timingDTO         `json:"local_timing"`
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
	Trigger        *triggerDTO    `json:"trigger,omitempty"`
	TriggeredAt    string         `json:"triggered_at"`
	StartedAt      string         `json:"started_at"`
	FinishedAt     string         `json:"finished_at"`
	Snapshots      []snapshotDTO  `json:"snapshots"`
	Candidates     []candidateDTO `json:"candidates"`
}

type triggerDTO struct {
	Market         string             `json:"market"`
	Source         string             `json:"source"`
	Position       *sourcePositionDTO `json:"position,omitempty"`
	ReferenceKind  string             `json:"reference_kind,omitempty"`
	ReferenceValue string             `json:"reference_value,omitempty"`
	At             string             `json:"at"`
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
	Size      quantityDTO `json:"size"`
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
	Source          string        `json:"source"`
	Market          string        `json:"market"`
	SnapshotVersion uint64        `json:"snapshot_version"`
	SnapshotHash    string        `json:"snapshot_hash"`
	Purpose         string        `json:"purpose"`
	Mode            string        `json:"mode"`
	TokenIn         string        `json:"token_in"`
	AmountIn        string        `json:"amount_in"`
	TokenOut        string        `json:"token_out"`
	AmountOut       string        `json:"amount_out"`
	Fees            []quoteFeeDTO `json:"fees"`
	QuotedAt        string        `json:"quoted_at"`
}

type quoteFeeDTO struct {
	Kind              string `json:"kind"`
	Effect            string `json:"effect"`
	Token             string `json:"token"`
	Amount            string `json:"amount"`
	IncludedInAmounts bool   `json:"included_in_amounts"`
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

type timingDTO struct {
	Duration   string            `json:"duration"`
	Discovery  *discoveryTiming  `json:"discovery,omitempty"`
	Directions []directionTiming `json:"directions"`
}

type discoveryTiming struct {
	Samples    int              `json:"samples"`
	Duration   string           `json:"duration"`
	Decision   string           `json:"decision"`
	BuyMarket  string           `json:"buy_market,omitempty"`
	SellMarket string           `json:"sell_market,omitempty"`
	Probes     []discoveryProbe `json:"probes"`
}

type discoveryProbe struct {
	Size     quantityDTO         `json:"size"`
	Outputs  []discoveryProbeOut `json:"outputs"`
	Winner   string              `json:"winner,omitempty"`
	Reason   string              `json:"reason,omitempty"`
	Duration string              `json:"duration"`
}

type discoveryProbeOut struct {
	Market   string      `json:"market"`
	Output   quantityDTO `json:"output"`
	Duration string      `json:"duration"`
	Cached   bool        `json:"cached"`
}

type directionTiming struct {
	BuyMarket  string        `json:"buy_market"`
	SellMarket string        `json:"sell_market"`
	Duration   string        `json:"duration"`
	Quotes     []quoteTiming `json:"quotes"`
}

type quoteTiming struct {
	Market   string      `json:"market"`
	Leg      string      `json:"leg"`
	Mode     string      `json:"mode"`
	Duration string      `json:"duration"`
	Cached   bool        `json:"cached"`
	Hops     []hopTiming `json:"hops,omitempty"`
}

type hopTiming struct {
	Market   string `json:"market"`
	Duration string `json:"duration"`
	Cached   bool   `json:"cached"`
}

func newReportDTO(report Report) reportDTO {
	dto := reportDTO{
		SchemaVersion: 1, RunID: string(report.RunID), ConfigHash: report.ConfigHash,
		Status: report.Status, Evaluations: report.Evaluations,
		Opportunities: make([]opportunityDTO, 0, len(report.Opportunities)),
		IgnoredEvents: make([]ignoredEventDTO, 0, len(report.IgnoredEvents)),
		FeedIncidents: make([]feedIncidentDTO, 0, len(report.FeedIncidents)),
		LocalTiming:   timing(report.LocalTiming),
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

func timing(value strategy.EvaluationTiming) timingDTO {
	dto := timingDTO{Duration: value.Duration.String(), Directions: make([]directionTiming, 0, len(value.Directions))}
	if value.Discovery != nil {
		discovery := value.Discovery
		dto.Discovery = &discoveryTiming{Samples: discovery.Samples, Duration: discovery.Duration.String(), Decision: discovery.Decision, BuyMarket: string(discovery.Selected.BuyMarket), SellMarket: string(discovery.Selected.SellMarket), Probes: make([]discoveryProbe, 0, len(discovery.Probes))}
		for _, probe := range discovery.Probes {
			item := discoveryProbe{Size: quantity(probe.Size), Winner: string(probe.Winner), Reason: probe.Reason, Duration: probe.Duration.String(), Outputs: make([]discoveryProbeOut, 0, len(probe.Outputs))}
			for _, output := range probe.Outputs {
				item.Outputs = append(item.Outputs, discoveryProbeOut{Market: string(output.Market), Output: quantity(output.Output), Duration: output.Duration.String(), Cached: output.Cached})
			}
			dto.Discovery.Probes = append(dto.Discovery.Probes, item)
		}
	}
	for _, direction := range value.Directions {
		item := directionTiming{BuyMarket: string(direction.Direction.BuyMarket), SellMarket: string(direction.Direction.SellMarket), Duration: direction.Duration.String(), Quotes: make([]quoteTiming, 0, len(direction.Quotes))}
		for _, quote := range direction.Quotes {
			item.Quotes = append(item.Quotes, quoteTiming{Market: string(quote.Market), Leg: quote.Leg, Mode: string(quote.Mode), Duration: quote.Duration.String(), Cached: quote.Cached, Hops: hopTimings(quote.Hops)})
		}
		dto.Directions = append(dto.Directions, item)
	}
	return dto
}

func hopTimings(values []quoteport.HopTiming) []hopTiming {
	if len(values) == 0 {
		return nil
	}
	result := make([]hopTiming, 0, len(values))
	for _, value := range values {
		result = append(result, hopTiming{Market: string(value.Market), Duration: value.Duration.String(), Cached: value.Cached})
	}
	return result
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
	if opportunity.HasTrigger {
		dto.Trigger = &triggerDTO{
			Market: string(opportunity.Trigger.Market), Source: string(opportunity.Trigger.Source),
			Position: sourcePosition(opportunity.Trigger.Position), ReferenceKind: string(opportunity.Trigger.Reference.Kind),
			ReferenceValue: opportunity.Trigger.Reference.Value, At: formatTime(opportunity.Trigger.At),
		}
	}
	for _, metadata := range opportunity.Snapshots {
		dto.Snapshots = append(dto.Snapshots, snapshot(metadata))
	}
	for _, candidate := range opportunity.Candidates {
		dto.Candidates = append(dto.Candidates, candidateDTO{
			Size: quantity(candidate.Size), Input: quantity(candidate.Input), Output: quantity(candidate.Output), GrossPnL: quantity(candidate.GrossPnL),
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
	dto := quoteDTO{
		Source: string(value.Source), Market: string(value.Market), SnapshotVersion: value.SnapshotVersion,
		SnapshotHash: hex.EncodeToString(value.SnapshotHash[:]), Purpose: string(value.Purpose), Mode: string(value.Mode),
		TokenIn: string(value.AmountIn.Token()), AmountIn: value.AmountIn.String(),
		TokenOut: string(value.AmountOut.Token()), AmountOut: value.AmountOut.String(),
		Fees: make([]quoteFeeDTO, 0, len(value.Fees())), QuotedAt: formatTime(value.QuotedAt),
	}
	for _, fee := range value.Fees() {
		dto.Fees = append(dto.Fees, quoteFeeDTO{
			Kind: fee.Kind(), Effect: string(fee.Effect()), Token: string(fee.Amount().Token()),
			Amount: fee.Amount().String(), IncludedInAmounts: fee.IncludedInAmounts(),
		})
	}
	return dto
}

func trimDecimal(value string) string {
	if !strings.Contains(value, ".") {
		return value
	}
	value = strings.TrimRight(value, "0")
	return strings.TrimRight(value, ".")
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }
