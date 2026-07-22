package livecompare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

// CalculationDetail controls how much of the sizing curve is rendered. It is
// deliberately separate from diagnostic log levels: debug explains runtime
// behavior, while full calculation output explains every economic sample.
type CalculationDetail string

const (
	CalculationSummary CalculationDetail = "summary"
	CalculationFull    CalculationDetail = "full"
)

type OutputOptions struct {
	Calculations CalculationDetail
}

func (o OutputOptions) validate() error {
	if o.Calculations == "" {
		o.Calculations = CalculationSummary
	}
	if o.Calculations != CalculationSummary && o.Calculations != CalculationFull {
		return fmt.Errorf("invalid calculation detail %q", o.Calculations)
	}
	return nil
}

func WriteText(writer io.Writer, report Report) error {
	return WriteTextWithOptions(writer, report, OutputOptions{Calculations: CalculationFull})
}

func WriteTextWithOptions(writer io.Writer, report Report, options OutputOptions) error {
	if options.Calculations == "" {
		options.Calculations = CalculationSummary
	}
	if err := options.validate(); err != nil {
		return err
	}
	if options.Calculations == CalculationSummary {
		return writeTextSummary(writer, report)
	}
	if err := runtimeresearch.WriteText(writer, report.Research); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(
		writer,
		"inventory: prepositioned\nfixed_cost: %s %s\nprice: %s %s/%s\nprice_source: %s\nmodeled_cost: %s %s\nlocal_evaluation_duration: %s\nexternal_reference_checks: %d\nparity_checks: %d\n",
		report.Cost.FixedAmount.FloatString(6), report.Cost.FixedAsset,
		report.Cost.Price.Value().FloatString(8), report.Cost.Price.Base(), report.Cost.Price.Quote(),
		report.Cost.Price.Source(), report.Cost.Cost.Decimal(18), report.Cost.Cost.Asset(), report.Research.LocalTiming.Duration, len(report.Reference), len(report.Parity),
	); err != nil {
		return err
	}
	if discovery := report.Research.LocalTiming.Discovery; discovery != nil {
		if _, err := fmt.Fprintf(writer, "direction_discovery samples=%d duration=%s decision=%s selected=%s->%s\n", discovery.Samples, discovery.Duration, discovery.Decision, discovery.Selected.BuyMarket, discovery.Selected.SellMarket); err != nil {
			return err
		}
		for index, probe := range discovery.Probes {
			if _, err := fmt.Fprintf(writer, "direction_probe index=%d size=%s %s winner=%s reason=%s duration=%s\n", index, probe.Size.Decimal(8), probe.Size.Asset(), probe.Winner, probe.Reason, probe.Duration); err != nil {
				return err
			}
			for _, output := range probe.Outputs {
				if _, err := fmt.Fprintf(writer, "direction_probe_output market=%s output=%s %s duration=%s cached=%t\n", output.Market, output.Output.Decimal(8), output.Output.Asset(), output.Duration, output.Cached); err != nil {
					return err
				}
			}
		}
	}
	for _, direction := range report.Research.LocalTiming.Directions {
		if _, err := fmt.Fprintf(writer, "local_direction %s->%s duration=%s\n", direction.Direction.BuyMarket, direction.Direction.SellMarket, direction.Duration); err != nil {
			return err
		}
		for _, quote := range direction.Quotes {
			if _, err := fmt.Fprintf(writer, "local_quote %s leg=%s mode=%s duration=%s cached=%t\n", quote.Market, quote.Leg, quote.Mode, quote.Duration, quote.Cached); err != nil {
				return err
			}
			for _, hop := range quote.Hops {
				if _, err := fmt.Fprintf(writer, "local_hop %s duration=%s cached=%t\n", hop.Market, hop.Duration, hop.Cached); err != nil {
					return err
				}
			}
		}
	}
	for _, opportunity := range report.Research.Opportunities {
		for index, candidate := range opportunity.Candidates {
			netQuote := candidate.NetPnL.Rat()
			netQuote.Mul(netQuote, report.Cost.Price.Value())
			if _, err := fmt.Fprintf(
				writer, "curve %s->%s index=%d size=%s %s input=%s %s output=%s %s gross=%s %s net=%s %s net_%s=%s\n",
				opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket, index,
				candidate.Size.Decimal(8), candidate.Size.Asset(), candidate.Input.Decimal(18), candidate.Input.Asset(),
				candidate.Output.Decimal(18), candidate.Output.Asset(), candidate.GrossPnL.Decimal(18), candidate.GrossPnL.Asset(),
				candidate.NetPnL.Decimal(18), candidate.NetPnL.Asset(), report.Cost.Price.Quote(),
				strings.TrimRight(strings.TrimRight(netQuote.FloatString(8), "0"), "."),
			); err != nil {
				return err
			}
		}
		if opportunity.SelectedIndex < 0 {
			continue
		}
		selected := opportunity.Candidates[opportunity.SelectedIndex]
		netQuote := selected.NetPnL.Rat()
		netQuote.Mul(netQuote, report.Cost.Price.Value())
		if _, err := fmt.Fprintf(
			writer, "selected %s->%s size=%s %s gross=%s %s net=%s %s net_%s=%s\n",
			opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket,
			selected.Size.Decimal(8), selected.Size.Asset(), selected.GrossPnL.Decimal(18), selected.GrossPnL.Asset(),
			selected.NetPnL.Decimal(18), selected.NetPnL.Asset(), report.Cost.Price.Quote(),
			strings.TrimRight(strings.TrimRight(netQuote.FloatString(8), "0"), "."),
		); err != nil {
			return err
		}
	}
	for _, value := range report.Reference {
		if _, err := fmt.Fprintf(writer, "reference %s->%s leg=%s provider=%s status=%s local_out=%s reference_out=%s delta_raw=%s local_quote=%s reference_latency=%s total=%s\n",
			value.Direction.BuyMarket, value.Direction.SellMarket, value.Leg, value.Provider, value.Status,
			value.LocalOutput.String(), value.ReferenceOutput.String(), value.OutputDeltaRaw,
			value.LocalQuoteDuration, value.ReferenceLatency, value.TotalDuration); err != nil {
			return err
		}
	}
	return nil
}

func writeTextSummary(writer io.Writer, report Report) error {
	if _, err := fmt.Fprintf(writer, "evaluation: %d\nstatus: %s\nopportunities: %d\ncost: %s %s via %s\nlocal_evaluation_duration: %s\nexternal_reference_checks: %d\nparity_checks: %d\n",
		report.Research.Evaluations, report.Research.Status, len(report.Research.Opportunities),
		report.Cost.Cost.Decimal(6), report.Cost.Cost.Asset(), report.Cost.Price.Source(), report.Research.LocalTiming.Duration, len(report.Reference), len(report.Parity)); err != nil {
		return err
	}
	if discovery := report.Research.LocalTiming.Discovery; discovery != nil {
		if _, err := fmt.Fprintf(writer, "direction_discovery samples=%d duration=%s decision=%s selected=%s->%s\n", discovery.Samples, discovery.Duration, discovery.Decision, discovery.Selected.BuyMarket, discovery.Selected.SellMarket); err != nil {
			return err
		}
	}
	for _, opportunity := range report.Research.Opportunities {
		selected := "none"
		if opportunity.SelectedIndex >= 0 && opportunity.SelectedIndex < len(opportunity.Candidates) {
			candidate := opportunity.Candidates[opportunity.SelectedIndex]
			selected = fmt.Sprintf("size=%s %s net=%s %s", candidate.Size.Decimal(8), candidate.Size.Asset(), candidate.NetPnL.Decimal(8), candidate.NetPnL.Asset())
		}
		if _, err := fmt.Fprintf(writer, "opportunity: %s->%s classification=%s candidates=%d selected=%s\n",
			opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket, opportunity.Classification,
			len(opportunity.Candidates), selected); err != nil {
			return err
		}
	}
	return nil
}

func WriteJSON(writer io.Writer, report Report) error {
	return WriteJSONWithOptions(writer, report, OutputOptions{Calculations: CalculationFull})
}

func WriteJSONWithOptions(writer io.Writer, report Report, options OutputOptions) error {
	if options.Calculations == "" {
		options.Calculations = CalculationSummary
	}
	if err := options.validate(); err != nil {
		return err
	}
	return writeJSON(writer, report, true, options.Calculations)
}

// WriteJSONLine writes one compact, deterministic JSON report followed by a
// newline. It is intended for continuous research streams.
func WriteJSONLine(writer io.Writer, report Report) error {
	return WriteJSONLineWithOptions(writer, report, OutputOptions{Calculations: CalculationFull})
}

func WriteJSONLineWithOptions(writer io.Writer, report Report, options OutputOptions) error {
	if options.Calculations == "" {
		options.Calculations = CalculationSummary
	}
	if err := options.validate(); err != nil {
		return err
	}
	return writeJSON(writer, report, false, options.Calculations)
}

// WriteReferenceJSONLine writes the asynchronous external-validation record
// emitted after a stream's local report.
func WriteReferenceJSONLine(writer io.Writer, report ReferenceReport) error {
	payload := struct {
		SchemaVersion int            `json:"schema_version"`
		Kind          string         `json:"kind"`
		Evaluation    int            `json:"evaluation"`
		Comparisons   []referenceDTO `json:"comparisons"`
	}{SchemaVersion: 1, Kind: "external_reference", Evaluation: report.Evaluation, Comparisons: make([]referenceDTO, 0, len(report.Comparisons))}
	for _, value := range report.Comparisons {
		payload.Comparisons = append(payload.Comparisons, referenceDTOFrom(value))
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(payload)
}

// WriteReferenceText is the human-readable counterpart of
// WriteReferenceJSONLine.
func WriteReferenceText(writer io.Writer, report ReferenceReport) error {
	if _, err := fmt.Fprintf(writer, "external_reference evaluation=%d checks=%d\n", report.Evaluation, len(report.Comparisons)); err != nil {
		return err
	}
	for _, value := range report.Comparisons {
		if _, err := fmt.Fprintf(writer, "reference %s->%s leg=%s provider=%s status=%s local_out=%s reference_out=%s delta_raw=%s local_quote=%s reference_latency=%s total=%s\n",
			value.Direction.BuyMarket, value.Direction.SellMarket, value.Leg, value.Provider, value.Status,
			value.LocalOutput.String(), value.ReferenceOutput.String(), value.OutputDeltaRaw,
			value.LocalQuoteDuration, value.ReferenceLatency, value.TotalDuration); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(writer io.Writer, report Report, indent bool, detail CalculationDetail) error {
	if detail == CalculationSummary {
		return writeJSONSummary(writer, report)
	}
	var researchJSON bytes.Buffer
	if err := runtimeresearch.WriteJSON(&researchJSON, report.Research); err != nil {
		return err
	}
	payload := struct {
		SchemaVersion int             `json:"schema_version"`
		Research      json.RawMessage `json:"research"`
		Cost          costDTO         `json:"cost_evidence"`
		Parity        []parityDTO     `json:"parity"`
		Reference     []referenceDTO  `json:"external_reference"`
	}{
		SchemaVersion: 1,
		Research:      json.RawMessage(bytes.TrimSpace(researchJSON.Bytes())),
		Cost: costDTO{
			FixedAmount: report.Cost.FixedAmount.RatString(), FixedAsset: string(report.Cost.FixedAsset),
			CostAmount: report.Cost.Cost.String(), CostAsset: string(report.Cost.Cost.Asset()),
			PriceSource: string(report.Cost.Price.Source()), PriceBase: string(report.Cost.Price.Base()),
			PriceQuote: string(report.Cost.Price.Quote()), PriceValue: report.Cost.Price.Value().RatString(),
			PriceReference: report.Cost.Price.Reference(), PriceUpdatedAt: report.Cost.Price.SourceUpdatedAt(),
			PriceObservedAt: report.Cost.Price.ObservedAt(),
		},
		Parity:    make([]parityDTO, 0, len(report.Parity)),
		Reference: make([]referenceDTO, 0, len(report.Reference)),
	}
	for _, value := range report.Parity {
		payload.Parity = append(payload.Parity, parityDTO{
			Market: string(value.Market), Leg: value.Leg, Mode: string(value.Mode),
			TokenIn: string(value.LocalIn.Token()), LocalIn: value.LocalIn.String(), ReferenceIn: value.ReferenceIn.String(),
			TokenOut: string(value.LocalOut.Token()), LocalOut: value.LocalOut.String(),
			ReferenceOut: value.ReferenceOut.String(), Matches: value.Matches,
		})
	}
	for _, value := range report.Reference {
		payload.Reference = append(payload.Reference, referenceDTOFrom(value))
	}
	encoder := json.NewEncoder(writer)
	if indent {
		encoder.SetIndent("", "  ")
	}
	encoder.SetEscapeHTML(false)
	return encoder.Encode(payload)
}

func referenceDTOFrom(value ReferenceComparison) referenceDTO {
	return referenceDTO{
		BuyMarket: string(value.Direction.BuyMarket), SellMarket: string(value.Direction.SellMarket),
		Market: string(value.Market), Leg: value.Leg, Provider: string(value.Provider),
		SnapshotVersion: value.SnapshotVersion, InputToken: string(value.Input.Token()), Input: value.Input.String(),
		LocalOutput: value.LocalOutput.String(), ReferenceOutput: value.ReferenceOutput.String(),
		OutputDeltaRaw: value.OutputDeltaRaw, Status: string(value.Status), ContextSlot: value.ContextSlot,
		LocalQuoteDuration: value.LocalQuoteDuration.String(), ReferenceLatency: value.ReferenceLatency.String(),
		TotalDuration: value.TotalDuration.String(), Error: value.Error,
	}
}

type summaryPayload struct {
	SchemaVersion   int                     `json:"schema_version"`
	Kind            string                  `json:"kind"`
	Evaluation      int                     `json:"evaluation"`
	Status          runtimeresearch.Status  `json:"status"`
	LocalDuration   string                  `json:"local_evaluation_duration"`
	Discovery       *discoverySummaryDTO    `json:"direction_discovery,omitempty"`
	Opportunities   []summaryOpportunityDTO `json:"opportunities"`
	Cost            summaryCostDTO          `json:"cost"`
	ParityChecks    int                     `json:"parity_checks"`
	ReferenceChecks int                     `json:"external_reference_checks"`
}

type discoverySummaryDTO struct {
	Samples    int    `json:"samples"`
	Duration   string `json:"duration"`
	Decision   string `json:"decision"`
	BuyMarket  string `json:"buy_market,omitempty"`
	SellMarket string `json:"sell_market,omitempty"`
}

type summaryOpportunityDTO struct {
	Strategy       string               `json:"strategy"`
	BuyMarket      string               `json:"buy_market"`
	SellMarket     string               `json:"sell_market"`
	Classification string               `json:"classification"`
	Candidates     int                  `json:"candidates"`
	Selected       *summaryCandidateDTO `json:"selected,omitempty"`
}

type summaryCandidateDTO struct {
	Size      string `json:"size"`
	SizeAsset string `json:"size_asset"`
	NetPnL    string `json:"net_pnl"`
	NetAsset  string `json:"net_asset"`
}

type summaryCostDTO struct {
	Amount string `json:"amount"`
	Asset  string `json:"asset"`
	Source string `json:"source"`
}

func writeJSONSummary(writer io.Writer, report Report) error {
	payload := summaryPayload{
		SchemaVersion: 1, Kind: "evaluation", Evaluation: report.Research.Evaluations,
		Status: report.Research.Status, LocalDuration: report.Research.LocalTiming.Duration.String(), Opportunities: make([]summaryOpportunityDTO, 0, len(report.Research.Opportunities)),
		Cost:            summaryCostDTO{Amount: report.Cost.Cost.Decimal(8), Asset: string(report.Cost.Cost.Asset()), Source: string(report.Cost.Price.Source())},
		ReferenceChecks: len(report.Reference), ParityChecks: len(report.Parity),
	}
	if discovery := report.Research.LocalTiming.Discovery; discovery != nil {
		payload.Discovery = &discoverySummaryDTO{Samples: discovery.Samples, Duration: discovery.Duration.String(), Decision: discovery.Decision, BuyMarket: string(discovery.Selected.BuyMarket), SellMarket: string(discovery.Selected.SellMarket)}
	}
	for _, opportunity := range report.Research.Opportunities {
		item := summaryOpportunityDTO{
			Strategy: string(opportunity.Strategy), BuyMarket: string(opportunity.Direction.BuyMarket),
			SellMarket: string(opportunity.Direction.SellMarket), Classification: string(opportunity.Classification),
			Candidates: len(opportunity.Candidates),
		}
		if opportunity.SelectedIndex >= 0 && opportunity.SelectedIndex < len(opportunity.Candidates) {
			candidate := opportunity.Candidates[opportunity.SelectedIndex]
			item.Selected = &summaryCandidateDTO{
				Size: candidate.Size.Decimal(8), SizeAsset: string(candidate.Size.Asset()),
				NetPnL: candidate.NetPnL.Decimal(8), NetAsset: string(candidate.NetPnL.Asset()),
			}
		}
		payload.Opportunities = append(payload.Opportunities, item)
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(payload)
}

type costDTO struct {
	FixedAmount     string    `json:"fixed_amount"`
	FixedAsset      string    `json:"fixed_asset"`
	CostAmount      string    `json:"cost_amount"`
	CostAsset       string    `json:"cost_asset"`
	PriceSource     string    `json:"price_source"`
	PriceBase       string    `json:"price_base"`
	PriceQuote      string    `json:"price_quote"`
	PriceValue      string    `json:"price_value"`
	PriceReference  string    `json:"price_reference"`
	PriceUpdatedAt  time.Time `json:"price_updated_at"`
	PriceObservedAt time.Time `json:"price_observed_at"`
}

type parityDTO struct {
	Market       string `json:"market"`
	Leg          string `json:"leg"`
	Mode         string `json:"mode"`
	TokenIn      string `json:"token_in"`
	LocalIn      string `json:"local_in"`
	ReferenceIn  string `json:"reference_in"`
	TokenOut     string `json:"token_out"`
	LocalOut     string `json:"local_out"`
	ReferenceOut string `json:"reference_out"`
	Matches      bool   `json:"matches"`
}

type referenceDTO struct {
	BuyMarket          string `json:"buy_market"`
	SellMarket         string `json:"sell_market"`
	Market             string `json:"market"`
	Leg                string `json:"leg"`
	Provider           string `json:"provider"`
	SnapshotVersion    uint64 `json:"snapshot_version"`
	InputToken         string `json:"input_token"`
	Input              string `json:"input"`
	LocalOutput        string `json:"local_output"`
	ReferenceOutput    string `json:"reference_output"`
	OutputDeltaRaw     string `json:"output_delta_raw"`
	Status             string `json:"status"`
	ContextSlot        uint64 `json:"context_slot,omitempty"`
	LocalQuoteDuration string `json:"local_quote_duration"`
	ReferenceLatency   string `json:"reference_latency"`
	TotalDuration      string `json:"total_duration"`
	Error              string `json:"error,omitempty"`
}
