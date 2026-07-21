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
		"inventory: prepositioned\nfixed_cost: %s %s\nprice: %s %s/%s\nprice_source: %s\nmodeled_cost: %s %s\nparity_checks: %d\n",
		report.Cost.FixedAmount.FloatString(6), report.Cost.FixedAsset,
		report.Cost.Price.Value().FloatString(8), report.Cost.Price.Base(), report.Cost.Price.Quote(),
		report.Cost.Price.Source(), report.Cost.Cost.Decimal(18), report.Cost.Cost.Asset(), len(report.Parity),
	); err != nil {
		return err
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
	return nil
}

func writeTextSummary(writer io.Writer, report Report) error {
	if _, err := fmt.Fprintf(writer, "evaluation: %d\nstatus: %s\nopportunities: %d\ncost: %s %s via %s\nparity_checks: %d\n",
		report.Research.Evaluations, report.Research.Status, len(report.Research.Opportunities),
		report.Cost.Cost.Decimal(6), report.Cost.Cost.Asset(), report.Cost.Price.Source(), len(report.Parity)); err != nil {
		return err
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
		Parity: make([]parityDTO, 0, len(report.Parity)),
	}
	for _, value := range report.Parity {
		payload.Parity = append(payload.Parity, parityDTO{
			Market: string(value.Market), Leg: value.Leg, Mode: string(value.Mode),
			TokenIn: string(value.LocalIn.Token()), LocalIn: value.LocalIn.String(), ReferenceIn: value.ReferenceIn.String(),
			TokenOut: string(value.LocalOut.Token()), LocalOut: value.LocalOut.String(),
			ReferenceOut: value.ReferenceOut.String(), Matches: value.Matches,
		})
	}
	encoder := json.NewEncoder(writer)
	if indent {
		encoder.SetIndent("", "  ")
	}
	encoder.SetEscapeHTML(false)
	return encoder.Encode(payload)
}

type summaryPayload struct {
	SchemaVersion int                     `json:"schema_version"`
	Kind          string                  `json:"kind"`
	Evaluation    int                     `json:"evaluation"`
	Status        runtimeresearch.Status  `json:"status"`
	Opportunities []summaryOpportunityDTO `json:"opportunities"`
	Cost          summaryCostDTO          `json:"cost"`
	ParityChecks  int                     `json:"parity_checks"`
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
		Status: report.Research.Status, Opportunities: make([]summaryOpportunityDTO, 0, len(report.Research.Opportunities)),
		Cost:         summaryCostDTO{Amount: report.Cost.Cost.Decimal(8), Asset: string(report.Cost.Cost.Asset()), Source: string(report.Cost.Price.Source())},
		ParityChecks: len(report.Parity),
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
