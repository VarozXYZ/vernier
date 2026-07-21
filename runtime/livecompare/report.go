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

func WriteText(writer io.Writer, report Report) error {
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

func WriteJSON(writer io.Writer, report Report) error {
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
	encoder.SetIndent("", "  ")
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
