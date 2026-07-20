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
		"inventory: prepositioned\nfixed_cost_usd: %s\nweth_usd: %s\nfixed_cost_weth: %s\nprice_block: %d\nparity_checks: %d\n",
		report.Cost.FixedUSD.FloatString(6), report.Cost.WETHUSD.FloatString(8),
		report.Cost.WETHCost.Decimal(18), report.Cost.PriceBlock.Number, len(report.Parity),
	); err != nil {
		return err
	}
	for _, opportunity := range report.Research.Opportunities {
		for index, candidate := range opportunity.Candidates {
			netUSD := candidate.NetPnL.Rat()
			netUSD.Mul(netUSD, report.Cost.WETHUSD)
			if _, err := fmt.Fprintf(
				writer, "curve %s->%s index=%d size=%s VIRTUAL input=%s WETH output=%s WETH gross=%s WETH net=%s WETH net_usd=%s\n",
				opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket, index,
				candidate.Size.Decimal(8), candidate.Input.Decimal(18), candidate.Output.Decimal(18),
				candidate.GrossPnL.Decimal(18), candidate.NetPnL.Decimal(18),
				strings.TrimRight(strings.TrimRight(netUSD.FloatString(8), "0"), "."),
			); err != nil {
				return err
			}
		}
		if opportunity.SelectedIndex < 0 {
			continue
		}
		selected := opportunity.Candidates[opportunity.SelectedIndex]
		netUSD := selected.NetPnL.Rat()
		netUSD.Mul(netUSD, report.Cost.WETHUSD)
		if _, err := fmt.Fprintf(
			writer, "selected %s->%s size=%s VIRTUAL gross=%s WETH net=%s WETH net_usd=%s\n",
			opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket,
			selected.Size.Decimal(8), selected.GrossPnL.Decimal(18), selected.NetPnL.Decimal(18),
			strings.TrimRight(strings.TrimRight(netUSD.FloatString(8), "0"), "."),
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
			FixedUSD: report.Cost.FixedUSD.RatString(), WETHCost: report.Cost.WETHCost.String(),
			WETHUSD: report.Cost.WETHUSD.RatString(), PriceBlock: report.Cost.PriceBlock.Number,
			PriceBlockHash: report.Cost.PriceBlock.Hash.Hex(), PriceRound: report.Cost.PriceRound.String(),
			PriceUpdatedAt: report.Cost.PriceUpdatedAt,
		},
		Parity: make([]parityDTO, 0, len(report.Parity)),
	}
	for _, value := range report.Parity {
		payload.Parity = append(payload.Parity, parityDTO{
			Market: string(value.Market), Leg: value.Leg,
			TokenIn: string(value.AmountIn.Token()), AmountIn: value.AmountIn.String(),
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
	FixedUSD       string    `json:"fixed_usd"`
	WETHCost       string    `json:"weth_cost"`
	WETHUSD        string    `json:"weth_usd"`
	PriceBlock     uint64    `json:"price_block"`
	PriceBlockHash string    `json:"price_block_hash"`
	PriceRound     string    `json:"price_round"`
	PriceUpdatedAt time.Time `json:"price_updated_at"`
}

type parityDTO struct {
	Market       string `json:"market"`
	Leg          string `json:"leg"`
	TokenIn      string `json:"token_in"`
	AmountIn     string `json:"amount_in"`
	TokenOut     string `json:"token_out"`
	LocalOut     string `json:"local_out"`
	ReferenceOut string `json:"reference_out"`
	Matches      bool   `json:"matches"`
}
