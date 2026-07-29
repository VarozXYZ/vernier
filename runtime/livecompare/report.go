package livecompare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
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
	// OmitCost is used by continuous streams after their first report. The
	// configured cost is invariant for the run and should not dominate every
	// evaluation block.
	OmitCost bool
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
		return writeTextSummary(writer, report, options)
	}
	if err := runtimeresearch.WriteText(writer, report.Research); err != nil {
		return err
	}
	if !options.OmitCost {
		if _, err := fmt.Fprintf(
			writer,
			"inventory: prepositioned\nfixed_cost: %s %s\n",
			trimNumber(report.Cost.FixedAmount.FloatString(18)), report.Cost.FixedAsset,
		); err != nil {
			return err
		}
	}
	if hasCostPrice(report.Cost) {
		if _, err := fmt.Fprintf(writer, "price: %s %s/%s\nprice_source: %s\n",
			report.Cost.Price.Value().FloatString(8), report.Cost.Price.Base(), report.Cost.Price.Quote(),
			report.Cost.Price.Source()); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintln(writer, "price: not_required"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "modeled_cost: %s %s\nlocal_evaluation_duration: %s\nexternal_reference_checks: %d\nparity_checks: %d\n",
		report.Cost.Cost.Decimal(18), report.Cost.Cost.Asset(), report.Research.LocalTiming.Duration, len(report.Reference), len(report.Parity)); err != nil {
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
				if _, err := fmt.Fprintf(writer, "direction_probe_output market=%s output=%s %s duration=%s cached=%t", output.Market, output.Output.Decimal(8), output.Output.Asset(), output.Duration, output.Cached); err != nil {
					return err
				}
				if output.Error != "" {
					if _, err := fmt.Fprintf(writer, " error=%q", output.Error); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprintln(writer); err != nil {
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
			if err := writeQuoteResult(writer, report.Research.Opportunities, quote); err != nil {
				return err
			}
			for _, hop := range quote.Hops {
				if _, err := fmt.Fprintf(writer, "local_hop %s input_raw=%s output_raw=%s duration=%s cached=%t\n", hop.Market, hop.AmountIn, hop.AmountOut, hop.Duration, hop.Cached); err != nil {
					return err
				}
			}
		}
	}
	for _, opportunity := range report.Research.Opportunities {
		for index, candidate := range opportunity.Candidates {
			convertedNet := ""
			if hasCostPrice(report.Cost) {
				netQuote := candidate.NetPnL.Rat()
				netQuote.Mul(netQuote, report.Cost.Price.Value())
				convertedNet = fmt.Sprintf(" net_%s=%s", report.Cost.Price.Quote(), strings.TrimRight(strings.TrimRight(netQuote.FloatString(8), "0"), "."))
			}
			if _, err := fmt.Fprintf(
				writer, "curve %s->%s index=%d size=%s %s input=%s %s output=%s %s gross=%s %s net=%s %s%s\n",
				opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket, index,
				candidate.Size.Decimal(8), candidate.Size.Asset(), candidate.Input.Decimal(18), candidate.Input.Asset(),
				candidate.Output.Decimal(18), candidate.Output.Asset(), candidate.GrossPnL.Decimal(18), candidate.GrossPnL.Asset(),
				candidate.NetPnL.Decimal(18), candidate.NetPnL.Asset(), convertedNet,
			); err != nil {
				return err
			}
		}
		if opportunity.SelectedIndex < 0 {
			continue
		}
		selected := opportunity.Candidates[opportunity.SelectedIndex]
		convertedNet := ""
		if hasCostPrice(report.Cost) {
			netQuote := selected.NetPnL.Rat()
			netQuote.Mul(netQuote, report.Cost.Price.Value())
			convertedNet = fmt.Sprintf(" net_%s=%s", report.Cost.Price.Quote(), strings.TrimRight(strings.TrimRight(netQuote.FloatString(8), "0"), "."))
		}
		if _, err := fmt.Fprintf(
			writer, "selected %s->%s size=%s %s gross=%s %s net=%s %s%s\n",
			opportunity.Direction.BuyMarket, opportunity.Direction.SellMarket,
			selected.Size.Decimal(8), selected.Size.Asset(), selected.GrossPnL.Decimal(18), selected.GrossPnL.Asset(),
			selected.NetPnL.Decimal(18), selected.NetPnL.Asset(), convertedNet,
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

func writeTextSummary(writer io.Writer, report Report, options OutputOptions) error {
	if _, err := fmt.Fprintln(writer, "------------------------------------------------------------------------"); err != nil {
		return err
	}
	if err := writeTrigger(writer, report.Research.Opportunities); err != nil {
		return err
	}
	if !options.OmitCost && report.Cost.Cost.Asset() != "" {
		if report.Cost.Model == "complete_flow_cache" {
			state := "warming"
			if report.Cost.Available {
				state = "ready (per route)"
			}
			if _, err := fmt.Fprintf(
				writer, "  Flow costs   %s\n", state,
			); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(writer, "  Fixed cost   %s\n",
				readableQuantity(report.Cost.Cost)); err != nil {
				return err
			}
		}
	}
	if discovery := report.Research.LocalTiming.Discovery; discovery != nil {
		if _, err := fmt.Fprintf(writer, "  Discovery    %s (%d samples, %s)\n",
			readableDuration(discovery.Duration), discovery.Samples, discovery.Decision); err != nil {
			return err
		}
	}
	if len(report.Research.LocalTiming.Directions) > 0 {
		if _, err := fmt.Fprintln(writer, "\nROUND TRIPS (parallel; buy -> sell within each route)"); err != nil {
			return err
		}
		for index, direction := range report.Research.LocalTiming.Directions {
			marker := ""
			if selectedDirection(report.Research.Opportunities, direction.Direction) {
				marker = "  [SELECTED]"
			}
			if _, err := fmt.Fprintf(
				writer, "\n  ROUTE %d: %s -> %s%s\n",
				index+1, readableMarket(direction.Direction.BuyMarket),
				readableMarket(direction.Direction.SellMarket), marker,
			); err != nil {
				return err
			}
			for _, quote := range direction.Quotes {
				if err := writeReadableQuote(writer, quote); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(writer, "    Route time  %s\n", readableDuration(direction.Duration)); err != nil {
				return err
			}
		}
	}
	for _, opportunity := range report.Research.Opportunities {
		if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
			continue
		}
		candidate := opportunity.Candidates[opportunity.SelectedIndex]
		if _, err := fmt.Fprintf(
			writer,
			"\nRESULT (%s -> %s)\n  Gross PnL    %s\n  Flow cost    %s\n  Net PnL      %s  [%s]\n",
			readableMarket(opportunity.Direction.BuyMarket), readableMarket(opportunity.Direction.SellMarket),
			readableQuantity(candidate.GrossPnL), readableQuantity(candidate.Cost.Amount),
			readableQuantity(candidate.NetPnL),
			readableVerdict(opportunity),
		); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(writer, "\nTOTAL          %s\n\n", readableDuration(report.Research.LocalTiming.Duration)); err != nil {
		return err
	}
	return nil
}

func writeReadableQuote(writer io.Writer, quote strategy.QuoteTiming) error {
	provider := readableProvider(quote.Source)
	chain := readableMarket(quote.Market)
	duration := readableDuration(quote.Duration)
	leg := strings.ToUpper(quote.Leg)
	if quote.Error != "" {
		_, err := fmt.Fprintf(writer, "    %-5s %-11s %-9s ERROR: %s  [%s]\n", leg, provider, chain, quote.Error, duration)
		return err
	}
	cache := ""
	if quote.Cached {
		cache = ", cached"
	}
	_, err := fmt.Fprintf(
		writer, "    %-5s %-11s %-9s %s  ->  %s  [%s%s]\n",
		leg, provider, chain, readableQuantity(quote.Input), readableQuantity(quote.Output),
		duration, cache,
	)
	return err
}

func selectedDirection(opportunities []arbitrage.Opportunity, direction arbitrage.Direction) bool {
	for _, opportunity := range opportunities {
		if opportunity.Direction == direction &&
			opportunity.SelectedIndex >= 0 &&
			opportunity.SelectedIndex < len(opportunity.Candidates) {
			return true
		}
	}
	return false
}

func writeQuoteResult(writer io.Writer, opportunities []arbitrage.Opportunity, quote strategy.QuoteTiming) error {
	input, inputRaw := observedAmount(quote.Input, quote.AmountIn)
	output, outputRaw := observedAmount(quote.Output, quote.AmountOut)
	if _, err := fmt.Fprintf(
		writer,
		"quote_result market=%s provider=%s leg=%s role=%s input=%s input_raw=%s output=%s output_raw=%s latency=%s cached=%t",
		quote.Market, quote.Source, quote.Leg, quoteRole(opportunities, quote),
		input, inputRaw, output, outputRaw, quote.Duration, quote.Cached,
	); err != nil {
		return err
	}
	if quote.Error != "" {
		if _, err := fmt.Fprintf(writer, " error=%q", quote.Error); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(writer)
	return err
}

func writeTrigger(writer io.Writer, opportunities []arbitrage.Opportunity) error {
	var trigger arbitrage.TriggerMetadata
	found := false
	for _, opportunity := range opportunities {
		if opportunity.HasTrigger {
			trigger, found = opportunity.Trigger, true
			break
		}
	}
	if !found {
		_, err := fmt.Fprintln(writer, "TRIGGER: POINT IN TIME")
		return err
	}
	triggerType := string(trigger.Reference.Kind)
	switch {
	case trigger.Reference.Value == "bootstrap":
		triggerType = "BOOTSTRAP"
	case trigger.Reference.Kind == "evm_transaction_hash":
		triggerType = "EVM TRANSACTION"
	case trigger.Reference.Kind == "solana_signature":
		triggerType = "SOLANA TRANSACTION"
	case trigger.Reference.Kind == "idle_timer":
		triggerType = "IDLE TIMER"
	default:
		triggerType = strings.ToUpper(strings.ReplaceAll(triggerType, "_", " "))
	}
	if _, err := fmt.Fprintf(
		writer, "TRIGGER: %s | %s\n  Market       %s\n",
		triggerType, trigger.At.UTC().Format("2006-01-02 15:04:05.000 UTC"),
		readableMarket(trigger.Market),
	); err != nil {
		return err
	}
	if trigger.Source != "" && trigger.Source != "research/idle" {
		if _, err := fmt.Fprintf(writer, "  Watcher      %s\n", trigger.Source); err != nil {
			return err
		}
	}
	switch trigger.Reference.Kind {
	case "evm_transaction_hash", "solana_signature":
		if trigger.Reference.Value != "bootstrap" {
			if _, err := fmt.Fprintf(writer, "  Transaction  %s\n", abbreviate(trigger.Reference.Value, 12, 10)); err != nil {
				return err
			}
		}
	case "idle_timer":
		if _, err := fmt.Fprintf(writer, "  Sequence     %s\n", trigger.Reference.Value); err != nil {
			return err
		}
	}
	if explorer := triggerExplorerURL(trigger); explorer != "" {
		if _, err := fmt.Fprintf(writer, "  Explorer     %s\n", explorer); err != nil {
			return err
		}
	}
	return nil
}

func triggerExplorerURL(trigger arbitrage.TriggerMetadata) string {
	reference := strings.TrimSpace(trigger.Reference.Value)
	if reference == "" || reference == "bootstrap" {
		return ""
	}
	switch trigger.Reference.Kind {
	case "solana_signature":
		return "https://solscan.io/tx/" + url.PathEscape(reference)
	case "evm_transaction_hash":
		chain := strings.ToLower(strings.SplitN(string(trigger.Source), "/", 2)[0])
		if chain == "polygon" {
			return "https://polygonscan.com/tx/" + url.PathEscape(reference)
		}
	}
	return ""
}

func observedAmount(quantity market.AssetQuantity, raw market.TokenAmount) (string, string) {
	human := "n/a"
	if quantity.Asset() != "" {
		human = trimNumber(quantity.Decimal(18)) + " " + string(quantity.Asset())
	}
	integer := "n/a"
	if raw.Token() != "" {
		integer = raw.String() + " " + string(raw.Token())
	}
	return human, integer
}

func quoteRole(opportunities []arbitrage.Opportunity, quote strategy.QuoteTiming) string {
	if quote.Error != "" {
		return "error"
	}
	for _, opportunity := range opportunities {
		if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
			continue
		}
		candidate := opportunity.Candidates[opportunity.SelectedIndex]
		if quote.Leg == "buy" && candidate.BuyQuote.Market == quote.Market {
			return "buy_selected"
		}
		if quote.Leg == "sell" && candidate.SellQuote.Market == quote.Market {
			return "sell_selected"
		}
	}
	if quote.Leg == "buy" {
		return "buy_candidate"
	}
	return quote.Leg
}

func trimNumber(value string) string {
	if !strings.Contains(value, ".") {
		return value
	}
	value = strings.TrimRight(value, "0")
	return strings.TrimRight(value, ".")
}

func readableQuantity(quantity market.AssetQuantity) string {
	if quantity.Asset() == "" {
		return "n/a"
	}
	return groupNumber(trimNumber(quantity.Decimal(18))) + " " + strings.ToUpper(string(quantity.Asset()))
}

func readableDuration(duration time.Duration) string {
	switch {
	case duration >= time.Millisecond:
		return fmt.Sprintf("%.2f ms", float64(duration)/float64(time.Millisecond))
	case duration >= time.Microsecond:
		return fmt.Sprintf("%.2f us", float64(duration)/float64(time.Microsecond))
	default:
		return duration.String()
	}
}

func readableMarket(id market.MarketID) string {
	value := string(id)
	lower := strings.ToLower(value)
	for _, chain := range []string{"solana", "polygon", "robinhood", "ethereum", "arbitrum", "optimism", "base"} {
		if lower != chain && !strings.HasSuffix(lower, "_"+chain) && !strings.HasSuffix(lower, "-"+chain) {
			continue
		}
		switch chain {
		case "solana":
			return "Solana"
		case "polygon":
			return "Polygon"
		case "robinhood":
			return "Robinhood"
		case "ethereum":
			return "Ethereum"
		case "arbitrum":
			return "Arbitrum"
		case "optimism":
			return "Optimism"
		case "base":
			return "Base"
		}
	}
	return value
}

func readableProvider(id market.SourceID) string {
	value := string(id)
	if separator := strings.IndexAny(value, "/_"); separator >= 0 {
		value = value[:separator]
	}
	switch strings.ToLower(value) {
	case "jupiter":
		return "Jupiter"
	case "kyberswap":
		return "KyberSwap"
	case "zerox", "0x":
		return "0x"
	case "okx":
		return "OKX"
	default:
		if value == "" {
			return "unknown"
		}
		return value
	}
}

func readableVerdict(opportunity arbitrage.Opportunity) string {
	switch opportunity.Classification {
	case arbitrage.ClassificationPolicyQualified, arbitrage.ClassificationExecutable:
		return "QUALIFIED"
	case arbitrage.ClassificationEconomic:
		return "POSITIVE, BELOW THRESHOLD"
	case arbitrage.ClassificationUnclassifiable:
		return "UNCLASSIFIABLE"
	default:
		if opportunity.SelectedIndex >= 0 &&
			opportunity.SelectedIndex < len(opportunity.Candidates) &&
			opportunity.Candidates[opportunity.SelectedIndex].NetPnL.Sign() > 0 {
			return "POSITIVE"
		}
		return "NOT PROFITABLE"
	}
}

func abbreviate(value string, head, tail int) string {
	if head < 0 || tail < 0 || len(value) <= head+tail+3 {
		return value
	}
	return value[:head] + "..." + value[len(value)-tail:]
}

func groupNumber(value string) string {
	parts := strings.SplitN(value, ".", 2)
	integer := parts[0]
	sign := ""
	if strings.HasPrefix(integer, "-") || strings.HasPrefix(integer, "+") {
		sign, integer = integer[:1], integer[1:]
	}
	for index := len(integer) - 3; index > 0; index -= 3 {
		integer = integer[:index] + "," + integer[index:]
	}
	if len(parts) == 2 {
		return sign + integer + "." + parts[1]
	}
	return sign + integer
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
	return writeJSON(writer, report, true, options)
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
	return writeJSON(writer, report, false, options)
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

func writeJSON(writer io.Writer, report Report, indent bool, options OutputOptions) error {
	if options.Calculations == CalculationSummary {
		return writeJSONSummary(writer, report, options)
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
			PriceRequired: hasCostPrice(report.Cost), PriceSource: costPriceSource(report.Cost),
		},
		Parity:    make([]parityDTO, 0, len(report.Parity)),
		Reference: make([]referenceDTO, 0, len(report.Reference)),
	}
	if hasCostPrice(report.Cost) {
		payload.Cost.PriceBase = string(report.Cost.Price.Base())
		payload.Cost.PriceQuote = string(report.Cost.Price.Quote())
		payload.Cost.PriceValue = report.Cost.Price.Value().RatString()
		payload.Cost.PriceReference = report.Cost.Price.Reference()
		payload.Cost.PriceUpdatedAt = report.Cost.Price.SourceUpdatedAt()
		payload.Cost.PriceObservedAt = report.Cost.Price.ObservedAt()
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
	Quotes          []summaryQuoteDTO       `json:"quotes"`
	Opportunities   []summaryOpportunityDTO `json:"opportunities"`
	Cost            *summaryCostDTO         `json:"cost,omitempty"`
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

type summaryQuoteDTO struct {
	Market      string `json:"market"`
	Provider    string `json:"provider"`
	Leg         string `json:"leg"`
	Role        string `json:"role"`
	Input       string `json:"input,omitempty"`
	InputAsset  string `json:"input_asset,omitempty"`
	InputRaw    string `json:"input_raw,omitempty"`
	InputToken  string `json:"input_token,omitempty"`
	Output      string `json:"output,omitempty"`
	OutputAsset string `json:"output_asset,omitempty"`
	OutputRaw   string `json:"output_raw,omitempty"`
	OutputToken string `json:"output_token,omitempty"`
	Latency     string `json:"latency"`
	Cached      bool   `json:"cached"`
	Error       string `json:"error,omitempty"`
}

type summaryCostDTO struct {
	Amount string `json:"amount"`
	Asset  string `json:"asset"`
	Source string `json:"source"`
}

func writeJSONSummary(writer io.Writer, report Report, options OutputOptions) error {
	payload := summaryPayload{
		SchemaVersion: 1, Kind: "evaluation", Evaluation: report.Research.Evaluations,
		Status: report.Research.Status, LocalDuration: report.Research.LocalTiming.Duration.String(),
		Quotes: make([]summaryQuoteDTO, 0, 3), Opportunities: make([]summaryOpportunityDTO, 0, len(report.Research.Opportunities)),
		ReferenceChecks: len(report.Reference), ParityChecks: len(report.Parity),
	}
	if !options.OmitCost {
		payload.Cost = &summaryCostDTO{
			Amount: trimNumber(report.Cost.Cost.Decimal(18)),
			Asset:  string(report.Cost.Cost.Asset()), Source: costPriceSource(report.Cost),
		}
	}
	if discovery := report.Research.LocalTiming.Discovery; discovery != nil {
		payload.Discovery = &discoverySummaryDTO{Samples: discovery.Samples, Duration: discovery.Duration.String(), Decision: discovery.Decision, BuyMarket: string(discovery.Selected.BuyMarket), SellMarket: string(discovery.Selected.SellMarket)}
	}
	for _, direction := range report.Research.LocalTiming.Directions {
		for _, quote := range direction.Quotes {
			item := summaryQuoteDTO{
				Market: string(quote.Market), Provider: string(quote.Source), Leg: quote.Leg,
				Role: quoteRole(report.Research.Opportunities, quote), Latency: quote.Duration.String(),
				Cached: quote.Cached, Error: quote.Error,
			}
			if quote.Input.Asset() != "" {
				item.Input, item.InputAsset = trimNumber(quote.Input.Decimal(18)), string(quote.Input.Asset())
			}
			if quote.AmountIn.Token() != "" {
				item.InputRaw, item.InputToken = quote.AmountIn.String(), string(quote.AmountIn.Token())
			}
			if quote.Output.Asset() != "" {
				item.Output, item.OutputAsset = trimNumber(quote.Output.Decimal(18)), string(quote.Output.Asset())
			}
			if quote.AmountOut.Token() != "" {
				item.OutputRaw, item.OutputToken = quote.AmountOut.String(), string(quote.AmountOut.Token())
			}
			payload.Quotes = append(payload.Quotes, item)
		}
	}
	for _, opportunity := range report.Research.Opportunities {
		if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
			continue
		}
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
	PriceRequired   bool      `json:"price_required"`
	PriceSource     string    `json:"price_source"`
	PriceBase       string    `json:"price_base"`
	PriceQuote      string    `json:"price_quote"`
	PriceValue      string    `json:"price_value"`
	PriceReference  string    `json:"price_reference"`
	PriceUpdatedAt  time.Time `json:"price_updated_at"`
	PriceObservedAt time.Time `json:"price_observed_at"`
}

func hasCostPrice(cost CostEvidence) bool {
	return cost.Price.Source() != ""
}

func costPriceSource(cost CostEvidence) string {
	if !hasCostPrice(cost) {
		return "not_required"
	}
	return string(cost.Price.Source())
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
