// Package telegram sends direct, best-effort Research notifications through
// Telegram's Bot API. It does not retry or persist messages.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	notificationport "github.com/VarozXYZ/vernier/ports/notification"
)

const defaultBaseURL = "https://api.telegram.org"

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

type Config struct {
	BotToken string
	ChatID   string
	BaseURL  string
	Client   Client
}

type Sender struct {
	sendEndpoint string
	editEndpoint string
	chatID       string
	client       Client

	liveMu       sync.Mutex
	liveMessages map[string]*liveMessageState
}

func New(config Config) (*Sender, error) {
	token := strings.TrimSpace(config.BotToken)
	if token == "" || strings.TrimSpace(config.ChatID) == "" {
		return nil, fmt.Errorf("telegram bot token and chat ID are required")
	}
	if config.BaseURL == "" {
		config.BaseURL = defaultBaseURL
	}
	base, err := url.Parse(strings.TrimRight(config.BaseURL, "/"))
	if err != nil || base.Scheme != "https" || base.Host == "" {
		return nil, fmt.Errorf("invalid Telegram base URL")
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Sender{
		sendEndpoint: strings.TrimRight(base.String(), "/") + "/bot" + token + "/sendMessage",
		editEndpoint: strings.TrimRight(base.String(), "/") + "/bot" + token + "/editMessageText",
		chatID:       strings.TrimSpace(config.ChatID), client: config.Client,
		liveMessages: make(map[string]*liveMessageState),
	}, nil
}

func (s *Sender) SendOpening(ctx context.Context, alert notificationport.OpportunityOpening) error {
	totalLatency := alert.BuyLatency + alert.SellLatency
	baseBought := "<b>" + escape(compactAmount(alert.BaseBought)) + "</b>"
	if alert.TriggerURL != "" {
		baseBought = `<a href="` + escape(alert.TriggerURL) + `">` + baseBought + `</a>`
	}
	lines := []string{
		"🎯 <b>ARB · " + escape(signedAmount(alert.NetPnL)) + " net</b>",
		"📍 " + escape(strings.ReplaceAll(alert.Direction, "->", "→")),
		"💱 <b>" + escape(compactAmount(alert.Input)) + "</b> → " +
			baseBought + " → <b>" +
			escape(compactAmount(alert.SellOutput)) + "</b>",
		"🔌 " + escape(alert.BuyProvider) + " → " + escape(alert.SellProvider),
		"⚡ " + compactDuration(totalLatency) + " · compra " + compactDuration(alert.BuyLatency) +
			" · venta " + compactDuration(alert.SellLatency),
	}
	if alert.TriggerURL == "" {
		lines = append(lines, triggerLine(alert.Trigger))
	}
	_, err := s.send(ctx, strings.Join(lines, "\n"))
	return err
}

func (s *Sender) SendConfigurationWarning(ctx context.Context, warning notificationport.ConfigurationWarning) error {
	router := warning.Details["router"]
	if router == "" {
		router = "not reported"
	}
	_, err := s.send(ctx, strings.Join([]string{
		"⚠️ <b>CONFIG · JUPITER</b>",
		"📍 " + escape(warning.Market) + " · " + escape(warning.Provider),
		"⚙️ " + escape(warning.Expected) + " → " + escape(warning.Observed) + " · " + escape(router),
		"✅ Quote aceptado",
	}, "\n"))
	return err
}

func (s *Sender) SendLiveRuntime(
	ctx context.Context,
	event notificationport.LiveRuntimeEvent,
) error {
	mode := strings.TrimSpace(event.Mode)
	if mode == "" {
		mode = "live"
	}
	occurredAt := event.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	lines := []string{
		"\U0001f7e2 <b>LIVE \u00b7 STARTED</b>",
		"\U0001f4cc Mode: <b>" + escape(mode) + "</b>",
		"\u23f1\ufe0f " + escape(occurredAt.UTC().Format("2006-01-02 15:04:05 UTC")),
	}
	if event.Kind == notificationport.LiveRuntimeStopped {
		reason := strings.TrimSpace(event.Reason)
		if reason == "" {
			reason = "stopped"
		}
		lines = []string{
			"\U0001f6d1 <b>LIVE \u00b7 STOPPED</b>",
			"\U0001f4cc Mode: <b>" + escape(mode) + "</b>",
			"\U0001f9ed Reason: " + escape(reason),
			"\u23f3 Uptime: " + escape(compactDuration(event.Uptime)),
			"\u23f1\ufe0f " + escape(occurredAt.UTC().Format("2006-01-02 15:04:05 UTC")),
		}
	}
	_, err := s.send(ctx, strings.Join(lines, "\n"))
	return err
}

func (s *Sender) SendLiveExecution(
	ctx context.Context,
	event notificationport.LiveExecutionEvent,
) error {
	if strings.TrimSpace(event.Operation) == "" {
		return fmt.Errorf("live Telegram event requires an operation")
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	state, ok := s.liveMessages[event.Operation]
	if !ok {
		state = newLiveMessageState(event.Operation)
		s.liveMessages[event.Operation] = state
	}
	state.apply(event)
	text := strings.Join(liveSummaryLines(state), "\n")
	if state.messageID == 0 {
		messageID, err := s.send(ctx, text)
		if err != nil {
			return err
		}
		state.messageID = messageID
		return nil
	}
	return s.edit(ctx, state.messageID, text)
}

type liveMessageState struct {
	operation string
	messageID int64
	started   *notificationport.LiveExecutionEvent
	starting  map[int]notificationport.LiveExecutionEvent
	completed map[int]notificationport.LiveExecutionEvent
	exit      *notificationport.LiveExecutionEvent
	recovery  *notificationport.LiveExecutionEvent
	terminal  *notificationport.LiveExecutionEvent
}

func newLiveMessageState(operation string) *liveMessageState {
	return &liveMessageState{
		operation: operation,
		starting:  make(map[int]notificationport.LiveExecutionEvent, 4),
		completed: make(map[int]notificationport.LiveExecutionEvent, 4),
	}
}

func (s *liveMessageState) apply(event notificationport.LiveExecutionEvent) {
	copyOf := event
	switch event.Kind {
	case notificationport.LiveExecutionStarted:
		s.started = &copyOf
	case notificationport.LiveExecutionStageStarted:
		s.starting[event.Ordinal] = event
	case notificationport.LiveExecutionStageCompleted:
		s.completed[event.Ordinal] = event
		delete(s.starting, event.Ordinal)
	case notificationport.LiveExecutionExitSelected:
		s.exit = &copyOf
	case notificationport.LiveExecutionRecoveryStarted,
		notificationport.LiveExecutionRecoveryProgress,
		notificationport.LiveExecutionRecoveryCompleted,
		notificationport.LiveExecutionRecoveryBlocked:
		s.recovery = &copyOf
		if event.Kind == notificationport.LiveExecutionRecoveryBlocked {
			s.terminal = &copyOf
		} else {
			// Recovery supersedes the original failed terminal state while
			// retaining the already-rendered stage evidence.
			s.terminal = nil
		}
	case notificationport.LiveExecutionRefuelCompleted,
		notificationport.LiveExecutionRefuelFailed,
		notificationport.LiveExecutionRefuelUncertain:
		s.terminal = &copyOf
	case notificationport.LiveExecutionCompleted, notificationport.LiveExecutionFailed:
		s.terminal = &copyOf
	}
}

func liveSummaryLines(state *liveMessageState) []string {
	title := "\U0001f7e1 <b>LIVE · RUNNING</b>"
	forcedCanary := state.started != nil &&
		state.started.State == "forced_canary"
	if forcedCanary {
		title = "\U0001f9ea <b>CANARY · FORCED</b>"
	}
	if state.terminal != nil {
		switch state.terminal.Kind {
		case notificationport.LiveExecutionCompleted:
			title = "\U0001f3c1 <b>LIVE · COMPLETE</b>"
			if forcedCanary {
				title = "\U0001f3c1 <b>CANARY · COMPLETE</b>"
			}
		case notificationport.LiveExecutionFailed:
			title = "\U0001f6a8 <b>LIVE · MANUAL ACTION</b>"
			if forcedCanary {
				title = "\U0001f6a8 <b>CANARY · MANUAL ACTION</b>"
			}
			if state.terminal.State == "aborted" ||
				state.terminal.State == "aborted_retrying" {
				title = "\u26d4 <b>LIVE · ABORTED</b>"
				if forcedCanary {
					title = "\u26d4 <b>CANARY · ABORTED</b>"
				}
			}
		case notificationport.LiveExecutionRecoveryBlocked:
			title = "\U0001f6d1 <b>LIVE · RECOVERY BLOCKED</b>"
		case notificationport.LiveExecutionRefuelCompleted:
			title = "\u26fd <b>GAS · REFUELED</b>"
		case notificationport.LiveExecutionRefuelFailed:
			title = "\u26a0\ufe0f <b>GAS · REFUEL FAILED</b>"
		case notificationport.LiveExecutionRefuelUncertain:
			title = "\U0001f6d1 <b>GAS · OUTCOME UNKNOWN</b>"
		}
	} else if state.recovery != nil {
		switch state.recovery.Kind {
		case notificationport.LiveExecutionRecoveryCompleted:
			title = "\u2705 <b>LIVE · RECOVERED</b>"
		default:
			title = "\U0001f6e1\ufe0f <b>LIVE · RECOVERING</b>"
		}
	}
	lines := []string{title}
	if state.started != nil {
		event := *state.started
		lines = append(lines,
			"\U0001f4cd "+escape(strings.ReplaceAll(event.Direction, "->", "\u2192")),
		)
		expected := compactLines(
			escape(compactAmount(event.Input)),
			escape(compactAmount(event.ExpectedBase)),
			escape(compactAmount(event.ExpectedOutput)),
		)
		if len(expected) > 0 {
			lines = append(
				lines,
				"\U0001f3af <b>"+strings.Join(expected, " \u2192 ")+"</b>",
			)
		}
		providers := compactLines(
			escape(event.BuyProvider),
			escape(event.SellProvider),
		)
		providerLine := ""
		if len(providers) > 0 {
			providerLine = "\U0001f50c " + strings.Join(providers, " \u2192 ")
		}
		if event.ExpectedNetPnL != "" {
			providerLine += " · expected <b>" +
				escape(signedAmount(event.ExpectedNetPnL)) + "</b>"
		}
		if providerLine != "" {
			lines = append(lines, providerLine)
		}
		if trigger := liveTriggerLine(event); trigger != "" {
			lines = append(lines, trigger)
		}
	}
	for ordinal := 1; ordinal <= 4; ordinal++ {
		if event, ok := state.completed[ordinal]; ok {
			lines = append(lines, completedStageLines(event)...)
			continue
		}
		if event, ok := state.starting[ordinal]; ok {
			lines = append(lines,
				"\u23f3 <b>"+escape(stageHeading(event))+"</b> · "+
					escape(chainPath(event))+" · "+
					escape(compactLiveAmount(event.Input)),
			)
		}
	}
	if state.exit != nil &&
		(state.terminal == nil ||
			state.terminal.Kind != notificationport.LiveExecutionCompleted ||
			strings.Contains(state.exit.Evidence, "automatic_recovery")) {
		lines = append(lines, exitDecisionLine(*state.exit))
	}
	if state.recovery != nil {
		switch state.recovery.Kind {
		case notificationport.LiveExecutionRecoveryStarted:
			lines = append(
				lines,
				"\U0001f504 Recovery started · "+
					escape(compactDetail(state.recovery.Detail)),
			)
		case notificationport.LiveExecutionRecoveryProgress:
			lines = append(
				lines,
				"\u23f3 Recovery · "+
					escape(compactDetail(state.recovery.Detail)),
			)
		case notificationport.LiveExecutionRecoveryCompleted:
			lines = append(lines, "\u2705 Recovery completed")
		case notificationport.LiveExecutionRecoveryBlocked:
			lines = append(
				lines,
				"\U0001f6d1 "+escape(compactDetail(state.recovery.Detail)),
			)
		}
	}
	if state.terminal != nil {
		lines = append(lines, terminalLines(*state.terminal)...)
	}
	if state.terminal == nil ||
		state.terminal.Kind == notificationport.LiveExecutionFailed {
		lines = append(
			lines,
			"\U0001f194 <code>"+escape(shortIdentity(state.operation))+"</code>",
		)
	}
	return compactLines(lines...)
}

func liveTriggerLine(event notificationport.LiveExecutionEvent) string {
	if event.Trigger == "" && event.TriggerURL == "" {
		return ""
	}
	lower := strings.ToLower(event.Trigger)
	if strings.Contains(lower, "bootstrap") {
		return ""
	}
	if strings.Contains(lower, "idle") {
		return "\u23f1\ufe0f Timer"
	}
	if event.TriggerURL != "" {
		return "\U0001f517 <a href=\"" + escape(event.TriggerURL) + `">` +
			"Trigger transaction</a>"
	}
	return "\u2699\ufe0f " + escape(compactDetail(event.Trigger))
}

func completedStageLines(event notificationport.LiveExecutionEvent) []string {
	heading := "\u2705 <b>" + escape(stageHeading(event)) + "</b> · " +
		escape(chainPath(event))
	result := []string{heading}
	switch event.Stage {
	case "bridge_base":
		result[0] += " · " + compactDuration(event.Duration)
	case "bridge_quote_return":
		result[0] += " · " + compactDuration(event.Duration)
		if output := compactLiveAmount(event.Output); output != "" {
			result = append(
				result,
				"   \U0001f4b1 <b>"+escape(output)+"</b> received",
			)
		}
	default:
		result = append(
			result,
			"   \U0001f4b1 "+escape(compactLiveAmount(event.Input))+
				" \u2192 <b>"+escape(compactLiveAmount(event.Output))+"</b> · "+
				compactDuration(event.Duration),
		)
	}
	if transaction := transactionLine(event); transaction != "" {
		result = append(result, "   "+transaction)
	}
	return result
}

func exitDecisionLine(event notificationport.LiveExecutionEvent) string {
	decision := "Sell at destination"
	if event.Stage == "return_to_origin" {
		decision = "Return to origin"
	}
	prefix := "\U0001f500 "
	if strings.Contains(event.Evidence, "automatic_recovery") {
		prefix = "\U0001f6e1\ufe0f <b>RECOVERY</b> \u00b7 "
	}
	values := compactLines(
		escape(compactLiveAmount(event.DestinationValue)),
		escape(compactLiveAmount(event.ReturnValue)),
	)
	if len(values) == 0 {
		return prefix + "<b>" + escape(decision) + "</b>"
	}
	return prefix + "<b>" + escape(decision) + "</b> · " +
		strings.Join(values, " vs ")
}

func terminalLines(event notificationport.LiveExecutionEvent) []string {
	if event.Kind == notificationport.LiveExecutionRefuelCompleted {
		return compactLines(
			"\U0001f4cd "+escape(event.SourceChain),
			"\U0001f4b1 "+escape(compactLiveAmount(event.Input))+
				" \u2192 <b>"+escape(compactLiveAmount(event.Output))+"</b>",
			"\U0001f4b8 Fee · "+escape(compactLiveMoney(event.ExecutionCost)),
		)
	}
	if event.Kind == notificationport.LiveExecutionFailed ||
		event.Kind == notificationport.LiveExecutionRecoveryBlocked ||
		event.Kind == notificationport.LiveExecutionRefuelFailed ||
		event.Kind == notificationport.LiveExecutionRefuelUncertain {
		return compactLines(
			"\U0001f4cd "+escape(stageHeading(event))+" · "+
				escape(chainPath(event)),
			"\u26a0\ufe0f "+escape(compactDetail(event.Detail)),
			"\u26a1 "+compactDuration(event.Duration),
		)
	}
	result := []string{"\U0001f4ca <b>RESULT</b>"}
	if output := compactLiveAmount(event.Output); output != "" {
		result = append(
			result,
			"   \U0001f4b0 Return   <b>"+escape(output)+"</b>",
		)
	}
	if cost := compactLiveMoney(event.ExecutionCost); cost != "" {
		result = append(result, "   \U0001f4b8 Costs    "+escape(cost))
	}
	if pnl := compactLiveMoney(event.NetPnL); pnl != "" {
		icon := "\U0001f4c8"
		if strings.HasPrefix(strings.TrimSpace(pnl), "-") {
			icon = "\U0001f4c9"
		}
		result = append(result, "   "+icon+" Net PnL  <b>"+escape(pnl)+"</b>")
	}
	result = append(result, "\u23f1\ufe0f Total · "+compactDuration(event.Duration))
	return compactLines(result...)
}

func compactLines(lines ...string) []string {
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			result = append(result, line)
		}
	}
	return result
}

func stageHeading(event notificationport.LiveExecutionEvent) string {
	label := friendlyStageLabel(event)
	if event.Ordinal > 0 && event.TotalStages > 0 {
		return fmt.Sprintf("%d/%d \u00b7 %s", event.Ordinal, event.TotalStages, label)
	}
	return label
}

func friendlyStageLabel(event notificationport.LiveExecutionEvent) string {
	switch event.Stage {
	case "bridge_base":
		if asset := amountAsset(event.Input); asset != "" {
			return "BRIDGE " + asset
		}
		return "BRIDGE"
	case "bridge_quote_return":
		if asset := amountAsset(event.Output); asset != "" {
			return "RETURN " + asset
		}
		return "RETURN"
	default:
		return strings.ToUpper(strings.ReplaceAll(event.Stage, "_", " "))
	}
}

func chainPath(event notificationport.LiveExecutionEvent) string {
	if event.DestinationChain == "" {
		return event.SourceChain
	}
	return event.SourceChain + " \u2192 " + event.DestinationChain
}

func transactionLine(event notificationport.LiveExecutionEvent) string {
	sourceLabel := "Transaction"
	destinationLabel := "Receipt"
	switch event.Stage {
	case "buy", "sell":
		sourceLabel = "Swap on " + event.SourceChain
	case "bridge_base", "bridge_quote_return":
		sourceLabel = "Departure from " + event.SourceChain
		destinationLabel = "Receipt on " + event.DestinationChain
	}
	source := transactionLink(event.SourceTransaction, event.SourceURL, sourceLabel)
	destination := transactionLink(
		event.DestinationTx, event.DestinationURL, destinationLabel,
	)
	switch {
	case source != "" && destination != "":
		return "\U0001f517 " + source + " \u2192 " + destination
	case source != "":
		return "\U0001f517 " + source
	default:
		return ""
	}
}

func transactionLink(identity, target, label string) string {
	if strings.TrimSpace(identity) == "" {
		return ""
	}
	if strings.TrimSpace(target) == "" {
		return "<code>" + escape(shortIdentity(identity)) + "</code>"
	}
	return `<a href="` + escape(target) + `">` + escape(label) + `</a>`
}

func shortIdentity(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 16 {
		return value
	}
	return value[:8] + "\u2026" + value[len(value)-6:]
}

func compactDetail(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	const maximum = 180
	if len(value) > maximum {
		return value[:maximum] + "\u2026"
	}
	return value
}

type telegramResponse struct {
	OK     bool `json:"ok"`
	Result struct {
		MessageID int64 `json:"message_id"`
	} `json:"result"`
}

func (s *Sender) send(ctx context.Context, text string) (int64, error) {
	payload := struct {
		ChatID                string `json:"chat_id"`
		Text                  string `json:"text"`
		ParseMode             string `json:"parse_mode"`
		DisableWebPagePreview bool   `json:"disable_web_page_preview"`
	}{
		ChatID: s.chatID, Text: text, ParseMode: "HTML",
		DisableWebPagePreview: true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	result, err := s.post(ctx, s.sendEndpoint, "sendMessage", body)
	if err != nil {
		return 0, err
	}
	if result.Result.MessageID <= 0 {
		return 0, fmt.Errorf("telegram sendMessage response omitted message_id")
	}
	return result.Result.MessageID, nil
}

func (s *Sender) edit(ctx context.Context, messageID int64, text string) error {
	payload := struct {
		ChatID                string `json:"chat_id"`
		MessageID             int64  `json:"message_id"`
		Text                  string `json:"text"`
		ParseMode             string `json:"parse_mode"`
		DisableWebPagePreview bool   `json:"disable_web_page_preview"`
	}{
		ChatID: s.chatID, MessageID: messageID, Text: text, ParseMode: "HTML",
		DisableWebPagePreview: true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.post(ctx, s.editEndpoint, "editMessageText", body)
	return err
}

func (s *Sender) post(
	ctx context.Context,
	endpoint string,
	operation string,
	body []byte,
) (telegramResponse, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return telegramResponse{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return telegramResponse{}, fmt.Errorf(
			"telegram %s request failed: %s",
			operation,
			redactBotCredential(err.Error()),
		)
	}
	if response == nil || response.Body == nil {
		return telegramResponse{}, fmt.Errorf("telegram returned an empty response")
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if readErr != nil {
		return telegramResponse{}, readErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if operation == "editMessageText" &&
			strings.Contains(strings.ToLower(string(responseBody)), "message is not modified") {
			return telegramResponse{OK: true}, nil
		}
		return telegramResponse{}, fmt.Errorf(
			"telegram %s failed with HTTP %d: %s",
			operation,
			response.StatusCode,
			redactBotCredential(strings.TrimSpace(string(responseBody))),
		)
	}
	var result telegramResponse
	if err := json.Unmarshal(responseBody, &result); err != nil || !result.OK {
		return telegramResponse{}, fmt.Errorf(
			"telegram %s returned an unsuccessful response",
			operation,
		)
	}
	return result, nil
}

func redactBotCredential(value string) string {
	var redacted strings.Builder
	for {
		start := strings.Index(value, "/bot")
		if start < 0 {
			redacted.WriteString(value)
			return redacted.String()
		}
		tokenStart := start + len("/bot")
		remainder := value[tokenStart:]
		if strings.HasPrefix(remainder, "[REDACTED]") {
			redacted.WriteString(value[:tokenStart+len("[REDACTED]")])
			value = remainder[len("[REDACTED]"):]
			continue
		}
		endOffset := strings.IndexByte(remainder, '/')
		redacted.WriteString(value[:tokenStart])
		redacted.WriteString("[REDACTED]")
		if endOffset < 0 {
			return redacted.String()
		}
		value = remainder[endOffset:]
	}
}

func triggerLine(trigger string) string {
	lower := strings.ToLower(trigger)
	switch {
	case strings.Contains(lower, "bootstrap"):
		return "🚀 Trigger · bootstrap"
	case strings.Contains(lower, "idle"):
		return "⏱ Trigger · timer"
	case strings.TrimSpace(trigger) == "":
		return "⚙️ Trigger · unknown"
	default:
		return "⚙️ Trigger · <code>" + escape(trigger) + "</code>"
	}
}

func compactAmount(value string) string {
	return compactAmountPrecision(value, 6)
}

func compactLiveAmount(value string) string {
	return compactAmountPrecision(value, 3)
}

func compactLiveMoney(value string) string {
	return compactAmountPrecision(value, 4)
}

func compactAmountPrecision(value string, decimals int) string {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) == 0 {
		return ""
	}
	number := fields[0]
	if parsed, ok := new(big.Rat).SetString(number); ok {
		number = strings.TrimRight(
			strings.TrimRight(parsed.FloatString(decimals), "0"),
			".",
		)
	}
	number = groupThousands(number)
	if len(fields) == 1 {
		return number
	}
	return number + " " + strings.Join(fields[1:], " ")
}

func amountAsset(value string) string {
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) < 2 {
		return ""
	}
	return strings.Join(fields[1:], " ")
}

func signedAmount(value string) string {
	value = compactAmount(value)
	if value == "" || strings.HasPrefix(value, "-") || strings.HasPrefix(value, "+") ||
		strings.HasPrefix(value, "0 ") || value == "0" {
		return value
	}
	return "+" + value
}

func groupThousands(value string) string {
	sign := ""
	if strings.HasPrefix(value, "-") || strings.HasPrefix(value, "+") {
		sign, value = value[:1], value[1:]
	}
	parts := strings.SplitN(value, ".", 2)
	integer := parts[0]
	for index := len(integer) - 3; index > 0; index -= 3 {
		integer = integer[:index] + "," + integer[index:]
	}
	if len(parts) == 2 {
		return sign + integer + "." + parts[1]
	}
	return sign + integer
}

func compactDuration(duration time.Duration) string {
	switch {
	case duration >= time.Second:
		return fmt.Sprintf("%.3f s", duration.Seconds())
	case duration >= time.Millisecond:
		return fmt.Sprintf("%.0f ms", float64(duration)/float64(time.Millisecond))
	case duration >= time.Microsecond:
		return fmt.Sprintf("%.0f µs", float64(duration)/float64(time.Microsecond))
	default:
		return duration.String()
	}
}

func escape(value string) string { return html.EscapeString(value) }

var _ notificationport.OpeningSender = (*Sender)(nil)
var _ notificationport.ConfigurationWarningSender = (*Sender)(nil)
var _ notificationport.LiveExecutionSender = (*Sender)(nil)
var _ notificationport.LiveRuntimeSender = (*Sender)(nil)
