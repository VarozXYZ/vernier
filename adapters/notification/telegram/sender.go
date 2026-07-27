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
	endpoint string
	chatID   string
	client   Client
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
		endpoint: strings.TrimRight(base.String(), "/") + "/bot" + token + "/sendMessage",
		chatID:   strings.TrimSpace(config.ChatID), client: config.Client,
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
	return s.send(ctx, strings.Join(lines, "\n"))
}

func (s *Sender) SendConfigurationWarning(ctx context.Context, warning notificationport.ConfigurationWarning) error {
	router := warning.Details["router"]
	if router == "" {
		router = "not reported"
	}
	return s.send(ctx, strings.Join([]string{
		"⚠️ <b>CONFIG · JUPITER</b>",
		"📍 " + escape(warning.Market) + " · " + escape(warning.Provider),
		"⚙️ " + escape(warning.Expected) + " → " + escape(warning.Observed) + " · " + escape(router),
		"✅ Quote aceptado",
	}, "\n"))
}

func (s *Sender) send(ctx context.Context, text string) error {
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
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	if response == nil || response.Body == nil {
		return fmt.Errorf("telegram returned an empty response")
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if readErr != nil {
		return readErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("telegram sendMessage failed with HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	var result struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(responseBody, &result); err != nil || !result.OK {
		return fmt.Errorf("telegram sendMessage returned an unsuccessful response")
	}
	return nil
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
	fields := strings.Fields(strings.TrimSpace(value))
	if len(fields) == 0 {
		return ""
	}
	number := fields[0]
	if parsed, ok := new(big.Rat).SetString(number); ok {
		number = strings.TrimRight(strings.TrimRight(parsed.FloatString(6), "0"), ".")
	}
	number = groupThousands(number)
	if len(fields) == 1 {
		return number
	}
	return number + " " + strings.Join(fields[1:], " ")
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
