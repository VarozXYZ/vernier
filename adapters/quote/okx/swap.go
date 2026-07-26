package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DefaultSwapInstructionPath = "/api/v6/dex/aggregator/swap-instruction"

// SwapInstructionRequest contains the public inputs required by OKX to build
// Solana swap instructions. It does not contain a signer or private key.
type SwapInstructionRequest struct {
	ChainIndex          string
	FromTokenAddress    string
	ToTokenAddress      string
	Amount              string
	Slippage            string
	UserWalletAddress   string
	SwapReceiverAddress string
	DexIDs              string
	ExcludeDexIDs       string
	DirectRoute         *bool
}

// SwapInstructionResult contains unsigned instruction data and timings. The
// raw response is retained for diagnostics but is never signed or broadcast.
type SwapInstructionResult struct {
	Request                 SwapInstructionRequest
	HTTPStatus              int
	QueueDuration           time.Duration
	HTTPDuration            time.Duration
	TotalDuration           time.Duration
	ResponseBytes           int
	InstructionCount        int
	AddressLookupTableCount int
	RawResponse             []byte
}

// SwapInstruction requests read-only Solana swap instructions from OKX.
// Source's limiter is owned by the caller, allowing quote and instruction
// calls to have separate pacing when the account allowance permits it.
func (s *Source) SwapInstruction(ctx context.Context, input SwapInstructionRequest) (SwapInstructionResult, error) {
	resolved, err := resolveSwapInstruction(input, s.chain)
	if err != nil {
		return SwapInstructionResult{}, err
	}
	query := url.Values{}
	query.Set("chainIndex", resolved.ChainIndex)
	query.Set("amount", resolved.Amount)
	query.Set("fromTokenAddress", resolved.FromTokenAddress)
	query.Set("toTokenAddress", resolved.ToTokenAddress)
	query.Set("slippagePercent", resolved.Slippage)
	query.Set("userWalletAddress", resolved.UserWalletAddress)
	if resolved.SwapReceiverAddress != "" {
		query.Set("swapReceiverAddress", resolved.SwapReceiverAddress)
	}
	if resolved.DexIDs != "" {
		query.Set("dexIds", resolved.DexIDs)
	}
	if resolved.ExcludeDexIDs != "" {
		query.Set("excludeDexIds", resolved.ExcludeDexIDs)
	}
	if resolved.DirectRoute != nil {
		query.Set("directRoute", strconv.FormatBool(*resolved.DirectRoute))
	}
	transport, err := s.doGET(ctx, DefaultSwapInstructionPath, query)
	result := SwapInstructionResult{
		Request:       resolved,
		HTTPStatus:    transport.status,
		QueueDuration: transport.queueDuration,
		HTTPDuration:  transport.httpDuration,
		TotalDuration: time.Duration(transport.totalDuration),
		ResponseBytes: len(transport.body),
		RawResponse:   append([]byte(nil), transport.body...),
	}
	if err != nil {
		return result, err
	}
	var envelope struct {
		Code string          `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(transport.body, &envelope); err != nil {
		if transport.status < 200 || transport.status >= 300 {
			return result, &APIError{Operation: "swap-instruction", HTTPStatus: transport.status, Message: strings.TrimSpace(string(transport.body))}
		}
		return result, fmt.Errorf("decode OKX swap-instruction response: %w", err)
	}
	if transport.status < 200 || transport.status >= 300 || envelope.Code != "" && envelope.Code != "0" {
		return result, &APIError{Operation: "swap-instruction", Code: envelope.Code, HTTPStatus: transport.status, Message: envelope.Msg}
	}
	if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return result, &APIError{Operation: "swap-instruction", Code: "empty_data", HTTPStatus: transport.status, Message: "OKX returned no swap instructions"}
	}
	var data struct {
		InstructionLists          []json.RawMessage `json:"instructionLists"`
		AddressLookupTableAccount []string          `json:"addressLookupTableAccount"`
	}
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		return result, fmt.Errorf("decode OKX swap instructions: %w", err)
	}
	result.InstructionCount = len(data.InstructionLists)
	result.AddressLookupTableCount = len(data.AddressLookupTableAccount)
	return result, nil
}

func resolveSwapInstruction(input SwapInstructionRequest, fallbackChain string) (SwapInstructionRequest, error) {
	input.ChainIndex = strings.TrimSpace(input.ChainIndex)
	if input.ChainIndex == "" {
		input.ChainIndex = fallbackChain
	}
	input.FromTokenAddress = strings.TrimSpace(input.FromTokenAddress)
	input.ToTokenAddress = strings.TrimSpace(input.ToTokenAddress)
	input.Amount = strings.TrimSpace(input.Amount)
	input.Slippage = strings.TrimSpace(input.Slippage)
	input.UserWalletAddress = strings.TrimSpace(input.UserWalletAddress)
	if input.ChainIndex == "" || input.FromTokenAddress == "" || input.ToTokenAddress == "" || input.FromTokenAddress == input.ToTokenAddress || input.Amount == "" || input.Slippage == "" || input.UserWalletAddress == "" {
		return SwapInstructionRequest{}, fmt.Errorf("OKX swap instructions require chain, distinct tokens, amount, slippage, and wallet")
	}
	amount, ok := new(big.Int).SetString(input.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		return SwapInstructionRequest{}, fmt.Errorf("OKX swap instruction amount must be a positive integer in minimum units")
	}
	return input, nil
}
