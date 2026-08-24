package wormholentt

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
)

const (
	vaaHeaderSize     = 6
	vaaSignatureSize  = 66
	vaaBodyHeaderSize = 51
	// A transfer contains the transceiver prefix, source and destination
	// managers, a minimally sized manager message, and a zero-length
	// transceiver payload.
	wormholeTransferSize = 4 + 32 + 32 + 2 + (32 + 32 + 2 + 79) + 2
)

var wormholeTransferPrefix = [4]byte{0x99, 0x45, 0xff, 0x10}

type GuardianSignature struct {
	Index     uint8
	Signature [65]byte
}

type VAA struct {
	Version          uint8
	GuardianSetIndex uint32
	Signatures       []GuardianSignature
	Timestamp        uint32
	Nonce            uint32
	Message          crosschainport.MessageID
	ConsistencyLevel uint8
	Payload          []byte
	Body             []byte
	Hash             [32]byte
	Raw              []byte
}

type NTTMessage struct {
	ID                    [32]byte
	Sender                [32]byte
	SourceManager         [32]byte
	DestinationManager    [32]byte
	ManagerPayload        []byte
	SourceToken           [32]byte
	Recipient             [32]byte
	DestinationChain      uint16
	TrimmedAmount         *big.Int
	TrimmedAmountDecimals uint8
}

func ParseVAA(raw []byte) (VAA, error) {
	if len(raw) < vaaHeaderSize {
		return VAA{}, fmt.Errorf("VAA is shorter than its header")
	}
	signatureCount := int(raw[5])
	bodyOffset := vaaHeaderSize + signatureCount*vaaSignatureSize
	if signatureCount == 0 || len(raw) < bodyOffset+vaaBodyHeaderSize {
		return VAA{}, fmt.Errorf("VAA has an invalid signature or body length")
	}
	result := VAA{
		Version:          raw[0],
		GuardianSetIndex: binary.BigEndian.Uint32(raw[1:5]),
		Signatures:       make([]GuardianSignature, signatureCount),
		Raw:              append([]byte(nil), raw...),
		Body:             append([]byte(nil), raw[bodyOffset:]...),
	}
	if result.Version != 1 {
		return VAA{}, fmt.Errorf("unsupported VAA version %d", result.Version)
	}
	offset := vaaHeaderSize
	for index := range result.Signatures {
		result.Signatures[index].Index = raw[offset]
		copy(result.Signatures[index].Signature[:], raw[offset+1:offset+vaaSignatureSize])
		offset += vaaSignatureSize
	}
	body := result.Body
	result.Timestamp = binary.BigEndian.Uint32(body[0:4])
	result.Nonce = binary.BigEndian.Uint32(body[4:8])
	result.Message.EmitterChain = binary.BigEndian.Uint16(body[8:10])
	copy(result.Message.EmitterAddress[:], body[10:42])
	result.Message.Sequence = binary.BigEndian.Uint64(body[42:50])
	result.ConsistencyLevel = body[50]
	result.Payload = append([]byte(nil), body[51:]...)
	// Wormhole's Solana verifier receives keccak256(body). The native
	// Secp256k1 program hashes those 32 bytes once more before recovering each
	// guardian key, matching the digest signed by guardians. The same
	// single-hash value derives the PostedVAA PDA.
	copy(result.Hash[:], crypto.Keccak256(body))
	return result, nil
}

func (v VAA) ValidateMessage(expected crosschainport.MessageID) error {
	if v.Message != expected {
		return fmt.Errorf(
			"VAA message mismatch: received %d/%s/%d",
			v.Message.EmitterChain,
			hex.EncodeToString(v.Message.EmitterAddress[:]),
			v.Message.Sequence,
		)
	}
	return nil
}

func (v VAA) NTTMessage() (NTTMessage, error) {
	payload := v.Payload
	if len(payload) < wormholeTransferSize || !equal4(payload[:4], wormholeTransferPrefix) {
		return NTTMessage{}, fmt.Errorf("VAA payload is not a Wormhole NTT transfer")
	}
	result := NTTMessage{}
	copy(result.SourceManager[:], payload[4:36])
	copy(result.DestinationManager[:], payload[36:68])
	managerLength := int(binary.BigEndian.Uint16(payload[68:70]))
	if managerLength < 66 || len(payload) < 70+managerLength+2 {
		return NTTMessage{}, fmt.Errorf("VAA NTT manager payload has an invalid length")
	}
	manager := payload[70 : 70+managerLength]
	result.ManagerPayload = append([]byte(nil), manager...)
	copy(result.ID[:], manager[:32])
	copy(result.Sender[:], manager[32:64])
	nativeLength := int(binary.BigEndian.Uint16(manager[64:66]))
	if nativeLength < 79 || len(manager) != 66+nativeLength {
		return NTTMessage{}, fmt.Errorf("VAA native-token payload has an invalid length")
	}
	native := manager[66:]
	if !equal4(native[:4], [4]byte{0x99, 0x4e, 0x54, 0x54}) {
		return NTTMessage{}, fmt.Errorf("VAA manager payload is not a native-token transfer")
	}
	result.TrimmedAmountDecimals = native[4]
	result.TrimmedAmount = new(big.Int).SetBytes(native[5:13])
	copy(result.SourceToken[:], native[13:45])
	copy(result.Recipient[:], native[45:77])
	result.DestinationChain = binary.BigEndian.Uint16(native[77:79])
	if result.TrimmedAmount.Sign() <= 0 {
		return NTTMessage{}, fmt.Errorf("VAA transfer amount is not positive")
	}
	transceiverLengthOffset := 70 + managerLength
	transceiverLength := int(binary.BigEndian.Uint16(payload[transceiverLengthOffset : transceiverLengthOffset+2]))
	if len(payload) != transceiverLengthOffset+2+transceiverLength {
		return NTTMessage{}, fmt.Errorf("VAA transceiver payload has an invalid length")
	}
	return result, nil
}

func (m NTTMessage) Digest(sourceChain uint16) [32]byte {
	encoded := make([]byte, 2+len(m.ManagerPayload))
	binary.BigEndian.PutUint16(encoded[:2], sourceChain)
	copy(encoded[2:], m.ManagerPayload)
	var result [32]byte
	copy(result[:], crypto.Keccak256(encoded))
	return result
}

func equal4(value []byte, expected [4]byte) bool {
	return len(value) >= 4 &&
		value[0] == expected[0] && value[1] == expected[1] &&
		value[2] == expected[2] && value[3] == expected[3]
}

type GuardianClientConfig struct {
	Endpoints    []string
	Client       *http.Client
	PollInterval time.Duration
	Clock        func() time.Time
}

type GuardianClient struct {
	endpoints    []*url.URL
	client       *http.Client
	pollInterval time.Duration
	clock        func() time.Time
	next         atomic.Uint64
}

func NewGuardianClient(config GuardianClientConfig) (*GuardianClient, error) {
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("at least one Guardian RPC endpoint is required")
	}
	endpoints := make([]*url.URL, 0, len(config.Endpoints))
	for _, endpoint := range config.Endpoints {
		parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(endpoint), "/"))
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("invalid Guardian RPC endpoint")
		}
		endpoints = append(endpoints, parsed)
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 3 * time.Second}
	}
	if config.PollInterval == 0 {
		config.PollInterval = 200 * time.Millisecond
	}
	if config.PollInterval < 25*time.Millisecond {
		return nil, fmt.Errorf("guardian RPC poll interval is too small")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &GuardianClient{
		endpoints: endpoints, client: config.Client,
		pollInterval: config.PollInterval, clock: config.Clock,
	}, nil
}

func (c *GuardianClient) Await(
	ctx context.Context,
	message crosschainport.MessageID,
) (crosschainport.Attestation, error) {
	if message.EmitterChain == 0 || message.EmitterAddress == ([32]byte{}) {
		return crosschainport.Attestation{}, fmt.Errorf("wormhole message identity is incomplete")
	}
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		raw, found, err := c.fetchRound(ctx, message)
		if err != nil {
			lastErr = err
		}
		if found {
			parsed, parseErr := ParseVAA(raw)
			if parseErr != nil {
				return crosschainport.Attestation{}, parseErr
			}
			if err := parsed.ValidateMessage(message); err != nil {
				return crosschainport.Attestation{}, err
			}
			return crosschainport.Attestation{
				Message: message, Raw: raw, ObservedAt: c.clock().UTC(),
			}, nil
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return crosschainport.Attestation{}, errors.Join(ctx.Err(), lastErr)
			}
			return crosschainport.Attestation{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

type guardianResult struct {
	raw   []byte
	found bool
	err   error
}

func (c *GuardianClient) fetchRound(
	ctx context.Context,
	message crosschainport.MessageID,
) ([]byte, bool, error) {
	results := make(chan guardianResult, len(c.endpoints))
	start := int(c.next.Add(1)-1) % len(c.endpoints)
	for index := range c.endpoints {
		endpoint := c.endpoints[(start+index)%len(c.endpoints)]
		go func() {
			raw, found, err := c.fetch(ctx, endpoint, message)
			results <- guardianResult{raw: raw, found: found, err: err}
		}()
	}
	var failures []error
	for range c.endpoints {
		result := <-results
		if result.found {
			return result.raw, true, nil
		}
		if result.err != nil {
			failures = append(failures, result.err)
		}
	}
	return nil, false, errors.Join(failures...)
}

func (c *GuardianClient) fetch(
	ctx context.Context,
	base *url.URL,
	message crosschainport.MessageID,
) ([]byte, bool, error) {
	requestURL := *base
	wormholeScan := strings.HasSuffix(strings.TrimRight(base.Path, "/"), "/api/v1/vaas")
	path := "/v1/signed_vaa"
	if wormholeScan {
		path = ""
	}
	requestURL.Path = strings.TrimRight(base.Path, "/") + path + fmt.Sprintf("/%d/%s/%d",
		message.EmitterChain, hex.EncodeToString(message.EmitterAddress[:]), message.Sequence)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return nil, false, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, false, fmt.Errorf("guardian RPC %s: %w", base.Host, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, false, fmt.Errorf("guardian RPC %s: %w", base.Host, err)
	}
	if response.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, false, fmt.Errorf("guardian RPC %s returned HTTP %s", base.Host, response.Status)
	}
	var envelope struct {
		VAABytes string          `json:"vaaBytes"`
		Data     json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, false, fmt.Errorf("decode Guardian RPC %s response: %w", base.Host, err)
	}
	encoded := envelope.VAABytes
	if wormholeScan {
		var item struct {
			VAA string `json:"vaa"`
		}
		if err := json.Unmarshal(envelope.Data, &item); err != nil {
			return nil, false, fmt.Errorf(
				"decode WormholeScan %s VAA payload: %w", base.Host, err,
			)
		}
		encoded = item.VAA
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) == 0 {
		return nil, false, fmt.Errorf("guardian RPC %s returned invalid VAA bytes", base.Host)
	}
	return raw, true, nil
}

func FingerprintVAA(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:8])
}

var _ crosschainport.AttestationSource = (*GuardianClient)(nil)
