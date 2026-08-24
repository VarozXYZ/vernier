package wormholentt_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	solanago "github.com/gagliardetto/solana-go"

	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholentt"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
)

func TestEVMManualTransferUsesSkipRelayerInstruction(t *testing.T) {
	adapter, err := wormholentt.NewEVMAdapter(wormholentt.EVMConfig{
		ChainID:       big.NewInt(91_337),
		WormholeChain: 17,
		Token:         common.HexToAddress("0x0000000000000000000000000000000000000011"),
		Manager:       common.HexToAddress("0x0000000000000000000000000000000000000022"),
		Transceiver:   common.HexToAddress("0x0000000000000000000000000000000000000033"),
		WormholeCore:  common.HexToAddress("0x0000000000000000000000000000000000000044"),
	})
	if err != nil {
		t.Fatal(err)
	}
	recipient := [32]byte{31: 0x55}
	refund := wormholentt.EVMUniversalAddress(
		common.HexToAddress("0x0000000000000000000000000000000000000066"),
	)
	call, err := adapter.BuildTransfer(big.NewInt(123_456), 23, recipient, refund, big.NewInt(7))
	if err != nil {
		t.Fatal(err)
	}
	expectedSelector, _ := hex.DecodeString("b293f97f")
	if got := call.Data[:4]; hex.EncodeToString(got) != hex.EncodeToString(expectedSelector) {
		t.Fatalf("selector = %x, want %x", got, expectedSelector)
	}
	if call.To != adapter.Manager() || call.Value.Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("unexpected transfer target or message value")
	}
	manual := wormholentt.ManualTransceiverInstructions()
	if hex.EncodeToString(manual) != "01000101" {
		t.Fatalf("manual instructions = %x", manual)
	}
	if !contains(call.Data, manual) {
		t.Fatalf("transfer calldata does not contain manual transceiver instructions")
	}
}

func TestSolanaTransferBuildsDirectThreeInstructionFlow(t *testing.T) {
	config := syntheticSolanaConfig()
	adapter, err := wormholentt.NewSolanaAdapter(config)
	if err != nil {
		t.Fatal(err)
	}
	payer := syntheticPublicKey("payer")
	recipient := [32]byte(syntheticPublicKey("destination"))
	plan, err := adapter.BuildTransferBurn(payer, 99_000, 23, recipient)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Instructions) != 3 {
		t.Fatalf("instruction count = %d, want 3", len(plan.Instructions))
	}
	wantPrograms := []solanago.PublicKey{
		config.TokenProgram,
		config.Manager,
		config.Transceiver,
	}
	for index, instruction := range plan.Instructions {
		if instruction.ProgramID() != wantPrograms[index] {
			t.Fatalf("instruction %d program = %s, want %s", index, instruction.ProgramID(), wantPrograms[index])
		}
	}
	if !plan.Instructions[1].Accounts()[5].IsSigner ||
		plan.Instructions[1].Accounts()[5].PublicKey != plan.Outbox.PublicKey() {
		t.Fatalf("transfer does not use generated outbox signer")
	}
	if plan.Emitter.IsZero() || plan.Message.IsZero() {
		t.Fatalf("missing Wormhole emitter or message PDA")
	}
}

func TestParseVAAAndBuildBothDestinationKinds(t *testing.T) {
	evmManager := common.HexToAddress("0x0000000000000000000000000000000000000022")
	var evmDestination [32]byte
	copy(evmDestination[12:], evmManager.Bytes())
	rawEVM, identity := syntheticVAA(t, 23, evmDestination, [32]byte{31: 0x44})

	parsed, err := wormholentt.ParseVAA(rawEVM)
	if err != nil {
		t.Fatal(err)
	}
	if err := parsed.ValidateMessage(identity); err != nil {
		t.Fatal(err)
	}
	signatureCount := int(rawEVM[5])
	body := rawEVM[6+signatureCount*66:]
	expectedHash := gethcrypto.Keccak256(body)
	if hex.EncodeToString(parsed.Hash[:]) != hex.EncodeToString(expectedHash) {
		t.Fatalf(
			"VAA hash = %x, want single keccak256(body) %x",
			parsed.Hash,
			expectedHash,
		)
	}
	message, err := parsed.NTTMessage()
	if err != nil {
		t.Fatal(err)
	}
	if message.DestinationChain != 23 || message.TrimmedAmount.Uint64() != 700 {
		t.Fatalf("unexpected NTT message: chain=%d amount=%s", message.DestinationChain, message.TrimmedAmount)
	}

	evmAdapter, err := wormholentt.NewEVMAdapter(wormholentt.EVMConfig{
		ChainID:       big.NewInt(91_337),
		WormholeChain: 23,
		Token:         common.HexToAddress("0x0000000000000000000000000000000000000011"),
		Manager:       evmManager,
		Transceiver:   common.HexToAddress("0x0000000000000000000000000000000000000033"),
		WormholeCore:  common.HexToAddress("0x0000000000000000000000000000000000000044"),
	})
	if err != nil {
		t.Fatal(err)
	}
	redeem, err := evmAdapter.BuildRedeem(rawEVM)
	if err != nil {
		t.Fatal(err)
	}
	wantRedeemSelector, _ := hex.DecodeString("f953cec7")
	if hex.EncodeToString(redeem.Data[:4]) != hex.EncodeToString(wantRedeemSelector) {
		t.Fatalf("unexpected EVM redeem selector")
	}

	solanaConfig := syntheticSolanaConfig()
	rawSolana, _ := syntheticVAA(
		t,
		solanaConfig.WormholeChain,
		[32]byte(solanaConfig.Manager),
		[32]byte(syntheticPublicKey("recipient")),
	)
	solanaAdapter, err := wormholentt.NewSolanaAdapter(solanaConfig)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([][20]byte, 1)
	keys[0] = syntheticGuardianKey(t)
	plan, err := solanaAdapter.BuildRedemption(rawSolana, keys, syntheticPublicKey("payer"))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Verify) != 1 || len(plan.Verify[0].Instructions) != 2 {
		t.Fatalf("unexpected guardian verification batches")
	}
	if len(plan.PostVAA.Instructions) != 1 || len(plan.Redeem.Instructions) != 3 {
		t.Fatalf("unexpected post/redeem plan")
	}
	wantRedeemAccountCounts := []int{6, 10, 8}
	wantRedeemDataLengths := []int{8, 8, 9}
	for index, instruction := range plan.Redeem.Instructions {
		if got := len(instruction.Accounts()); got != wantRedeemAccountCounts[index] {
			t.Fatalf(
				"redeem instruction %d has %d accounts, want %d",
				index,
				got,
				wantRedeemAccountCounts[index],
			)
		}
		data, dataErr := instruction.Data()
		if dataErr != nil {
			t.Fatal(dataErr)
		}
		if got := len(data); got != wantRedeemDataLengths[index] {
			t.Fatalf(
				"redeem instruction %d has %d data bytes, want %d",
				index,
				got,
				wantRedeemDataLengths[index],
			)
		}
	}
	postAccounts := plan.PostVAA.Instructions[0].Accounts()
	if postAccounts[1].IsWritable {
		t.Fatalf("Wormhole Bridge config must be read-only in postVAA")
	}
	if !postAccounts[3].IsWritable || !postAccounts[4].IsWritable {
		t.Fatalf("posted VAA and payer must be writable")
	}
	if plan.PostedVAA.IsZero() || plan.InboxItem.IsZero() {
		t.Fatalf("missing redemption PDAs")
	}
	secpData, err := plan.Verify[0].Instructions[0].Data()
	if err != nil {
		t.Fatal(err)
	}
	if len(secpData) < len(expectedHash) ||
		hex.EncodeToString(secpData[len(secpData)-len(expectedHash):]) !=
			hex.EncodeToString(parsedSolanaHash(t, rawSolana)) {
		t.Fatalf("secp256k1 instruction does not contain single VAA body hash")
	}
	rebased, err := wormholentt.RebaseSecp256k1Instruction(
		plan.Verify[0].Instructions[0],
		2,
	)
	if err != nil {
		t.Fatal(err)
	}
	rebasedData, err := rebased.Data()
	if err != nil {
		t.Fatal(err)
	}
	count := int(rebasedData[0])
	for index := 0; index < count; index++ {
		offset := 1 + index*11
		if rebasedData[offset+2] != 2 ||
			rebasedData[offset+5] != 2 ||
			rebasedData[offset+10] != 2 {
			t.Fatalf("secp256k1 offsets were not rebased to instruction 2")
		}
	}
}

func TestGuardianClientReturnsMatchingAttestation(t *testing.T) {
	manager := [32]byte{31: 0x22}
	raw, identity := syntheticVAA(t, 23, manager, [32]byte{31: 0x44})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		wantPath := fmt.Sprintf(
			"/v1/signed_vaa/%d/%s/%d",
			identity.EmitterChain,
			hex.EncodeToString(identity.EmitterAddress[:]),
			identity.Sequence,
		)
		if request.URL.Path != wantPath {
			http.NotFound(writer, request)
			return
		}
		fmt.Fprintf(writer, `{"vaaBytes":%q}`, base64.StdEncoding.EncodeToString(raw))
	}))
	defer server.Close()

	client, err := wormholentt.NewGuardianClient(wormholentt.GuardianClientConfig{
		Endpoints:    []string{server.URL},
		PollInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attestation, err := client.Await(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if attestation.Message != identity || len(attestation.Raw) != len(raw) {
		t.Fatalf("unexpected attestation")
	}
}

func TestGuardianClientUsesWormholeScanVAAFallback(t *testing.T) {
	manager := [32]byte{31: 0x22}
	raw, identity := syntheticVAA(t, 23, manager, [32]byte{31: 0x44})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		wantPath := fmt.Sprintf("/api/v1/vaas/%d/%s/%d", identity.EmitterChain,
			hex.EncodeToString(identity.EmitterAddress[:]), identity.Sequence)
		if request.URL.Path != wantPath {
			http.NotFound(writer, request)
			return
		}
		fmt.Fprintf(writer, `{"data":{"vaa":%q}}`, base64.StdEncoding.EncodeToString(raw))
	}))
	defer server.Close()

	client, err := wormholentt.NewGuardianClient(wormholentt.GuardianClientConfig{
		Endpoints: []string{server.URL + "/api/v1/vaas"}, PollInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attestation, err := client.Await(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if attestation.Message != identity || len(attestation.Raw) != len(raw) {
		t.Fatal("unexpected WormholeScan attestation")
	}
}

func syntheticSolanaConfig() wormholentt.SolanaConfig {
	return wormholentt.SolanaConfig{
		WormholeChain: 23,
		Manager:       syntheticPublicKey("manager"),
		Transceiver:   syntheticPublicKey("transceiver"),
		WormholeCore:  syntheticPublicKey("core"),
		TokenMint:     syntheticPublicKey("mint"),
		TokenProgram:  solanago.TokenProgramID,
	}
}

func syntheticPublicKey(label string) solanago.PublicKey {
	sum := sha256.Sum256([]byte("synthetic/" + label))
	return solanago.PublicKey(sum)
}

func syntheticVAA(
	t *testing.T,
	destinationChain uint16,
	destinationManager [32]byte,
	recipient [32]byte,
) ([]byte, crosschainport.MessageID) {
	t.Helper()
	native := make([]byte, 79)
	copy(native[0:4], []byte{0x99, 0x4e, 0x54, 0x54})
	native[4] = 8
	binary.BigEndian.PutUint64(native[5:13], 700)
	native[44] = 0x11
	copy(native[45:77], recipient[:])
	binary.BigEndian.PutUint16(native[77:79], destinationChain)

	manager := make([]byte, 66+len(native))
	manager[31] = 0xaa
	manager[63] = 0xbb
	binary.BigEndian.PutUint16(manager[64:66], uint16(len(native)))
	copy(manager[66:], native)

	payload := make([]byte, 70+len(manager)+2)
	copy(payload[0:4], []byte{0x99, 0x45, 0xff, 0x10})
	payload[35] = 0x99
	copy(payload[36:68], destinationManager[:])
	binary.BigEndian.PutUint16(payload[68:70], uint16(len(manager)))
	copy(payload[70:], manager)

	identity := crosschainport.MessageID{
		EmitterChain:   17,
		EmitterAddress: [32]byte{31: 0x77},
		Sequence:       42,
	}
	body := make([]byte, 51+len(payload))
	binary.BigEndian.PutUint32(body[0:4], 1_700_000_000)
	binary.BigEndian.PutUint32(body[4:8], 9)
	binary.BigEndian.PutUint16(body[8:10], identity.EmitterChain)
	copy(body[10:42], identity.EmitterAddress[:])
	binary.BigEndian.PutUint64(body[42:50], identity.Sequence)
	body[50] = 32
	copy(body[51:], payload)

	raw := make([]byte, 6+66+len(body))
	raw[0] = 1
	binary.BigEndian.PutUint32(raw[1:5], 3)
	raw[5] = 1
	raw[6] = 0
	copy(raw[72:], body)
	privateKey, err := gethcrypto.HexToECDSA(
		"59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
	)
	if err != nil {
		t.Fatal(err)
	}
	firstHash := gethcrypto.Keccak256(body)
	signature, err := gethcrypto.Sign(gethcrypto.Keccak256(firstHash), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	copy(raw[7:72], signature)
	return raw, identity
}

func contains(haystack, needle []byte) bool {
	if len(needle) == 0 || len(needle) > len(haystack) {
		return false
	}
	for index := 0; index <= len(haystack)-len(needle); index++ {
		match := true
		for offset := range needle {
			if haystack[index+offset] != needle[offset] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func parsedSolanaHash(t *testing.T, raw []byte) []byte {
	t.Helper()
	parsed, err := wormholentt.ParseVAA(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Hash[:]
}

func syntheticGuardianKey(t *testing.T) [20]byte {
	t.Helper()
	privateKey, err := gethcrypto.HexToECDSA(
		"59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
	)
	if err != nil {
		t.Fatal(err)
	}
	var result [20]byte
	copy(result[:], gethcrypto.PubkeyToAddress(privateKey.PublicKey).Bytes())
	return result
}
