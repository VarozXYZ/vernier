package wormholentt

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	solanago "github.com/gagliardetto/solana-go"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
)

const (
	solanaSequenceLogPrefix = "Program log: Sequence: "
)

type SolanaConfig struct {
	WormholeChain uint16
	Manager       solanago.PublicKey
	Transceiver   solanago.PublicKey
	WormholeCore  solanago.PublicKey
	TokenMint     solanago.PublicKey
	TokenProgram  solanago.PublicKey
}

type SolanaTransferPlan struct {
	Instructions []solanago.Instruction
	Outbox       solanago.PrivateKey
	Message      solanago.PublicKey
	Emitter      solanago.PublicKey
}

type SolanaAdapter struct {
	config SolanaConfig
}

func NewSolanaAdapter(config SolanaConfig) (*SolanaAdapter, error) {
	if config.WormholeChain == 0 || config.Manager.IsZero() ||
		config.Transceiver.IsZero() || config.WormholeCore.IsZero() ||
		config.TokenMint.IsZero() {
		return nil, fmt.Errorf("solana NTT profile requires chain ID, manager, transceiver, core, and mint")
	}
	if config.TokenProgram.IsZero() {
		config.TokenProgram = solanago.TokenProgramID
	}
	return &SolanaAdapter{config: config}, nil
}

func (a *SolanaAdapter) WormholeChainID() uint16 { return a.config.WormholeChain }
func (a *SolanaAdapter) WormholeCore() solanago.PublicKey {
	return a.config.WormholeCore
}
func (a *SolanaAdapter) TokenMint() solanago.PublicKey {
	return a.config.TokenMint
}

// BuildTransferBurn builds the direct NTT source transaction. The caller owns
// blockhash selection, fee policy, signing, and broadcast. The generated outbox
// key must sign alongside payer.
func (a *SolanaAdapter) BuildTransferBurn(
	payer solanago.PublicKey,
	amount uint64,
	destinationChain uint16,
	recipient [32]byte,
) (SolanaTransferPlan, error) {
	if payer.IsZero() || amount == 0 || destinationChain == 0 ||
		recipient == ([32]byte{}) {
		return SolanaTransferPlan{}, fmt.Errorf("manual Solana NTT transfer requires payer, amount, destination, and recipient")
	}
	outbox, err := solanago.NewRandomPrivateKey()
	if err != nil {
		return SolanaTransferPlan{}, fmt.Errorf("generate NTT outbox: %w", err)
	}
	outboxKey := outbox.PublicKey()
	config := a.pda([]byte("config"))
	outboxRate := a.pda([]byte("outbox_rate_limit"))
	inboxRate := a.pda([]byte("inbox_rate_limit"), chainSeed(destinationChain))
	peer := a.pda([]byte("peer"), chainSeed(destinationChain))
	tokenAuthority := a.pda([]byte("token_authority"))
	sessionAuthority := a.pda(
		[]byte("session_authority"),
		payer.Bytes(),
		transferArgsHash(amount, destinationChain, recipient, false),
	)
	registeredTransceiver := a.pda(
		[]byte("registered_transceiver"),
		a.config.Transceiver.Bytes(),
	)
	emitter := transceiverPDA(a.config.Transceiver, []byte("emitter"))
	message := transceiverPDA(a.config.Transceiver, []byte("message"), outboxKey.Bytes())
	bridge := corePDA(a.config.WormholeCore, []byte("Bridge"))
	feeCollector := corePDA(a.config.WormholeCore, []byte("fee_collector"))
	sequence := corePDA(a.config.WormholeCore, []byte("Sequence"), emitter.Bytes())

	source, _, err := solanago.FindAssociatedTokenAddress(payer, a.config.TokenMint)
	if err != nil {
		return SolanaTransferPlan{}, fmt.Errorf("derive source token account: %w", err)
	}
	custody, _, err := solanago.FindAssociatedTokenAddress(tokenAuthority, a.config.TokenMint)
	if err != nil {
		return SolanaTransferPlan{}, fmt.Errorf("derive NTT custody account: %w", err)
	}

	approveData := make([]byte, 9)
	approveData[0] = 4 // SPL Token Approve
	binary.LittleEndian.PutUint64(approveData[1:], amount)
	approve := solanago.NewInstruction(
		a.config.TokenProgram,
		solanago.AccountMetaSlice{
			solanago.Meta(source).WRITE(),
			solanago.Meta(sessionAuthority),
			solanago.Meta(payer).SIGNER(),
		},
		approveData,
	)

	transferData := append(anchorDiscriminator("transfer_burn"), encodeTransferArgs(
		amount, destinationChain, recipient, false,
	)...)
	transfer := solanago.NewInstruction(
		a.config.Manager,
		solanago.AccountMetaSlice{
			solanago.Meta(payer).WRITE().SIGNER(),
			solanago.Meta(config),
			solanago.Meta(a.config.TokenMint).WRITE(),
			solanago.Meta(source).WRITE(),
			solanago.Meta(a.config.TokenProgram),
			solanago.Meta(outboxKey).WRITE().SIGNER(),
			solanago.Meta(outboxRate).WRITE(),
			solanago.Meta(custody).WRITE(),
			solanago.Meta(solanago.SystemProgramID),
			solanago.Meta(inboxRate).WRITE(),
			solanago.Meta(peer),
			solanago.Meta(sessionAuthority),
			solanago.Meta(tokenAuthority),
		},
		transferData,
	)

	releaseData := append(anchorDiscriminator("release_wormhole_outbound"), 1)
	release := solanago.NewInstruction(
		a.config.Transceiver,
		solanago.AccountMetaSlice{
			solanago.Meta(payer).WRITE().SIGNER(),
			solanago.Meta(config),
			solanago.Meta(outboxKey).WRITE(),
			solanago.Meta(registeredTransceiver),
			solanago.Meta(message).WRITE(),
			solanago.Meta(emitter),
			solanago.Meta(bridge).WRITE(),
			solanago.Meta(feeCollector).WRITE(),
			solanago.Meta(sequence).WRITE(),
			solanago.Meta(a.config.WormholeCore),
			solanago.Meta(solanago.SystemProgramID),
			solanago.Meta(solanago.SysVarClockPubkey),
			solanago.Meta(solanago.SysVarRentPubkey),
		},
		releaseData,
	)

	return SolanaTransferPlan{
		Instructions: []solanago.Instruction{approve, transfer, release},
		Outbox:       outbox,
		Message:      message,
		Emitter:      emitter,
	}, nil
}

func (a *SolanaAdapter) MessageFromLogs(logs []string) (crosschainport.MessageID, error) {
	for _, line := range logs {
		position := strings.Index(line, solanaSequenceLogPrefix)
		if position < 0 {
			continue
		}
		value := strings.TrimSpace(line[position+len(solanaSequenceLogPrefix):])
		sequence, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return crosschainport.MessageID{}, fmt.Errorf("decode Wormhole sequence: %w", err)
		}
		emitter := transceiverPDA(a.config.Transceiver, []byte("emitter"))
		return crosschainport.MessageID{
			EmitterChain:   a.config.WormholeChain,
			EmitterAddress: [32]byte(emitter),
			Sequence:       sequence,
		}, nil
	}
	return crosschainport.MessageID{}, fmt.Errorf("transaction logs contain no Wormhole sequence")
}

func (a *SolanaAdapter) pda(seeds ...[]byte) solanago.PublicKey {
	return findPDA(a.config.Manager, seeds...)
}

func findPDA(program solanago.PublicKey, seeds ...[]byte) solanago.PublicKey {
	value, _, err := solanaadapter.FindProgramAddress(seeds, [32]byte(program))
	if err != nil {
		panic(err)
	}
	return solanago.PublicKey(value)
}

func transceiverPDA(program solanago.PublicKey, seeds ...[]byte) solanago.PublicKey {
	return findPDA(program, seeds...)
}

func corePDA(program solanago.PublicKey, seeds ...[]byte) solanago.PublicKey {
	return findPDA(program, seeds...)
}

func chainSeed(chain uint16) []byte {
	result := make([]byte, 2)
	binary.BigEndian.PutUint16(result, chain)
	return result
}

func transferArgsHash(
	amount uint64,
	chain uint16,
	recipient [32]byte,
	shouldQueue bool,
) []byte {
	data := make([]byte, 8+2+32+1)
	binary.BigEndian.PutUint64(data[0:8], amount)
	binary.BigEndian.PutUint16(data[8:10], chain)
	copy(data[10:42], recipient[:])
	if shouldQueue {
		data[42] = 1
	}
	return crypto.Keccak256(data)
}

func encodeTransferArgs(
	amount uint64,
	chain uint16,
	recipient [32]byte,
	shouldQueue bool,
) []byte {
	data := make([]byte, 8+2+32+1)
	binary.LittleEndian.PutUint64(data[0:8], amount)
	binary.LittleEndian.PutUint16(data[8:10], chain)
	copy(data[10:42], recipient[:])
	if shouldQueue {
		data[42] = 1
	}
	return data
}

func anchorDiscriminator(name string) []byte {
	sum := sha256.Sum256([]byte("global:" + name))
	return append([]byte(nil), sum[:8]...)
}

func SolanaUniversalAddress(address solanago.PublicKey) [32]byte {
	return [32]byte(address)
}
