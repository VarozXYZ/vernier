package wormholentt

import (
	"bytes"
	"encoding/binary"
	"fmt"

	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	solanago "github.com/gagliardetto/solana-go"
)

const (
	maxGuardianKeys      = 19
	guardianBatchSize    = 7
	coreVerifySignatures = byte(7)
	corePostVAA          = byte(2)
)

var secp256k1Program = solanago.MustPublicKeyFromBase58(
	"KeccakSecp256k11111111111111111111111111111",
)

func IsSecp256k1Instruction(instruction solanago.Instruction) bool {
	return instruction != nil && instruction.ProgramID() == secp256k1Program
}

// RebaseSecp256k1Instruction updates all instruction-index references in a
// batched Secp256k1 instruction. Wormhole's canonical serializer assumes the
// secp instruction is transaction instruction zero. Callers that prepend
// Compute Budget instructions must rebase these references to its real index.
func RebaseSecp256k1Instruction(
	instruction solanago.Instruction,
	index uint8,
) (solanago.Instruction, error) {
	if !IsSecp256k1Instruction(instruction) {
		return nil, fmt.Errorf("instruction is not Secp256k1")
	}
	data, err := instruction.Data()
	if err != nil || len(data) == 0 {
		return nil, fmt.Errorf("read Secp256k1 instruction data")
	}
	count := int(data[0])
	if count == 0 || len(data) < 1+count*11 {
		return nil, fmt.Errorf("Secp256k1 instruction offsets are malformed")
	}
	rebased := append([]byte(nil), data...)
	for signature := 0; signature < count; signature++ {
		offset := 1 + signature*11
		rebased[offset+2] = index
		rebased[offset+5] = index
		rebased[offset+10] = index
	}
	return solanago.NewInstruction(
		instruction.ProgramID(),
		instruction.Accounts(),
		rebased,
	), nil
}

type SolanaInstructionBatch struct {
	Kind         string
	Instructions []solanago.Instruction
	Signers      []solanago.PrivateKey
}

type SolanaRedemptionPlan struct {
	VAAFingerprint string
	Message        NTTMessage
	Verify         []SolanaInstructionBatch
	PostVAA        SolanaInstructionBatch
	Redeem         SolanaInstructionBatch
	PostedVAA      solanago.PublicKey
	InboxItem      solanago.PublicKey
}

type GuardianSet struct {
	Index          uint32
	Keys           [][20]byte
	CreationTime   uint32
	ExpirationTime uint32
}

type SignatureSet struct {
	Signatures       []bool
	Hash             [32]byte
	GuardianSetIndex uint32
}

func DecodeSignatureSet(data []byte) (SignatureSet, error) {
	if len(data) < 4+32+4 {
		return SignatureSet{}, fmt.Errorf("wormhole SignatureSet account is too short")
	}
	count := int(binary.LittleEndian.Uint32(data[:4]))
	hashOffset := 4 + count
	if count <= 0 || count > maxGuardianKeys || len(data) != hashOffset+32+4 {
		return SignatureSet{}, fmt.Errorf("wormhole SignatureSet account has an invalid length")
	}
	result := SignatureSet{Signatures: make([]bool, count)}
	for index, value := range data[4:hashOffset] {
		result.Signatures[index] = value != 0
	}
	copy(result.Hash[:], data[hashOffset:hashOffset+32])
	result.GuardianSetIndex = binary.LittleEndian.Uint32(data[hashOffset+32:])
	return result, nil
}

func DecodeGuardianSet(data []byte) (GuardianSet, error) {
	if len(data) < 16 {
		return GuardianSet{}, fmt.Errorf("wormhole GuardianSet account is too short")
	}
	result := GuardianSet{Index: binary.LittleEndian.Uint32(data[0:4])}
	count := int(binary.LittleEndian.Uint32(data[4:8]))
	if count <= 0 || count > maxGuardianKeys {
		return GuardianSet{}, fmt.Errorf("wormhole GuardianSet contains %d keys", count)
	}
	keysEnd := 8 + count*20
	if len(data) != keysEnd+8 {
		return GuardianSet{}, fmt.Errorf("wormhole GuardianSet account has an invalid length")
	}
	result.Keys = make([][20]byte, count)
	for index := range result.Keys {
		copy(result.Keys[index][:], data[8+index*20:8+(index+1)*20])
	}
	result.CreationTime = binary.LittleEndian.Uint32(data[keysEnd : keysEnd+4])
	result.ExpirationTime = binary.LittleEndian.Uint32(data[keysEnd+4 : keysEnd+8])
	return result, nil
}

func (a *SolanaAdapter) GuardianSetAddress(index uint32) solanago.PublicKey {
	return corePDA(a.config.WormholeCore, []byte("GuardianSet"), uint32Seed(index))
}

func (a *SolanaAdapter) PostedVAAAddress(hash [32]byte) solanago.PublicKey {
	return corePDA(a.config.WormholeCore, []byte("PostedVAA"), hash[:])
}

// BuildRedemption creates the complete legacy Wormhole Core self-relay path.
// Guardian keys come from the on-chain GuardianSet account and are intentionally
// an input: building instructions never performs an RPC call.
func (a *SolanaAdapter) BuildRedemption(
	rawVAA []byte,
	guardianKeys [][20]byte,
	payer solanago.PublicKey,
) (SolanaRedemptionPlan, error) {
	return a.buildRedemption(rawVAA, guardianKeys, payer, solanago.PublicKey{})
}

// BuildRedemptionWithVerifiedSignatureSet resumes after guardian verification.
// The caller must validate that signatureSet is initialized for this VAA.
func (a *SolanaAdapter) BuildRedemptionWithVerifiedSignatureSet(
	rawVAA []byte,
	guardianKeys [][20]byte,
	payer, signatureSet solanago.PublicKey,
) (SolanaRedemptionPlan, error) {
	if signatureSet.IsZero() {
		return SolanaRedemptionPlan{}, fmt.Errorf("verified Wormhole signature set is required")
	}
	return a.buildRedemption(rawVAA, guardianKeys, payer, signatureSet)
}

func (a *SolanaAdapter) buildRedemption(
	rawVAA []byte,
	guardianKeys [][20]byte,
	payer, verifiedSignatureSet solanago.PublicKey,
) (SolanaRedemptionPlan, error) {
	if payer.IsZero() {
		return SolanaRedemptionPlan{}, fmt.Errorf("solana redemption payer is required")
	}
	vaa, err := ParseVAA(rawVAA)
	if err != nil {
		return SolanaRedemptionPlan{}, err
	}
	message, err := vaa.NTTMessage()
	if err != nil {
		return SolanaRedemptionPlan{}, err
	}
	if message.DestinationChain != a.config.WormholeChain {
		return SolanaRedemptionPlan{}, fmt.Errorf(
			"VAA targets Wormhole chain %d, adapter is chain %d",
			message.DestinationChain,
			a.config.WormholeChain,
		)
	}
	if !bytes.Equal(message.DestinationManager[:], a.config.Manager.Bytes()) {
		return SolanaRedemptionPlan{}, fmt.Errorf("VAA targets another destination NTT manager")
	}
	if len(guardianKeys) == 0 || len(guardianKeys) > maxGuardianKeys {
		return SolanaRedemptionPlan{}, fmt.Errorf("GuardianSet must contain between 1 and %d keys", maxGuardianKeys)
	}
	for _, signature := range vaa.Signatures {
		if int(signature.Index) >= len(guardianKeys) {
			return SolanaRedemptionPlan{}, fmt.Errorf(
				"VAA signature references absent guardian %d",
				signature.Index,
			)
		}
		signingDigest := gethcrypto.Keccak256(vaa.Hash[:])
		publicKey, recoverErr := gethcrypto.SigToPub(
			signingDigest,
			signature.Signature[:],
		)
		if recoverErr != nil || !bytes.Equal(
			gethcrypto.PubkeyToAddress(*publicKey).Bytes(),
			guardianKeys[signature.Index][:],
		) {
			return SolanaRedemptionPlan{}, fmt.Errorf(
				"VAA signature for guardian %d does not match on-chain GuardianSet",
				signature.Index,
			)
		}
	}

	var signatureSet solanago.PrivateKey
	signatureSetKey := verifiedSignatureSet
	if signatureSetKey.IsZero() {
		signatureSet, err = solanago.NewRandomPrivateKey()
		if err != nil {
			return SolanaRedemptionPlan{}, fmt.Errorf("generate Wormhole signature set: %w", err)
		}
		signatureSetKey = signatureSet.PublicKey()
	}
	guardianSet := corePDA(
		a.config.WormholeCore,
		[]byte("GuardianSet"),
		uint32Seed(vaa.GuardianSetIndex),
	)
	postedVAA := corePDA(a.config.WormholeCore, []byte("PostedVAA"), vaa.Hash[:])

	result := SolanaRedemptionPlan{
		VAAFingerprint: FingerprintVAA(rawVAA),
		Message:        message,
		PostedVAA:      postedVAA,
	}
	for start := 0; verifiedSignatureSet.IsZero() &&
		start < len(vaa.Signatures); start += guardianBatchSize {
		end := start + guardianBatchSize
		if end > len(vaa.Signatures) {
			end = len(vaa.Signatures)
		}
		secpData, statuses, encodeErr := encodeGuardianBatch(
			vaa.Signatures[start:end],
			guardianKeys,
			vaa.Hash,
		)
		if encodeErr != nil {
			return SolanaRedemptionPlan{}, encodeErr
		}
		verifyData := append([]byte{coreVerifySignatures}, statuses...)
		result.Verify = append(result.Verify, SolanaInstructionBatch{
			Kind: "wormhole_verify_guardians",
			Instructions: []solanago.Instruction{
				solanago.NewInstruction(secp256k1Program, nil, secpData),
				solanago.NewInstruction(
					a.config.WormholeCore,
					solanago.AccountMetaSlice{
						solanago.Meta(payer).WRITE().SIGNER(),
						solanago.Meta(guardianSet),
						solanago.Meta(signatureSetKey).WRITE().SIGNER(),
						solanago.Meta(solanago.SysVarInstructionsPubkey),
						solanago.Meta(solanago.SysVarRentPubkey),
						solanago.Meta(solanago.SystemProgramID),
					},
					verifyData,
				),
			},
			Signers: []solanago.PrivateKey{signatureSet},
		})
	}

	result.PostVAA = SolanaInstructionBatch{
		Kind: "wormhole_post_vaa",
		Instructions: []solanago.Instruction{solanago.NewInstruction(
			a.config.WormholeCore,
			solanago.AccountMetaSlice{
				solanago.Meta(guardianSet),
				solanago.Meta(corePDA(a.config.WormholeCore, []byte("Bridge"))),
				solanago.Meta(signatureSetKey),
				solanago.Meta(postedVAA).WRITE(),
				solanago.Meta(payer).WRITE().SIGNER(),
				solanago.Meta(solanago.SysVarClockPubkey),
				solanago.Meta(solanago.SysVarRentPubkey),
				solanago.Meta(solanago.SystemProgramID),
			},
			encodePostVAA(vaa),
		)},
	}

	sourceChain := vaa.Message.EmitterChain
	chain := chainSeed(sourceChain)
	transceiverPeer := transceiverPDA(a.config.Transceiver, []byte("transceiver_peer"), chain)
	transceiverMessage := transceiverPDA(
		a.config.Transceiver,
		[]byte("transceiver_message"),
		chain,
		message.ID[:],
	)
	config := a.pda([]byte("config"))
	peer := a.pda([]byte("peer"), chain)
	registeredTransceiver := a.pda(
		[]byte("registered_transceiver"),
		a.config.Transceiver.Bytes(),
	)
	digest := message.Digest(sourceChain)
	inboxItem := a.pda([]byte("inbox_item"), digest[:])
	inboxRate := a.pda([]byte("inbox_rate_limit"), chain)
	outboxRate := a.pda([]byte("outbox_rate_limit"))
	tokenAuthority := a.pda([]byte("token_authority"))
	recipient := solanago.PublicKey(message.Recipient)
	recipientATA, _, err := solanago.FindAssociatedTokenAddress(recipient, a.config.TokenMint)
	if err != nil {
		return SolanaRedemptionPlan{}, fmt.Errorf("derive NTT recipient token account: %w", err)
	}
	custody, _, err := solanago.FindAssociatedTokenAddress(tokenAuthority, a.config.TokenMint)
	if err != nil {
		return SolanaRedemptionPlan{}, fmt.Errorf("derive NTT custody account: %w", err)
	}

	receive := solanago.NewInstruction(
		a.config.Transceiver,
		solanago.AccountMetaSlice{
			solanago.Meta(payer).WRITE().SIGNER(),
			solanago.Meta(config),
			solanago.Meta(transceiverPeer),
			solanago.Meta(postedVAA),
			solanago.Meta(transceiverMessage).WRITE(),
			solanago.Meta(solanago.SystemProgramID),
		},
		anchorDiscriminator("receive_wormhole_message"),
	)
	redeem := solanago.NewInstruction(
		a.config.Manager,
		solanago.AccountMetaSlice{
			solanago.Meta(payer).WRITE().SIGNER(),
			solanago.Meta(config),
			solanago.Meta(peer),
			solanago.Meta(transceiverMessage),
			solanago.Meta(registeredTransceiver),
			solanago.Meta(a.config.TokenMint),
			solanago.Meta(inboxItem).WRITE(),
			solanago.Meta(inboxRate).WRITE(),
			solanago.Meta(outboxRate).WRITE(),
			solanago.Meta(solanago.SystemProgramID),
		},
		anchorDiscriminator("redeem"),
	)
	releaseData := append(anchorDiscriminator("release_inbound_mint"), 0)
	release := solanago.NewInstruction(
		a.config.Manager,
		solanago.AccountMetaSlice{
			solanago.Meta(payer).WRITE().SIGNER(),
			solanago.Meta(config),
			solanago.Meta(inboxItem).WRITE(),
			solanago.Meta(recipientATA).WRITE(),
			solanago.Meta(tokenAuthority),
			solanago.Meta(a.config.TokenMint).WRITE(),
			solanago.Meta(a.config.TokenProgram),
			solanago.Meta(custody).WRITE(),
		},
		releaseData,
	)
	result.InboxItem = inboxItem
	result.Redeem = SolanaInstructionBatch{
		Kind:         "ntt_redeem_manual",
		Instructions: []solanago.Instruction{receive, redeem, release},
	}
	return result, nil
}

func encodeGuardianBatch(
	signatures []GuardianSignature,
	guardianKeys [][20]byte,
	hash [32]byte,
) ([]byte, []byte, error) {
	if len(signatures) == 0 || len(signatures) > guardianBatchSize {
		return nil, nil, fmt.Errorf("guardian batch must contain between 1 and %d signatures", guardianBatchSize)
	}
	const (
		offsetSpan = 11
		itemSize   = 65 + 20
	)
	dataStart := 1 + len(signatures)*offsetSpan
	messageOffset := dataStart + len(signatures)*itemSize
	data := make([]byte, messageOffset+len(hash))
	data[0] = byte(len(signatures))
	copy(data[messageOffset:], hash[:])
	statuses := bytes.Repeat([]byte{0xff}, maxGuardianKeys)
	for index, item := range signatures {
		if int(item.Index) >= len(guardianKeys) || int(item.Index) >= maxGuardianKeys {
			return nil, nil, fmt.Errorf("guardian signature index %d is out of range", item.Index)
		}
		signatureOffset := dataStart + index*itemSize
		addressOffset := signatureOffset + 65
		offset := 1 + index*offsetSpan
		binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(signatureOffset))
		data[offset+2] = 0
		binary.LittleEndian.PutUint16(data[offset+3:offset+5], uint16(addressOffset))
		data[offset+5] = 0
		binary.LittleEndian.PutUint16(data[offset+6:offset+8], uint16(messageOffset))
		binary.LittleEndian.PutUint16(data[offset+8:offset+10], uint16(len(hash)))
		data[offset+10] = 0
		copy(data[signatureOffset:signatureOffset+65], item.Signature[:])
		copy(data[addressOffset:addressOffset+20], guardianKeys[item.Index][:])
		statuses[item.Index] = byte(index)
	}
	return data, statuses, nil
}

func encodePostVAA(vaa VAA) []byte {
	data := make([]byte, 1+1+4+4+4+2+32+8+1+4+len(vaa.Payload))
	offset := 0
	data[offset] = corePostVAA
	offset++
	data[offset] = vaa.Version
	offset++
	binary.LittleEndian.PutUint32(data[offset:offset+4], vaa.GuardianSetIndex)
	offset += 4
	binary.LittleEndian.PutUint32(data[offset:offset+4], vaa.Timestamp)
	offset += 4
	binary.LittleEndian.PutUint32(data[offset:offset+4], vaa.Nonce)
	offset += 4
	binary.LittleEndian.PutUint16(data[offset:offset+2], vaa.Message.EmitterChain)
	offset += 2
	copy(data[offset:offset+32], vaa.Message.EmitterAddress[:])
	offset += 32
	binary.LittleEndian.PutUint64(data[offset:offset+8], vaa.Message.Sequence)
	offset += 8
	data[offset] = vaa.ConsistencyLevel
	offset++
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(vaa.Payload)))
	offset += 4
	copy(data[offset:], vaa.Payload)
	return data
}

func uint32Seed(value uint32) []byte {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, value)
	return result
}
