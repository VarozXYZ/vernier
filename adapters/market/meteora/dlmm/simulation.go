package dlmm

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/gagliardetto/solana-go"
)

const (
	swap2Discriminator = "414b3f4ceb5b5b88"
	eventAuthoritySeed = "__event_authority"
	memoProgramAddress = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
)

// SimulationConfig describes only the public pool side of a Meteora swap.
// The owner is supplied at call time so a canary can use a real read-only
// wallet without placing it in the repository.
type SimulationConfig struct {
	Pool         string
	TokenX       string
	TokenY       string
	Program      string
	ComputeLimit uint32
}

// BuildSimulationTransaction builds the canonical Meteora swap2 instruction
// against the current pool state. It intentionally does not sign. RPC
// simulation with signature verification disabled is therefore safe for a
// wallet whose private key is not held by this process.
func BuildSimulationTransaction(
	ctx context.Context,
	network *solanaadapter.ReadOnlyNetwork,
	config SimulationConfig,
	snapshot market.MarketSnapshot,
	inputToken, outputToken string,
	amountIn, minimumOut *big.Int,
	owner solana.PublicKey,
) ([]byte, error) {
	if network == nil || owner.IsZero() || amountIn == nil || amountIn.Sign() <= 0 || minimumOut == nil || minimumOut.Sign() < 0 {
		return nil, fmt.Errorf("meteora simulation request is incomplete")
	}
	state, ok := snapshot.Data().(Snapshot)
	if !ok {
		return nil, fmt.Errorf("meteora simulation snapshot has an incompatible type")
	}
	poolText := config.Pool
	if poolText == "" {
		return nil, fmt.Errorf("meteora simulation pool is required")
	}
	pool, err := solana.PublicKeyFromBase58(poolText)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora pool: %w", err)
	}
	programText := config.Program
	if programText == "" {
		programText = meteoraProgram
	}
	program, err := solana.PublicKeyFromBase58(programText)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora program: %w", err)
	}
	inMint, err := solana.PublicKeyFromBase58(inputToken)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora input mint: %w", err)
	}
	outMint, err := solana.PublicKeyFromBase58(outputToken)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora output mint: %w", err)
	}
	xMint, err := solana.PublicKeyFromBase58(config.TokenX)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora token X: %w", err)
	}
	yMint, err := solana.PublicKeyFromBase58(config.TokenY)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora token Y: %w", err)
	}
	swapForY := inMint.Equals(xMint) && outMint.Equals(yMint)
	if !swapForY && !(inMint.Equals(yMint) && outMint.Equals(xMint)) {
		return nil, fmt.Errorf("meteora simulation token direction does not match pool")
	}
	poolAccount, err := network.ReadAccount(ctx, pool.String())
	if err != nil {
		return nil, fmt.Errorf("read Meteora pool for simulation: %w", err)
	}
	if len(poolAccount.Data) < 904 || string(poolAccount.Data[:8]) != string([]byte{33, 11, 49, 98, 181, 101, 177, 13}) {
		return nil, fmt.Errorf("meteora pool account has an invalid layout")
	}
	reserveX := publicKeyAt(poolAccount.Data, 152)
	reserveY := publicKeyAt(poolAccount.Data, 184)
	oracle := publicKeyAt(poolAccount.Data, 552)
	if reserveX.IsZero() || reserveY.IsZero() || oracle.IsZero() {
		return nil, fmt.Errorf("meteora pool has incomplete swap accounts")
	}
	quoteAt := quoteTime(snapshot.Metadata().AppliedAt, stateUpdateTime(state))
	indexes, err := RequiredBinArrayIndexes(state, amountIn, swapForY, quoteAt.Unix())
	if err != nil {
		return nil, err
	}
	if len(indexes) == 0 {
		return nil, fmt.Errorf("meteora simulation quote consumed no bin arrays")
	}
	binAccounts := make([]*solana.AccountMeta, 0, len(indexes))
	for _, index := range indexes {
		address, _, err := solanaadapter.FindProgramAddress([][]byte{[]byte(binArraySeed), pool[:], int64Bytes(index)}, [32]byte(program))
		if err != nil {
			return nil, err
		}
		binAccounts = append(binAccounts, solana.NewAccountMeta(solana.PublicKey(address), true, false))
	}
	// This PDA uses the SDK's canonical `bitmap` seed.  Using the account type
	// name here derives an unrelated address which may be occupied by another
	// program and then gets interpreted as the optional bitmap account.
	bitmap, _, err := solanaadapter.FindProgramAddress([][]byte{[]byte("bitmap"), pool[:]}, [32]byte(program))
	if err != nil {
		return nil, err
	}
	bitmapAccount, err := network.ReadAccount(ctx, solana.PublicKey(bitmap).String())
	if err != nil {
		return nil, fmt.Errorf("read Meteora bitmap extension: %w", err)
	}
	metas := solana.AccountMetaSlice{
		solana.NewAccountMeta(pool, true, false),
	}
	// Anchor's optional accounts are represented by a program-ID placeholder
	// when absent; omitting the meta shifts every following account one slot to
	// the left.  Some RPCs return a non-empty account for the derived address
	// even when that address is occupied by another program, so use the same
	// placeholder in that case.
	bitmapMeta := solana.PublicKey(program)
	if len(bitmapAccount.Data) > 0 && bitmapAccount.Owner == program.String() {
		bitmapMeta = solana.PublicKey(bitmap)
	}
	metas = append(metas, solana.NewAccountMeta(bitmapMeta, bitmapMeta != program, false))
	metas = append(metas,
		solana.NewAccountMeta(reserveX, true, false),
		solana.NewAccountMeta(reserveY, true, false),
		solana.NewAccountMeta(mustATA(owner, inMint), true, false),
		solana.NewAccountMeta(mustATA(owner, outMint), true, false),
		solana.NewAccountMeta(xMint, false, false),
		solana.NewAccountMeta(yMint, false, false),
		solana.NewAccountMeta(oracle, true, false),
		// Optional host_fee_in placeholder (see the optional bitmap above).
		solana.NewAccountMeta(program, false, false),
		solana.NewAccountMeta(owner, false, true),
		solana.NewAccountMeta(solana.TokenProgramID, false, false),
		solana.NewAccountMeta(solana.TokenProgramID, false, false),
		solana.NewAccountMeta(solana.MustPublicKeyFromBase58(memoProgramAddress), false, false),
	)
	eventAuthority, _, err := solanaadapter.FindProgramAddress([][]byte{[]byte(eventAuthoritySeed)}, [32]byte(program))
	if err != nil {
		return nil, err
	}
	metas = append(metas, solana.NewAccountMeta(solana.PublicKey(eventAuthority), false, false), solana.NewAccountMeta(program, false, false))
	metas = append(metas, binAccounts...)
	if !amountIn.IsUint64() || !minimumOut.IsUint64() {
		return nil, fmt.Errorf("meteora simulation amounts exceed uint64")
	}
	data := make([]byte, 8+8+8+4)
	decoded, err := hexDecode(swap2Discriminator)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora swap discriminator: %w", err)
	}
	copy(data[:8], decoded)
	binary.LittleEndian.PutUint64(data[8:16], amountIn.Uint64())
	binary.LittleEndian.PutUint64(data[16:24], minimumOut.Uint64())
	// RemainingAccountsInfo is a struct containing a Borsh vec. There are no
	// transfer-hook accounts for the ordinary SPL-token path.
	binary.LittleEndian.PutUint32(data[24:28], 0)
	hashText, err := network.LatestBlockhash(ctx)
	if err != nil {
		return nil, err
	}
	hashBytes, err := solanaadapter.DecodePublicKey(hashText)
	if err != nil {
		return nil, fmt.Errorf("decode Meteora blockhash: %w", err)
	}
	hash := solana.HashFromBytes(hashBytes[:])
	instruction := solana.NewInstruction(program, metas, data)
	tx, err := solana.NewTransaction([]solana.Instruction{instruction}, hash, solana.TransactionPayer(owner))
	if err != nil {
		return nil, fmt.Errorf("compile Meteora simulation transaction: %w", err)
	}
	if config.ComputeLimit > 0 {
		_ = config.ComputeLimit // The default limit is safer than adding an extra account/message instruction here.
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialize Meteora simulation transaction: %w", err)
	}
	return raw, nil
}

func publicKeyAt(data []byte, offset int) solana.PublicKey {
	var key solana.PublicKey
	if offset >= 0 && offset+32 <= len(data) {
		copy(key[:], data[offset:offset+32])
	}
	return key
}

func mustATA(owner, mint solana.PublicKey) solana.PublicKey {
	ata, _, err := solana.FindAssociatedTokenAddress(owner, mint)
	if err != nil {
		return solana.PublicKey{}
	}
	return ata
}

func hexDecode(value string) ([]byte, error) {
	result := make([]byte, len(value)/2)
	for i := range result {
		var high, low byte
		for _, pair := range []struct {
			index  int
			target *byte
		}{{i * 2, &high}, {i*2 + 1, &low}} {
			digit := value[pair.index]
			switch {
			case digit >= '0' && digit <= '9':
				*pair.target = digit - '0'
			case digit >= 'a' && digit <= 'f':
				*pair.target = digit - 'a' + 10
			case digit >= 'A' && digit <= 'F':
				*pair.target = digit - 'A' + 10
			default:
				return nil, fmt.Errorf("invalid hex discriminator")
			}
		}
		result[i] = high<<4 | low
	}
	return result, nil
}
