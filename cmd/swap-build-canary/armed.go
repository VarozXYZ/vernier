package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	solanago "github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	solanajsonrpc "github.com/gagliardetto/solana-go/rpc/jsonrpc"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/execution"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
)

const evmGasLimitMultiplierBPS = uint64(12_000)

type unusedSolanaReconciliation struct{}

func (unusedSolanaReconciliation) ReadSignatureStatus(
	context.Context,
	string,
) (solanaadapter.SignatureStatus, error) {
	return solanaadapter.SignatureStatus{}, errors.New(
		"manual canary reconciliation adapter must not be called",
	)
}

func (unusedSolanaReconciliation) ReadTransaction(
	context.Context,
	string,
) (solanaadapter.Transaction, error) {
	return solanaadapter.Transaction{}, errors.New(
		"manual canary reconciliation adapter must not be called",
	)
}

func (unusedSolanaReconciliation) CurrentBlockHeight(
	context.Context,
) (uint64, error) {
	return 0, errors.New("manual canary reconciliation adapter must not be called")
}

func (unusedSolanaReconciliation) IsBlockhashValid(
	context.Context,
	string,
) (bool, error) {
	return false, errors.New(
		"manual canary reconciliation adapter must not be called",
	)
}

func newSwapCanaryOperationID() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate swap canary operation ID: %w", err)
	}
	return "swap-" + hex.EncodeToString(entropy[:]), nil
}

func broadcastJupiterCanary(
	ctx context.Context,
	output io.Writer,
	armed armedCanary,
	client *solanarpc.Client,
	transaction *solanago.Transaction,
	raw []byte,
	signatureText string,
	blockhash string,
	lastValidBlockHeight uint64,
	expectedInput, expectedOutput *big.Int,
	selected selectedSwap,
	broadcastTransport string,
	senderManager *solanaadapter.TxManager,
) error {
	signature, err := solanago.SignatureFromBase58(signatureText)
	if err != nil {
		return fmt.Errorf("parse signed jupiter identity: %w", err)
	}
	if err := armed.Store.Prepared(
		ctx,
		armed.OperationID,
		"solana",
		signature.String(),
	); err != nil {
		return fmt.Errorf("persist solana identity before broadcast: %w", err)
	}
	fmt.Fprintf(
		output,
		"prepared chain=solana tx=%s persistence=wal_full\n",
		signature,
	)
	broadcastStarted := time.Now()
	broadcastEndpoint := ""
	switch broadcastTransport {
	case "helius-sender":
		if senderManager == nil {
			return fmt.Errorf("helius Sender manager is unavailable")
		}
		leg := execution.Leg{
			ID: "canary", Side: sideToLeg(selected.Side),
			Chain: selected.Input.Token.Chain, Account: "manual-canary",
			Market: selected.Market.ID,
		}
		result, sendErr := senderManager.Broadcast(
			ctx,
			chainport.PreparedTransaction{
				Leg: leg,
				Identity: execution.TransactionIdentity{
					Chain: selected.Input.Token.Chain, Account: "manual-canary",
					Hash: signature.String(), Blockhash: blockhash,
					LastValidBlockHeight: lastValidBlockHeight,
				},
				SignedPayload: append([]byte(nil), raw...),
				PreparedAt:    time.Now().UTC(),
			},
		)
		if sendErr != nil {
			status := "outcome_unknown"
			if result.Disposition == chainport.BroadcastRejected {
				status = "failed"
			}
			_ = armed.Store.Mark(
				context.WithoutCancel(ctx),
				armed.OperationID,
				status,
				sendErr,
			)
			return fmt.Errorf(
				"helius Sender broadcast failed with disposition %s: %w",
				result.Disposition,
				sendErr,
			)
		}
		if !result.Accepted ||
			result.Identity.Hash != signature.String() {
			mismatch := fmt.Errorf("helius Sender did not accept the expected identity")
			_ = armed.Store.Mark(
				context.WithoutCancel(ctx),
				armed.OperationID,
				"outcome_unknown",
				mismatch,
			)
			return mismatch
		}
		broadcastEndpoint = result.Endpoint
	case "rpc":
		maxRetries := uint(0)
		returned, sendErr := client.SendTransactionWithOpts(
			ctx,
			transaction,
			solanarpc.TransactionOpts{
				SkipPreflight:       false,
				PreflightCommitment: solanarpc.CommitmentConfirmed,
				MaxRetries:          &maxRetries,
			},
		)
		if sendErr != nil {
			var rpcError *solanajsonrpc.RPCError
			if errors.As(sendErr, &rpcError) && rpcError.Code == -32002 {
				_ = armed.Store.Mark(
					context.WithoutCancel(ctx),
					armed.OperationID,
					"failed",
					sendErr,
				)
				return fmt.Errorf(
					"solana preflight rejected the swap; it was not broadcast: %w",
					sendErr,
				)
			}
			_ = armed.Store.Mark(
				context.WithoutCancel(ctx),
				armed.OperationID,
				"outcome_unknown",
				sendErr,
			)
			return fmt.Errorf(
				"solana broadcast returned an uncertain result; do not resend automatically: %w",
				sendErr,
			)
		}
		if returned != signature {
			mismatch := fmt.Errorf(
				"solana RPC returned identity %s instead of %s",
				returned,
				signature,
			)
			_ = armed.Store.Mark(
				context.WithoutCancel(ctx),
				armed.OperationID,
				"outcome_unknown",
				mismatch,
			)
			return mismatch
		}
		broadcastEndpoint = "configured-rpc"
	default:
		return fmt.Errorf("unsupported Solana broadcast transport")
	}
	if err := armed.Store.Mark(
		ctx,
		armed.OperationID,
		"broadcast",
		nil,
	); err != nil {
		return err
	}
	fmt.Fprintf(
		output,
		"broadcast chain=solana transport=%s endpoint=%s tx=%s latency=%s\n",
		broadcastTransport,
		broadcastEndpoint,
		signature,
		formatDuration(time.Since(broadcastStarted)),
	)
	confirmationStarted := time.Now()
	if err := awaitSolanaSwapConfirmation(
		ctx,
		client,
		signature,
		armed.Timeout,
	); err != nil {
		_ = armed.Store.Mark(
			context.WithoutCancel(ctx),
			armed.OperationID,
			"outcome_unknown",
			err,
		)
		return err
	}
	if err := armed.Store.Mark(
		ctx,
		armed.OperationID,
		"confirmed",
		nil,
	); err != nil {
		return err
	}
	fmt.Fprintf(
		output,
		"confirmed chain=solana tx=%s latency=%s\n",
		signature,
		formatDuration(time.Since(confirmationStarted)),
	)
	metaCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	meta, err := readSolanaSwapMeta(metaCtx, client, signature)
	if err != nil {
		fmt.Fprintf(output, "settlement_warning chain=solana error=%q\n", err)
		return nil
	}
	inputMint, err := solanago.PublicKeyFromBase58(selected.Input.AddressText)
	if err != nil {
		return err
	}
	outputMint, err := solanago.PublicKeyFromBase58(selected.Output.AddressText)
	if err != nil {
		return err
	}
	owner := transaction.Message.AccountKeys[0]
	actualInput := spentSolanaToken(meta, owner, inputMint)
	actualOutput := receivedSolanaToken(meta, owner, outputMint)
	networkFee, additionalDebit, payerDebit :=
		solanaadapter.PayerLamportDebits(meta)
	fmt.Fprintf(
		output,
		"settlement chain=solana input_spent=%s %s output_received=%s %s "+
			"expected_input_units=%s expected_output_units=%s "+
			"network_fee_lamports=%d additional_debit_lamports=%d "+
			"payer_debit_lamports=%d\n",
		formatUnits(actualInput, selected.Input.Token.Decimals),
		selected.Input.Token.Symbol,
		formatUnits(actualOutput, selected.Output.Token.Decimals),
		selected.Output.Token.Symbol,
		expectedInput,
		expectedOutput,
		networkFee,
		additionalDebit,
		payerDebit,
	)
	return nil
}

func awaitSolanaSwapConfirmation(
	ctx context.Context,
	client *solanarpc.Client,
	signature solanago.Signature,
	timeout time.Duration,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		statuses, err := client.GetSignatureStatuses(waitCtx, true, signature)
		if err != nil {
			return fmt.Errorf("read solana signature status: %w", err)
		}
		if len(statuses.Value) == 1 && statuses.Value[0] != nil {
			status := statuses.Value[0]
			if status.Err != nil {
				return fmt.Errorf("solana swap %s failed: %v", signature, status.Err)
			}
			if status.ConfirmationStatus == solanarpc.ConfirmationStatusConfirmed ||
				status.ConfirmationStatus == solanarpc.ConfirmationStatusFinalized {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf(
				"solana swap %s outcome is unknown after confirmation timeout",
				signature,
			)
		case <-ticker.C:
		}
	}
}

func readSolanaSwapMeta(
	ctx context.Context,
	client *solanarpc.Client,
	signature solanago.Signature,
) (*solanarpc.TransactionMeta, error) {
	version := uint64(0)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := client.GetTransaction(
			ctx,
			signature,
			&solanarpc.GetTransactionOpts{
				Encoding:                       solanago.EncodingBase64,
				Commitment:                     solanarpc.CommitmentConfirmed,
				MaxSupportedTransactionVersion: &version,
			},
		)
		if err == nil && result != nil && result.Meta != nil {
			return result.Meta, nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return nil, fmt.Errorf("read solana swap metadata: %w", err)
			}
			return nil, fmt.Errorf("solana swap metadata is unavailable")
		case <-ticker.C:
		}
	}
}

func spentSolanaToken(
	meta *solanarpc.TransactionMeta,
	owner, mint solanago.PublicKey,
) *big.Int {
	pre := sumSolanaTokenBalances(meta.PreTokenBalances, owner, mint)
	post := sumSolanaTokenBalances(meta.PostTokenBalances, owner, mint)
	if pre.Cmp(post) <= 0 {
		return new(big.Int)
	}
	return new(big.Int).Sub(pre, post)
}

func receivedSolanaToken(
	meta *solanarpc.TransactionMeta,
	owner, mint solanago.PublicKey,
) *big.Int {
	pre := sumSolanaTokenBalances(meta.PreTokenBalances, owner, mint)
	post := sumSolanaTokenBalances(meta.PostTokenBalances, owner, mint)
	if post.Cmp(pre) <= 0 {
		return new(big.Int)
	}
	return new(big.Int).Sub(post, pre)
}

func sumSolanaTokenBalances(
	balances []solanarpc.TokenBalance,
	owner, mint solanago.PublicKey,
) *big.Int {
	total := new(big.Int)
	for _, balance := range balances {
		if balance.Owner == nil || !balance.Owner.Equals(owner) ||
			!balance.Mint.Equals(mint) || balance.UiTokenAmount == nil {
			continue
		}
		units, ok := new(big.Int).SetString(balance.UiTokenAmount.Amount, 10)
		if ok {
			total.Add(total, units)
		}
	}
	return total
}

func broadcastKyberCanary(
	ctx context.Context,
	output io.Writer,
	armed armedCanary,
	rpc *ethclient.Client,
	privateKey *ecdsa.PrivateKey,
	sender, router common.Address,
	value *big.Int,
	calldata []byte,
	estimatedGas uint64,
	selected selectedSwap,
	expectedOutput *big.Int,
) error {
	inputToken := common.HexToAddress(selected.Input.AddressText)
	outputToken := common.HexToAddress(selected.Output.AddressText)
	inputBefore, err := readERC20Uint(
		ctx,
		rpc,
		inputToken,
		"70a08231",
		sender,
	)
	if err != nil {
		return fmt.Errorf("read polygon input balance before swap: %w", err)
	}
	outputBefore, err := readERC20Uint(
		ctx,
		rpc,
		outputToken,
		"70a08231",
		sender,
	)
	if err != nil {
		return fmt.Errorf("read polygon output balance before swap: %w", err)
	}
	chainID, err := rpc.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("read polygon chain ID: %w", err)
	}
	if selected.Chain.ChainID == nil ||
		chainID.Cmp(selected.Chain.ChainID) != 0 {
		return fmt.Errorf(
			"polygon RPC chain ID %s does not match configured chain ID %s",
			chainID,
			selected.Chain.ChainID,
		)
	}
	nonce, err := rpc.PendingNonceAt(ctx, sender)
	if err != nil {
		return fmt.Errorf("read polygon nonce: %w", err)
	}
	tip, err := rpc.SuggestGasTipCap(ctx)
	if err != nil {
		return fmt.Errorf("read polygon priority fee: %w", err)
	}
	header, err := rpc.HeaderByNumber(ctx, nil)
	if err != nil || header == nil || header.BaseFee == nil {
		return fmt.Errorf("read polygon base fee: %w", err)
	}
	feeCap := new(big.Int).Add(
		new(big.Int).Mul(header.BaseFee, big.NewInt(2)),
		tip,
	)
	gasLimit := estimatedGas * evmGasLimitMultiplierBPS / 10_000
	if gasLimit < estimatedGas || gasLimit == 0 {
		return fmt.Errorf("invalid polygon gas limit")
	}
	nativeBalance, err := rpc.BalanceAt(ctx, sender, nil)
	if err != nil {
		return fmt.Errorf("read polygon native balance: %w", err)
	}
	maximumCost := new(big.Int).Add(
		new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), feeCap),
		value,
	)
	if nativeBalance.Cmp(maximumCost) < 0 {
		return fmt.Errorf(
			"polygon native balance %s cannot cover maximum transaction cost %s",
			nativeBalance,
			maximumCost,
		)
	}
	transaction := types.NewTx(&types.DynamicFeeTx{
		ChainID: chainID, Nonce: nonce, GasTipCap: tip, GasFeeCap: feeCap,
		Gas: gasLimit, To: &router, Value: value, Data: calldata,
	})
	signed, err := types.SignTx(
		transaction,
		types.LatestSignerForChainID(chainID),
		privateKey,
	)
	if err != nil {
		return fmt.Errorf("sign polygon swap: %w", err)
	}
	if err := armed.Store.Prepared(
		ctx,
		armed.OperationID,
		"polygon",
		signed.Hash().Hex(),
	); err != nil {
		return fmt.Errorf("persist polygon identity before broadcast: %w", err)
	}
	fmt.Fprintf(
		output,
		"prepared chain=polygon tx=%s nonce=%d gas=%d max_fee_wei=%s "+
			"persistence=wal_full\n",
		signed.Hash().Hex(),
		nonce,
		gasLimit,
		maximumCost,
	)
	broadcastStarted := time.Now()
	if err := rpc.SendTransaction(ctx, signed); err != nil {
		_ = armed.Store.Mark(
			context.WithoutCancel(ctx),
			armed.OperationID,
			"outcome_unknown",
			err,
		)
		return fmt.Errorf(
			"polygon broadcast returned an uncertain result; do not resend automatically: %w",
			err,
		)
	}
	if err := armed.Store.Mark(
		ctx,
		armed.OperationID,
		"broadcast",
		nil,
	); err != nil {
		return err
	}
	fmt.Fprintf(
		output,
		"broadcast chain=polygon tx=%s latency=%s\n",
		signed.Hash().Hex(),
		formatDuration(time.Since(broadcastStarted)),
	)
	confirmationStarted := time.Now()
	receipt, err := awaitKyberReceipt(
		ctx,
		rpc,
		signed.Hash(),
		armed.Timeout,
	)
	if err != nil {
		_ = armed.Store.Mark(
			context.WithoutCancel(ctx),
			armed.OperationID,
			"outcome_unknown",
			err,
		)
		return err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		reverted := fmt.Errorf("polygon swap reverted: %s", signed.Hash())
		_ = armed.Store.Mark(
			context.WithoutCancel(ctx),
			armed.OperationID,
			"failed",
			reverted,
		)
		return reverted
	}
	if err := armed.Store.Mark(
		ctx,
		armed.OperationID,
		"confirmed",
		nil,
	); err != nil {
		return err
	}
	networkFee := new(big.Int)
	if receipt.EffectiveGasPrice != nil {
		networkFee.Mul(
			new(big.Int).SetUint64(receipt.GasUsed),
			receipt.EffectiveGasPrice,
		)
	}
	fmt.Fprintf(
		output,
		"confirmed chain=polygon tx=%s block=%d gas_used=%d "+
			"network_fee_wei=%s latency=%s\n",
		signed.Hash().Hex(),
		receipt.BlockNumber.Uint64(),
		receipt.GasUsed,
		networkFee,
		formatDuration(time.Since(confirmationStarted)),
	)
	inputAfter, inputErr := readERC20Uint(
		context.WithoutCancel(ctx),
		rpc,
		inputToken,
		"70a08231",
		sender,
	)
	outputAfter, outputErr := readERC20Uint(
		context.WithoutCancel(ctx),
		rpc,
		outputToken,
		"70a08231",
		sender,
	)
	if inputErr != nil || outputErr != nil {
		fmt.Fprintf(
			output,
			"settlement_warning chain=polygon input_error=%q output_error=%q\n",
			inputErr,
			outputErr,
		)
		return nil
	}
	actualInput := positiveDifference(inputBefore, inputAfter)
	actualOutput := positiveDifference(outputAfter, outputBefore)
	fmt.Fprintf(
		output,
		"settlement chain=polygon input_spent=%s %s output_received=%s %s "+
			"expected_input_units=%s expected_output_units=%s\n",
		formatUnits(actualInput, selected.Input.Token.Decimals),
		selected.Input.Token.Symbol,
		formatUnits(actualOutput, selected.Output.Token.Decimals),
		selected.Output.Token.Symbol,
		selected.Amount,
		expectedOutput,
	)
	return nil
}

func awaitKyberReceipt(
	ctx context.Context,
	client *ethclient.Client,
	hash common.Hash,
	timeout time.Duration,
) (*types.Receipt, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		receipt, err := client.TransactionReceipt(waitCtx, hash)
		if err == nil {
			return receipt, nil
		}
		if !errors.Is(err, geth.NotFound) {
			return nil, fmt.Errorf("read polygon receipt %s: %w", hash, err)
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf(
				"polygon swap %s outcome is unknown after confirmation timeout",
				hash,
			)
		case <-ticker.C:
		}
	}
}

func positiveDifference(larger, smaller *big.Int) *big.Int {
	if larger == nil || smaller == nil || larger.Cmp(smaller) <= 0 {
		return new(big.Int)
	}
	return new(big.Int).Sub(larger, smaller)
}
