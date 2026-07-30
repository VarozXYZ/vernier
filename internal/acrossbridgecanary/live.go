package acrossbridgecanary

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type LiveServiceConfig struct {
	Configuration    configuration.ParsedConfig
	Client           *across.Client
	StorePath        string
	Timeout          time.Duration
	Accounts         map[market.ChainID]domainexecution.AccountID
	NativeAssets     map[market.ChainID]market.AssetID
	NonceCoordinator chainport.EVMNonceCoordinator
	Output           io.Writer
}

type LiveService struct {
	config LiveServiceConfig
}

type CostApproval struct {
	Approval across.Approval
	Request  across.ApprovalRequest
}

func NewLiveService(config LiveServiceConfig) (*LiveService, error) {
	if config.Client == nil || config.StorePath == "" || config.Timeout <= 0 ||
		len(config.Accounts) != 2 {
		return nil, fmt.Errorf("across Live service configuration is incomplete")
	}
	if config.Output == nil {
		config.Output = io.Discard
	}
	return &LiveService{config: config}, nil
}

func (s *LiveService) Transfer(
	ctx context.Context,
	request domainexecution.SequentialStageRequest,
	journal executionport.SequentialJournal,
) (crosschainport.LiveTransferResult, error) {
	if err := request.Validate(); err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	selected, err := s.direction(request.Stage.SourceChain)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	approvalRequestValue, _, _, err := approvalRequest(
		s.config.Configuration, selected, request.Input.String(), "auto",
	)
	if err != nil {
		return crosschainport.LiveTransferResult{}, err
	}
	approval, err := s.config.Client.Approval(ctx, approvalRequestValue)
	if err != nil {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(executionport.DispositionRejected, err)
	}
	if len(approval.ApprovalTransactions) != 0 {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf("across requires an approval transaction"),
			)
	}
	hookResult := &liveExecutionResult{}
	hooks := &liveExecutionHooks{
		Request: request, Journal: journal, Result: hookResult,
		Accounts: map[string]domainexecution.AccountID{
			"solana":  s.accountForKind("solana"),
			"polygon": s.accountForKind("evm"),
		},
		NativeAssets:     s.config.NativeAssets,
		NonceCoordinator: s.config.NonceCoordinator,
	}
	err = executeArmed(
		ctx, s.config.Output, s.config.Configuration, s.config.Client,
		approvalRequestValue, approval, selected, s.config.StorePath,
		s.config.Timeout, hooks,
	)
	if err != nil {
		disposition := executionport.DispositionRejected
		if hookResult.SourceIdentity != "" {
			disposition = executionport.DispositionPossible
		}
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(disposition, err)
	}
	if hookResult.SourceIdentity == "" || hookResult.DestinationIdentity == "" ||
		hookResult.BalanceBefore == nil || hookResult.BalanceAfter == nil {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(
				executionport.DispositionPossible,
				fmt.Errorf("across settlement identity or balance evidence is incomplete"),
			)
	}
	outputUnits := new(big.Int).Sub(
		hookResult.BalanceAfter, hookResult.BalanceBefore,
	)
	output, err := market.NewTokenAmount(request.Stage.OutputToken, outputUnits)
	if err != nil || output.IsZero() {
		return crosschainport.LiveTransferResult{},
			executionport.NewStageError(
				executionport.DispositionPossible,
				fmt.Errorf("across destination output is not positive"),
			)
	}
	sourceIdentity := domainexecution.TransactionIdentity{
		Chain:   request.Stage.SourceChain,
		Account: s.config.Accounts[request.Stage.SourceChain],
		Hash:    hookResult.SourceIdentity,
	}
	if hookResult.SourceNonce != nil {
		nonce := *hookResult.SourceNonce
		sourceIdentity.Nonce = &nonce
	}
	destinationIdentity := domainexecution.TransactionIdentity{
		Chain:   request.Stage.DestinationChain,
		Account: s.config.Accounts[request.Stage.DestinationChain],
		Hash:    hookResult.DestinationIdentity,
	}
	costs := append(
		[]domainexecution.CostComponent(nil),
		hookResult.Costs...,
	)
	spreadUnits := new(big.Int).Sub(request.Input.Units(), outputUnits)
	if spreadUnits.Sign() > 0 {
		token, tokenOK := s.token(request.Stage.InputToken)
		if !tokenOK {
			return crosschainport.LiveTransferResult{},
				executionport.NewStageError(
					executionport.DispositionPossible,
					fmt.Errorf("across quote token metadata is unavailable"),
				)
		}
		spread, spreadErr := market.NewAssetQuantity(
			token.Asset,
			new(big.Rat).SetFrac(
				spreadUnits,
				new(big.Int).Exp(
					big.NewInt(10),
					big.NewInt(int64(token.Decimals)),
					nil,
				),
			),
		)
		if spreadErr != nil {
			return crosschainport.LiveTransferResult{}, spreadErr
		}
		costs = append(costs, domainexecution.CostComponent{
			Kind: "bridge_spread", Chain: request.Stage.SourceChain,
			Amount: spread, IncludedInOutput: true,
			Evidence: "across_actual_input_minus_destination_output",
		})
	}
	return crosschainport.LiveTransferResult{
		ActualInput: request.Input, ActualOutput: output,
		Costs:          costs,
		SourceIdentity: sourceIdentity, DestinationIdentity: destinationIdentity,
		ObservedAt: time.Now().UTC(), Evidence: hookResult.Evidence,
	}, nil
}

// CostApproval performs the same authenticated Across approval request used by
// execution, but never signs or broadcasts its artifact. It is intended for
// the background complete-flow cost oracle.
func (s *LiveService) CostApproval(
	ctx context.Context,
	source market.ChainID,
	amountUnits *big.Int,
) (CostApproval, error) {
	if amountUnits == nil || amountUnits.Sign() <= 0 {
		return CostApproval{}, fmt.Errorf("across cost amount must be positive")
	}
	selected, err := s.direction(source)
	if err != nil {
		return CostApproval{}, err
	}
	request, _, _, err := approvalRequest(
		s.config.Configuration, selected, amountUnits.String(), "auto",
	)
	if err != nil {
		return CostApproval{}, err
	}
	request.CostOnly = true
	approval, err := s.config.Client.Approval(ctx, request)
	if err != nil {
		return CostApproval{}, err
	}
	return CostApproval{Approval: approval, Request: request}, nil
}

func (s *LiveService) token(id market.TokenID) (market.Token, bool) {
	for _, configured := range s.config.Configuration.Markets {
		for _, token := range []market.Token{
			configured.Base.Token,
			configured.Quote.Token,
		} {
			if token.ID == id {
				return token, true
			}
		}
	}
	return market.Token{}, false
}

func (s *LiveService) direction(source market.ChainID) (direction, error) {
	for _, candidate := range s.config.Configuration.Markets {
		if market.ChainID(candidate.Chain) != source {
			continue
		}
		switch s.config.Configuration.Chains[candidate.Chain].Kind {
		case "solana":
			return solanaToEVM, nil
		case "evm":
			return evmToSolana, nil
		}
	}
	return "", fmt.Errorf("across source chain is not configured")
}

func (s *LiveService) accountForKind(kind string) domainexecution.AccountID {
	for id, chain := range s.config.Configuration.Chains {
		if chain.Kind == kind {
			return s.config.Accounts[market.ChainID(id)]
		}
	}
	return ""
}

var _ crosschainport.LiveTransferService = (*LiveService)(nil)
