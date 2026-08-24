package livecanary

import (
	"context"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type ExecutableArtifactSource interface {
	TakeExecutableArtifact(market.Quote) (executionport.Artifact, bool)
}

// RetainedArtifactValidator consumes the exact provider build produced by the
// Research validation round. It only retargets operational identity fields;
// route, calldata, output and build metadata are immutable.
type RetainedArtifactValidator struct {
	Source              ExecutableArtifactSource
	AllowedDestinations []common.Address
}

func (v RetainedArtifactValidator) Validate(_ context.Context, request executionport.ValidationRequest) (executionport.Artifact, error) {
	if v.Source == nil {
		return executionport.Artifact{}, fmt.Errorf("retained executable artifact source is unavailable")
	}
	artifact, ok := v.Source.TakeExecutableArtifact(request.Discovery)
	if !ok {
		return executionport.Artifact{}, fmt.Errorf("exact Research build is unavailable or already consumed")
	}
	if artifact.ValidatedQuote.Market != request.Leg.Market ||
		artifact.ValidatedQuote.AmountIn.Token() != request.Leg.Input.Token() ||
		artifact.ValidatedQuote.AmountIn.Units().Cmp(request.Leg.Input.Units()) != 0 {
		return executionport.Artifact{}, fmt.Errorf("retained Research build does not match the fixed Live leg")
	}
	to := strings.TrimSpace(artifact.Metadata["to"])
	if !common.IsHexAddress(to) || !allowedDestination(common.HexToAddress(to), v.AllowedDestinations) {
		return executionport.Artifact{}, fmt.Errorf("retained Research build destination is not allowlisted")
	}
	artifact.Leg = request.Leg
	artifact.Leg.ExpectedOutput = artifact.ValidatedQuote.AmountOut
	return artifact, nil
}

func allowedDestination(candidate common.Address, allowlist []common.Address) bool {
	if candidate == (common.Address{}) || len(allowlist) == 0 {
		return false
	}
	for _, allowed := range allowlist {
		if candidate == allowed && allowed != (common.Address{}) {
			return true
		}
	}
	return false
}

var _ executionport.Validator = RetainedArtifactValidator{}
