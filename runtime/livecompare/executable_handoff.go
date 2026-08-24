package livecompare

import (
	"fmt"

	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func executableQuoteKey(quote market.Quote) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%d",
		quote.Source, quote.Market, quote.AmountIn.Token(),
		quote.AmountIn.Units(), quote.AmountOut.Units(), quote.QuotedAt.UnixNano(),
	)
}

func (r *Runner) retainExecutableArtifact(discovery, validated market.Quote, artifact executionport.Artifact) {
	r.executableMu.Lock()
	defer r.executableMu.Unlock()
	// Capacity one is intentional: blocked Live execution never accumulates
	// provider payloads or turns the hand-off into a stale candidate queue. The
	// forced canary retains the discovery candidate while normal Live receives
	// the revalued candidate, whose remote output comes from build. Both aliases
	// refer to the same single-use artifact.
	clear(r.executableArtifacts)
	r.executableArtifacts[executableQuoteKey(discovery)] = artifact
	r.executableArtifacts[executableQuoteKey(validated)] = artifact
}

// TakeExecutableArtifact atomically consumes the exact build retained by the
// Research validation that admitted quote. Payload bytes remain memory-only.
func (r *Runner) TakeExecutableArtifact(quote market.Quote) (executionport.Artifact, bool) {
	r.executableMu.Lock()
	defer r.executableMu.Unlock()
	key := executableQuoteKey(quote)
	artifact, ok := r.executableArtifacts[key]
	if ok {
		clear(r.executableArtifacts)
	}
	return artifact, ok
}
