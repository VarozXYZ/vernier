package live

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

var (
	NearInfiniteAllowance = new(big.Int).Lsh(big.NewInt(1), 255)
	MaximumAllowance      = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
)

type AllowanceRequirement struct {
	Chain   market.ChainID
	Token   market.TokenID
	Spender string
	Purpose string
}

func (r AllowanceRequirement) Validate() error {
	if r.Chain == "" || r.Token == "" || r.Spender == "" || r.Purpose == "" {
		return fmt.Errorf("allowance requirement is incomplete")
	}
	return nil
}

type AllowanceReader interface {
	Allowance(context.Context, AllowanceRequirement) (*big.Int, error)
}

// ApprovalWriter owns durable identity persistence, broadcast, confirmation,
// and reconciliation. An error with ConfirmedRevert=true proves that the
// approval had no token-state effect; all other failures remain blocking.
type ApprovalWriter interface {
	Approve(context.Context, AllowanceRequirement, *big.Int) error
}

type ApprovalError struct {
	ConfirmedRevert bool
	Err             error
}

func (e *ApprovalError) Error() string {
	if e == nil || e.Err == nil {
		return "approval failed"
	}
	return e.Err.Error()
}
func (e *ApprovalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type AllowanceResult struct {
	Requirement AllowanceRequirement
	Before      *big.Int
	After       *big.Int
	Changed     bool
	Reset       bool
	CheckedAt   time.Time
}

type AllowanceBootstrap struct {
	Reader         AllowanceReader
	Writer         ApprovalWriter
	Requirements   []AllowanceRequirement
	Revocations    []AllowanceRequirement
	Clock          func() time.Time
	VerifyTimeout  time.Duration
	VerifyInterval time.Duration
}

func (b AllowanceBootstrap) Ensure(ctx context.Context, armed bool) ([]AllowanceResult, error) {
	if b.Reader == nil || len(b.Requirements)+len(b.Revocations) == 0 {
		return nil, fmt.Errorf("allowance bootstrap requires a reader and allowlist")
	}
	if armed && b.Writer == nil {
		return nil, fmt.Errorf("armed allowance bootstrap requires a durable approval writer")
	}
	clock := b.Clock
	if clock == nil {
		clock = time.Now
	}
	results := make([]AllowanceResult, 0, len(b.Requirements))
	seen := make(map[string]struct{}, len(b.Requirements))
	for _, requirement := range b.Requirements {
		if err := requirement.Validate(); err != nil {
			return results, err
		}
		key := string(requirement.Chain) + "\x00" + string(requirement.Token) + "\x00" + requirement.Spender
		if _, duplicate := seen[key]; duplicate {
			return results, fmt.Errorf("allowance allowlist repeats %s/%s", requirement.Chain, requirement.Token)
		}
		seen[key] = struct{}{}
		before, err := b.Reader.Allowance(ctx, requirement)
		if err != nil || before == nil || before.Sign() < 0 {
			if err == nil {
				err = fmt.Errorf("invalid allowance value")
			}
			return results, fmt.Errorf("read %s allowance: %w", requirement.Purpose, err)
		}
		result := AllowanceResult{Requirement: requirement, Before: new(big.Int).Set(before),
			After: new(big.Int).Set(before), CheckedAt: clock().UTC()}
		if before.Cmp(NearInfiniteAllowance) >= 0 || !armed {
			results = append(results, result)
			continue
		}
		err = b.Writer.Approve(ctx, requirement, new(big.Int).Set(MaximumAllowance))
		if err != nil {
			var approval *ApprovalError
			if !errors.As(err, &approval) || !approval.ConfirmedRevert {
				return results, fmt.Errorf("approve %s: %w", requirement.Purpose, err)
			}
			result.Reset = true
			if resetErr := b.Writer.Approve(ctx, requirement, new(big.Int)); resetErr != nil {
				return results, fmt.Errorf("reset %s allowance: %w", requirement.Purpose, resetErr)
			}
			if maxErr := b.Writer.Approve(ctx, requirement, new(big.Int).Set(MaximumAllowance)); maxErr != nil {
				return results, fmt.Errorf("approve %s after reset: %w", requirement.Purpose, maxErr)
			}
		}
		verifyTimeout := b.VerifyTimeout
		if verifyTimeout <= 0 {
			verifyTimeout = 5 * time.Second
		}
		verifyInterval := b.VerifyInterval
		if verifyInterval <= 0 {
			verifyInterval = 100 * time.Millisecond
		}
		verifyCtx, cancel := context.WithTimeout(ctx, verifyTimeout)
		var after *big.Int
		for {
			after, err = b.Reader.Allowance(verifyCtx, requirement)
			if err == nil && after != nil && after.Cmp(NearInfiniteAllowance) >= 0 {
				break
			}
			select {
			case <-verifyCtx.Done():
				cancel()
				if err == nil {
					err = fmt.Errorf("confirmed allowance remains below near-infinite threshold: %w", verifyCtx.Err())
				}
				return results, fmt.Errorf("verify %s allowance: %w", requirement.Purpose, err)
			case <-time.After(verifyInterval):
			}
		}
		cancel()
		result.After, result.Changed = new(big.Int).Set(after), true
		result.CheckedAt = clock().UTC()
		results = append(results, result)
	}
	for _, requirement := range b.Revocations {
		if err := requirement.Validate(); err != nil {
			return results, err
		}
		key := string(requirement.Chain) + "\x00" + string(requirement.Token) + "\x00" + requirement.Spender
		if _, duplicate := seen[key]; duplicate {
			return results, fmt.Errorf("allowance is both required and revoked")
		}
		seen[key] = struct{}{}
		before, err := b.Reader.Allowance(ctx, requirement)
		if err != nil || before == nil || before.Sign() < 0 {
			if err == nil {
				err = fmt.Errorf("invalid allowance value")
			}
			return results, fmt.Errorf("read %s allowance: %w", requirement.Purpose, err)
		}
		result := AllowanceResult{Requirement: requirement, Before: new(big.Int).Set(before),
			After: new(big.Int).Set(before), CheckedAt: clock().UTC()}
		if before.Sign() == 0 || !armed {
			results = append(results, result)
			continue
		}
		if err := b.Writer.Approve(ctx, requirement, new(big.Int)); err != nil {
			return results, fmt.Errorf("revoke %s: %w", requirement.Purpose, err)
		}
		after, err := b.Reader.Allowance(ctx, requirement)
		if err != nil || after == nil || after.Sign() != 0 {
			return results, fmt.Errorf("verify %s revocation: allowance remains nonzero", requirement.Purpose)
		}
		result.After, result.Changed, result.CheckedAt = new(big.Int), true, clock().UTC()
		results = append(results, result)
	}
	return results, nil
}
