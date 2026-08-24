package costing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type QuoteConversionWorkerConfig struct {
	Provider        quoteport.ConversionProvider
	Input           market.TokenAmount
	OutputToken     market.TokenID
	RefreshInterval time.Duration
	InitialDelay    time.Duration
	TTL             time.Duration
	Clock           func() time.Time
	OnUpdate        func()
	OnError         func(error)
	OnSuccess       func(market.QuoteConversionSnapshot)
}

// QuoteConversionWorker is single-flight by construction: the refresh delay
// begins only after the previous provider response has completed.
type QuoteConversionWorker struct {
	config  QuoteConversionWorkerConfig
	mu      sync.RWMutex
	current market.QuoteConversionSnapshot
	err     error
}

func NewQuoteConversionWorker(config QuoteConversionWorkerConfig) (*QuoteConversionWorker, error) {
	if config.Provider == nil || config.Input.IsZero() || config.OutputToken == "" ||
		config.OutputToken == config.Input.Token() || config.RefreshInterval <= 0 ||
		config.TTL < config.RefreshInterval || config.InitialDelay < 0 {
		return nil, fmt.Errorf("quote conversion worker configuration is incomplete")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &QuoteConversionWorker{config: config}, nil
}

func (w *QuoteConversionWorker) Run(ctx context.Context) error {
	if w.config.InitialDelay > 0 {
		timer := time.NewTimer(w.config.InitialDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	for {
		if err := w.Refresh(ctx); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		timer := time.NewTimer(w.config.RefreshInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (w *QuoteConversionWorker) Refresh(ctx context.Context) error {
	output, err := w.config.Provider.QuoteConversion(ctx, quoteport.ConversionRequest{
		Input: w.config.Input, OutputToken: w.config.OutputToken})
	now := w.config.Clock().UTC()
	w.mu.Lock()
	if err != nil {
		previous := w.err
		w.current, w.err = market.QuoteConversionSnapshot{}, err
		w.mu.Unlock()
		if w.config.OnError != nil && (previous == nil || previous.Error() != err.Error()) {
			w.config.OnError(err)
		}
		if w.config.OnUpdate != nil {
			w.config.OnUpdate()
		}
		return err
	}
	snapshot, snapshotErr := market.NewQuoteConversionSnapshot(w.config.Provider.ID(),
		w.config.Input, output, now, now.Add(w.config.TTL))
	w.current, w.err = snapshot, snapshotErr
	w.mu.Unlock()
	if snapshotErr == nil && w.config.OnSuccess != nil {
		w.config.OnSuccess(snapshot)
	}
	if w.config.OnUpdate != nil {
		w.config.OnUpdate()
	}
	return snapshotErr
}

func (w *QuoteConversionWorker) Current() (market.QuoteConversionSnapshot, error) {
	return w.CurrentAt(w.config.Clock().UTC())
}

func (w *QuoteConversionWorker) CurrentAt(at time.Time) (market.QuoteConversionSnapshot, error) {
	w.mu.RLock()
	snapshot, err := w.current, w.err
	w.mu.RUnlock()
	if err != nil {
		return market.QuoteConversionSnapshot{}, err
	}
	if !snapshot.ValidAt(at) {
		return market.QuoteConversionSnapshot{}, fmt.Errorf("quote conversion cache is stale")
	}
	return snapshot, nil
}

type quoteConversionPair struct{ input, output market.TokenID }

// QuoteConversionAlias projects one canonical on-chain conversion onto an
// economically equivalent token pair. This is useful when the provider quotes
// a bridge representation on one chain while evaluation uses the destination
// chain's token identity. Amounts are converted through the shared Asset so
// differing token decimals are handled conservatively.
type QuoteConversionAlias struct {
	Input, Output                   market.Token
	CanonicalInput, CanonicalOutput market.Token
}

type quoteConversionProjection struct {
	input, output                   market.Token
	canonicalInput, canonicalOutput market.Token
	canonical                       quoteConversionPair
}

// QuoteConversionBook is a read-only view over independently refreshed
// direction workers. Reads never trigger provider work.
type QuoteConversionBook struct {
	workers     map[quoteConversionPair]*QuoteConversionWorker
	projections map[quoteConversionPair]quoteConversionProjection
}

func NewQuoteConversionBook(workers ...*QuoteConversionWorker) (*QuoteConversionBook, error) {
	return NewQuoteConversionBookWithAliases(workers, nil)
}

func NewQuoteConversionBookWithAliases(workers []*QuoteConversionWorker,
	aliases []QuoteConversionAlias) (*QuoteConversionBook, error) {
	book := &QuoteConversionBook{workers: make(map[quoteConversionPair]*QuoteConversionWorker, len(workers)),
		projections: make(map[quoteConversionPair]quoteConversionProjection, len(aliases))}
	for _, worker := range workers {
		if worker == nil {
			return nil, fmt.Errorf("quote conversion worker is nil")
		}
		pair := quoteConversionPair{worker.config.Input.Token(), worker.config.OutputToken}
		if _, duplicate := book.workers[pair]; duplicate {
			return nil, fmt.Errorf("duplicate quote conversion worker")
		}
		book.workers[pair] = worker
	}
	for _, alias := range aliases {
		pair := quoteConversionPair{alias.Input.ID, alias.Output.ID}
		canonical := quoteConversionPair{alias.CanonicalInput.ID, alias.CanonicalOutput.ID}
		if alias.Input.ID == "" || alias.Output.ID == "" || alias.Input.ID == alias.Output.ID ||
			alias.Input.Asset == "" || alias.Input.Asset != alias.Output.Asset ||
			alias.CanonicalInput.ID == "" || alias.CanonicalOutput.ID == "" ||
			alias.CanonicalInput.Asset != alias.Input.Asset || alias.CanonicalOutput.Asset != alias.Output.Asset ||
			book.workers[canonical] == nil || book.workers[pair] != nil {
			return nil, fmt.Errorf("quote conversion alias is incomplete")
		}
		if _, duplicate := book.projections[pair]; duplicate {
			return nil, fmt.Errorf("duplicate quote conversion alias")
		}
		book.projections[pair] = quoteConversionProjection{input: alias.Input, output: alias.Output,
			canonicalInput: alias.CanonicalInput, canonicalOutput: alias.CanonicalOutput, canonical: canonical}
	}
	return book, nil
}

func (b *QuoteConversionBook) Snapshot(input, output market.TokenID, at time.Time) (market.QuoteConversionSnapshot, bool) {
	if b == nil {
		return market.QuoteConversionSnapshot{}, false
	}
	pair := quoteConversionPair{input, output}
	worker := b.workers[pair]
	if worker == nil {
		projection, ok := b.projections[pair]
		if !ok {
			return market.QuoteConversionSnapshot{}, false
		}
		worker = b.workers[projection.canonical]
		snapshot, err := worker.CurrentAt(at)
		if err != nil {
			return market.QuoteConversionSnapshot{}, false
		}
		canonicalInput := worker.config.Input
		inputQuantity, err := canonicalInput.ToAssetQuantity(projection.canonicalInput)
		if err != nil {
			return market.QuoteConversionSnapshot{}, false
		}
		projectedInput, err := inputQuantity.ToTokenAmount(projection.input)
		if err != nil {
			return market.QuoteConversionSnapshot{}, false
		}
		outputQuantity, err := snapshot.Output.ToAssetQuantity(projection.canonicalOutput)
		if err != nil {
			return market.QuoteConversionSnapshot{}, false
		}
		projectedOutput, err := outputQuantity.ToTokenAmount(projection.output)
		if err != nil || projectedOutput.IsZero() {
			return market.QuoteConversionSnapshot{}, false
		}
		projected, err := market.NewQuoteConversionSnapshot(snapshot.Provider,
			projectedInput, projectedOutput, snapshot.ReceivedAt, snapshot.ExpiresAt)
		return projected, err == nil
	}
	snapshot, err := worker.CurrentAt(at)
	return snapshot, err == nil
}
