package livecanary

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

const (
	defaultBalanceReconcileInterval = time.Minute
	defaultBalanceAlertInterval     = 5 * time.Minute
)

type PhysicalBalanceReader func(context.Context) (*big.Int, error)

type BalanceManagerConfig struct {
	Balances      []configuration.ResolvedInventoryBalance
	Readers       map[inventory.Key]PhysicalBalanceReader
	NativeTokens  map[market.ChainID]market.Token
	Accounts      map[market.ChainID]execution.AccountID
	Gate          *RuntimeGate
	Notifier      *LiveNotifier
	Logger        *slog.Logger
	Output        io.Writer
	PollInterval  time.Duration
	AlertInterval time.Duration
	Clock         func() time.Time
}

type balanceShortage struct {
	key       inventory.Key
	required  *big.Int
	available *big.Int
	lastSent  time.Time
}

// InsufficientLocalBalanceError is a definitive local admission rejection.
// It is intentionally raised before quote/build/simulation I/O.
type InsufficientLocalBalanceError struct {
	Key       inventory.Key
	Required  *big.Int
	Available *big.Int
}

func (e *InsufficientLocalBalanceError) Error() string {
	return fmt.Sprintf(
		"local balance is insufficient for chain %q token %q: available_units=%s required_units=%s",
		e.Key.Chain,
		e.Key.Token,
		e.Available,
		e.Required,
	)
}

// BalanceManager owns the process-local wallet projection. Network reads are
// limited to startup and idle background reconciliation; admission and stage
// preparation only consult memory.
type BalanceManager struct {
	config BalanceManagerConfig

	mu          sync.Mutex
	owner       *inventory.Inventory
	keysByToken map[market.ChainID]map[market.TokenID]inventory.Key
	seen        map[string]struct{}
	shortages   map[inventory.Key]balanceShortage
	version     uint64
}

func NewBalanceManager(config BalanceManagerConfig) (*BalanceManager, error) {
	if len(config.Balances) == 0 || len(config.Readers) == 0 ||
		len(config.Accounts) == 0 || config.Gate == nil {
		return nil, fmt.Errorf("balance manager configuration is incomplete")
	}
	if config.Output == nil {
		config.Output = io.Discard
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaultBalanceReconcileInterval
	}
	if config.AlertInterval <= 0 {
		config.AlertInterval = defaultBalanceAlertInterval
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &BalanceManager{
		config: config, keysByToken: make(map[market.ChainID]map[market.TokenID]inventory.Key),
		seen: make(map[string]struct{}), shortages: make(map[inventory.Key]balanceShortage),
	}, nil
}

func (m *BalanceManager) Warm(ctx context.Context) error {
	observed, err := m.readAll(ctx)
	if err != nil {
		return fmt.Errorf("warm wallet balances: %w", err)
	}
	policies := make(map[inventory.Key]inventory.BalancePolicy, len(observed))
	configured := make(map[inventory.Key]configuration.ResolvedInventoryBalance)
	for _, balance := range m.config.Balances {
		key := inventory.Key{
			Chain: market.ChainID(balance.Chain), Account: execution.AccountID(balance.Account),
			Token: balance.Token.ID,
		}
		configured[key] = balance
	}
	for key, amount := range observed {
		zero, _ := market.NewTokenAmount(key.Token, new(big.Int))
		capUnits := maxUint256()
		targetUnits := amount.Units()
		bufferUnits := new(big.Int)
		if balance, ok := configured[key]; ok {
			capUnits = ratToUnits(balance.AllocationCap, balance.Token.Decimals)
			targetUnits = ratToUnits(balance.Target, balance.Token.Decimals)
			bufferUnits = ratToUnits(balance.Buffer, balance.Token.Decimals)
		}
		capAmount, _ := market.NewTokenAmount(key.Token, capUnits)
		target, _ := market.NewTokenAmount(key.Token, targetUnits)
		buffer, _ := market.NewTokenAmount(key.Token, bufferUnits)
		policies[key] = inventory.BalancePolicy{
			WalletBalance: amount, AllocationCap: capAmount, Target: target,
			Buffer: buffer, InFlightOut: zero,
		}
		if m.keysByToken[key.Chain] == nil {
			m.keysByToken[key.Chain] = make(map[market.TokenID]inventory.Key)
		}
		m.keysByToken[key.Chain][key.Token] = key
	}
	owner, err := inventory.NewWithPolicies(policies)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.owner = owner
	m.version++
	m.mu.Unlock()
	fmt.Fprintf(m.config.Output, "balance_manager=ready balances=%d poll_interval=%s source=rpc_bootstrap\n", len(observed), m.config.PollInterval)
	return nil
}

func (m *BalanceManager) readAll(ctx context.Context) (map[inventory.Key]market.TokenAmount, error) {
	type result struct {
		key   inventory.Key
		units *big.Int
		err   error
	}
	results := make(chan result, len(m.config.Readers))
	for key, reader := range m.config.Readers {
		key, reader := key, reader
		go func() {
			units, err := reader(ctx)
			results <- result{key: key, units: units, err: err}
		}()
	}
	observed := make(map[inventory.Key]market.TokenAmount, len(m.config.Readers))
	for range m.config.Readers {
		result := <-results
		if result.err != nil {
			return nil, fmt.Errorf("read %s/%s: %w", result.key.Chain, result.key.Token, result.err)
		}
		if result.units == nil || result.units.Sign() < 0 {
			return nil, fmt.Errorf("read %s/%s returned an invalid balance", result.key.Chain, result.key.Token)
		}
		amount, err := market.NewTokenAmount(result.key.Token, result.units)
		if err != nil {
			return nil, err
		}
		observed[result.key] = amount
	}
	return observed, nil
}

func (m *BalanceManager) Available(chain market.ChainID, token market.TokenID) (*big.Int, error) {
	m.mu.Lock()
	owner := m.owner
	key, ok := m.keysByToken[chain][token]
	m.mu.Unlock()
	if owner == nil || !ok {
		return nil, fmt.Errorf("local balance is unavailable for %s/%s", chain, token)
	}
	value, ok := owner.Available(key)
	if !ok {
		return nil, fmt.Errorf("local balance is unavailable for %s/%s", chain, token)
	}
	return value, nil
}

func (m *BalanceManager) Snapshot(
	chain market.ChainID,
	token market.TokenID,
) (*big.Int, uint64, error) {
	m.mu.Lock()
	owner := m.owner
	key, ok := m.keysByToken[chain][token]
	version := m.version
	m.mu.Unlock()
	if owner == nil || !ok {
		return nil, 0, fmt.Errorf("local balance is unavailable for %s/%s", chain, token)
	}
	value, ok := owner.WalletBalance(key)
	if !ok {
		return nil, 0, fmt.Errorf("local balance is unavailable for %s/%s", chain, token)
	}
	return value, version, nil
}

func (m *BalanceManager) SpendableReader(chain market.ChainID) SpendableBalanceReader {
	return SpendableBalanceReaderFunc(func(_ context.Context, token market.TokenID) (*big.Int, error) {
		return m.Available(chain, token)
	})
}

func (m *BalanceManager) Admit(plan execution.SequentialPlan) error {
	requirements, err := m.planRequirements(plan)
	if err != nil {
		return err
	}
	for _, requirement := range requirements {
		available, availableErr := m.Available(requirement.Key.Chain, requirement.Key.Token)
		if availableErr != nil {
			return availableErr
		}
		if available.Cmp(requirement.Amount.Units()) >= 0 {
			m.clearShortage(requirement.Key)
			continue
		}
		shortage := &InsufficientLocalBalanceError{
			Key: requirement.Key, Required: requirement.Amount.Units(), Available: available,
		}
		m.recordShortage(shortage)
		return shortage
	}
	return nil
}

func (m *BalanceManager) planRequirements(plan execution.SequentialPlan) ([]inventory.Requirement, error) {
	if len(plan.Stages) == 0 {
		return nil, fmt.Errorf("balance admission plan is incomplete")
	}
	buy := plan.Stages[0]
	buyKey, ok := m.key(buy.SourceChain, buy.InputToken)
	if !ok {
		return nil, fmt.Errorf("buy input balance is not managed")
	}
	result := []inventory.Requirement{{Key: buyKey, Amount: plan.InitialInput}}
	if plan.EffectivePolicy() != execution.PolicyPrefundedSequential &&
		plan.EffectivePolicy() != execution.PolicyPrefundedParallel {
		return result, nil
	}
	if plan.Opportunity.SelectedIndex < 0 || plan.Opportunity.SelectedIndex >= len(plan.Opportunity.Candidates) {
		return nil, fmt.Errorf("balance admission opportunity has no candidate")
	}
	candidate := plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex]
	if candidate.BuyQuote.AmountIn.IsZero() || candidate.BuyQuote.AmountOut.IsZero() {
		return nil, fmt.Errorf("balance admission buy quote is incomplete")
	}
	discoveryBase := candidate.BuyQuote.AmountOut
	if plan.EffectivePolicy() == execution.PolicyPrefundedParallel {
		if candidate.SellQuote.AmountIn.IsZero() {
			return nil, fmt.Errorf("balance admission sell quote is incomplete")
		}
		// Parallel execution fixes the sale independently from the purchase.
		// Admission must reserve that exact scaled discovery input rather than
		// assuming the purchase's expected output will fund it.
		discoveryBase = candidate.SellQuote.AmountIn
	}
	expected := new(big.Int).Quo(
		new(big.Int).Mul(discoveryBase.Units(), plan.InitialInput.Units()),
		candidate.BuyQuote.AmountIn.Units(),
	)
	if len(plan.Stages) < 2 {
		return nil, fmt.Errorf("balance admission plan is incomplete")
	}
	sell := plan.Stages[1]
	converted, err := m.convertUnits(discoveryBase.Token(), sell.InputToken, expected)
	if err != nil {
		return nil, err
	}
	sellAmount, err := market.NewTokenAmount(sell.InputToken, converted)
	if err != nil {
		return nil, err
	}
	sellKey, ok := m.key(sell.SourceChain, sell.InputToken)
	if !ok {
		return nil, fmt.Errorf("prefunded sell balance is not managed")
	}
	return append(result, inventory.Requirement{Key: sellKey, Amount: sellAmount}), nil
}

func (m *BalanceManager) convertUnits(from, to market.TokenID, units *big.Int) (*big.Int, error) {
	fromDecimals, fromOK := m.decimals(from)
	toDecimals, toOK := m.decimals(to)
	if !fromOK || !toOK {
		return nil, fmt.Errorf("token decimals are unavailable for local balance admission")
	}
	result := new(big.Int).Set(units)
	if toDecimals > fromDecimals {
		result.Mul(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(toDecimals-fromDecimals)), nil))
	} else if fromDecimals > toDecimals {
		result.Quo(result, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(fromDecimals-toDecimals)), nil))
	}
	return result, nil
}

func (m *BalanceManager) decimals(token market.TokenID) (uint8, bool) {
	for _, balance := range m.config.Balances {
		if balance.Token.ID == token {
			return balance.Token.Decimals, true
		}
	}
	for _, native := range m.config.NativeTokens {
		if native.ID == token {
			return native.Decimals, true
		}
	}
	return 0, false
}

func (m *BalanceManager) key(chain market.ChainID, token market.TokenID) (inventory.Key, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok := m.keysByToken[chain][token]
	return key, ok
}

func (m *BalanceManager) ObserveSettlement(settlement execution.SequentialStageSettlement) error {
	fingerprint := settlementFingerprint(settlement)
	m.mu.Lock()
	if _, ok := m.seen[fingerprint]; ok {
		m.mu.Unlock()
		return nil
	}
	owner := m.owner
	m.seen[fingerprint] = struct{}{}
	m.mu.Unlock()
	if owner == nil {
		return fmt.Errorf("balance manager is not warm")
	}
	source := settlement.Request.Stage.SourceChain
	destination := source
	if settlement.Request.Stage.DestinationChain != "" {
		destination = settlement.Request.Stage.DestinationChain
	}
	inputKey, inputOK := m.key(source, settlement.ActualInput.Token())
	outputKey, outputOK := m.key(destination, settlement.ActualOutput.Token())
	if !inputOK || !outputOK {
		return fmt.Errorf("settlement balance key is not managed")
	}
	effects := []inventory.Effect{
		{Key: inputKey, Delta: new(big.Int).Neg(settlement.ActualInput.Units())},
		{Key: outputKey, Delta: settlement.ActualOutput.Units()},
	}
	for _, cost := range settlement.Costs {
		if cost.Kind != "network_fee" && cost.Kind != "additional_payer_debit" {
			continue
		}
		native, ok := m.config.NativeTokens[cost.Chain]
		if !ok || native.Asset != cost.Amount.Asset() {
			continue
		}
		key, ok := m.key(cost.Chain, native.ID)
		if !ok {
			continue
		}
		units := ratToUnits(cost.Amount.Rat(), native.Decimals)
		effects = append(effects, inventory.Effect{Key: key, Delta: units.Neg(units)})
	}
	if err := owner.ApplyEffects(effects); err != nil {
		m.mu.Lock()
		delete(m.seen, fingerprint)
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.version++
	m.mu.Unlock()
	return nil
}

func (m *BalanceManager) ObserveRefuel(record executionport.RefuelRecord) error {
	quoteKey, quoteOK := m.key(record.Chain, record.Input.Token())
	native := m.config.NativeTokens[record.Chain]
	nativeKey, nativeOK := m.key(record.Chain, native.ID)
	if !quoteOK || !nativeOK || record.BalanceAfter.Asset() != native.Asset {
		return fmt.Errorf("refuel balance keys are not managed")
	}
	m.mu.Lock()
	owner := m.owner
	m.mu.Unlock()
	if owner == nil {
		return fmt.Errorf("balance manager is not warm")
	}
	if err := owner.ApplyEffects([]inventory.Effect{{
		Key: quoteKey, Delta: new(big.Int).Neg(record.Input.Units()),
	}}); err != nil {
		return err
	}
	after, err := market.NewTokenAmount(
		native.ID,
		ratToUnits(record.BalanceAfter.Rat(), native.Decimals),
	)
	if err != nil {
		return err
	}
	if err := owner.ObserveWalletBalance(nativeKey, after); err != nil {
		return err
	}
	m.mu.Lock()
	m.version++
	m.mu.Unlock()
	return nil
}

func (m *BalanceManager) MarkSettlementsObserved(settlements []execution.SequentialStageSettlement) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, settlement := range settlements {
		m.seen[settlementFingerprint(settlement)] = struct{}{}
	}
}

func (m *BalanceManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcileOnce(ctx)
		}
	}
}

func (m *BalanceManager) reconcileOnce(ctx context.Context) {
	if m.config.Gate.State() != RuntimeGateIdle {
		return
	}
	observed, err := m.readAll(ctx)
	if err != nil {
		if m.config.Logger != nil {
			m.config.Logger.Warn("wallet balance reconciliation failed", "error", err)
		}
		return
	}
	// An operation may have acquired the gate while RPC reads were in flight.
	// In that case discard the snapshot; never overwrite projected settlements.
	if m.config.Gate.State() != RuntimeGateIdle {
		return
	}
	m.mu.Lock()
	owner := m.owner
	m.mu.Unlock()
	if owner == nil {
		return
	}
	keys := make([]inventory.Key, 0, len(observed))
	for key := range observed {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j])
	})
	for _, key := range keys {
		_ = owner.ObserveWalletBalance(key, observed[key])
		m.refreshShortage(key)
	}
	m.mu.Lock()
	m.version++
	m.mu.Unlock()
	fmt.Fprintf(m.config.Output, "balance_reconcile=ok balances=%d source=rpc_poll\n", len(observed))
}

func (m *BalanceManager) ReconcileChains(
	ctx context.Context,
	chains ...market.ChainID,
) error {
	wanted := make(map[market.ChainID]struct{}, len(chains))
	for _, chain := range chains {
		wanted[chain] = struct{}{}
	}
	m.mu.Lock()
	owner := m.owner
	m.mu.Unlock()
	if owner == nil {
		return fmt.Errorf("balance manager is not warm")
	}
	for key, reader := range m.config.Readers {
		if _, ok := wanted[key.Chain]; !ok {
			continue
		}
		units, err := reader(ctx)
		if err != nil {
			return fmt.Errorf("reconcile %s/%s: %w", key.Chain, key.Token, err)
		}
		amount, err := market.NewTokenAmount(key.Token, units)
		if err != nil {
			return err
		}
		if err := owner.ObserveWalletBalance(key, amount); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.version++
	m.mu.Unlock()
	return nil
}

func (m *BalanceManager) recordShortage(err *InsufficientLocalBalanceError) {
	now := m.config.Clock().UTC()
	m.mu.Lock()
	state := m.shortages[err.Key]
	state.key = err.Key
	state.required = new(big.Int).Set(err.Required)
	state.available = new(big.Int).Set(err.Available)
	shouldSend := state.lastSent.IsZero() || now.Sub(state.lastSent) >= m.config.AlertInterval
	if shouldSend {
		state.lastSent = now
	}
	m.shortages[err.Key] = state
	m.mu.Unlock()
	if shouldSend {
		m.notifyBalance(notificationport.LiveRuntimeBalanceInsufficient, state, now)
	}
}

func (m *BalanceManager) clearShortage(key inventory.Key) {
	m.mu.Lock()
	state, ok := m.shortages[key]
	if ok {
		delete(m.shortages, key)
	}
	m.mu.Unlock()
	if ok {
		m.notifyBalance(notificationport.LiveRuntimeBalanceRecovered, state, m.config.Clock().UTC())
	}
}

func (m *BalanceManager) refreshShortage(key inventory.Key) {
	m.mu.Lock()
	state, ok := m.shortages[key]
	m.mu.Unlock()
	if !ok {
		return
	}
	available, err := m.Available(key.Chain, key.Token)
	if err != nil {
		return
	}
	if available.Cmp(state.required) >= 0 {
		m.clearShortage(key)
		return
	}
	state.available = available
	m.recordShortage(&InsufficientLocalBalanceError{Key: key, Required: state.required, Available: available})
}

func (m *BalanceManager) notifyBalance(kind notificationport.LiveRuntimeEventKind, state balanceShortage, now time.Time) {
	if m.config.Notifier == nil {
		return
	}
	token := m.token(state.key.Chain, state.key.Token)
	symbol := token.Symbol
	if symbol == "" {
		symbol = string(state.key.Token)
	}
	m.config.Notifier.NotifyRuntime(notificationport.LiveRuntimeEvent{
		Kind: kind, Chain: string(state.key.Chain), Token: symbol,
		AvailableUnits: humanTokenUnits(state.available, token.Decimals),
		RequiredUnits:  humanTokenUnits(state.required, token.Decimals),
		OccurredAt:     now,
	})
}

func (m *BalanceManager) token(chain market.ChainID, id market.TokenID) market.Token {
	for _, balance := range m.config.Balances {
		if market.ChainID(balance.Chain) == chain && balance.Token.ID == id {
			return balance.Token
		}
	}
	if token := m.config.NativeTokens[chain]; token.ID == id {
		return token
	}
	return market.Token{ID: id, Chain: chain}
}

func humanTokenUnits(units *big.Int, decimals uint8) string {
	if units == nil {
		return "0"
	}
	return compactRat(
		new(big.Rat).SetFrac(
			units,
			new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil),
		),
		6,
	)
}

func settlementFingerprint(settlement execution.SequentialStageSettlement) string {
	destination := ""
	if settlement.DestinationIdentity != nil {
		destination = settlement.DestinationIdentity.Hash
	}
	return strings.Join([]string{
		string(settlement.Request.Operation), fmt.Sprint(settlement.Request.Stage.Ordinal),
		settlement.SourceIdentity.Hash, destination,
	}, "/")
}

func ratToUnits(value *big.Rat, decimals uint8) *big.Int {
	if value == nil {
		return new(big.Int)
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	return new(big.Int).Quo(new(big.Int).Mul(value.Num(), scale), value.Denom())
}

func maxUint256() *big.Int {
	return new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
}

type balanceProgressObserver struct {
	progress *ProgressObserver
	balances *BalanceManager
}

// balanceRecoveryDriver advances the local projection after each recovered
// stage, not only when the whole recovery completes. This is required when a
// later recovery stage consumes inventory delivered by the preceding one.
type balanceRecoveryDriver struct {
	driver   executionport.SequentialStageDriver
	balances *BalanceManager
}

func (d balanceRecoveryDriver) ExecuteStage(
	ctx context.Context,
	request execution.SequentialStageRequest,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	return d.driver.ExecuteStage(ctx, request, journal)
}

func (d balanceRecoveryDriver) RecoverStage(
	ctx context.Context,
	request execution.SequentialStageRequest,
	transactions []executionport.SequentialTransactionRecord,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	recovery, ok := d.driver.(executionport.SequentialRecoveryDriver)
	if !ok {
		return execution.SequentialStageSettlement{}, fmt.Errorf("recovery driver is unavailable")
	}
	settlement, err := recovery.RecoverStage(ctx, request, transactions, journal)
	if err != nil {
		return settlement, err
	}
	var balanceErr error
	if recoveredExistingIdentity(settlement, transactions) {
		chains := []market.ChainID{settlement.Request.Stage.SourceChain}
		if settlement.Request.Stage.DestinationChain != "" {
			chains = append(chains, settlement.Request.Stage.DestinationChain)
		}
		balanceErr = d.balances.ReconcileChains(ctx, chains...)
		d.balances.MarkSettlementsObserved([]execution.SequentialStageSettlement{settlement})
	} else {
		balanceErr = d.balances.ObserveSettlement(settlement)
	}
	if balanceErr != nil {
		return execution.SequentialStageSettlement{}, executionport.NewRecoveryError(
			executionport.RecoveryFailureBalanceMismatch,
			fmt.Errorf("reconcile recovered local balance settlement: %w", balanceErr),
		)
	}
	return settlement, nil
}

func recoveredExistingIdentity(
	settlement execution.SequentialStageSettlement,
	transactions []executionport.SequentialTransactionRecord,
) bool {
	for _, transaction := range transactions {
		if transaction.Identity.Hash == settlement.SourceIdentity.Hash {
			return true
		}
		if settlement.DestinationIdentity != nil &&
			transaction.Identity.Hash == settlement.DestinationIdentity.Hash {
			return true
		}
	}
	return false
}

func (o balanceProgressObserver) OperationStarted(operation execution.SequentialOperation, plan execution.SequentialPlan) {
	if o.progress != nil {
		o.progress.OperationStarted(operation, plan)
	}
}

func (o balanceProgressObserver) StageStarted(request execution.SequentialStageRequest) {
	if o.progress != nil {
		o.progress.StageStarted(request)
	}
}

func (o balanceProgressObserver) StageSettled(settlement execution.SequentialStageSettlement) {
	if err := o.balances.ObserveSettlement(settlement); err != nil && o.balances.config.Logger != nil {
		o.balances.config.Logger.Error("apply local balance settlement", "error", err)
	}
	if o.progress != nil {
		o.progress.StageSettled(settlement)
	}
}

func (o balanceProgressObserver) OperationFinished(operation execution.SequentialOperation, state execution.SequentialOperationState, result executionport.SequentialResult, err error) {
	if o.progress != nil {
		o.progress.OperationFinished(operation, state, result, err)
	}
}
