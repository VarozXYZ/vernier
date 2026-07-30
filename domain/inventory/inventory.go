// Package inventory owns prefunded balances and in-flight reservations.
package inventory

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type ReservationID string

type Key struct {
	Chain   market.ChainID
	Account execution.AccountID
	Token   market.TokenID
}

type Requirement struct {
	Key    Key
	Amount market.TokenAmount
}

type Effect struct {
	Key   Key
	Delta *big.Int
}

type BalancePolicy struct {
	WalletBalance market.TokenAmount
	AllocationCap market.TokenAmount
	Target        market.TokenAmount
	Buffer        market.TokenAmount
	InFlightOut   market.TokenAmount
}

type Reservation struct {
	ID           ReservationID
	Operation    execution.OperationID
	requirements []Requirement
}

func NewReservation(id ReservationID, operation execution.OperationID, requirements []Requirement) (Reservation, error) {
	if id == "" || operation == "" || len(requirements) == 0 {
		return Reservation{}, fmt.Errorf("reservation identity, operation, and requirements are required")
	}
	for _, requirement := range requirements {
		if requirement.Key.Chain == "" || requirement.Key.Account == "" || requirement.Key.Token == "" ||
			requirement.Amount.Token() != requirement.Key.Token || requirement.Amount.IsZero() {
			return Reservation{}, fmt.Errorf("reservation contains an invalid requirement")
		}
	}
	return Reservation{
		ID: id, Operation: operation, requirements: append([]Requirement(nil), requirements...),
	}, nil
}

func (r Reservation) Requirements() []Requirement {
	return append([]Requirement(nil), r.requirements...)
}

// Inventory is the single mutable owner of configured prefunded balances.
type Inventory struct {
	mu             sync.Mutex
	balances       map[Key]*big.Int
	allocationCaps map[Key]*big.Int
	targets        map[Key]*big.Int
	buffers        map[Key]*big.Int
	inFlightOut    map[Key]*big.Int
	reserved       map[Key]*big.Int
	reservations   map[ReservationID]Reservation
	settled        map[ReservationID]string
}

func New(balances map[Key]market.TokenAmount) (*Inventory, error) {
	policies := make(map[Key]BalancePolicy, len(balances))
	for key, amount := range balances {
		zero, err := market.NewTokenAmount(key.Token, new(big.Int))
		if err != nil {
			return nil, err
		}
		unbounded, err := market.NewTokenAmount(
			key.Token,
			new(big.Int).Sub(
				new(big.Int).Lsh(big.NewInt(1), 256),
				big.NewInt(1),
			),
		)
		if err != nil {
			return nil, err
		}
		policies[key] = BalancePolicy{
			WalletBalance: amount, AllocationCap: unbounded,
			Target: amount, Buffer: zero, InFlightOut: zero,
		}
	}
	return NewWithPolicies(policies)
}

func NewWithPolicies(policies map[Key]BalancePolicy) (*Inventory, error) {
	result := &Inventory{
		balances:       make(map[Key]*big.Int, len(policies)),
		allocationCaps: make(map[Key]*big.Int, len(policies)),
		targets:        make(map[Key]*big.Int, len(policies)),
		buffers:        make(map[Key]*big.Int, len(policies)),
		inFlightOut:    make(map[Key]*big.Int, len(policies)),
		reserved:       make(map[Key]*big.Int),
		reservations:   make(map[ReservationID]Reservation), settled: make(map[ReservationID]string),
	}
	for key, policy := range policies {
		if key.Chain == "" || key.Account == "" || key.Token == "" ||
			policy.WalletBalance.Token() != key.Token ||
			policy.AllocationCap.Token() != key.Token ||
			policy.Target.Token() != key.Token ||
			policy.Buffer.Token() != key.Token ||
			policy.InFlightOut.Token() != key.Token ||
			policy.Target.Units().Cmp(policy.AllocationCap.Units()) > 0 ||
			policy.Buffer.Units().Cmp(policy.AllocationCap.Units()) >= 0 {
			return nil, fmt.Errorf("inventory balance key and token amount do not match")
		}
		result.balances[key] = policy.WalletBalance.Units()
		result.allocationCaps[key] = policy.AllocationCap.Units()
		result.targets[key] = policy.Target.Units()
		result.buffers[key] = policy.Buffer.Units()
		result.inFlightOut[key] = policy.InFlightOut.Units()
		result.reserved[key] = new(big.Int)
	}
	return result, nil
}

func (i *Inventory) Reserve(id ReservationID, operation execution.OperationID, requirements []Requirement) (Reservation, error) {
	if i == nil || id == "" || operation == "" || len(requirements) == 0 {
		return Reservation{}, fmt.Errorf("reservation identity, operation, and requirements are required")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if existing, ok := i.reservations[id]; ok {
		if existing.Operation != operation {
			return Reservation{}, fmt.Errorf("reservation %q already belongs to another operation", id)
		}
		return existing, nil
	}
	if _, ok := i.settled[id]; ok {
		return Reservation{}, fmt.Errorf("reservation %q is already settled", id)
	}
	combined := make(map[Key]*big.Int)
	for _, requirement := range requirements {
		if requirement.Key.Token == "" || requirement.Amount.Token() != requirement.Key.Token || requirement.Amount.IsZero() {
			return Reservation{}, fmt.Errorf("reservation contains an invalid requirement")
		}
		if combined[requirement.Key] == nil {
			combined[requirement.Key] = new(big.Int)
		}
		combined[requirement.Key].Add(combined[requirement.Key], requirement.Amount.Units())
	}
	for key, amount := range combined {
		balance, ok := i.balances[key]
		if !ok {
			return Reservation{}, fmt.Errorf("inventory has no balance for chain %q token %q", key.Chain, key.Token)
		}
		available := i.effectiveAvailableLocked(key, balance)
		if available.Cmp(amount) < 0 {
			return Reservation{}, fmt.Errorf("insufficient unreserved inventory for chain %q token %q", key.Chain, key.Token)
		}
	}
	for key, amount := range combined {
		i.reserved[key].Add(i.reserved[key], amount)
	}
	reservation, err := NewReservation(id, operation, requirements)
	if err != nil {
		return Reservation{}, err
	}
	i.reservations[id] = reservation
	return reservation, nil
}

func (i *Inventory) Release(id ReservationID) error {
	if i == nil || id == "" {
		return fmt.Errorf("reservation ID is required")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	reservation, ok := i.reservations[id]
	if !ok {
		return nil
	}
	for _, requirement := range reservation.requirements {
		i.reserved[requirement.Key].Sub(i.reserved[requirement.Key], requirement.Amount.Units())
	}
	delete(i.reservations, id)
	return nil
}

func (i *Inventory) Available(key Key) (*big.Int, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	balance, ok := i.balances[key]
	if !ok {
		return nil, false
	}
	return i.effectiveAvailableLocked(key, balance), true
}

func (i *Inventory) effectiveAvailableLocked(key Key, wallet *big.Int) *big.Int {
	effective := new(big.Int).Set(wallet)
	if cap := i.allocationCaps[key]; cap != nil && effective.Cmp(cap) > 0 {
		effective.Set(cap)
	}
	effective.Sub(effective, i.buffers[key])
	effective.Sub(effective, i.reserved[key])
	effective.Sub(effective, i.inFlightOut[key])
	if effective.Sign() < 0 {
		effective.SetInt64(0)
	}
	return effective
}

// ObserveWalletBalance updates physical truth outside the decision hot path.
func (i *Inventory) ObserveWalletBalance(key Key, amount market.TokenAmount) error {
	if i == nil || amount.Token() != key.Token {
		return fmt.Errorf("wallet inventory observation is invalid")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.balances[key]; !ok {
		return fmt.Errorf("wallet inventory observation references an unknown balance")
	}
	i.balances[key] = amount.Units()
	return nil
}

func (i *Inventory) SetInFlightOut(key Key, amount market.TokenAmount) error {
	if i == nil || amount.Token() != key.Token {
		return fmt.Errorf("in-flight inventory amount is invalid")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.balances[key]; !ok {
		return fmt.Errorf("in-flight inventory references an unknown balance")
	}
	i.inFlightOut[key] = amount.Units()
	return nil
}

// AdjustReservation atomically replaces the reserved requirements. It is
// idempotent for an identical adjustment and never leaves a partial increase.
func (i *Inventory) AdjustReservation(
	id ReservationID,
	requirements []Requirement,
) (Reservation, error) {
	if i == nil || id == "" || len(requirements) == 0 {
		return Reservation{}, fmt.Errorf("reservation adjustment is incomplete")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	current, ok := i.reservations[id]
	if !ok {
		return Reservation{}, fmt.Errorf("reservation %q does not exist", id)
	}
	replacement, err := NewReservation(id, current.Operation, requirements)
	if err != nil {
		return Reservation{}, err
	}
	old := combinedRequirements(current.requirements)
	next := combinedRequirements(requirements)
	for key, wanted := range next {
		wallet, exists := i.balances[key]
		if !exists {
			return Reservation{}, fmt.Errorf("reservation adjustment references unknown inventory")
		}
		available := i.effectiveAvailableLocked(key, wallet)
		available.Add(available, old[key])
		if available.Cmp(wanted) < 0 {
			return Reservation{}, fmt.Errorf(
				"insufficient unreserved inventory for chain %q token %q",
				key.Chain, key.Token,
			)
		}
	}
	for key, amount := range old {
		i.reserved[key].Sub(i.reserved[key], amount)
	}
	for key, amount := range next {
		i.reserved[key].Add(i.reserved[key], amount)
	}
	i.reservations[id] = replacement
	return replacement, nil
}

func combinedRequirements(requirements []Requirement) map[Key]*big.Int {
	result := make(map[Key]*big.Int)
	for _, requirement := range requirements {
		if result[requirement.Key] == nil {
			result[requirement.Key] = new(big.Int)
		}
		result[requirement.Key].Add(
			result[requirement.Key], requirement.Amount.Units(),
		)
	}
	return result
}

// Settle applies demonstrated balance deltas for both legs atomically and
// releases their shared reservation.
func (i *Inventory) Settle(id ReservationID, effects []Effect) error {
	if i == nil || id == "" || len(effects) == 0 {
		return fmt.Errorf("settlement requires reservation and effects")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	fingerprint, err := effectFingerprint(effects)
	if err != nil {
		return err
	}
	reservation, ok := i.reservations[id]
	if !ok {
		if existing, settled := i.settled[id]; settled {
			if existing == fingerprint {
				return nil
			}
			return fmt.Errorf("reservation %q was settled with different effects", id)
		}
		return fmt.Errorf("reservation %q does not exist", id)
	}
	deltas := make(map[Key]*big.Int)
	for _, effect := range effects {
		if effect.Key.Chain == "" || effect.Key.Account == "" || effect.Key.Token == "" || effect.Delta == nil {
			return fmt.Errorf("settlement contains an invalid inventory effect")
		}
		if _, exists := i.balances[effect.Key]; !exists {
			return fmt.Errorf("settlement references unknown inventory balance")
		}
		if deltas[effect.Key] == nil {
			deltas[effect.Key] = new(big.Int)
		}
		deltas[effect.Key].Add(deltas[effect.Key], effect.Delta)
	}
	for key, delta := range deltas {
		if new(big.Int).Add(i.balances[key], delta).Sign() < 0 {
			return fmt.Errorf("settlement would make inventory negative")
		}
	}
	for key, delta := range deltas {
		i.balances[key].Add(i.balances[key], delta)
	}
	for _, requirement := range reservation.requirements {
		i.reserved[requirement.Key].Sub(i.reserved[requirement.Key], requirement.Amount.Units())
	}
	delete(i.reservations, id)
	i.settled[id] = fingerprint
	return nil
}

func effectFingerprint(effects []Effect) (string, error) {
	parts := make([]string, len(effects))
	for index, effect := range effects {
		if effect.Key.Chain == "" || effect.Key.Account == "" || effect.Key.Token == "" || effect.Delta == nil {
			return "", fmt.Errorf("settlement contains an invalid inventory effect")
		}
		parts[index] = strings.Join([]string{
			string(effect.Key.Chain), string(effect.Key.Account), string(effect.Key.Token), effect.Delta.String(),
		}, "/")
	}
	sort.Strings(parts)
	return strings.Join(parts, "|"), nil
}
