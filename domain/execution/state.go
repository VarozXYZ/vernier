package execution

import "fmt"

const (
	StateNoExecution TechnicalState = "no_execution_proven"
)

const (
	EconomicSettled  EconomicState = "settled"
	EconomicReleased EconomicState = "released"
)

// ValidateTechnicalTransition centralizes the allowed durable state changes.
// Reapplying the current state is idempotent.
func ValidateTechnicalTransition(from, to TechnicalState) error {
	if from == to {
		return nil
	}
	allowed := map[TechnicalState]map[TechnicalState]struct{}{
		StatePrepared: {
			StateBroadcastRejected: {}, StateBroadcastPossible: {}, StateOutcomeUnknown: {},
			StateConfirmedSuccess: {}, StateConfirmedRevert: {}, StateNoExecution: {},
			StateManualIntervention: {},
		},
		StateCommitted: {
			StateBroadcastRejected: {}, StateBroadcastPossible: {}, StateOutcomeUnknown: {},
			StateConfirmedSuccess: {}, StateConfirmedRevert: {}, StateNoExecution: {},
			StateManualIntervention: {},
		},
		StateBroadcastPossible: {
			StateConfirmedSuccess: {}, StateConfirmedRevert: {}, StateOutcomeUnknown: {},
			StateNoExecution: {}, StateManualIntervention: {},
		},
		StateOutcomeUnknown: {
			StateConfirmedSuccess: {}, StateConfirmedRevert: {}, StateNoExecution: {},
			StateManualIntervention: {},
		},
		StateBroadcastRejected: {
			StateNoExecution: {}, StateManualIntervention: {},
		},
	}
	if _, ok := allowed[from][to]; !ok {
		return fmt.Errorf("invalid technical transition %q -> %q", from, to)
	}
	return nil
}

// ValidateEconomicTransition centralizes reservation and settlement changes.
// Reapplying the current state is idempotent.
func ValidateEconomicTransition(from, to EconomicState) error {
	if from == to {
		return nil
	}
	allowed := map[EconomicState]map[EconomicState]struct{}{
		EconomicReserved: {
			EconomicEffectVerified: {}, EconomicEffectMismatch: {}, EconomicExposureOpen: {},
			EconomicReleased: {},
		},
		EconomicEffectVerified: {
			EconomicSettled: {}, EconomicExposureOpen: {},
		},
		EconomicEffectMismatch: {
			EconomicExposureOpen: {},
		},
	}
	if _, ok := allowed[from][to]; !ok {
		return fmt.Errorf("invalid economic transition %q -> %q", from, to)
	}
	return nil
}
