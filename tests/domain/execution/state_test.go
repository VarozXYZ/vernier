package execution_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/domain/execution"
)

func TestExecutionStateTransitionsRejectRegressions(t *testing.T) {
	if err := execution.ValidateTechnicalTransition(
		execution.StatePrepared, execution.StateBroadcastPossible,
	); err != nil {
		t.Fatal(err)
	}
	if err := execution.ValidateTechnicalTransition(
		execution.StateConfirmedSuccess, execution.StatePrepared,
	); err == nil {
		t.Fatal("terminal technical state regressed")
	}
	if err := execution.ValidateEconomicTransition(
		execution.EconomicReserved, execution.EconomicEffectVerified,
	); err != nil {
		t.Fatal(err)
	}
	if err := execution.ValidateEconomicTransition(
		execution.EconomicSettled, execution.EconomicReserved,
	); err == nil {
		t.Fatal("settled economic state regressed")
	}
}
