package localquoteexperiment_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/cmd/localquoteexperiment"
)

func TestMeasurementScheduleCoversOneHourAtFiveMinuteIntervals(t *testing.T) {
	initializedAt := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	schedule, err := localquoteexperiment.MeasurementSchedule(initializedAt, 5*time.Minute, 12)
	if err != nil {
		t.Fatal(err)
	}
	if len(schedule) != 12 {
		t.Fatalf("got %d measurements, want 12", len(schedule))
	}
	if got := schedule[0].Sub(initializedAt); got != 5*time.Minute {
		t.Fatalf("first measurement offset=%s, want 5m", got)
	}
	if got := schedule[len(schedule)-1].Sub(initializedAt); got != time.Hour {
		t.Fatalf("last measurement offset=%s, want 1h", got)
	}
	for index := 1; index < len(schedule); index++ {
		if got := schedule[index].Sub(schedule[index-1]); got != 5*time.Minute {
			t.Fatalf("measurement %d interval=%s, want 5m", index+1, got)
		}
	}
}

func TestMeasurementScheduleRejectsInvalidConfiguration(t *testing.T) {
	if _, err := localquoteexperiment.MeasurementSchedule(time.Time{}, 5*time.Minute, 12); err == nil {
		t.Fatal("zero initialization time was accepted")
	}
	if _, err := localquoteexperiment.MeasurementSchedule(time.Now(), 0, 12); err == nil {
		t.Fatal("zero interval was accepted")
	}
	if _, err := localquoteexperiment.MeasurementSchedule(time.Now(), 5*time.Minute, 0); err == nil {
		t.Fatal("zero samples were accepted")
	}
}
