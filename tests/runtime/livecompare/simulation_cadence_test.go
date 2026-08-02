package livecompare_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

func TestSimulationCadenceCoalescesWhileRateLimited(t *testing.T) {
	start := time.Unix(100, 0)
	cadence := livecompare.NewSimulationCadence(time.Second)
	if ok, _ := cadence.Request(start); !ok {
		t.Fatal("first simulation was not immediate")
	}
	if ok, _ := cadence.Request(start.Add(100 * time.Millisecond)); ok {
		t.Fatal("simulation overlapped active round")
	}
	cadence.Finished(start.Add(200 * time.Millisecond))
	if ok, wait := cadence.PendingAt(start.Add(500 * time.Millisecond)); ok || wait <= 0 {
		t.Fatalf("pending simulation started too early: start=%t wait=%s", ok, wait)
	}
	if ok, wait := cadence.PendingAt(start.Add(1*time.Second + 1*time.Nanosecond)); !ok || wait != 0 {
		t.Fatalf("pending simulation was not released after interval: start=%t wait=%s", ok, wait)
	}
}

func TestSimulationCadenceDoesNotRepeatWithoutPendingSnapshot(t *testing.T) {
	start := time.Unix(100, 0)
	cadence := livecompare.NewSimulationCadence(time.Second)
	if ok, _ := cadence.Request(start); !ok {
		t.Fatal("first simulation was not immediate")
	}
	cadence.Finished(start.Add(200 * time.Millisecond))
	if ok, wait := cadence.PendingAt(start.Add(2 * time.Second)); ok || wait != 0 {
		t.Fatalf("cadence repeated without pending snapshot: start=%t wait=%s", ok, wait)
	}
}
