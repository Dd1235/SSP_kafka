package sysperf

import (
	"context"
	"testing"
	"time"
)

func TestSummaryAggregatesSamples(t *testing.T) {
	t.Parallel()

	s := &Sampler{samples: []Sample{
		{CPUFractionPct: 10, Goroutines: 3, HeapAllocMB: 5, HeapInUseMB: 6, NumGC: 1, GCPauseTotalMs: 2},
		{CPUFractionPct: 30, Goroutines: 5, HeapAllocMB: 7, HeapInUseMB: 8, NumGC: 2, GCPauseTotalMs: 4},
	}}

	got := s.Summary()
	if got.Count != 2 || got.AvgCPUPct != 20 || got.MaxCPUPct != 30 {
		t.Fatalf("cpu summary = %+v", got)
	}
	if got.AvgGoroutines != 4 || got.MaxGoroutines != 5 {
		t.Fatalf("goroutine summary = %+v", got)
	}
	if got.MaxHeapAllocMB != 7 || got.MaxHeapInUseMB != 8 {
		t.Fatalf("heap summary = %+v", got)
	}
	if got.NumGC != 2 || got.GCPauseTotalMs != 4 {
		t.Fatalf("gc summary = %+v", got)
	}
}

func TestSummaryEmpty(t *testing.T) {
	t.Parallel()

	if got := (&Sampler{}).Summary(); got != (Summary{}) {
		t.Fatalf("summary = %+v", got)
	}
}

func TestSamplerLifecycle(t *testing.T) {
	s := NewSampler(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)

	time.Sleep(10 * time.Millisecond)
	cancel()
	s.Wait()

	if len(s.Samples()) == 0 {
		t.Fatal("no samples captured")
	}
	if s.Summary().Count != len(s.Samples()) {
		t.Fatalf("summary count = %d samples = %d", s.Summary().Count, len(s.Samples()))
	}
}
