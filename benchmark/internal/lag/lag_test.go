package lag

import (
	"testing"
	"time"
)

func TestSummarizeSamplesEmpty(t *testing.T) {
	t.Parallel()

	if got := summarizeSamples(nil); got != (Summary{}) {
		t.Fatalf("summary = %+v", got)
	}
}

func TestSummarizeSamplesRecovery(t *testing.T) {
	t.Parallel()

	got := summarizeSamples([]Sample{
		{ElapsedS: 0, TotalLag: 0},
		{ElapsedS: 1, TotalLag: 100},
		{ElapsedS: 2.5, TotalLag: 0},
		{ElapsedS: 3, TotalLag: 10},
	})
	if got.MaxTotalLag != 100 || got.FinalTotalLag != 10 || got.SampleCount != 4 {
		t.Fatalf("summary headline = %+v", got)
	}
	if got.RecoveryDrainRate != 45 {
		t.Fatalf("RecoveryDrainRate = %v, want 45", got.RecoveryDrainRate)
	}
	if got.TimeToDrainS != 1.5 {
		t.Fatalf("TimeToDrainS = %v, want 1.5", got.TimeToDrainS)
	}
}

func TestBuildSampleComputesLagPerPartition(t *testing.T) {
	t.Parallel()

	start := time.Unix(10, 0)
	now := start.Add(2 * time.Second)
	got := buildSample(start, now, []int32{0, 1, 2},
		map[int32]int64{0: 10, 1: 5, 2: 7},
		map[int32]int64{0: 4, 1: -1, 2: 9})

	if got.ElapsedS != 2 || got.TotalLag != 11 || got.MaxLag != 6 {
		t.Fatalf("sample headline = %+v", got)
	}
	if got.PerPartition[0] != 6 || got.PerPartition[1] != 5 || got.PerPartition[2] != 0 {
		t.Fatalf("per partition = %#v", got.PerPartition)
	}
	if got.EndOffset != 22 || got.Committed != 13 {
		t.Fatalf("end/committed = %d/%d", got.EndOffset, got.Committed)
	}
}

func TestSimulateUsesRatesAndPhase(t *testing.T) {
	t.Parallel()

	got := Simulate(SimulationConfig{
		Partitions:        2,
		Duration:          4 * time.Second,
		PollInterval:      time.Second,
		ProducerRate:      100,
		ConsumerRate:      40,
		PhaseAt:           2 * time.Second,
		PhaseProducerRate: -1,
		PhaseConsumerRate: 160,
	})

	if len(got) != 5 {
		t.Fatalf("sample count = %d, want 5", len(got))
	}
	if got[1].TotalLag != 60 || got[2].TotalLag != 120 {
		t.Fatalf("growth samples = %d/%d, want 60/120", got[1].TotalLag, got[2].TotalLag)
	}
	if got[3].TotalLag != 60 || got[4].TotalLag != 0 {
		t.Fatalf("recovery samples = %d/%d, want 60/0", got[3].TotalLag, got[4].TotalLag)
	}
	if got[2].MaxLag != 60 {
		t.Fatalf("max partition lag = %d, want 60", got[2].MaxLag)
	}
}

func TestSamplesReturnsCopy(t *testing.T) {
	t.Parallel()

	p := &Poller{samples: []Sample{{TotalLag: 1}}}
	got := p.Samples()
	got[0].TotalLag = 99
	if p.samples[0].TotalLag != 1 {
		t.Fatal("Samples returned mutable internal slice")
	}
}
