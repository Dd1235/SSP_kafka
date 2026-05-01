package lag

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// this doesn't touch kafka at all
// SimulationConfig describes a standalone offset simulation. ProducerRate
// advances broker end offsets. ConsumerRate advances committed offsets. Lag is
// then calculated through the same buildSample/summarizeSamples path used by
// the real Kafka poller.
type SimulationConfig struct {
	Partitions   int
	Duration     time.Duration
	PollInterval time.Duration

	ProducerRate float64
	ConsumerRate float64

	// Optional phase switch. Use -1 for phase rates to keep the base rate.
	PhaseAt           time.Duration
	PhaseProducerRate float64
	PhaseConsumerRate float64
}

func DefaultSimulationConfig() SimulationConfig {
	return SimulationConfig{
		Partitions:        4,
		Duration:          16 * time.Second,
		PollInterval:      time.Second,
		ProducerRate:      10000,
		ConsumerRate:      4000,
		PhaseAt:           8 * time.Second,
		PhaseProducerRate: -1,
		PhaseConsumerRate: 16000,
	}
}

func Visualize() {
	VisualizeSimulation(DefaultSimulationConfig())
}

func VisualizeSimulation(cfg SimulationConfig) []Sample {
	samples := Simulate(cfg)
	printSimulationConfig(normalizeSimulationConfig(cfg))
	printSamples(samples)
	printSimulationSummary(samples)
	return samples
}

func VisualizeLive(p *Poller) {
	if p == nil {
		fmt.Println("no lag poller")
		return
	}
	samples := p.Samples()
	fmt.Println("=== Live Lag Samples ===")
	printSamples(samples)
	printSimulationSummary(samples)
}

func Simulate(cfg SimulationConfig) []Sample {
	cfg = normalizeSimulationConfig(cfg)

	parts := make([]int32, cfg.Partitions)
	for i := range parts {
		parts[i] = int32(i)
	}

	produced := make([]float64, cfg.Partitions)
	consumed := make([]float64, cfg.Partitions)
	start := time.Unix(0, 0)

	var samples []Sample
	appendSample := func(elapsed time.Duration) {
		ends := make(map[int32]int64, len(parts))
		committed := make(map[int32]int64, len(parts))
		for i, part := range parts {
			end := roundOffset(produced[i])
			commit := roundOffset(consumed[i])
			if commit > end {
				commit = end
			}
			ends[part] = end
			committed[part] = commit
		}
		samples = append(samples, buildSample(start, start.Add(elapsed), parts, ends, committed))
	}

	appendSample(0)
	for elapsed := time.Duration(0); elapsed < cfg.Duration; {
		next := elapsed + cfg.PollInterval
		if next > cfg.Duration {
			next = cfg.Duration
		}
		advanceSimulationWindow(cfg, elapsed, next, produced, consumed)
		appendSample(next)
		elapsed = next
	}
	return samples
}

func normalizeSimulationConfig(cfg SimulationConfig) SimulationConfig {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 10 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ProducerRate < 0 {
		cfg.ProducerRate = 0
	}
	if cfg.ConsumerRate < 0 {
		cfg.ConsumerRate = 0
	}
	return cfg
}

func advanceSimulationWindow(cfg SimulationConfig, from, to time.Duration, produced, consumed []float64) {
	for from < to {
		segmentEnd := to
		if cfg.PhaseAt > 0 && from < cfg.PhaseAt && cfg.PhaseAt < to {
			segmentEnd = cfg.PhaseAt
		}

		producerRate, consumerRate := ratesAt(cfg, from)
		seconds := (segmentEnd - from).Seconds()
		producedPerPartition := producerRate * seconds / float64(cfg.Partitions)
		consumedPerPartition := consumerRate * seconds / float64(cfg.Partitions)

		for i := range produced {
			produced[i] += producedPerPartition
			consumed[i] += consumedPerPartition
			if consumed[i] > produced[i] {
				consumed[i] = produced[i]
			}
		}
		from = segmentEnd
	}
}

func ratesAt(cfg SimulationConfig, elapsed time.Duration) (producerRate, consumerRate float64) {
	producerRate = cfg.ProducerRate
	consumerRate = cfg.ConsumerRate
	if cfg.PhaseAt > 0 && elapsed >= cfg.PhaseAt {
		if cfg.PhaseProducerRate >= 0 {
			producerRate = cfg.PhaseProducerRate
		}
		if cfg.PhaseConsumerRate >= 0 {
			consumerRate = cfg.PhaseConsumerRate
		}
	}
	return producerRate, consumerRate
}

func roundOffset(v float64) int64 {
	if v <= 0 {
		return 0
	}
	return int64(math.Round(v))
}

func printSimulationConfig(cfg SimulationConfig) {
	fmt.Println("=== Lag Simulation ===")
	fmt.Printf("producer %.0f msg/s | consumer %.0f msg/s | partitions %d | poll %v | duration %v\n",
		cfg.ProducerRate, cfg.ConsumerRate, cfg.Partitions, cfg.PollInterval, cfg.Duration)
	if cfg.PhaseAt > 0 {
		phaseProducer, phaseConsumer := ratesAt(cfg, cfg.PhaseAt)
		fmt.Printf("phase at %v -> producer %.0f msg/s | consumer %.0f msg/s\n",
			cfg.PhaseAt, phaseProducer, phaseConsumer)
	}
	fmt.Println("lag = end_offset - committed_offset")
}

func printSamples(samples []Sample) {
	if len(samples) == 0 {
		fmt.Println("no samples")
		return
	}

	maxLag := int64(1)
	for _, s := range samples {
		if s.TotalLag > maxLag {
			maxLag = s.TotalLag
		}
	}

	fmt.Printf("\n%8s %12s %12s %12s %12s  %s\n",
		"elapsed", "end", "committed", "lag", "max_part", "shape")
	for _, s := range samples {
		fmt.Printf("%7.1fs %12d %12d %12d %12d  %s\n",
			s.ElapsedS, s.EndOffset, s.Committed, s.TotalLag, s.MaxLag, lagBar(s.TotalLag, maxLag, 32))
	}
}

func printSimulationSummary(samples []Sample) {
	sum := summarizeSamples(samples)
	fmt.Printf("\nmax lag: %d | final lag: %d | samples: %d\n",
		sum.MaxTotalLag, sum.FinalTotalLag, sum.SampleCount)
	if sum.RecoveryDrainRate > 0 {
		fmt.Printf("recovery drain rate: %.0f msg/s | time to drain: %.1fs\n",
			sum.RecoveryDrainRate, sum.TimeToDrainS)
	}
}

func lagBar(value, max int64, width int) string {
	if width <= 0 {
		return ""
	}
	if max <= 0 || value <= 0 {
		return ""
	}
	n := int(math.Round(float64(value) / float64(max) * float64(width)))
	if n < 1 {
		n = 1
	}
	if n > width {
		n = width
	}
	return strings.Repeat("#", n)
}
