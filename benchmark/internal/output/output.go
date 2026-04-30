// Package output formats final results for humans (stdout) and machines (JSON).
//
// The JSON schema is stable; the human-readable report is the only thing that
// should change when you're iterating on presentation.
package output

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/lag"
	"kafka-bench-v4/internal/metrics"
	"kafka-bench-v4/internal/sysperf"
)

type Report struct {
	Config    any                        `json:"config"`
	Final     metrics.FinalStats         `json:"final"`
	Timeline  []metrics.IntervalSnapshot `json:"timeline"`
	Lag       []lag.Sample               `json:"lag_timeline"`
	LagSum    lag.Summary                `json:"lag_summary"`
	SysPerf   []sysperf.Sample           `json:"sysperf_timeline"`
	SysSum    sysperf.Summary            `json:"sysperf_summary"`
	Compress  metrics.CompressionReport  `json:"compression_offline"`
	// Adaptive backpressure stats (zero when -max-lag not set).
	BPEvents  int64                      `json:"bp_events"`
	BPPausedS float64                    `json:"bp_paused_s"`
}

func Build(cfg *config.BenchConfig, final metrics.FinalStats,
	tl []metrics.IntervalSnapshot,
	lagTL []lag.Sample, lagSum lag.Summary,
	sp []sysperf.Sample, spSum sysperf.Summary,
	cr metrics.CompressionReport,
	bpEvents int64, bpPaused time.Duration,
) Report {
	return Report{
		Config: map[string]any{
			"brokers":       cfg.Brokers,
			"topic":         cfg.Topic,
			"group":         cfg.GroupID,
			"partitions":    cfg.Partitions,
			"message_size":  cfg.MessageSize,
			"target_rate":   cfg.TargetRate,
			"duration_s":    cfg.Duration.Seconds(),
			"warmup_s":      cfg.WarmupDuration.Seconds(),
			"producers":     cfg.ProducerWorkers,
			"consumers":     cfg.ConsumerWorkers,
			"acks":          cfg.Acks,
			"compression":   cfg.Compression,
			"batch_bytes":   cfg.BatchBytes,
			"linger_ms":     cfg.LingerMs,
			"payload_mode":  cfg.PayloadMode,
			"consumer_delay": cfg.ConsumerDelay.String(),
			"consumer_jitter": cfg.ConsumerJitter.String(),
			"phase_duration": cfg.PhaseDuration.String(),
			"phase_delay":    cfg.PhaseDelay.String(),
			"initial_offset": cfg.InitialOffset,
			"burst_rate":     cfg.BurstRate,
			"burst_duration": cfg.BurstDuration.String(),
			"burst_period":   cfg.BurstPeriod.String(),
			"bp_max_lag":     cfg.MaxLag,
			"bp_resume_lag":  cfg.ResumeLag,
			"bp_poll":        cfg.BPPollInterval.String(),
		},
		Final:     final,
		Timeline:  tl,
		Lag:       lagTL,
		LagSum:    lagSum,
		SysPerf:   sp,
		SysSum:    spSum,
		Compress:  cr,
		BPEvents:  bpEvents,
		BPPausedS: bpPaused.Seconds(),
	}
}

// Write optionally emits the report to stdout and/or a file.
func Write(r Report, jsonStdout bool, path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if jsonStdout {
		fmt.Println(string(b))
	}
	if path != "" {
		if err := os.WriteFile(path, b, 0644); err != nil {
			return err
		}
		fmt.Printf("[output] wrote %s (%d bytes)\n", path, len(b))
	}
	return nil
}

// PrintHuman prints a final summary table to stdout.
func PrintHuman(r Report) {
	s := r.Final
	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  FINAL RESULTS")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Bench window:    %.2fs (warmup excluded; teardown excluded)\n", s.Elapsed)
	fmt.Printf("  Sent:            %d  (%.1f msg/s, %.2f MB/s)\n", s.Sent, s.AvgRate, s.ThroughputMB)
	fmt.Printf("  Received:        %d  (%.1f msg/s)\n", s.Received, s.AvgRecvRate)
	deliv := 0.0
	if s.Sent > 0 {
		deliv = 100.0 * float64(s.Received) / float64(s.Sent)
	}
	fmt.Printf("  Delivery ratio:  %.2f%%\n", deliv)
	fmt.Printf("  Errors:          %d\n", s.Errors)

	printLatencyBlock("Ack latency  (producer→broker ack)", s.AckLatency)
	printLatencyBlock("E2E latency  (producer→consumer  )", s.E2ELatency)

	if r.LagSum.SampleCount > 0 {
		fmt.Println()
		fmt.Println("  ── Consumer Lag ──")
		fmt.Printf("  Peak total lag:   %d msgs\n", r.LagSum.MaxTotalLag)
		fmt.Printf("  Final total lag:  %d msgs\n", r.LagSum.FinalTotalLag)
		if r.LagSum.RecoveryDrainRate > 0 {
			fmt.Printf("  Drain rate:       %.1f msgs/s\n", r.LagSum.RecoveryDrainRate)
			fmt.Printf("  Time peak→drain:  %.2fs\n", r.LagSum.TimeToDrainS)
		}
	}

	if r.BPEvents > 0 || r.BPPausedS > 0 {
		fmt.Println()
		fmt.Println("  ── Adaptive backpressure ──")
		fmt.Printf("  Throttle events:  %d\n", r.BPEvents)
		fmt.Printf("  Paused total:     %.2fs\n", r.BPPausedS)
	}
	if r.SysSum.Count > 0 {
		fmt.Println()
		fmt.Println("  ── SysPerf ──")
		fmt.Printf("  CPU:  avg=%.1f%% max=%.1f%% (fraction of all cores)\n",
			r.SysSum.AvgCPUPct, r.SysSum.MaxCPUPct)
		fmt.Printf("  Goroutines: avg=%.0f max=%d\n", r.SysSum.AvgGoroutines, r.SysSum.MaxGoroutines)
		fmt.Printf("  Heap peak:  alloc=%.1fMB inuse=%.1fMB\n", r.SysSum.MaxHeapAllocMB, r.SysSum.MaxHeapInUseMB)
		fmt.Printf("  GC:         %d collections, %.1fms total pause\n", r.SysSum.NumGC, r.SysSum.GCPauseTotalMs)
	}

	fmt.Print(r.Compress.Pretty())
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	printTimelineASCII(r.Timeline)
	if len(r.Lag) > 0 {
		printLagASCII(r.Lag)
	}
}

func printLatencyBlock(title string, l metrics.LatencyStats) {
	if l.Count == 0 {
		fmt.Printf("\n  ── %s ──  (no samples)\n", title)
		return
	}
	fmt.Printf("\n  ── %s ── (%d samples)\n", title, l.Count)
	fmt.Printf("   min=%8.3f  mean=%8.3f  stddev=%8.3f  max=%8.3f  mad=%8.3f (ms)\n",
		l.Min, l.Mean, l.StdDev, l.Max, l.MAD)
	fmt.Printf("   p50=%6.2f  p75=%6.2f  p90=%6.2f  p95=%6.2f  p99=%6.2f  p99.5=%6.2f  p99.9=%6.2f  p99.99=%6.2f\n",
		l.P50, l.P75, l.P90, l.P95, l.P99, l.P995, l.P999, l.P9999)
}

func printTimelineASCII(tl []metrics.IntervalSnapshot) {
	if len(tl) < 2 {
		return
	}
	fmt.Println()
	fmt.Println("  Throughput timeline (sent msg/s — filled bar)")
	var maxRate float64
	for _, s := range tl {
		if s.InstRate > maxRate {
			maxRate = s.InstRate
		}
	}
	if maxRate == 0 {
		return
	}
	w := 40
	for _, s := range tl {
		fill := int(s.InstRate / maxRate * float64(w))
		fmt.Printf("  %6.1fs │%s│ sent=%7.0f recv=%7.0f  p99-e2e=%6.1fms\n",
			s.ElapsedS,
			strings.Repeat("█", fill)+strings.Repeat("·", w-fill),
			s.InstRate, s.RecvRate, s.E2EP99Ms,
		)
	}
}

func printLagASCII(samples []lag.Sample) {
	fmt.Println()
	fmt.Println("  Lag timeline (total consumer lag)")
	var maxLag int64
	for _, s := range samples {
		if s.TotalLag > maxLag {
			maxLag = s.TotalLag
		}
	}
	if maxLag == 0 {
		fmt.Println("  (lag stayed at 0 throughout)")
		return
	}
	w := 40
	for _, s := range samples {
		fill := int(float64(s.TotalLag) / float64(maxLag) * float64(w))
		if fill < 0 {
			fill = 0
		}
		fmt.Printf("  %6.1fs │%s│ %10d msgs\n",
			s.ElapsedS,
			strings.Repeat("▓", fill)+strings.Repeat("·", w-fill),
			s.TotalLag,
		)
	}
}
