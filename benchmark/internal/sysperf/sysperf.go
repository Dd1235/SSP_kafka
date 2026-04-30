// Package sysperf samples process-level system-performance metrics alongside
// the Kafka throughput/latency metrics.
//
// On a single-machine benchmark these are especially meaningful:
//   - CPU utilization tells you whether the bench is CPU-bound (compression,
//     encoding) or I/O-bound (disk flush, network).
//   - GoGC pauses show up as tail-latency outliers — correlating them with
//     p99 spikes is exactly what "The Tail at Scale" recommends.
//   - Goroutine count catches runaway-goroutine bugs.
//
// We sample via the `runtime` package (cheap, in-process). Everything is
// snapshot-at-a-time; no streaming. Result is returned as a list of
// Samples that gets JSON-serialized.
//
// To add a metric:
//  1. Add a field to Sample.
//  2. Fill it in `takeSample()`.
//  3. It will flow through to JSON automatically.
package sysperf

import (
	"context"
	"runtime"
	"runtime/metrics"
	"time"
)

type Sample struct {
	ElapsedS       float64 `json:"elapsed_s"`
	Goroutines     int     `json:"goroutines"`
	HeapAllocMB    float64 `json:"heap_alloc_mb"`
	HeapInUseMB    float64 `json:"heap_inuse_mb"`
	StackInUseMB   float64 `json:"stack_inuse_mb"`
	SysMB          float64 `json:"sys_mb"`
	NumGC          uint32  `json:"num_gc"`
	GCPauseTotalMs float64 `json:"gc_pause_total_ms"`
	GCPauseLastMs  float64 `json:"gc_pause_last_ms"`
	// runtime/metrics:
	CPUFractionPct float64 `json:"cpu_fraction_pct"` // estimated fraction of all CPUs
	Gomaxprocs     int     `json:"gomaxprocs"`
}

// Sampler runs until ctx is cancelled, collecting Samples at `interval`.
// Call Samples() once the Sampler's goroutine has returned.
type Sampler struct {
	interval time.Duration
	start    time.Time
	samples  []Sample
	done     chan struct{}

	lastGCPauseNs uint64
	metricSamples []metrics.Sample
}

func NewSampler(interval time.Duration) *Sampler {
	return &Sampler{
		interval: interval,
		done:     make(chan struct{}),
		metricSamples: []metrics.Sample{
			{Name: "/sched/gomaxprocs:threads"},
			// cpu fraction is not directly exposed across Go versions;
			// we approximate via rusage-free method: /cpu/classes/total:cpu-seconds
			// and delta between samples.
			{Name: "/cpu/classes/total:cpu-seconds"},
		},
	}
}

// Run blocks until ctx is cancelled. Intended to be called in its own goroutine.
func (s *Sampler) Run(ctx context.Context) {
	s.start = time.Now()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	// Prime metrics sampler
	metrics.Read(s.metricSamples)
	prevCPU := cpuSeconds(s.metricSamples)
	prevT := s.start
	for {
		select {
		case <-ctx.Done():
			close(s.done)
			return
		case now := <-ticker.C:
			metrics.Read(s.metricSamples)
			curCPU := cpuSeconds(s.metricSamples)
			dCPU := curCPU - prevCPU
			dT := now.Sub(prevT).Seconds()
			prevCPU = curCPU
			prevT = now

			cpuPct := 0.0
			if dT > 0 {
				// dCPU is seconds of CPU across all goroutines/OS threads.
				// Fraction of a single core = dCPU / dT. Divide by NumCPU to
				// get "fraction of machine".
				cpuPct = (dCPU / dT) / float64(runtime.NumCPU()) * 100.0
			}
			s.samples = append(s.samples, s.takeSample(now, cpuPct))
		}
	}
}

// Wait blocks until Run returns (after ctx cancel). Safe to call once.
func (s *Sampler) Wait() { <-s.done }

// Samples returns the captured samples (safe after Wait()).
func (s *Sampler) Samples() []Sample { return s.samples }

// Summary computes aggregates over the captured samples.
type Summary struct {
	Count          int     `json:"count"`
	AvgCPUPct      float64 `json:"avg_cpu_fraction_pct"`
	MaxCPUPct      float64 `json:"max_cpu_fraction_pct"`
	AvgGoroutines  float64 `json:"avg_goroutines"`
	MaxGoroutines  int     `json:"max_goroutines"`
	MaxHeapAllocMB float64 `json:"max_heap_alloc_mb"`
	MaxHeapInUseMB float64 `json:"max_heap_inuse_mb"`
	NumGC          uint32  `json:"num_gc"`
	GCPauseTotalMs float64 `json:"gc_pause_total_ms"`
}

func (s *Sampler) Summary() Summary {
	if len(s.samples) == 0 {
		return Summary{}
	}
	var sumCPU, sumGo float64
	var maxCPU, maxHeap, maxHeapInUse float64
	var maxGo int
	last := s.samples[len(s.samples)-1]
	for _, sm := range s.samples {
		sumCPU += sm.CPUFractionPct
		sumGo += float64(sm.Goroutines)
		if sm.CPUFractionPct > maxCPU {
			maxCPU = sm.CPUFractionPct
		}
		if sm.Goroutines > maxGo {
			maxGo = sm.Goroutines
		}
		if sm.HeapAllocMB > maxHeap {
			maxHeap = sm.HeapAllocMB
		}
		if sm.HeapInUseMB > maxHeapInUse {
			maxHeapInUse = sm.HeapInUseMB
		}
	}
	n := float64(len(s.samples))
	return Summary{
		Count:          len(s.samples),
		AvgCPUPct:      sumCPU / n,
		MaxCPUPct:      maxCPU,
		AvgGoroutines:  sumGo / n,
		MaxGoroutines:  maxGo,
		MaxHeapAllocMB: maxHeap,
		MaxHeapInUseMB: maxHeapInUse,
		NumGC:          last.NumGC,
		GCPauseTotalMs: last.GCPauseTotalMs,
	}
}

func (s *Sampler) takeSample(now time.Time, cpuPct float64) Sample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return Sample{
		ElapsedS:       now.Sub(s.start).Seconds(),
		Goroutines:     runtime.NumGoroutine(),
		HeapAllocMB:    mb(m.HeapAlloc),
		HeapInUseMB:    mb(m.HeapInuse),
		StackInUseMB:   mb(m.StackInuse),
		SysMB:          mb(m.Sys),
		NumGC:          m.NumGC,
		GCPauseTotalMs: float64(m.PauseTotalNs) / 1_000_000.0,
		GCPauseLastMs:  float64(lastPauseNs(&m)) / 1_000_000.0,
		CPUFractionPct: cpuPct,
		Gomaxprocs:     runtime.GOMAXPROCS(0),
	}
}

func mb(v uint64) float64 { return float64(v) / 1024.0 / 1024.0 }

func lastPauseNs(m *runtime.MemStats) uint64 {
	if m.NumGC == 0 {
		return 0
	}
	return m.PauseNs[(m.NumGC+255)%256]
}

func cpuSeconds(samples []metrics.Sample) float64 {
	for _, s := range samples {
		if s.Name == "/cpu/classes/total:cpu-seconds" {
			if s.Value.Kind() == metrics.KindFloat64 {
				return s.Value.Float64()
			}
		}
	}
	return 0
}
