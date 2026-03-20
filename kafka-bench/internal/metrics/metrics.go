package metrics

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Package metrics implements the benchmark's measurement pipeline: it keeps the
// hot path cheap by using atomics for frequently updated counters (sent, received,
// errors, bytes) and pushes more expensive work like latency storage, percentile
// calculation, interval snapshots, and timeline generation onto colder paths
// protected by mutexes. Producers/consumers call Record* methods concurrently,
// a single reporter goroutine periodically calls IntervalReport() to compute
// per-interval throughput and latency stats, and FinalSnapshot() aggregates the
// full-run results by sorting the recorded latency samples and deriving summary
// statistics such as p50/p99/p99.9, mean, and standard deviation.

// Snapshot of a single reporting interval — used for timeline
type IntervalSnapshot struct {
	ElapsedS     float64
	Rate         float64 // msg/s in this interval
	ThroughputMB float64
	P50Ms        float64
	P99Ms        float64
	P999Ms       float64
	Errors       int64
}

// Collector collects benchmark metrics with a lock-free hot path.
type Collector struct {
	// Hot-path counters: atomic + explicit cache-line padding (64 bytes each)
	sent      atomic.Int64
	_pad0     [56]byte
	errs      atomic.Int64
	_pad1     [56]byte
	received  atomic.Int64
	_pad2     [56]byte
	bytesSent atomic.Int64
	_pad3     [56]byte

	// Latency histogram — append-only, protected by a single mutex (cold path)
	latMu     sync.Mutex
	latencies []int64 // microseconds

	// Reporter state — only touched by the single reporter goroutine
	reportMu        sync.Mutex
	lastReportSent  int64
	lastReportBytes int64
	lastReportTime  time.Time
	lastReportLats  int // index into latencies at last report

	// Timeline of per-interval snapshots
	timelineMu sync.Mutex
	Timeline   []IntervalSnapshot

	startTime time.Time
}

func New() *Collector {
	return &Collector{
		latencies: make([]int64, 0, 1<<20), // pre-alloc 1M entries
	}
}

func (c *Collector) Start() {
	now := time.Now()
	c.startTime = now
	c.lastReportTime = now
}

// SetWarmupEnd resets all counters so warmup traffic is excluded.
func (c *Collector) SetWarmupEnd() {
	c.sent.Store(0)
	c.errs.Store(0)
	c.received.Store(0)
	c.bytesSent.Store(0)

	c.latMu.Lock()
	c.latencies = c.latencies[:0]
	c.latMu.Unlock()

	now := time.Now()
	c.startTime = now

	c.reportMu.Lock()
	c.lastReportSent = 0
	c.lastReportBytes = 0
	c.lastReportTime = now
	c.lastReportLats = 0
	c.reportMu.Unlock()

	c.timelineMu.Lock()
	c.Timeline = c.Timeline[:0]
	c.timelineMu.Unlock()
}

// RecordSent is called on the hot path — atomic only, no locks.
func (c *Collector) RecordSent(msgBytes int) {
	c.sent.Add(1)
	c.bytesSent.Add(int64(msgBytes))
}

func (c *Collector) RecordError() { c.errs.Add(1) }

func (c *Collector) RecordReceived() { c.received.Add(1) }

// RecordLatency is called from ack-reader goroutines — single mutex, cold path.
func (c *Collector) RecordLatency(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	c.latMu.Lock()
	c.latencies = append(c.latencies, us)
	c.latMu.Unlock()
}

// Stats is the full benchmark result.
type Stats struct {
	Elapsed      float64
	Sent         int64
	Errors       int64
	Received     int64
	BytesSent    int64
	AvgRate      float64 // overall msg/s
	InstantRate  float64 // this interval msg/s
	ThroughputMB float64 // overall MB/s
	Latency      LatencyStats
}

type LatencyStats struct {
	Min, P50, P90, P95, P99, P999, Max float64 // ms
	Mean, StdDev                       float64 // ms
	Count                              int
}

// IntervalReport is called by the reporter goroutine; not concurrent with itself.
func (c *Collector) IntervalReport() Stats {
	now := time.Now()
	elapsed := now.Sub(c.startTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	sent := c.sent.Load()
	bytes := c.bytesSent.Load()

	c.reportMu.Lock()
	interval := now.Sub(c.lastReportTime).Seconds()
	if interval < 0.001 {
		interval = 0.001
	}
	deltaSent := sent - c.lastReportSent
	deltaBytes := bytes - c.lastReportBytes
	instantRate := float64(deltaSent) / interval
	instantMB := float64(deltaBytes) / interval / 1024 / 1024

	// Latency for this interval only
	c.latMu.Lock()
	intervalLats := make([]int64, len(c.latencies)-c.lastReportLats)
	copy(intervalLats, c.latencies[c.lastReportLats:])
	c.lastReportLats = len(c.latencies)
	c.latMu.Unlock()

	c.lastReportSent = sent
	c.lastReportBytes = bytes
	c.lastReportTime = now
	c.reportMu.Unlock()

	var latStats LatencyStats
	if len(intervalLats) > 0 {
		latStats = computePercentiles(intervalLats)
	}

	// Append to timeline
	snap := IntervalSnapshot{
		ElapsedS:     elapsed,
		Rate:         instantRate,
		ThroughputMB: instantMB,
		P50Ms:        latStats.P50,
		P99Ms:        latStats.P99,
		P999Ms:       latStats.P999,
		Errors:       c.errs.Load(),
	}
	c.timelineMu.Lock()
	c.Timeline = append(c.Timeline, snap)
	c.timelineMu.Unlock()

	return Stats{
		Elapsed:      elapsed,
		Sent:         sent,
		Errors:       c.errs.Load(),
		Received:     c.received.Load(),
		BytesSent:    bytes,
		AvgRate:      float64(sent) / elapsed,
		InstantRate:  instantRate,
		ThroughputMB: float64(bytes) / elapsed / 1024 / 1024,
		Latency:      latStats,
	}
}

// FinalSnapshot computes the full-run percentiles across all recorded latencies.
func (c *Collector) FinalSnapshot() Stats {
	now := time.Now()
	elapsed := now.Sub(c.startTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	sent := c.sent.Load()
	bytes := c.bytesSent.Load()

	c.latMu.Lock()
	allLats := make([]int64, len(c.latencies))
	copy(allLats, c.latencies)
	c.latMu.Unlock()

	var latStats LatencyStats
	if len(allLats) > 0 {
		latStats = computePercentiles(allLats)
	}

	return Stats{
		Elapsed:      elapsed,
		Sent:         sent,
		Errors:       c.errs.Load(),
		Received:     c.received.Load(),
		BytesSent:    bytes,
		AvgRate:      float64(sent) / elapsed,
		ThroughputMB: float64(bytes) / elapsed / 1024 / 1024,
		Latency:      latStats,
	}
}

func computePercentiles(us []int64) LatencyStats {
	n := len(us)
	if n == 0 {
		return LatencyStats{}
	}
	sorted := make([]int64, n)
	copy(sorted, us)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	toMs := func(v int64) float64 { return float64(v) / 1000.0 }
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p/100.0*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return toMs(sorted[idx])
	}

	var sum float64
	for _, v := range sorted {
		sum += float64(v)
	}
	mean := sum / float64(n)
	var vari float64
	for _, v := range sorted {
		d := float64(v) - mean
		vari += d * d
	}
	vari /= float64(n)

	return LatencyStats{
		Min:    toMs(sorted[0]),
		P50:    pct(50),
		P90:    pct(90),
		P95:    pct(95),
		P99:    pct(99),
		P999:   pct(99.9),
		Max:    toMs(sorted[n-1]),
		Mean:   mean / 1000.0,
		StdDev: math.Sqrt(vari) / 1000.0,
		Count:  n,
	}
}

func (s Stats) PrintInterval() {
	errPct := float64(0)
	if s.Sent > 0 {
		errPct = float64(s.Errors) / float64(s.Sent) * 100
	}
	fmt.Printf("[%6.1fs] sent=%-9d inst=%-8.0f avg=%-8.0f MB/s=%-5.2f err=%.2f%%",
		s.Elapsed, s.Sent, s.InstantRate, s.AvgRate, s.ThroughputMB, errPct)
	if s.Latency.Count > 0 {
		fmt.Printf("  p50=%5.1fms p99=%6.1fms p99.9=%7.1fms",
			s.Latency.P50, s.Latency.P99, s.Latency.P999)
	}
	fmt.Println()
}

func (s Stats) PrintFinal() {
	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  FINAL RESULTS")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Duration:         %.2fs\n", s.Elapsed)
	fmt.Printf("  Messages Sent:    %d\n", s.Sent)
	fmt.Printf("  Messages Rcvd:    %d\n", s.Received)
	fmt.Printf("  Errors:           %d (%.4f%%)\n", s.Errors,
		func() float64 {
			if s.Sent == 0 {
				return 0
			}
			return float64(s.Errors) / float64(s.Sent) * 100
		}())
	fmt.Printf("  Avg Throughput:   %.2f msg/s\n", s.AvgRate)
	fmt.Printf("  Avg Bandwidth:    %.2f MB/s\n", s.ThroughputMB)
	fmt.Printf("  Total Data:       %.2f MB\n", float64(s.BytesSent)/1024/1024)
	l := s.Latency
	if l.Count > 0 {
		fmt.Printf("\n  ── Latency (%d samples) ───────────────────\n", l.Count)
		fmt.Printf("  Min:    %8.3f ms   Max:    %8.3f ms\n", l.Min, l.Max)
		fmt.Printf("  Mean:   %8.3f ms   StdDev: %8.3f ms\n", l.Mean, l.StdDev)
		fmt.Printf("  p50:    %8.3f ms   p90:    %8.3f ms\n", l.P50, l.P90)
		fmt.Printf("  p95:    %8.3f ms   p99:    %8.3f ms\n", l.P95, l.P99)
		fmt.Printf("  p99.9:  %8.3f ms\n", l.P999)
	}
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

// PrintTimeline prints a compact ASCII chart of throughput over time.
// Called once at the end of the benchmark.
func (c *Collector) PrintTimeline() {
	c.timelineMu.Lock()
	tl := make([]IntervalSnapshot, len(c.Timeline))
	copy(tl, c.Timeline)
	c.timelineMu.Unlock()

	if len(tl) < 2 {
		return
	}

	fmt.Println()
	fmt.Println("  Throughput Timeline (msg/s)")
	fmt.Println("  ──────────────────────────────────────────")

	// Find max rate for scaling
	var maxRate float64
	for _, s := range tl {
		if s.Rate > maxRate {
			maxRate = s.Rate
		}
	}
	if maxRate == 0 {
		return
	}

	barWidth := 40
	for _, s := range tl {
		filled := int(s.Rate / maxRate * float64(barWidth))
		bar := ""
		for i := 0; i < barWidth; i++ {
			if i < filled {
				bar += "█"
			} else {
				bar += "░"
			}
		}
		fmt.Printf("  %5.0fs │%s│ %7.0f\n", s.ElapsedS, bar, s.Rate)
	}
	fmt.Println()
}

// TimelineMu exposes the timeline mutex so main can copy the slice for JSON output.
// Exported for use in main package only.
var _ = (*Collector)(nil) // type assertion — keeps the field accessible

// GetTimeline returns a copy of the timeline (thread-safe).
func (c *Collector) GetTimeline() []IntervalSnapshot {
	c.timelineMu.Lock()
	defer c.timelineMu.Unlock()
	tl := make([]IntervalSnapshot, len(c.Timeline))
	copy(tl, c.Timeline)
	return tl
}
