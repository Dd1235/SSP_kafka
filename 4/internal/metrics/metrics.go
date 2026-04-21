// Package metrics is the measurement pipeline.
//
// Design:
//   - Hot-path counters are atomics (sent / recv / errors / bytes) with cache
//     line padding to avoid false sharing.
//   - Latency buckets are append-only slices behind single mutexes. There are
//     TWO separate channels:
//       1. AckLatency  — producer enqueue → broker ack  (producer side)
//       2. E2ELatency  — producer enqueue → consumer receive  (end-to-end)
//     The interim bench only had (1). Real systems care about (2).
//   - The "bench window" (post-warmup, pre-shutdown) is tracked explicitly so
//     AvgRate isn't distorted by teardown time (interim had this bug).
//
// Every reporting method takes its own snapshot; callers never touch the
// internal slices directly.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

type Collector struct {
	// --- hot-path counters, 64-byte padded ---
	sent      atomic.Int64
	_pad0     [56]byte
	errs      atomic.Int64
	_pad1     [56]byte
	received  atomic.Int64
	_pad2     [56]byte
	bytesSent atomic.Int64
	_pad3     [56]byte
	bytesRecv atomic.Int64
	_pad4     [56]byte

	// --- latency sample banks ---
	ackMu  sync.Mutex
	ackLat []int64 // microseconds — producer enqueue → broker ack

	e2eMu  sync.Mutex
	e2eLat []int64 // microseconds — producer enqueue → consumer receive

	// --- reporter state (single caller: IntervalReport goroutine) ---
	repMu           sync.Mutex
	lastTime        time.Time
	lastSent        int64
	lastRecv        int64
	lastBytesSent   int64
	lastBytesRecv   int64
	lastAckIdx      int
	lastE2EIdx      int

	// --- timeline / run state ---
	timelineMu sync.Mutex
	timeline   []IntervalSnapshot

	startTime   time.Time // bench start (after warmup)
	endTime     time.Time // fixed at FinalSnapshot(); used to exclude teardown
	endTimeSet  bool
}

// IntervalSnapshot is emitted every ReportInterval.
type IntervalSnapshot struct {
	ElapsedS     float64 `json:"elapsed_s"`
	InstRate     float64 `json:"inst_rate"`     // sent msg/s this interval
	RecvRate     float64 `json:"recv_rate"`     // received msg/s this interval
	ThroughputMB float64 `json:"throughput_mb"` // producer payload MB/s
	AckP50Ms     float64 `json:"ack_p50_ms"`
	AckP99Ms     float64 `json:"ack_p99_ms"`
	E2EP50Ms     float64 `json:"e2e_p50_ms"`
	E2EP99Ms     float64 `json:"e2e_p99_ms"`
	Errors       int64   `json:"errors"`
}

func New() *Collector {
	return &Collector{
		ackLat: make([]int64, 0, 1<<20),
		e2eLat: make([]int64, 0, 1<<20),
	}
}

// Start marks the beginning of the bench window.
func (c *Collector) Start() {
	now := time.Now()
	c.startTime = now
	c.lastTime = now
}

// ResetAfterWarmup wipes counters once warmup is done so the bench window
// is clean.
func (c *Collector) ResetAfterWarmup() {
	c.sent.Store(0)
	c.errs.Store(0)
	c.received.Store(0)
	c.bytesSent.Store(0)
	c.bytesRecv.Store(0)

	c.ackMu.Lock()
	c.ackLat = c.ackLat[:0]
	c.ackMu.Unlock()
	c.e2eMu.Lock()
	c.e2eLat = c.e2eLat[:0]
	c.e2eMu.Unlock()

	now := time.Now()
	c.startTime = now

	c.repMu.Lock()
	c.lastTime = now
	c.lastSent = 0
	c.lastRecv = 0
	c.lastBytesSent = 0
	c.lastBytesRecv = 0
	c.lastAckIdx = 0
	c.lastE2EIdx = 0
	c.repMu.Unlock()

	c.timelineMu.Lock()
	c.timeline = c.timeline[:0]
	c.timelineMu.Unlock()
}

// MarkEnd freezes the bench window (called as soon as ctx cancels). Any
// work happening after this (goroutine drain, consumer group close) does
// not inflate elapsed time.
func (c *Collector) MarkEnd() {
	if !c.endTimeSet {
		c.endTime = time.Now()
		c.endTimeSet = true
	}
}

func (c *Collector) RecordSent(bytes int) {
	c.sent.Add(1)
	c.bytesSent.Add(int64(bytes))
}

func (c *Collector) RecordError() { c.errs.Add(1) }

func (c *Collector) RecordReceived(bytes int) {
	c.received.Add(1)
	c.bytesRecv.Add(int64(bytes))
}

func (c *Collector) RecordAckLatency(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	c.ackMu.Lock()
	c.ackLat = append(c.ackLat, us)
	c.ackMu.Unlock()
}

func (c *Collector) RecordE2ELatency(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	c.e2eMu.Lock()
	c.e2eLat = append(c.e2eLat, us)
	c.e2eMu.Unlock()
}

// IntervalReport is invoked by the reporter goroutine only.
func (c *Collector) IntervalReport() IntervalSnapshot {
	now := time.Now()
	elapsed := now.Sub(c.startTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	sent := c.sent.Load()
	recv := c.received.Load()
	bytesS := c.bytesSent.Load()
	bytesR := c.bytesRecv.Load()

	c.repMu.Lock()
	interval := now.Sub(c.lastTime).Seconds()
	if interval < 0.001 {
		interval = 0.001
	}
	ds := sent - c.lastSent
	dr := recv - c.lastRecv
	dbs := bytesS - c.lastBytesSent
	_ = bytesR - c.lastBytesRecv
	c.lastTime = now
	c.lastSent = sent
	c.lastRecv = recv
	c.lastBytesSent = bytesS
	c.lastBytesRecv = bytesR

	// Only this-interval latency samples
	c.ackMu.Lock()
	iAck := make([]int64, len(c.ackLat)-c.lastAckIdx)
	copy(iAck, c.ackLat[c.lastAckIdx:])
	c.lastAckIdx = len(c.ackLat)
	c.ackMu.Unlock()

	c.e2eMu.Lock()
	iE2E := make([]int64, len(c.e2eLat)-c.lastE2EIdx)
	copy(iE2E, c.e2eLat[c.lastE2EIdx:])
	c.lastE2EIdx = len(c.e2eLat)
	c.e2eMu.Unlock()
	c.repMu.Unlock()

	ackStats := ComputeLatencyStats(iAck)
	e2eStats := ComputeLatencyStats(iE2E)

	snap := IntervalSnapshot{
		ElapsedS:     elapsed,
		InstRate:     float64(ds) / interval,
		RecvRate:     float64(dr) / interval,
		ThroughputMB: float64(dbs) / interval / 1024 / 1024,
		AckP50Ms:     ackStats.P50,
		AckP99Ms:     ackStats.P99,
		E2EP50Ms:     e2eStats.P50,
		E2EP99Ms:     e2eStats.P99,
		Errors:       c.errs.Load(),
	}
	c.timelineMu.Lock()
	c.timeline = append(c.timeline, snap)
	c.timelineMu.Unlock()
	return snap
}

// FinalStats is the consolidated result.
type FinalStats struct {
	Elapsed       float64      `json:"elapsed_s"`
	Sent          int64        `json:"messages_sent"`
	Received      int64        `json:"messages_received"`
	Errors        int64        `json:"errors"`
	BytesSent     int64        `json:"bytes_sent"`
	BytesReceived int64        `json:"bytes_received"`
	AvgRate       float64      `json:"avg_rate_msg_per_sec"`
	AvgRecvRate   float64      `json:"avg_recv_rate_msg_per_sec"`
	ThroughputMB  float64      `json:"throughput_mb_per_sec"`
	AckLatency    LatencyStats `json:"ack_latency_ms"`
	E2ELatency    LatencyStats `json:"e2e_latency_ms"`
}

// FinalSnapshot uses the frozen end-time so teardown doesn't count.
func (c *Collector) FinalSnapshot() FinalStats {
	end := c.endTime
	if !c.endTimeSet {
		end = time.Now()
	}
	elapsed := end.Sub(c.startTime).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	sent := c.sent.Load()
	recv := c.received.Load()
	bytesS := c.bytesSent.Load()
	bytesR := c.bytesRecv.Load()

	c.ackMu.Lock()
	ack := make([]int64, len(c.ackLat))
	copy(ack, c.ackLat)
	c.ackMu.Unlock()
	c.e2eMu.Lock()
	e2e := make([]int64, len(c.e2eLat))
	copy(e2e, c.e2eLat)
	c.e2eMu.Unlock()

	return FinalStats{
		Elapsed:       elapsed,
		Sent:          sent,
		Received:      recv,
		Errors:        c.errs.Load(),
		BytesSent:     bytesS,
		BytesReceived: bytesR,
		AvgRate:       float64(sent) / elapsed,
		AvgRecvRate:   float64(recv) / elapsed,
		ThroughputMB:  float64(bytesS) / elapsed / 1024 / 1024,
		AckLatency:    ComputeLatencyStats(ack),
		E2ELatency:    ComputeLatencyStats(e2e),
	}
}

// Timeline returns a copy of the per-interval snapshots for output.
func (c *Collector) Timeline() []IntervalSnapshot {
	c.timelineMu.Lock()
	defer c.timelineMu.Unlock()
	out := make([]IntervalSnapshot, len(c.timeline))
	copy(out, c.timeline)
	return out
}
