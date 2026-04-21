// Package metrics — percentiles.go
//
// A fat set of latency statistics. Unlike the interim version (p50/p90/p95/p99/p99.9
// only), we compute:
//
//   min, p50, p75, p90, p95, p99, p99.5, p99.9, p99.99, max
//   mean, stddev, variance, median absolute deviation (MAD)
//   HdrHistogram-style "latency CDF bucket counts" (log-bucketed)
//
// Why all of them:
//   - p99.99 exposes real outliers that p99.9 can hide at high sample counts.
//   - MAD is robust to outliers and is what ops teams actually use for alerting.
//   - CDF buckets let a report plot latency distribution without storing all samples.
package metrics

import (
	"math"
	"sort"
)

// LatencyStats holds everything we compute from a latency sample set.
// All fields in milliseconds unless noted.
type LatencyStats struct {
	Min, Max                             float64
	Mean, StdDev, Variance               float64
	P50, P75, P90, P95                   float64
	P99, P995, P999, P9999               float64
	MAD                                  float64 // median absolute deviation
	Count                                int
	// CDFBuckets: log-spaced buckets, values in ms at right-edge (see BucketEdges()).
	CDFBuckets []int64
}

// BucketEdges returns the log-spaced upper edges (ms) used for the CDF histogram.
// 22 buckets covering 0.05ms .. ~10s.
func BucketEdges() []float64 {
	return []float64{
		0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10,
		20, 50, 100, 200, 500, 1_000, 2_000,
		5_000, 10_000, 20_000, 50_000, 100_000,
		200_000, 500_000,
	}
}

// ComputeLatencyStats takes raw microsecond samples and returns full stats.
func ComputeLatencyStats(us []int64) LatencyStats {
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
	meanMs := mean / 1000.0
	stdMs := math.Sqrt(vari) / 1000.0

	// MAD: median of |x - median|
	median := float64(sorted[n/2])
	deviations := make([]float64, n)
	for i, v := range sorted {
		deviations[i] = math.Abs(float64(v) - median)
	}
	sort.Float64s(deviations)
	madMs := deviations[n/2] / 1000.0

	// CDF buckets
	edges := BucketEdges()
	buckets := make([]int64, len(edges)+1)
	for _, v := range sorted {
		ms := toMs(v)
		placed := false
		for bi, e := range edges {
			if ms <= e {
				buckets[bi]++
				placed = true
				break
			}
		}
		if !placed {
			buckets[len(edges)]++ // overflow
		}
	}

	return LatencyStats{
		Min:      toMs(sorted[0]),
		Max:      toMs(sorted[n-1]),
		Mean:     meanMs,
		StdDev:   stdMs,
		Variance: stdMs * stdMs,
		P50:      pct(50),
		P75:      pct(75),
		P90:      pct(90),
		P95:      pct(95),
		P99:      pct(99),
		P995:     pct(99.5),
		P999:     pct(99.9),
		P9999:    pct(99.99),
		MAD:      madMs,
		Count:    n,
		CDFBuckets: buckets,
	}
}
