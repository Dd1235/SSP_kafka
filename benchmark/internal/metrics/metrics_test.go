package metrics

import (
	"math"
	"testing"
	"time"
)

func TestComputeLatencyStatsEmpty(t *testing.T) {
	t.Parallel()

	got := ComputeLatencyStats(nil)
	if got.Count != 0 || got.Min != 0 || got.Max != 0 || got.CDFBuckets != nil {
		t.Fatalf("empty stats = %+v", got)
	}
}

func TestComputeLatencyStatsPercentilesAndBuckets(t *testing.T) {
	t.Parallel()

	got := ComputeLatencyStats([]int64{4000, 1000, 3000, 2000})
	if got.Count != 4 {
		t.Fatalf("Count = %d", got.Count)
	}
	if got.Min != 1 || got.Max != 4 || got.Mean != 2.5 {
		t.Fatalf("min/max/mean = %v/%v/%v", got.Min, got.Max, got.Mean)
	}
	if got.P50 != 2 || got.P75 != 3 || got.P99 != 4 || got.P9999 != 4 {
		t.Fatalf("percentiles = p50 %v p75 %v p99 %v p9999 %v", got.P50, got.P75, got.P99, got.P9999)
	}
	if len(got.CDFBuckets) != len(BucketEdges())+1 {
		t.Fatalf("bucket count = %d", len(got.CDFBuckets))
	}
	var total int64
	for _, n := range got.CDFBuckets {
		total += n
	}
	if total != int64(got.Count) {
		t.Fatalf("bucket total = %d, want %d", total, got.Count)
	}
}

func TestBuildCompressionReportEmptySamplesNoNaN(t *testing.T) {
	t.Parallel()

	got := BuildCompressionReport(nil)
	if got.RawBytes != 0 || got.SampleCount != 0 {
		t.Fatalf("report sizes = raw %d samples %d", got.RawBytes, got.SampleCount)
	}
	for name, codec := range got.Codecs {
		if math.IsNaN(codec.Ratio) || math.IsInf(codec.Ratio, 0) {
			t.Fatalf("%s ratio = %v", name, codec.Ratio)
		}
		if math.IsNaN(codec.SpaceSavedPct) || math.IsInf(codec.SpaceSavedPct, 0) {
			t.Fatalf("%s saved = %v", name, codec.SpaceSavedPct)
		}
	}
}

func TestBuildCompressionReportZeros(t *testing.T) {
	t.Parallel()

	samples := make([][]byte, 4)
	for i := range samples {
		samples[i] = make([]byte, 256)
	}
	got := BuildCompressionReport(samples)
	if got.RawBytes != 1024 || got.SampleCount != 4 {
		t.Fatalf("report sizes = raw %d samples %d", got.RawBytes, got.SampleCount)
	}
	none := got.Codecs["none"]
	if none.CompressedBytes != got.RawBytes || none.Ratio != 1 || none.SpaceSavedPct != 0 {
		t.Fatalf("none codec = %+v", none)
	}
	if got.Codecs["snappy"].Ratio <= 1 {
		t.Fatalf("snappy ratio = %v, want > 1", got.Codecs["snappy"].Ratio)
	}
}

func TestCollectorSnapshotsAndReset(t *testing.T) {
	t.Parallel()

	c := New()
	c.Start()
	c.RecordSent(100)
	c.RecordReceived(80)
	c.RecordError()
	c.RecordAckLatency(2 * time.Millisecond)
	c.RecordE2ELatency(4 * time.Millisecond)

	snap := c.IntervalReport()
	if snap.InstRate <= 0 || snap.RecvRate <= 0 || snap.ThroughputMB <= 0 {
		t.Fatalf("interval rates = sent %v recv %v throughput %v", snap.InstRate, snap.RecvRate, snap.ThroughputMB)
	}
	if snap.AckP50Ms != 2 || snap.E2EP99Ms != 4 || snap.Errors != 1 {
		t.Fatalf("interval lat/errors = ack %v e2e %v errors %d", snap.AckP50Ms, snap.E2EP99Ms, snap.Errors)
	}

	c.MarkEnd()
	final := c.FinalSnapshot()
	if final.Sent != 1 || final.Received != 1 || final.Errors != 1 {
		t.Fatalf("final counters = sent %d recv %d errors %d", final.Sent, final.Received, final.Errors)
	}
	if final.BytesSent != 100 || final.BytesReceived != 80 {
		t.Fatalf("final bytes = %d/%d", final.BytesSent, final.BytesReceived)
	}
	if final.AckLatency.Count != 1 || final.E2ELatency.Count != 1 {
		t.Fatalf("latency counts = ack %d e2e %d", final.AckLatency.Count, final.E2ELatency.Count)
	}

	c.ResetAfterWarmup()
	reset := c.FinalSnapshot()
	if reset.Sent != 0 || reset.Received != 0 || reset.Errors != 0 || reset.AckLatency.Count != 0 || reset.E2ELatency.Count != 0 {
		t.Fatalf("reset final = %+v", reset)
	}
}

func TestCollectorTimelineReturnsCopy(t *testing.T) {
	t.Parallel()

	c := New()
	c.Start()
	c.RecordSent(10)
	_ = c.IntervalReport()

	tl := c.Timeline()
	if len(tl) != 1 {
		t.Fatalf("len timeline = %d", len(tl))
	}
	tl[0].InstRate = -1
	if c.Timeline()[0].InstRate == -1 {
		t.Fatal("Timeline returned mutable internal slice")
	}
}
