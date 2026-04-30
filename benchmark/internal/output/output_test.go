package output

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/lag"
	"kafka-bench-v4/internal/metrics"
	"kafka-bench-v4/internal/sysperf"
)

func TestBuildReportJSONShape(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	final := metrics.FinalStats{Sent: 10, Received: 9, Errors: 1}
	rep := Build(cfg, final, []metrics.IntervalSnapshot{{ElapsedS: 1}}, []lag.Sample{{TotalLag: 3}},
		lag.Summary{MaxTotalLag: 3}, []sysperf.Sample{{Goroutines: 2}}, sysperf.Summary{Count: 1},
		metrics.CompressionReport{RawBytes: 8, SampleCount: 1}, 2, 1500*time.Millisecond)

	if rep.Final.Sent != 10 || rep.LagSum.MaxTotalLag != 3 || rep.SysSum.Count != 1 {
		t.Fatalf("report summaries = %+v", rep)
	}
	if rep.BPEvents != 2 || rep.BPPausedS != 1.5 {
		t.Fatalf("bp = %d/%v", rep.BPEvents, rep.BPPausedS)
	}
	cfgMap, ok := rep.Config.(map[string]any)
	if !ok {
		t.Fatalf("Config type = %T", rep.Config)
	}
	if cfgMap["topic"] != cfg.Topic || cfgMap["payload_mode"] != cfg.PayloadMode {
		t.Fatalf("config map = %#v", cfgMap)
	}

	if _, err := json.Marshal(rep); err != nil {
		t.Fatalf("Marshal report error = %v", err)
	}
}

func TestWriteOutputsJSONAndFile(t *testing.T) {
	rep := Build(testConfig(), metrics.FinalStats{Sent: 1}, nil, nil, lag.Summary{}, nil, sysperf.Summary{},
		metrics.CompressionReport{}, 0, 0)
	path := filepath.Join(t.TempDir(), "report.json")

	var err error
	out := captureStdout(t, func() {
		err = Write(rep, true, path)
	})
	if err != nil {
		t.Fatalf("Write error = %v", err)
	}
	if !strings.Contains(out, `"messages_sent": 1`) || !strings.Contains(out, "[output] wrote") {
		t.Fatalf("stdout = %s", out)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile error = %v", err)
	}
	var decoded Report
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal written JSON error = %v", err)
	}
	if decoded.Final.Sent != 1 {
		t.Fatalf("decoded sent = %d", decoded.Final.Sent)
	}
}

func TestPrintHumanSmoke(t *testing.T) {
	rep := Build(testConfig(), metrics.FinalStats{
		Elapsed:       1,
		Sent:          10,
		Received:      10,
		BytesSent:     1024,
		AvgRate:       10,
		AvgRecvRate:   10,
		ThroughputMB:  1,
		AckLatency:    metrics.LatencyStats{Count: 1, P50: 1, P99: 2},
		E2ELatency:    metrics.LatencyStats{Count: 1, P50: 3, P99: 4},
		BytesReceived: 1024,
	}, []metrics.IntervalSnapshot{{ElapsedS: 1, InstRate: 10}, {ElapsedS: 2, InstRate: 20}},
		[]lag.Sample{{ElapsedS: 1, TotalLag: 0}}, lag.Summary{SampleCount: 1},
		[]sysperf.Sample{{Goroutines: 2}}, sysperf.Summary{Count: 1, AvgCPUPct: 1, MaxCPUPct: 2},
		metrics.CompressionReport{}, 0, 0)

	out := captureStdout(t, func() {
		PrintHuman(rep)
	})
	if !strings.Contains(out, "FINAL RESULTS") || !strings.Contains(out, "Delivery ratio") {
		t.Fatalf("PrintHuman output = %s", out)
	}
}

func testConfig() *config.BenchConfig {
	return &config.BenchConfig{
		Brokers:         []string{"localhost:9092"},
		Topic:           "bench-test",
		GroupID:         "bench-group",
		Partitions:      1,
		MessageSize:     64,
		TargetRate:      100,
		Duration:        time.Second,
		WarmupDuration:  time.Second,
		ProducerWorkers: 1,
		ConsumerWorkers: 1,
		Acks:            1,
		Compression:     "none",
		BatchBytes:      1024,
		LingerMs:        1,
		PayloadMode:     "mixed",
		ConsumerDelay:   time.Millisecond,
		ConsumerJitter:  time.Millisecond,
		PhaseDuration:   time.Second,
		PhaseDelay:      time.Millisecond,
		InitialOffset:   "oldest",
		BurstRate:       100,
		BurstDuration:   time.Second,
		BurstPeriod:     2 * time.Second,
		MaxLag:          10,
		ResumeLag:       5,
		BPPollInterval:  time.Millisecond,
		LagPollInterval: time.Second,
		ReportInterval:  time.Second,
		SysPerfInterval: time.Second,
		MaxMessageBytes: 1 << 20,
		ProducerOnly:    false,
		ConsumerOnly:    false,
		OutputFile:      "",
		JSONOutput:      false,
		ResetTopic:      true,
		PayloadSeed:     1,
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe error = %v", err)
	}
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = old
	})

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer error = %v", err)
	}
	os.Stdout = old

	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll stdout error = %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close reader error = %v", err)
	}
	return string(b)
}
