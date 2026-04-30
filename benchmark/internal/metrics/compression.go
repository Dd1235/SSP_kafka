// Package metrics — compression.go
//
// Off-line compression-ratio measurement.
//
// Why this exists: Kafka's wire / disk bytes aren't exposed by the producer
// API. To answer "what was the actual compression ratio of snappy vs zstd
// on MY payload?" we take a representative sample of messages and compress
// them with every codec. This is pure Go, independent of the broker.
//
// To add a codec: extend the `codecs` map below.
package metrics

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/s2"    // Sarama uses klauspost; s2 is snappy-compatible
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

func nowMicros() int64 { return time.Now().UnixMicro() }

// CompressionReport describes how well each codec squashes a batch of sample messages.
type CompressionReport struct {
	RawBytes    int                    `json:"raw_bytes"`
	SampleCount int                    `json:"sample_count"`
	Codecs      map[string]CodecResult `json:"codecs"`
}

type CodecResult struct {
	CompressedBytes int     `json:"compressed_bytes"`
	Ratio           float64 `json:"ratio"`          // raw / compressed; >1 means shrinkage
	SpaceSavedPct   float64 `json:"space_saved_pct"` // 100 * (1 - comp/raw)
	EncodeMicros    int64   `json:"encode_micros"`
}

// BuildCompressionReport compresses a concatenation of samples through each codec.
func BuildCompressionReport(samples [][]byte) CompressionReport {
	var raw bytes.Buffer
	for _, s := range samples {
		raw.Write(s)
	}
	rb := raw.Bytes()

	rep := CompressionReport{
		RawBytes:    len(rb),
		SampleCount: len(samples),
		Codecs:      map[string]CodecResult{},
	}
	for name, fn := range codecs {
		comp, us, err := fn(rb)
		if err != nil {
			continue
		}
		ratio := 0.0
		if len(comp) > 0 {
			ratio = float64(len(rb)) / float64(len(comp))
		}
		rep.Codecs[name] = CodecResult{
			CompressedBytes: len(comp),
			Ratio:           ratio,
			SpaceSavedPct:   100.0 * (1.0 - float64(len(comp))/float64(len(rb))),
			EncodeMicros:    us,
		}
	}
	return rep
}

// compressorFn returns (compressed, encode_microseconds, err)
type compressorFn func([]byte) ([]byte, int64, error)

var codecs = map[string]compressorFn{
	"none":   encNone,
	"gzip":   encGzip,
	"snappy": encSnappy,
	"lz4":    encLz4,
	"zstd":   encZstd,
}

func encNone(in []byte) ([]byte, int64, error) {
	return in, 0, nil
}

func encGzip(in []byte) ([]byte, int64, error) {
	var buf bytes.Buffer
	t := nowMicros()
	w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if _, err := w.Write(in); err != nil {
		return nil, 0, err
	}
	if err := w.Close(); err != nil {
		return nil, 0, err
	}
	return buf.Bytes(), nowMicros() - t, nil
}

func encSnappy(in []byte) ([]byte, int64, error) {
	t := nowMicros()
	out := s2.EncodeSnappy(nil, in)
	return out, nowMicros() - t, nil
}

func encLz4(in []byte) ([]byte, int64, error) {
	var buf bytes.Buffer
	t := nowMicros()
	w := lz4.NewWriter(&buf)
	if _, err := io.Copy(w, bytes.NewReader(in)); err != nil {
		return nil, 0, err
	}
	if err := w.Close(); err != nil {
		return nil, 0, err
	}
	return buf.Bytes(), nowMicros() - t, nil
}

func encZstd(in []byte) ([]byte, int64, error) {
	t := nowMicros()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, 0, err
	}
	defer enc.Close()
	out := enc.EncodeAll(in, nil)
	return out, nowMicros() - t, nil
}

func (r CompressionReport) Pretty() string {
	var sb bytes.Buffer
	fmt.Fprintf(&sb, "\n  Offline compression ratio (%d samples, %d raw bytes)\n", r.SampleCount, r.RawBytes)
	fmt.Fprintf(&sb, "  ─────────────────────────────────────────────────\n")
	fmt.Fprintf(&sb, "  %-8s %12s %8s %10s %12s\n", "codec", "comp-bytes", "ratio", "saved%%", "encode(us)")
	for _, name := range []string{"none", "snappy", "lz4", "gzip", "zstd"} {
		c, ok := r.Codecs[name]
		if !ok {
			continue
		}
		fmt.Fprintf(&sb, "  %-8s %12d %8.2fx %9.1f%% %12d\n",
			name, c.CompressedBytes, c.Ratio, c.SpaceSavedPct, c.EncodeMicros)
	}
	return sb.String()
}
