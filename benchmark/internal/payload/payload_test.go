package payload

import (
	"bytes"
	"testing"
	"time"
)

func TestGeneratorModesProduceExactSize(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"random", "zeros", "text", "json", "logline", "mixed"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			g, err := New(mode, 64, 7)
			if err != nil {
				t.Fatalf("New(%q) error = %v", mode, err)
			}
			buf := make([]byte, 64)
			g.Next(buf, 1)
			if len(buf) != 64 {
				t.Fatalf("len(buf) = %d", len(buf))
			}
			if g.Mode() != Mode(mode) {
				t.Fatalf("Mode() = %q", g.Mode())
			}
			sample := g.SampleFor(2)
			if len(sample) != 64 {
				t.Fatalf("len(sample) = %d", len(sample))
			}
		})
	}
}

func TestGeneratorRejectsUnknownMode(t *testing.T) {
	t.Parallel()

	if _, err := New("bogus", 32, 1); err == nil {
		t.Fatal("New unknown mode error = nil")
	}
}

func TestGeneratorPanicsOnWrongBufferSize(t *testing.T) {
	t.Parallel()

	g, err := New("zeros", 32, 1)
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Next did not panic")
		}
	}()
	g.Next(make([]byte, 31), 0)
}

func TestDeterministicModes(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"random", "zeros", "text"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			g1, err := New(mode, 128, 42)
			if err != nil {
				t.Fatalf("New g1 error = %v", err)
			}
			g2, err := New(mode, 128, 42)
			if err != nil {
				t.Fatalf("New g2 error = %v", err)
			}
			b1 := make([]byte, 128)
			b2 := make([]byte, 128)
			g1.Next(b1, 3)
			g2.Next(b2, 3)
			if !bytes.Equal(b1, b2) {
				t.Fatalf("%s with same seed produced different output", mode)
			}
		})
	}
}

func TestMixedModeRotationIncludesZeros(t *testing.T) {
	t.Parallel()

	g, err := New("mixed", 32, 1)
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	buf := bytes.Repeat([]byte{1}, 32)
	g.Next(buf, 4)
	if !bytes.Equal(buf, make([]byte, 32)) {
		t.Fatalf("mixed seq 4 = %q, want zeros", buf)
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 16)
	ts := time.Now().UnixNano()
	WriteHeader(buf, 17, 99, ts)

	worker, seq, gotTS, ok := ReadHeader(buf)
	if !ok {
		t.Fatal("ReadHeader ok = false")
	}
	if worker != 17 || seq != 99 || gotTS != ts {
		t.Fatalf("header = %d/%d/%d, want 17/99/%d", worker, seq, gotTS, ts)
	}
}

func TestReadHeaderShortBuffer(t *testing.T) {
	t.Parallel()

	if _, _, _, ok := ReadHeader(make([]byte, 15)); ok {
		t.Fatal("ReadHeader short buffer ok = true")
	}
}

func FuzzHeaderRoundTrip(f *testing.F) {
	f.Add(uint32(1), uint32(2), int64(3))
	f.Add(uint32(0), uint32(0), int64(0))
	f.Add(^uint32(0), ^uint32(0), int64(-1))

	f.Fuzz(func(t *testing.T, worker, seq uint32, ts int64) {
		buf := make([]byte, 16)
		WriteHeader(buf, worker, seq, ts)
		gotWorker, gotSeq, gotTS, ok := ReadHeader(buf)
		if !ok {
			t.Fatal("ReadHeader ok = false")
		}
		if gotWorker != worker || gotSeq != seq || gotTS != ts {
			t.Fatalf("header = %d/%d/%d, want %d/%d/%d", gotWorker, gotSeq, gotTS, worker, seq, ts)
		}
	})
}
