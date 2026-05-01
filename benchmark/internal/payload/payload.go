// Package payload generates message bodies of different compressibility classes.
//
// Why this matters: the interim benchmark used purely random bytes. Random bytes
// have maximum entropy, so every compressor produces output ≥ input and codec
// sweeps become almost meaningless. Real Kafka workloads — JSON events, log
// lines, protobuf with repeated fields — are highly compressible. We model
// several classes so that compression numbers reflect actual workloads.
//
// To add a new payload class:
//  1. Add a constant Mode.
//  2. Add a generator function below.
//  3. Wire it into New().
//
// Every generator returns a buffer of exactly `size` bytes. The caller writes
// a 16-byte header over the first bytes (worker id, seq, timestamp) — so
// generators must produce ≥16 bytes of meaningful content.
package payload

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type Mode string

const (
	ModeRandom  Mode = "random"  // cryptographic-like entropy, worst case for compressors
	ModeZeros   Mode = "zeros"   // all-zero body, best case (sanity-check upper bound)
	ModeText    Mode = "text"    // English-like lorem ipsum, realistic gzip/zstd ~3-4x
	ModeJSON    Mode = "json"    // repeated JSON keys with varying values, like event streams
	ModeLogline Mode = "logline" // Apache-style log, highly compressible
	ModeMixed   Mode = "mixed"   // rotate through the above per message — a realistic mix
)

type Generator struct {
	size int
	mode Mode
	mu   sync.Mutex // guards r; Next is called concurrently from producer workers
	r    *rand.Rand
	// cached immutable template for deterministic modes
	zeros []byte
	text  []byte
}

func New(mode string, size int, seed int64) (*Generator, error) {
	m := Mode(mode)
	switch m {
	case ModeRandom, ModeZeros, ModeText, ModeJSON, ModeLogline, ModeMixed:
	default:
		return nil, fmt.Errorf("unknown payload mode %q", mode)
	}
	g := &Generator{
		size: size,
		mode: m,
		r:    rand.New(rand.NewSource(seed)),
	}
	g.zeros = make([]byte, size)
	g.text = loremTemplate(size, g.r)
	return g, nil
}

// Next fills `buf` (len must equal size) with a body. The first 16 bytes of buf
// will be overwritten by the producer with a header — generators should write
// there too, but it's fine: the header overwrite is intentional.
func (g *Generator) Next(buf []byte, seq uint64) {
	if len(buf) != g.size {
		panic("payload: buf size mismatch")
	}
	m := g.mode
	if m == ModeMixed {
		// 5 classes rotate by seq
		switch seq % 5 {
		case 0:
			m = ModeText
		case 1:
			m = ModeJSON
		case 2:
			m = ModeLogline
		case 3:
			m = ModeRandom
		case 4:
			m = ModeZeros
		}
	}
	switch m {
	case ModeRandom:
		g.mu.Lock()
		g.r.Read(buf)
		g.mu.Unlock()
	case ModeZeros:
		copy(buf, g.zeros)
	case ModeText:
		copy(buf, g.text)
	case ModeJSON:
		g.mu.Lock()
		writeJSON(buf, g.r, seq)
		g.mu.Unlock()
	case ModeLogline:
		g.mu.Lock()
		writeLogline(buf, g.r, seq)
		g.mu.Unlock()
	}
}

// SampleFor returns a representative buffer for off-line compression-ratio
// measurement (see internal/metrics/compression.go).
func (g *Generator) SampleFor(n int) []byte {
	buf := make([]byte, g.size)
	g.Next(buf, uint64(n))
	return buf
}

func (g *Generator) Mode() Mode { return g.mode }

// --- generators ---

var loremWords = strings.Fields(`
lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua enim ad minim veniam quis nostrud
exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute
irure dolor in reprehenderit voluptate velit esse cillum fugiat nulla pariatur
excepteur sint occaecat cupidatat non proident sunt culpa officia deserunt
mollit anim id est laborum
`)

func loremTemplate(size int, r *rand.Rand) []byte {
	var sb strings.Builder
	sb.Grow(size + 32)
	for sb.Len() < size {
		sb.WriteString(loremWords[r.Intn(len(loremWords))])
		sb.WriteByte(' ')
	}
	b := []byte(sb.String())
	return b[:size]
}

func writeJSON(buf []byte, r *rand.Rand, seq uint64) {
	// ~realistic event: stable schema, varying numeric + a category word
	// JSON with repeated keys compresses very well on real traffic.
	tmpl := `{"ts":%d,"seq":%d,"user_id":%d,"event":"%s","region":"%s","val":%d,"ok":%s}`
	evs := []string{"click", "view", "purchase", "scroll", "login", "signup"}
	regs := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1"}
	ok := "true"
	if r.Intn(8) == 0 {
		ok = "false"
	}
	s := fmt.Sprintf(tmpl,
		time.Now().UnixNano(),
		seq,
		r.Int63n(1_000_000_000),
		evs[r.Intn(len(evs))],
		regs[r.Intn(len(regs))],
		r.Int63n(100000),
		ok,
	)
	b := []byte(s)
	if len(b) >= len(buf) {
		copy(buf, b[:len(buf)])
		return
	}
	// pad with spaces (still highly compressible)
	copy(buf, b)
	for i := len(b); i < len(buf); i++ {
		buf[i] = ' '
	}
}

func writeLogline(buf []byte, r *rand.Rand, seq uint64) {
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{"/api/v1/users", "/api/v1/orders", "/health", "/metrics", "/api/v1/feed"}
	agents := []string{"Mozilla/5.0", "curl/8.1.2", "okhttp/4.9", "Go-http-client/1.1"}
	s := fmt.Sprintf(
		`%d 127.0.0.1 - - [%s] "%s %s HTTP/1.1" %d %d "-" "%s" seq=%d`,
		time.Now().Unix(),
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		methods[r.Intn(len(methods))],
		paths[r.Intn(len(paths))],
		[]int{200, 200, 200, 301, 404, 500}[r.Intn(6)],
		r.Intn(65536),
		agents[r.Intn(len(agents))],
		seq,
	)
	b := []byte(s)
	if len(b) >= len(buf) {
		copy(buf, b[:len(buf)])
		return
	}
	copy(buf, b)
	for i := len(b); i < len(buf); i++ {
		buf[i] = '\n'
	}
}

// WriteHeader stamps worker/seq/timestamp into the first 16 bytes of the
// payload so consumer can measure end-to-end latency.
func WriteHeader(buf []byte, worker uint32, seq uint32, ts int64) {
	_ = buf[15]
	binary.LittleEndian.PutUint32(buf[0:4], worker)
	binary.LittleEndian.PutUint32(buf[4:8], seq)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(ts))
}

// ReadHeader pulls the timestamp back out on the consumer side.
func ReadHeader(buf []byte) (worker, seq uint32, ts int64, ok bool) {
	if len(buf) < 16 {
		return
	}
	worker = binary.LittleEndian.Uint32(buf[0:4])
	seq = binary.LittleEndian.Uint32(buf[4:8])
	ts = int64(binary.LittleEndian.Uint64(buf[8:16]))
	ok = true
	return
}

/*
the idea is that compression depends on what type of payload you send.
Generator gives you payloads of a fixed size, in a chosen style
Love the information theory flavour here,
[10 12 32 43 523] has more entropy than [0 0 0 0 0]
Random has high entropy
Zeroes is the best
Text is english like,
Json is even streams very realistic, the keys repeat so compressible
Logline server logs - very compressible, GET api/v1/.. repeated
mixed - rotates the 5

rand.Rand() is not thread safe, so random / json / logline -> lock
zeroes / text -> no lock
g.Next(buf, uint64(seq)) would be called on the same Generator instance, RNG keeps an internal state, g.r.Read() etc. mutate that internal state.
Only one go routine should use the rng at a time
Custome header: worker, seq, timestamp

We introduce contention as payload generator is a data resource, will my thorughput drop becuse generator is throttling input?
can kafka handle more?

data generation cost + kafka performance is being mixed in?
should I use generator per worker?
or pre generate buffers?
or pre generate a sample pool and randomly sample?
*/
