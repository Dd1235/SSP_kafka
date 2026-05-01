// Package lag polls Kafka for per-partition consumer lag.
//
// Lag = broker high-water-mark - committed consumer offset.
//
// Reported metrics:
//   - total lag across partitions (sum)
//   - max per-partition lag (worst partition)
//   - drain rate when lag is decreasing (msgs/sec) — this is the recovery metric
//   - a timeline so we can plot growth and recovery
//
// Uses Sarama's ClusterAdmin.ListConsumerGroupOffsets + Client.GetOffset.
package lag

import (
	"context"
	"fmt"
	"kafka-bench-v4/internal/config"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

type Sample struct {
	ElapsedS     float64         `json:"elapsed_s"`
	TotalLag     int64           `json:"total_lag"`
	MaxLag       int64           `json:"max_partition_lag"`
	Committed    int64           `json:"committed_offset_sum"`
	EndOffset    int64           `json:"end_offset_sum"`
	PerPartition map[int32]int64 `json:"per_partition_lag"`
}

type Poller struct {
	cfg     *config.BenchConfig
	client  sarama.Client
	admin   sarama.ClusterAdmin
	samples []Sample
	mu      sync.Mutex
	start   time.Time
	done    chan struct{}
	// current is the most recent total-lag observation. Exposed via
	// Current() so the producer pool can read it atomically without
	// touching the samples slice. -1 means "no sample yet."
	current atomic.Int64
}

func NewPoller(cfg *config.BenchConfig) (*Poller, error) {
	scfg := sarama.NewConfig()
	scfg.Version = sarama.V3_6_0_0
	cli, err := sarama.NewClient(cfg.Brokers, scfg)
	if err != nil {
		return nil, fmt.Errorf("lag client: %w", err)
	}
	adm, err := sarama.NewClusterAdminFromClient(cli)
	if err != nil {
		cli.Close()
		return nil, fmt.Errorf("lag admin: %w", err)
	}
	p := &Poller{
		cfg:    cfg,
		client: cli,
		admin:  adm,
		done:   make(chan struct{}),
	}
	p.current.Store(-1)
	return p, nil
}

// Current returns the most recent observed total lag, or -1 if no sample
// has been taken yet. Safe for concurrent reads from any goroutine.
func (p *Poller) Current() int64 { return p.current.Load() }

// Run blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	p.start = time.Now()
	t := time.NewTicker(p.cfg.LagPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			close(p.done)
			return
		case now := <-t.C:
			s, err := p.takeSample(now)
			if err != nil {
				// Transient errors are fine; log once per unique error type.
				continue
			}
			p.mu.Lock()
			p.samples = append(p.samples, s)
			p.mu.Unlock()
		}
	}
}

func (p *Poller) Wait() { <-p.done }
func (p *Poller) Close() {
	if p.admin != nil {
		_ = p.admin.Close()
	}
	if p.client != nil {
		_ = p.client.Close()
	}
}

func (p *Poller) Samples() []Sample {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Sample, len(p.samples))
	copy(out, p.samples)
	return out
}

// Summary aggregates the timeline into a few headline numbers.
type Summary struct {
	MaxTotalLag   int64 `json:"max_total_lag"`
	FinalTotalLag int64 `json:"final_total_lag"`
	// DrainRate is msg/s at which lag decreased during the recovery portion
	// (only meaningful if lag went up then down).
	RecoveryDrainRate float64 `json:"recovery_drain_rate_msg_per_sec"`
	// TimeToDrainS is seconds from peak-lag sample to lag==0 (or final sample
	// if never fully drained).
	TimeToDrainS float64 `json:"time_to_drain_s"`
	SampleCount  int     `json:"sample_count"`
}

func (p *Poller) Summary() Summary {
	p.mu.Lock()
	s := make([]Sample, len(p.samples))
	copy(s, p.samples)
	p.mu.Unlock()
	return summarizeSamples(s)
}

func summarizeSamples(s []Sample) Summary {
	if len(s) == 0 {
		return Summary{}
	}
	var peakLag int64
	peakIdx := 0
	for i, x := range s {
		if x.TotalLag > peakLag {
			peakLag = x.TotalLag
			peakIdx = i
		}
	}
	final := s[len(s)-1]
	sum := Summary{
		MaxTotalLag:   peakLag,
		FinalTotalLag: final.TotalLag,
		SampleCount:   len(s),
	}
	// Drain from peak to end: if lag fell, compute rate.
	if peakIdx < len(s)-1 && peakLag > final.TotalLag {
		dt := s[len(s)-1].ElapsedS - s[peakIdx].ElapsedS
		if dt > 0 {
			sum.RecoveryDrainRate = float64(peakLag-final.TotalLag) / dt
		}
		// find first sample where lag<=0 after peak
		zeroIdx := -1
		for i := peakIdx; i < len(s); i++ {
			if s[i].TotalLag <= 0 {
				zeroIdx = i
				break
			}
		}
		if zeroIdx > 0 {
			sum.TimeToDrainS = s[zeroIdx].ElapsedS - s[peakIdx].ElapsedS
		} else {
			sum.TimeToDrainS = s[len(s)-1].ElapsedS - s[peakIdx].ElapsedS
		}
	}
	return sum
}

func (p *Poller) takeSample(now time.Time) (Sample, error) {
	// Get end offsets (high-water-mark) for every partition of the topic.
	parts, err := p.client.Partitions(p.cfg.Topic)
	if err != nil {
		return Sample{}, err
	}
	ends := make(map[int32]int64, len(parts))
	var endSum int64
	for _, part := range parts {
		ho, err := p.client.GetOffset(p.cfg.Topic, part, sarama.OffsetNewest)
		if err != nil {
			continue
		}
		ends[part] = ho
		endSum += ho
	}

	// Get committed offsets for the group.
	topicParts := map[string][]int32{p.cfg.Topic: parts}
	gr, err := p.admin.ListConsumerGroupOffsets(p.cfg.GroupID, topicParts)
	if err != nil {
		return Sample{}, err
	}
	committed := make(map[int32]int64, len(parts))
	var commSum int64
	if gr != nil {
		for _, blocks := range gr.Blocks {
			for part, block := range blocks {
				committed[part] = block.Offset
				if block.Offset >= 0 {
					commSum += block.Offset
				}
			}
		}
	}

	s := buildSample(p.start, now, parts, ends, committed)
	p.current.Store(s.TotalLag)
	s.Committed = commSum
	s.EndOffset = endSum
	return s, nil
}

func buildSample(start, now time.Time, parts []int32, ends, committed map[int32]int64) Sample {
	per := make(map[int32]int64, len(parts))
	var total, maxLag, commSum, endSum int64
	for _, part := range parts {
		end := ends[part]
		endSum += end
		c, ok := committed[part]
		if !ok || c < 0 {
			c = 0
		}
		commSum += c
		diff := end - c
		if diff < 0 {
			diff = 0
		}
		per[part] = diff
		total += diff
		if diff > maxLag {
			maxLag = diff
		}
	}
	return Sample{
		ElapsedS:     now.Sub(start).Seconds(),
		TotalLag:     total,
		MaxLag:       maxLag,
		Committed:    commSum,
		EndOffset:    endSum,
		PerPartition: per,
	}
}

/*
lag ≈ constant (e.g. ~9000) => healthy
because it's every 1s that sarama commits the offset
so messages are processed by consumer at 9000 msg / sec
but because of the commit lag, it shows 9k
so committed != processed
Backpressure: lag keeps increasing → 10k → 20k → 50k
We measure per partition lag also as there may be hot partitions

Lag = EndOffset (latest produced) - CommitOffset (consumer progress)
= Broker HWM - committed consumer offset

concurrency design:
samples -> protected by mutex
current -> atomic (fast path)

we get
- real time lag signal
- historical timeline
- derived metrics


Creates a new sarama client
Multiple goroutines may read the current
So to avoid data race we use atomic

atomic operation is a cpu level safe read/write operation with memory ordering guarantees

we may get artifical contention if every guy has to lock the same mutex, so atomic is better

we don't want one goroutine to append while another reads samples so we lock it

reading while appending to a slide is dangerous, a slide has a pointer, length, and capacity. Appending can reallocate the underlying array!

struct{} - empty, zero bytes

run closes done when it exists
Wait() blocks until done is closed

need to learn underlying implementation of context in go for sure! its all open source!

poller runs until someone cancels the conetxt
we use pointer receivers to take on methods on Poller


ticker -> sends the current time repeatedly on a channel

ticker uses runtime resources, if it's not stopped, it can leak

so we use the client to get the partitions a topic lives in.
get the end offset per partition.
Then get committed offset per cg
clamp negative lag to zero

summary gets the max total lag, final total lag, recovery drain rate, time to drain, and sample count

notice we copy sample instead of return the slude directly, so that external code can't corrupt poller history

*/
