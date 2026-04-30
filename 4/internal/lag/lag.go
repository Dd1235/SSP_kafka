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
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"kafka-bench-v4/internal/config"
)

type Sample struct {
	ElapsedS     float64          `json:"elapsed_s"`
	TotalLag     int64            `json:"total_lag"`
	MaxLag       int64            `json:"max_partition_lag"`
	Committed    int64            `json:"committed_offset_sum"`
	EndOffset    int64            `json:"end_offset_sum"`
	PerPartition map[int32]int64  `json:"per_partition_lag"`
}

type Poller struct {
	cfg      *config.BenchConfig
	client   sarama.Client
	admin    sarama.ClusterAdmin
	samples  []Sample
	mu       sync.Mutex
	start    time.Time
	done     chan struct{}
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

func (p *Poller) Wait()           { <-p.done }
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
	MaxTotalLag   int64   `json:"max_total_lag"`
	FinalTotalLag int64   `json:"final_total_lag"`
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

	per := make(map[int32]int64, len(parts))
	var total, maxLag int64
	for _, part := range parts {
		end := ends[part]
		c, ok := committed[part]
		if !ok || c < 0 {
			c = 0
		}
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
	p.current.Store(total)
	return Sample{
		ElapsedS:     now.Sub(p.start).Seconds(),
		TotalLag:     total,
		MaxLag:       maxLag,
		Committed:    commSum,
		EndOffset:    endSum,
		PerPartition: per,
	}, nil
}
