package main

import (
	"flag"
	"fmt"
	"kafka-bench-v4/internal/lag"
	"kafka-bench-v4/internal/payload"
	"os"
	"time"
)

/*
go run ./cmd/visualize
go run ./cmd/visualize --mode=payload_single
go run ./cmd/visualize --mode=payload_batch
go run ./cmd/visualize --mode=lag
go run ./cmd/visualize --mode=lag --lag-producer-rate=10000 --lag-consumer-rate=4000 --lag-phase-at=8s --lag-phase-consumer-rate=16000

interesting: logline gave 0.94, json just a lil more at 1.05, ofcourse random the worst 0.82 as expected, so gzip compression actually made is worse
the thing is a single logline doesn't give much compression
Producer → [batch messages] → compress → send  ====> this is when compression helps out, multiple messages batched and written to the same partition.

With batching, its as expected
zeroes > text > loglin > json > mixed > random = 1

interestingly enough, even random improves from 0.82 to 1.00 due to batching, gzip overhead got amortized.

compressed ~ 128 bytes (single message size) + ~20 bytes (gzip header)
*/

func main() {

	mode := flag.String("mode", "payload_single", "what to visualize")

	lagPartitions := flag.Int("lag-partitions", 4, "lag simulation partition count")
	lagDuration := flag.Duration("lag-duration", 16*time.Second, "lag simulation duration")
	lagPoll := flag.Duration("lag-poll", time.Second, "lag simulation sample interval")
	lagProducerRate := flag.Float64("lag-producer-rate", 10000, "lag simulation producer rate in msg/s")
	lagConsumerRate := flag.Float64("lag-consumer-rate", 4000, "lag simulation consumer commit rate in msg/s")
	lagPhaseAt := flag.Duration("lag-phase-at", 8*time.Second, "lag simulation phase switch time; 0 disables")
	lagPhaseProducerRate := flag.Float64("lag-phase-producer-rate", -1, "producer rate after phase switch; -1 keeps base rate")
	lagPhaseConsumerRate := flag.Float64("lag-phase-consumer-rate", 16000, "consumer rate after phase switch; -1 keeps base rate")
	flag.Parse()

	switch *mode {
	case "payload_single":
		payload.Visualize()
	case "payload_batch":
		payload.VisualizeBatch()
	case "lag":
		lag.VisualizeSimulation(lag.SimulationConfig{
			Partitions:        *lagPartitions,
			Duration:          *lagDuration,
			PollInterval:      *lagPoll,
			ProducerRate:      *lagProducerRate,
			ConsumerRate:      *lagConsumerRate,
			PhaseAt:           *lagPhaseAt,
			PhaseProducerRate: *lagPhaseProducerRate,
			PhaseConsumerRate: *lagPhaseConsumerRate,
		})
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (use payload_single, payload_batch, or lag)\n", *mode)
		os.Exit(2)
	}
}
