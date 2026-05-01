package main

import (
	"flag"
	"kafka-bench-v4/internal/payload"
)

/*
go run ./cmd/visualize
go run ./cmd/visualize --mode=payload

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
	flag.Parse()

	switch *mode {
	case "payload_single":
		payload.Visualize()
	case "payload_batch":
		payload.VisualizeBatch()
	}
}
