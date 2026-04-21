// kafka-bench-v4 — main entry point.
//
// High-level flow:
//   1. parse flags → config
//   2. ensure/reset topic
//   3. optionally compute an off-line compression report over sample payloads
//   4. start lag-poller and sysperf sampler
//   5. run warmup (counters wiped at end)
//   6. run bench: producer + consumer pools, interval reporter
//   7. freeze bench window, drain, collect, emit report
//
// All the moving parts live in internal/*. This file is just wiring.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"kafka-bench-v4/internal/config"
	"kafka-bench-v4/internal/consumer"
	"kafka-bench-v4/internal/lag"
	"kafka-bench-v4/internal/metrics"
	"kafka-bench-v4/internal/output"
	"kafka-bench-v4/internal/payload"
	"kafka-bench-v4/internal/producer"
	"kafka-bench-v4/internal/sysperf"
	"kafka-bench-v4/internal/topic"

	"github.com/IBM/sarama"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// Quieter Sarama — prints too much at default level
	sarama.Logger = log.New(os.Stderr, "[sarama] ", log.LstdFlags)

	cfg := config.Parse()
	cfg.Print()

	if err := topic.Ensure(cfg); err != nil {
		log.Fatalf("topic setup: %v", err)
	}

	// Payload generator (shared across producer workers)
	gen, err := payload.New(cfg.PayloadMode, cfg.MessageSize, cfg.PayloadSeed)
	if err != nil {
		log.Fatalf("payload: %v", err)
	}

	// Off-line compression report — 2000 sample messages so numbers are stable.
	var compReport metrics.CompressionReport
	{
		samples := make([][]byte, 2000)
		for i := range samples {
			samples[i] = gen.SampleFor(i)
		}
		compReport = metrics.BuildCompressionReport(samples)
		fmt.Print(compReport.Pretty())
	}

	col := metrics.New()

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigC
		fmt.Println("\n[signal] interrupt — shutting down")
		rootCancel()
	}()

	// Lag poller and sysperf sampler run for the entire duration
	// (including warmup — harmless, samples are timestamped).
	lagPoll, err := lag.NewPoller(cfg)
	if err != nil {
		log.Fatalf("lag poller: %v", err)
	}
	defer lagPoll.Close()
	lagCtx, lagCancel := context.WithCancel(rootCtx)
	go lagPoll.Run(lagCtx)

	sp := sysperf.NewSampler(cfg.SysPerfInterval)
	spCtx, spCancel := context.WithCancel(rootCtx)
	go sp.Run(spCtx)

	// Warmup
	if cfg.WarmupDuration > 0 {
		fmt.Printf("\n[warmup] %s — results discarded\n", cfg.WarmupDuration)
		col.Start()
		warmupCtx, warmupCancel := context.WithTimeout(rootCtx, cfg.WarmupDuration)
		runBench(warmupCtx, cfg, col, gen)
		warmupCancel()
		col.ResetAfterWarmup()
		fmt.Println("[warmup] done — counters reset")
	} else {
		col.Start()
	}

	// Bench
	fmt.Printf("\n[bench] %s @ %d msg/s target\n", cfg.Duration, cfg.TargetRate)
	benchCtx, benchCancel := context.WithTimeout(rootCtx, cfg.Duration)
	defer benchCancel()

	// Interval reporter
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		t := time.NewTicker(cfg.ReportInterval)
		defer t.Stop()
		printHeader()
		for {
			select {
			case <-benchCtx.Done():
				return
			case <-t.C:
				snap := col.IntervalReport()
				printInterval(snap)
			}
		}
	}()

	runBench(benchCtx, cfg, col, gen)
	col.MarkEnd() // freeze the bench window — everything after this is teardown
	<-reporterDone

	// Stop samplers cleanly and wait
	lagCancel()
	spCancel()
	lagPoll.Wait()
	sp.Wait()

	final := col.FinalSnapshot()
	tl := col.Timeline()
	lagSamples := lagPoll.Samples()
	lagSum := lagPoll.Summary()
	spSamples := sp.Samples()
	spSum := sp.Summary()

	rep := output.Build(cfg, final, tl, lagSamples, lagSum, spSamples, spSum, compReport)
	output.PrintHuman(rep)

	if cfg.JSONOutput || cfg.OutputFile != "" {
		if err := output.Write(rep, cfg.JSONOutput, cfg.OutputFile); err != nil {
			log.Printf("output: %v", err)
		}
	}
}

func runBench(ctx context.Context, cfg *config.BenchConfig, col *metrics.Collector, gen *payload.Generator) {
	var wg sync.WaitGroup
	if !cfg.ConsumerOnly {
		pool, err := producer.NewPool(cfg, col, gen)
		if err != nil {
			log.Fatalf("producer pool: %v", err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); pool.Run(ctx) }()
	}
	if !cfg.ProducerOnly {
		pool, err := consumer.NewPool(cfg, col)
		if err != nil {
			log.Fatalf("consumer pool: %v", err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); pool.Run(ctx) }()
	}
	wg.Wait()
}

func printHeader() {
	fmt.Printf("\n%-10s %-10s %-10s %-10s %-10s  latency(ms)\n",
		"elapsed", "inst/s", "recv/s", "MB/s", "errors")
	fmt.Println("──────────────────────────────────────────────────────────────────────")
}

func printInterval(s metrics.IntervalSnapshot) {
	fmt.Printf("%8.1fs  %-10.0f %-10.0f %-10.2f %-10d  ack p50=%5.1f p99=%6.1f | e2e p50=%5.1f p99=%6.1f\n",
		s.ElapsedS, s.InstRate, s.RecvRate, s.ThroughputMB, s.Errors,
		s.AckP50Ms, s.AckP99Ms, s.E2EP50Ms, s.E2EP99Ms)
}
