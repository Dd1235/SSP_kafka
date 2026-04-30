export type Category = {
  id: string;
  label: string;
  description: string;
};

export type RunSummary = {
  elapsedS: number | null;
  messagesSent: number;
  messagesReceived: number;
  deliveryPct: number | null;
  avgRate: number | null;
  avgRecvRate: number | null;
  throughputMB: number | null;
  errors: number | null;
  ackP50: number | null;
  ackP99: number | null;
  e2eP50: number | null;
  e2eP95: number | null;
  e2eP99: number | null;
  e2eP999: number | null;
  peakLag: number | null;
  finalLag: number | null;
  drainRate: number | null;
  timeToDrainS: number | null;
  gcPauseMs: number | null;
  maxCpuPct: number | null;
  bpEvents: number | null;
  bpPausedS: number | null;
  compressionRatios: Record<string, number | null>;
};

export type RunInfo = {
  id: string;
  file: string;
  label: string;
  category: string;
  description: string;
  order: number;
  config: Record<string, unknown>;
  summary: RunSummary;
};

export type RunIndex = {
  generatedAt: string;
  source: string;
  categories: Category[];
  runs: RunInfo[];
};

export type LatencyStats = {
  Min: number;
  Max: number;
  Mean: number;
  StdDev: number;
  Variance: number;
  P50: number;
  P75: number;
  P90: number;
  P95: number;
  P99: number;
  P995: number;
  P999: number;
  P9999: number;
  MAD: number;
  Count: number;
  CDFBuckets: number[];
};

export type TimelinePoint = {
  elapsed_s: number;
  inst_rate: number;
  recv_rate: number;
  throughput_mb: number;
  ack_p50_ms: number;
  ack_p99_ms: number;
  e2e_p50_ms: number;
  e2e_p99_ms: number;
  errors: number;
};

export type LagPoint = {
  elapsed_s: number;
  total_lag: number;
  committed_sum?: number;
  end_offset_sum?: number;
  per_partition_lag?: Record<string, number>;
};

export type SysPerfPoint = {
  elapsed_s: number;
  goroutines: number;
  heap_alloc_mb: number;
  heap_inuse_mb: number;
  stack_inuse_mb: number;
  sys_mb: number;
  num_gc: number;
  gc_pause_total_ms: number;
  gc_pause_last_ms: number;
  cpu_fraction_pct: number;
  gomaxprocs: number;
};

export type CodecStats = {
  compressed_bytes: number;
  ratio: number;
  space_saved_pct: number;
  encode_micros: number;
};

export type BenchmarkReport = {
  config: Record<string, unknown>;
  final: {
    elapsed_s: number;
    messages_sent: number;
    messages_received: number;
    errors: number;
    bytes_sent: number;
    bytes_received: number;
    avg_rate_msg_per_sec: number;
    avg_recv_rate_msg_per_sec: number;
    throughput_mb_per_sec: number;
    ack_latency_ms: LatencyStats;
    e2e_latency_ms: LatencyStats;
  };
  timeline: TimelinePoint[];
  lag_timeline: LagPoint[];
  lag_summary: {
    max_total_lag: number;
    final_total_lag: number;
    recovery_drain_rate_msg_per_sec: number;
    time_to_drain_s: number;
    sample_count: number;
  };
  sysperf_timeline: SysPerfPoint[];
  sysperf_summary: {
    count: number;
    avg_cpu_fraction_pct: number;
    max_cpu_fraction_pct: number;
    avg_goroutines: number;
    max_goroutines: number;
    max_heap_alloc_mb: number;
    max_heap_inuse_mb: number;
    num_gc: number;
    gc_pause_total_ms: number;
  };
  compression_offline: {
    raw_bytes: number;
    sample_count: number;
    codecs: Record<string, CodecStats>;
  };
  bp_events: number;
  bp_paused_s: number;
};
