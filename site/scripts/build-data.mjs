import { copyFile, mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "../..");
const inputDir = path.join(root, "paper", "data");
const outputDir = path.join(root, "site", "public", "data");

const categories = [
  {
    id: "baseline",
    label: "Baseline",
    description: "Healthy 10k msg/s mixed-payload run.",
  },
  {
    id: "compression",
    label: "Compression",
    description: "Codec sweep over a realistic mixed payload.",
  },
  {
    id: "acks",
    label: "Acks",
    description: "Producer acknowledgment policy sweep.",
  },
  {
    id: "payload",
    label: "Payloads",
    description: "Compressibility and latency across payload classes.",
  },
  {
    id: "message-size",
    label: "Message Size",
    description: "Throughput and tail latency as payload bytes grow.",
  },
  {
    id: "lag-recovery",
    label: "Lag & Recovery",
    description: "Slow-consumer lag growth and drain behavior.",
  },
  {
    id: "bursty",
    label: "Bursty",
    description: "Transient over-capacity demand scenarios.",
  },
  {
    id: "backpressure",
    label: "Backpressure",
    description: "Lag-bounded producer throttling experiments.",
  },
  {
    id: "other",
    label: "Other",
    description: "Additional benchmark reports.",
  },
];

const meta = {
  "results-bench": {
    category: "baseline",
    label: "Baseline",
    order: 0,
    description: "30s healthy baseline at 10k msg/s, mixed payload, snappy.",
  },
  "results-comp-none": { category: "compression", label: "Codec: none", order: 10 },
  "results-comp-snappy": { category: "compression", label: "Codec: snappy", order: 11 },
  "results-comp-lz4": { category: "compression", label: "Codec: lz4", order: 12 },
  "results-comp-gzip": { category: "compression", label: "Codec: gzip", order: 13 },
  "results-comp-zstd": { category: "compression", label: "Codec: zstd", order: 14 },
  "results-acks0": { category: "acks", label: "acks=0", order: 20 },
  "results-acks1": { category: "acks", label: "acks=1", order: 21 },
  "results-acks-1": { category: "acks", label: "acks=-1", order: 22 },
  "results-payload-random": { category: "payload", label: "Payload: random", order: 30 },
  "results-payload-mixed": { category: "payload", label: "Payload: mixed", order: 31 },
  "results-payload-json": { category: "payload", label: "Payload: json", order: 32 },
  "results-payload-logline": { category: "payload", label: "Payload: logline", order: 33 },
  "results-payload-text": { category: "payload", label: "Payload: text", order: 34 },
  "results-payload-zeros": { category: "payload", label: "Payload: zeros", order: 35 },
  "results-size-64": { category: "message-size", label: "64 B", order: 40 },
  "results-size-256": { category: "message-size", label: "256 B", order: 41 },
  "results-size-1024": { category: "message-size", label: "1 KiB", order: 42 },
  "results-size-4096": { category: "message-size", label: "4 KiB", order: 43 },
  "results-size-16384": { category: "message-size", label: "16 KiB", order: 44 },
  "results-slow-consumer": {
    category: "lag-recovery",
    label: "Slow consumer",
    order: 50,
  },
  "results-slow-aggr": {
    category: "lag-recovery",
    label: "Slow consumer, aggressive",
    order: 51,
  },
  "results-recovery": { category: "lag-recovery", label: "Recovery", order: 52 },
  "results-recovery-aggr": {
    category: "lag-recovery",
    label: "Recovery, aggressive",
    order: 53,
  },
  "results-bursty-light": { category: "bursty", label: "Bursty: light", order: 60 },
  "results-bursty-heavy": { category: "bursty", label: "Bursty: heavy", order: 61 },
  "results-bursty-overload": {
    category: "bursty",
    label: "Bursty: overload",
    order: 62,
  },
  "results-bp-on": {
    category: "backpressure",
    label: "Backpressure: loose",
    order: 70,
  },
  "results-bp-tight": {
    category: "backpressure",
    label: "Backpressure: tight",
    order: 71,
  },
};

function mustHave(report, key, file) {
  if (!(key in report)) {
    throw new Error(`${file}: missing required top-level key "${key}"`);
  }
}

function latencyValue(latency, key) {
  return typeof latency?.[key] === "number" ? latency[key] : null;
}

function numberValue(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function compressionRatios(report) {
  const codecs = report.compression_offline?.codecs ?? {};
  return Object.fromEntries(
    Object.entries(codecs).map(([codec, value]) => [codec, numberValue(value?.ratio)]),
  );
}

function buildSummary(report) {
  const final = report.final ?? {};
  const sent = numberValue(final.messages_sent) ?? 0;
  const received = numberValue(final.messages_received) ?? 0;
  return {
    elapsedS: numberValue(final.elapsed_s),
    messagesSent: sent,
    messagesReceived: received,
    deliveryPct: sent > 0 ? (received / sent) * 100 : null,
    avgRate: numberValue(final.avg_rate_msg_per_sec),
    avgRecvRate: numberValue(final.avg_recv_rate_msg_per_sec),
    throughputMB: numberValue(final.throughput_mb_per_sec),
    errors: numberValue(final.errors),
    ackP50: latencyValue(final.ack_latency_ms, "P50"),
    ackP99: latencyValue(final.ack_latency_ms, "P99"),
    e2eP50: latencyValue(final.e2e_latency_ms, "P50"),
    e2eP95: latencyValue(final.e2e_latency_ms, "P95"),
    e2eP99: latencyValue(final.e2e_latency_ms, "P99"),
    e2eP999: latencyValue(final.e2e_latency_ms, "P999"),
    peakLag: numberValue(report.lag_summary?.max_total_lag),
    finalLag: numberValue(report.lag_summary?.final_total_lag),
    drainRate: numberValue(report.lag_summary?.recovery_drain_rate_msg_per_sec),
    timeToDrainS: numberValue(report.lag_summary?.time_to_drain_s),
    gcPauseMs: numberValue(report.sysperf_summary?.gc_pause_total_ms),
    maxCpuPct: numberValue(report.sysperf_summary?.max_cpu_fraction_pct),
    bpEvents: numberValue(report.bp_events),
    bpPausedS: numberValue(report.bp_paused_s),
    compressionRatios: compressionRatios(report),
  };
}

function inferMeta(id, report) {
  if (meta[id]) {
    return meta[id];
  }
  const cfg = report.config ?? {};
  return {
    category: "other",
    label: id.replace(/^results-/, "").replaceAll("-", " "),
    order: 999,
    description: `${cfg.target_rate ?? "unknown"} msg/s benchmark run.`,
  };
}

await mkdir(outputDir, { recursive: true });

const files = (await readdir(inputDir)).filter((file) => /^results-.*\.json$/.test(file));
const runs = [];

for (const file of files) {
  const source = path.join(inputDir, file);
  const raw = await readFile(source, "utf8");
  const report = JSON.parse(raw);
  mustHave(report, "config", file);
  mustHave(report, "final", file);
  mustHave(report, "timeline", file);
  if (!report.final.ack_latency_ms || !report.final.e2e_latency_ms) {
    throw new Error(`${file}: missing ack/e2e latency blocks`);
  }

  const id = file.replace(/\.json$/, "");
  const info = inferMeta(id, report);
  const summary = buildSummary(report);
  runs.push({
    id,
    file,
    label: info.label,
    category: info.category,
    description: info.description ?? "",
    order: info.order ?? 999,
    config: report.config,
    summary,
  });

  await copyFile(source, path.join(outputDir, file));
}

runs.sort((a, b) => a.order - b.order || a.id.localeCompare(b.id));

const index = {
  generatedAt: new Date().toISOString(),
  source: "paper/data",
  categories,
  runs,
};

await writeFile(path.join(outputDir, "index.json"), `${JSON.stringify(index, null, 2)}\n`);
console.log(`Wrote ${runs.length} benchmark reports to ${path.relative(root, outputDir)}`);
