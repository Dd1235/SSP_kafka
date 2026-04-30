import { useEffect, useMemo, useState } from "react";
import { BarChart, LineChart } from "./charts";
import type { BenchmarkReport, Category, RunIndex, RunInfo } from "./types";

const DATA_ROOT = `${import.meta.env.BASE_URL}data/`;
const COLORS = [
  "#2f6fed",
  "#0e8f68",
  "#d86422",
  "#8c5ccf",
  "#c93448",
  "#607d8b",
];
const CODECS = ["none", "snappy", "lz4", "gzip", "zstd"];

async function loadJson<T>(file: string): Promise<T> {
  const response = await fetch(`${DATA_ROOT}${file}`);
  if (!response.ok) {
    throw new Error(`Failed to load ${file}: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

function fmt(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "n/a";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

function compact(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "n/a";
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: digits,
  }).format(value);
}

function fmtMs(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "n/a";
  if (value >= 1000) return `${fmt(value / 1000, 2)} s`;
  return `${fmt(value, value < 10 ? 2 : 1)} ms`;
}

function fmtRate(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "n/a";
  return `${compact(value, 2)} msg/s`;
}

function fmtPct(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "n/a";
  return `${fmt(value, 2)}%`;
}

function fmtSeconds(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "n/a";
  return `${fmt(value, value < 10 ? 2 : 1)} s`;
}

function configValue(value: unknown) {
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "number")
    return fmt(value, Number.isInteger(value) ? 0 : 2);
  if (typeof value === "boolean") return value ? "true" : "false";
  if (value === null || value === undefined || value === "") return "n/a";
  return String(value);
}

function firstRunForCategory(runs: RunInfo[], categoryId: string) {
  return runs.find((run) => run.category === categoryId) ?? runs[0];
}

function useRunFromUrl() {
  const [runId, setRunIdState] = useState(
    () =>
      new URLSearchParams(window.location.search).get("run") ?? "results-bench",
  );

  useEffect(() => {
    const onPop = () =>
      setRunIdState(
        new URLSearchParams(window.location.search).get("run") ??
          "results-bench",
      );
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const setRunId = (next: string) => {
    const url = new URL(window.location.href);
    url.searchParams.set("run", next);
    window.history.pushState({}, "", url);
    setRunIdState(next);
  };

  return [runId, setRunId] as const;
}

export default function App() {
  const [index, setIndex] = useState<RunIndex | null>(null);
  const [report, setReport] = useState<BenchmarkReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [reportLoading, setReportLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runId, setRunId] = useRunFromUrl();

  useEffect(() => {
    let cancelled = false;
    loadJson<RunIndex>("index.json")
      .then((data) => {
        if (!cancelled) {
          setIndex(data);
          setLoading(false);
        }
      })
      .catch((err: Error) => {
        if (!cancelled) {
          setError(err.message);
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const selected = useMemo(() => {
    if (!index) return null;
    return (
      index.runs.find((run) => run.id === runId) ??
      index.runs.find((run) => run.id === "results-bench") ??
      index.runs[0]
    );
  }, [index, runId]);

  useEffect(() => {
    if (!selected) return;
    let cancelled = false;
    setReportLoading(true);
    loadJson<BenchmarkReport>(selected.file)
      .then((data) => {
        if (!cancelled) {
          setReport(data);
          setReportLoading(false);
        }
      })
      .catch((err: Error) => {
        if (!cancelled) {
          setError(err.message);
          setReportLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selected]);

  if (loading) {
    return <StatusScreen title="Loading benchmark data" />;
  }

  if (error || !index || !selected) {
    return (
      <StatusScreen
        title="Unable to load explorer"
        detail={error ?? "Missing index data."}
      />
    );
  }

  const activeCategory = selected.category;

  return (
    <div className="app-shell">
      <header className="app-header">
        <div>
          <p className="eyebrow">SSP Kafka project</p>
          <h1>Kafka Bench Explorer</h1>
        </div>
        <nav className="header-links" aria-label="Project links">
          <a href="https://github.com/Dd1235/SSP_kafka/tree/main/4">
            Runner docs
          </a>
          <a href="https://github.com/Dd1235/SSP_kafka/tree/main/paper">
            Paper source
          </a>
          <a href={`${DATA_ROOT}index.json`}>Data index</a>
        </nav>
      </header>

      <div className="workspace">
        <aside className="sidebar" aria-label="Benchmark runs">
          <CategoryTabs
            categories={index.categories}
            activeCategory={activeCategory}
            runs={index.runs}
            onSelectCategory={(categoryId) => {
              const next = firstRunForCategory(index.runs, categoryId);
              if (next) setRunId(next.id);
            }}
          />
          <RunList
            runs={index.runs}
            activeRunId={selected.id}
            activeCategory={activeCategory}
            onSelect={setRunId}
          />
        </aside>

        <main className="content">
          <Overview
            runs={index.runs}
            generatedAt={index.generatedAt}
            onSelect={setRunId}
          />
          {reportLoading || !report ? (
            <StatusPanel title="Loading selected run" />
          ) : (
            <RunDetail run={selected} report={report} />
          )}
          <ComparisonSections runs={index.runs} onSelect={setRunId} />
          <ReproductionPanel />
        </main>
      </div>
    </div>
  );
}

function StatusScreen({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className="status-screen">
      <h1>{title}</h1>
      {detail ? <p>{detail}</p> : null}
    </div>
  );
}

function StatusPanel({ title }: { title: string }) {
  return (
    <section className="panel">
      <h2>{title}</h2>
    </section>
  );
}

function CategoryTabs({
  categories,
  activeCategory,
  runs,
  onSelectCategory,
}: {
  categories: Category[];
  activeCategory: string;
  runs: RunInfo[];
  onSelectCategory: (categoryId: string) => void;
}) {
  return (
    <div
      className="category-tabs"
      role="tablist"
      aria-label="Result categories"
    >
      {categories
        .filter((category) => runs.some((run) => run.category === category.id))
        .map((category) => (
          <button
            key={category.id}
            className={category.id === activeCategory ? "active" : ""}
            type="button"
            onClick={() => onSelectCategory(category.id)}
          >
            {category.label}
          </button>
        ))}
    </div>
  );
}

function RunList({
  runs,
  activeRunId,
  activeCategory,
  onSelect,
}: {
  runs: RunInfo[];
  activeRunId: string;
  activeCategory: string;
  onSelect: (runId: string) => void;
}) {
  const visible = runs.filter((run) => run.category === activeCategory);
  return (
    <div className="run-list">
      {visible.map((run) => (
        <button
          key={run.id}
          className={run.id === activeRunId ? "active" : ""}
          type="button"
          onClick={() => onSelect(run.id)}
        >
          <span>{run.label}</span>
          <small>
            {fmtRate(run.summary.avgRate)} · p99 {fmtMs(run.summary.e2eP99)}
          </small>
        </button>
      ))}
    </div>
  );
}

function Overview({
  runs,
  generatedAt,
  onSelect,
}: {
  runs: RunInfo[];
  generatedAt: string;
  onSelect: (runId: string) => void;
}) {
  const baseline = runs.find((run) => run.id === "results-bench");
  const worstP99 = runs.reduce<RunInfo | null>((best, run) => {
    if (typeof run.summary.e2eP99 !== "number") return best;
    if (!best || (best.summary.e2eP99 ?? -1) < run.summary.e2eP99) return run;
    return best;
  }, null);
  const maxLag = runs.reduce<RunInfo | null>((best, run) => {
    if (typeof run.summary.peakLag !== "number") return best;
    if (!best || (best.summary.peakLag ?? -1) < run.summary.peakLag) return run;
    return best;
  }, null);

  return (
    <section className="panel overview-panel">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Static results dashboard</p>
          <h2>Runs, sweeps, and raw reports</h2>
        </div>
        <p className="muted">
          Generated from paper/data on {new Date(generatedAt).toLocaleString()}.
        </p>
      </div>
      <div className="metric-grid">
        <MetricCard
          label="Reports"
          value={String(runs.length)}
          detail="JSON files packaged for GitHub Pages"
        />
        <MetricCard
          label="Baseline rate"
          value={fmtRate(baseline?.summary.avgRate)}
          detail="Healthy mixed-payload run"
        />
        <MetricCard
          label="Worst e2e p99"
          value={fmtMs(worstP99?.summary.e2eP99)}
          detail={worstP99 ? worstP99.label : "n/a"}
          onClick={worstP99 ? () => onSelect(worstP99.id) : undefined}
        />
        <MetricCard
          label="Peak lag"
          value={`${compact(maxLag?.summary.peakLag, 2)} msgs`}
          detail={maxLag ? maxLag.label : "n/a"}
          onClick={maxLag ? () => onSelect(maxLag.id) : undefined}
        />
      </div>
    </section>
  );
}

function MetricCard({
  label,
  value,
  detail,
  onClick,
}: {
  label: string;
  value: string;
  detail?: string;
  onClick?: () => void;
}) {
  const content = (
    <>
      <span>{label}</span>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
    </>
  );
  if (onClick) {
    return (
      <button className="metric-card clickable" type="button" onClick={onClick}>
        {content}
      </button>
    );
  }
  return <div className="metric-card">{content}</div>;
}

function RunDetail({ run, report }: { run: RunInfo; report: BenchmarkReport }) {
  const timeline = report.timeline ?? [];
  const lag = report.lag_timeline ?? [];
  const sysperf = report.sysperf_timeline ?? [];
  const rawUrl = `${DATA_ROOT}${run.file}`;
  const configEntries = Object.entries(report.config).filter(([key]) =>
    [
      "target_rate",
      "message_size",
      "payload_mode",
      "compression",
      "acks",
      "producers",
      "consumers",
      "partitions",
      "consumer_delay",
    ].includes(key),
  );

  return (
    <section className="panel run-detail">
      <div className="section-heading">
        <div>
          <p className="eyebrow">{run.category}</p>
          <h2>{run.label}</h2>
        </div>
        <a className="button-link" href={rawUrl} download>
          Raw JSON
        </a>
      </div>

      <div className="metric-grid compact">
        <MetricCard
          label="Sent"
          value={compact(report.final.messages_sent, 2)}
          detail={fmtRate(report.final.avg_rate_msg_per_sec)}
        />
        <MetricCard
          label="Received"
          value={compact(report.final.messages_received, 2)}
          detail={fmtPct(run.summary.deliveryPct)}
        />
        <MetricCard
          label="E2E p99"
          value={fmtMs(report.final.e2e_latency_ms.P99)}
          detail={`p50 ${fmtMs(report.final.e2e_latency_ms.P50)}`}
        />
        <MetricCard
          label="Peak lag"
          value={`${compact(run.summary.peakLag, 2)} msgs`}
          detail={`final ${compact(run.summary.finalLag, 2)}`}
        />
      </div>

      <div className="chart-grid">
        <LineChart
          title="Throughput timeline"
          yUnit="k/s"
          series={[
            {
              label: "sent",
              color: COLORS[0],
              values: timeline.map((point) => ({
                x: point.elapsed_s,
                y: point.inst_rate / 1000,
              })),
            },
            {
              label: "received",
              color: COLORS[1],
              values: timeline.map((point) => ({
                x: point.elapsed_s,
                y: point.recv_rate / 1000,
              })),
            },
          ]}
        />
        <LineChart
          title="End-to-end latency timeline"
          yUnit="ms"
          series={[
            {
              label: "e2e p50",
              color: COLORS[1],
              values: timeline.map((point) => ({
                x: point.elapsed_s,
                y: point.e2e_p50_ms,
              })),
            },
            {
              label: "e2e p99",
              color: COLORS[4],
              values: timeline.map((point) => ({
                x: point.elapsed_s,
                y: point.e2e_p99_ms,
              })),
            },
          ]}
        />
        {lag.length > 0 ? (
          <LineChart
            title="Consumer lag"
            yUnit="k"
            series={[
              {
                label: "total lag",
                color: COLORS[4],
                values: lag.map((point) => ({
                  x: point.elapsed_s,
                  y: point.total_lag / 1000,
                })),
              },
            ]}
          />
        ) : null}
        {sysperf.length > 0 ? (
          <LineChart
            title="Process CPU"
            yUnit="%"
            series={[
              {
                label: "cpu",
                color: COLORS[3],
                values: sysperf.map((point) => ({
                  x: point.elapsed_s,
                  y: point.cpu_fraction_pct,
                })),
              },
            ]}
          />
        ) : null}
      </div>

      <div className="detail-grid">
        <LatencyTable title="Latency summary" report={report} />
        <CompressionTable report={report} />
        <div className="mini-panel">
          <h3>Run config</h3>
          <dl className="config-list">
            {configEntries.map(([key, value]) => (
              <div key={key}>
                <dt>{key.replaceAll("_", " ")}</dt>
                <dd>{configValue(value)}</dd>
              </div>
            ))}
          </dl>
        </div>
      </div>
    </section>
  );
}

function LatencyTable({
  title,
  report,
}: {
  title: string;
  report: BenchmarkReport;
}) {
  const rows = [
    ["Ack", report.final.ack_latency_ms],
    ["E2E", report.final.e2e_latency_ms],
  ] as const;
  return (
    <div className="mini-panel">
      <h3>{title}</h3>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Path</th>
              <th>p50</th>
              <th>p95</th>
              <th>p99</th>
              <th>max</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(([label, stats]) => (
              <tr key={label}>
                <td>{label}</td>
                <td>{fmtMs(stats.P50)}</td>
                <td>{fmtMs(stats.P95)}</td>
                <td>{fmtMs(stats.P99)}</td>
                <td>{fmtMs(stats.Max)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function CompressionTable({ report }: { report: BenchmarkReport }) {
  const codecs = Object.entries(report.compression_offline?.codecs ?? {}).sort(
    ([a], [b]) => CODECS.indexOf(a) - CODECS.indexOf(b),
  );
  return (
    <div className="mini-panel">
      <h3>Offline compression</h3>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Codec</th>
              <th>Ratio</th>
              <th>Saved</th>
              <th>Encode</th>
            </tr>
          </thead>
          <tbody>
            {codecs.map(([codec, stats]) => (
              <tr key={codec}>
                <td>{codec}</td>
                <td>{fmt(stats.ratio, 2)}x</td>
                <td>{fmtPct(stats.space_saved_pct)}</td>
                <td>{fmt(stats.encode_micros, 0)} us</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ComparisonSections({
  runs,
  onSelect,
}: {
  runs: RunInfo[];
  onSelect: (runId: string) => void;
}) {
  const byCategory = (category: string) =>
    runs.filter((run) => run.category === category);
  const compression = byCategory("compression");
  const payload = byCategory("payload");
  const lagRecovery = byCategory("lag-recovery");
  const bursty = byCategory("bursty");
  const backpressure = [
    "results-slow-aggr",
    "results-bp-on",
    "results-bp-tight",
  ]
    .map((id) => runs.find((run) => run.id === id))
    .filter((run): run is RunInfo => Boolean(run));
  const acks = byCategory("acks");
  const sizes = byCategory("message-size");

  return (
    <section className="panel comparisons">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Cross-run analysis</p>
          <h2>Sweeps and scenario comparisons</h2>
        </div>
      </div>

      <div className="chart-grid">
        <BarChart
          title="Compression sweep: e2e p99"
          unit="ms"
          data={compression.map((run, index) => ({
            label: codecLabel(run),
            value: run.summary.e2eP99,
            color: COLORS[index % COLORS.length],
          }))}
        />
        <BarChart
          title="Compression sweep: GC pause"
          unit="ms"
          data={compression.map((run, index) => ({
            label: codecLabel(run),
            value: run.summary.gcPauseMs,
            color: COLORS[index % COLORS.length],
          }))}
        />
        <BarChart
          title="Payload sweep: zstd ratio"
          unit="x"
          data={payload.map((run, index) => ({
            label: String(
              run.config.payload_mode ?? run.label.replace("Payload: ", ""),
            ),
            value: run.summary.compressionRatios.zstd,
            color: COLORS[index % COLORS.length],
          }))}
        />
        <BarChart
          title="Message size: e2e p99"
          unit="ms"
          data={sizes.map((run, index) => ({
            label: String(run.config.message_size ?? run.label),
            value: run.summary.e2eP99,
            color: COLORS[index % COLORS.length],
          }))}
        />
        <BarChart
          title="Lag and recovery: peak lag"
          unit="k"
          data={lagRecovery.map((run, index) => ({
            label: shortScenarioLabel(run),
            value:
              run.summary.peakLag === null ? null : run.summary.peakLag / 1000,
            color: COLORS[index % COLORS.length],
          }))}
        />
        <BarChart
          title="Bursty workload: e2e p99"
          unit="s"
          data={bursty.map((run, index) => ({
            label: run.label.replace("Bursty: ", ""),
            value:
              run.summary.e2eP99 === null ? null : run.summary.e2eP99 / 1000,
            color: COLORS[index % COLORS.length],
          }))}
        />
        <BarChart
          title="Adaptive backpressure: peak lag"
          unit="k"
          data={backpressure.map((run, index) => ({
            label:
              run.id === "results-slow-aggr"
                ? "off"
                : run.label.replace("Backpressure: ", ""),
            value:
              run.summary.peakLag === null ? null : run.summary.peakLag / 1000,
            color: COLORS[index % COLORS.length],
          }))}
        />
        <BarChart
          title="Acks sweep: e2e p99"
          unit="ms"
          data={acks.map((run, index) => ({
            label: run.label.replace("acks=", ""),
            value: run.summary.e2eP99,
            color: COLORS[index % COLORS.length],
          }))}
        />
      </div>

      <RunMatrix
        title="Quick comparison"
        runs={[
          ...compression,
          ...payload,
          ...lagRecovery,
          ...bursty,
          ...backpressure,
        ]}
        onSelect={onSelect}
      />
    </section>
  );
}

function codecLabel(run: RunInfo) {
  const value = run.config.compression;
  return typeof value === "string" ? value : run.label.replace("Codec: ", "");
}

function shortScenarioLabel(run: RunInfo) {
  return run.label
    .replace("Slow consumer, aggressive", "slow aggr")
    .replace("Slow consumer", "slow")
    .replace("Recovery, aggressive", "recovery aggr")
    .replace("Recovery", "recovery");
}

function RunMatrix({
  title,
  runs,
  onSelect,
}: {
  title: string;
  runs: RunInfo[];
  onSelect: (runId: string) => void;
}) {
  return (
    <div className="mini-panel full-span">
      <h3>{title}</h3>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Run</th>
              <th>Rate</th>
              <th>E2E p99</th>
              <th>Peak lag</th>
              <th>Delivery</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr key={run.id}>
                <td>
                  <button
                    className="table-action"
                    type="button"
                    onClick={() => onSelect(run.id)}
                  >
                    {run.label}
                  </button>
                </td>
                <td>{fmtRate(run.summary.avgRate)}</td>
                <td>{fmtMs(run.summary.e2eP99)}</td>
                <td>{compact(run.summary.peakLag, 2)}</td>
                <td>{fmtPct(run.summary.deliveryPct)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ReproductionPanel() {
  return (
    <section className="panel reproduction" id="local-reproduction">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Local reproduction</p>
          <h2>Run the benchmark on your machine</h2>
        </div>
      </div>
      <div className="command-grid">
        <code>cd benchmark</code>
        <code>./run.sh start</code>
        <code>./run.sh bench</code>
        <code>./run.sh sweep-compression</code>
      </div>
      <p className="muted">
        GitHub Pages hosts the explorer and result files only. Kafka runs
        locally through Docker Compose and the Go CLI in the repository.
      </p>
    </section>
  );
}
