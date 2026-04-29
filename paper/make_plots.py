"""Generate all plots for the SSP Kafka paper from results JSON files."""
import json, glob, os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA = os.path.join(os.path.dirname(__file__), "data")
ASSETS = os.path.join(os.path.dirname(__file__), "assets")
os.makedirs(ASSETS, exist_ok=True)

plt.rcParams.update({
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "legend.fontsize": 8,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "figure.dpi": 150,
    "savefig.dpi": 200,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
})


def load(name):
    with open(os.path.join(DATA, name)) as f:
        return json.load(f)


# 1. Baseline timeline: throughput + e2e p99 over time
def plot_baseline_timeline():
    d = load("results-bench.json")
    tl = d["timeline"]
    t = [s["elapsed_s"] for s in tl]
    sent = [s["inst_rate"] for s in tl]
    recv = [s["recv_rate"] for s in tl]
    e99 = [s["e2e_p99_ms"] for s in tl]

    fig, ax1 = plt.subplots(figsize=(5.0, 2.4))
    l1 = ax1.plot(t, sent, "o-", color="#1f77b4", label="sent msg/s")
    l2 = ax1.plot(t, recv, "s--", color="#2ca02c", label="received msg/s")
    ax1.set_xlabel("Elapsed time (s)")
    ax1.set_ylabel("Throughput (msg/s)")
    ax1.set_ylim(0, max(sent + recv) * 1.15)
    ax2 = ax1.twinx()
    l3 = ax2.plot(t, e99, "^:", color="#d62728", label="e2e p99 (ms)")
    ax2.set_ylabel("E2E p99 latency (ms)")
    ax2.grid(False)
    lines = l1 + l2 + l3
    ax1.legend(lines, [l.get_label() for l in lines], loc="lower right", fontsize=7, ncol=3)
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "baseline_timeline.pdf"))
    plt.close(fig)


# 2. Compression sweep: ack p99 vs e2e p99 vs MB/s
def plot_compression_sweep():
    codecs = ["none", "snappy", "lz4", "gzip", "zstd"]
    rows = {}
    for c in codecs:
        d = load(f"results-comp-{c}.json")
        rows[c] = d["final"]
    x = np.arange(len(codecs))
    width = 0.35
    fig, ax1 = plt.subplots(figsize=(5.0, 2.6))
    ack_p99 = [rows[c]["ack_latency_ms"]["P99"] for c in codecs]
    e2e_p99 = [rows[c]["e2e_latency_ms"]["P99"] for c in codecs]
    b1 = ax1.bar(x - width/2, ack_p99, width, label="ack p99", color="#4c72b0")
    b2 = ax1.bar(x + width/2, e2e_p99, width, label="e2e p99", color="#dd8452")
    ax1.set_xticks(x); ax1.set_xticklabels(codecs)
    ax1.set_ylabel("Latency p99 (ms)")
    ax1.legend(loc="upper left", fontsize=8)
    ax2 = ax1.twinx()
    rate = [rows[c]["avg_rate_msg_per_sec"]/1000 for c in codecs]
    ax2.plot(x, rate, "kD-", label="send rate (k msg/s)")
    ax2.set_ylabel("Send rate (k msg/s)")
    ax2.set_ylim(8.5, 10.5)
    ax2.grid(False)
    ax2.legend(loc="upper right", fontsize=8)
    for i, v in enumerate(e2e_p99):
        ax1.text(i + width/2, v + 1.5, f"{v:.1f}", ha="center", fontsize=7)
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "compression_sweep.pdf"))
    plt.close(fig)


# 3. Acks sweep
def plot_acks_sweep():
    rows = {}
    for a in ["0", "1", "-1"]:
        rows[a] = load(f"results-acks{a}.json")["final"]
    acks = ["0", "1", "-1"]
    x = np.arange(len(acks))
    fig, ax = plt.subplots(figsize=(5.0, 2.4))
    pcts = ["P50", "P95", "P99", "P999"]
    colors = ["#4c72b0", "#55a868", "#c44e52", "#8172b2"]
    width = 0.18
    for i, p in enumerate(pcts):
        vals = [rows[a]["e2e_latency_ms"][p] for a in acks]
        ax.bar(x + (i - 1.5) * width, vals, width, label=p.lower(), color=colors[i])
    ax.set_yscale("log")
    ax.set_xticks(x); ax.set_xticklabels([f"acks={a}" for a in acks])
    ax.set_ylabel("E2E latency (ms, log)")
    ax.legend(ncol=4, fontsize=7, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "acks_sweep.pdf"))
    plt.close(fig)


# 4. Message size: throughput vs e2e p99
def plot_msgsize():
    sizes = [64, 256, 1024, 4096, 16384]
    rows = {s: load(f"results-size-{s}.json")["final"] for s in sizes}
    mb = [rows[s]["throughput_mb_per_sec"] for s in sizes]
    p99 = [rows[s]["e2e_latency_ms"]["P99"] for s in sizes]
    fig, ax1 = plt.subplots(figsize=(5.0, 2.4))
    ax1.plot(sizes, mb, "o-", color="#2a7", label="MB/s (payload)")
    ax1.set_xscale("log"); ax1.set_yscale("log")
    ax1.set_xlabel("Message size (bytes, log)")
    ax1.set_ylabel("Throughput (MB/s, log)")
    ax2 = ax1.twinx()
    ax2.plot(sizes, p99, "s--", color="#d62728", label="e2e p99 (ms)")
    ax2.set_ylabel("E2E p99 (ms)")
    ax2.set_yscale("log")
    ax2.grid(False)
    lines, labels = ax1.get_legend_handles_labels()
    l2, lb2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + l2, labels + lb2, loc="lower right", fontsize=8)
    for s, v in zip(sizes, p99):
        ax2.text(s, v * 1.15, f"{v:.0f}", color="#d62728", fontsize=7, ha="center")
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "msgsize.pdf"))
    plt.close(fig)


# 5. Payload compression ratios
def plot_payload_compression():
    payloads = ["random", "mixed", "json", "logline", "text", "zeros"]
    codecs = ["snappy", "lz4", "gzip", "zstd"]
    fig, ax = plt.subplots(figsize=(5.2, 2.6))
    x = np.arange(len(payloads))
    width = 0.2
    colors = ["#4c72b0", "#55a868", "#c44e52", "#8172b2"]
    for i, c in enumerate(codecs):
        ratios = []
        for p in payloads:
            d = load(f"results-payload-{p}.json")
            ratios.append(d["compression_offline"]["codecs"][c]["ratio"])
        ax.bar(x + (i - 1.5) * width, ratios, width, label=c, color=colors[i])
    ax.set_yscale("log")
    ax.set_xticks(x); ax.set_xticklabels(payloads)
    ax.set_ylabel("Compression ratio (raw / compressed, log)")
    ax.set_ylim(0.8, 12000)
    ax.axhline(1.0, color="black", linestyle=":", linewidth=0.8)
    ax.legend(ncol=4, fontsize=7, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "payload_compression.pdf"))
    plt.close(fig)


# 6. Slow consumer aggressive: lag growth + recv vs send
def plot_slow_consumer():
    d = load("results-slow-aggr.json")
    lag = d["lag_timeline"]
    tl = d["timeline"]
    fig, ax1 = plt.subplots(figsize=(5.0, 2.6))
    ax1.plot([s["elapsed_s"] for s in lag], [s["total_lag"]/1000 for s in lag],
             "o-", color="#c44e52", label="total lag (k msgs)", markersize=3)
    ax1.set_xlabel("Elapsed time (s)")
    ax1.set_ylabel("Consumer lag (k msgs)", color="#c44e52")
    ax1.tick_params(axis="y", labelcolor="#c44e52")
    ax2 = ax1.twinx()
    ax2.plot([s["elapsed_s"] for s in tl], [s["inst_rate"] for s in tl],
             "^-", color="#1f77b4", label="sent msg/s", markersize=4)
    ax2.plot([s["elapsed_s"] for s in tl], [s["recv_rate"] for s in tl],
             "s--", color="#2ca02c", label="recv msg/s", markersize=4)
    ax2.set_ylabel("Throughput (msg/s)")
    ax2.grid(False)
    lines1, lab1 = ax1.get_legend_handles_labels()
    lines2, lab2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, lab1 + lab2, loc="center left", fontsize=7)
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "slow_consumer.pdf"))
    plt.close(fig)


# 7. Recovery: lag with phase switch annotation
def plot_recovery():
    d = load("results-recovery-aggr.json")
    lag = d["lag_timeline"]
    cfg = d["config"]
    phase = float(cfg["phase_duration"].rstrip("s"))
    t = [s["elapsed_s"] for s in lag]
    L = [s["total_lag"]/1000 for s in lag]
    fig, ax1 = plt.subplots(figsize=(5.2, 2.6))
    ax1.plot(t, L, "o-", color="#c44e52", markersize=3, label="total lag (k msgs)")
    ax1.axvline(phase, color="black", linestyle=":", linewidth=1.2,
                label=f"phase switch (delay→0 at {phase:.0f}s)")
    ax1.set_xlabel("Elapsed time (s)")
    ax1.set_ylabel("Consumer lag (k msgs)")

    # annotate peak and drain rate
    ls = d["lag_summary"]
    ax1.annotate(f"peak {ls['max_total_lag']/1000:.0f}k", xy=(phase, max(L)), xytext=(phase + 5, max(L) - 20),
                 fontsize=7, arrowprops=dict(arrowstyle="->", lw=0.5))
    ax1.text(phase + 5, max(L) * 0.55,
             f"drain rate {ls['recovery_drain_rate_msg_per_sec']:.0f} msg/s\nt$_{{drain}}$ = {ls['time_to_drain_s']:.0f}s",
             fontsize=7)
    ax1.legend(fontsize=7, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "recovery.pdf"))
    plt.close(fig)


# 8. CDF of e2e latency (baseline) vs slow-aggr — log
def plot_e2e_cdf():
    edges = [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000,
             5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000, 1_000_000]
    fig, ax = plt.subplots(figsize=(5.0, 2.4))
    for name, label, color in [
        ("results-bench.json", "baseline (10k msg/s, healthy)", "#1f77b4"),
        ("results-slow-aggr.json", "slow-consumer (4ms/msg)", "#d62728"),
        ("results-recovery-aggr.json", "recovery (3ms→0)", "#2ca02c"),
    ]:
        d = load(name)
        b = d["final"]["e2e_latency_ms"]["CDFBuckets"]
        cum = np.cumsum(b)
        if cum[-1] == 0:
            continue
        cdf = cum / cum[-1]
        ax.semilogx(edges[:len(cdf)], cdf * 100, label=label, color=color, lw=1.4)
    ax.set_xlabel("End-to-end latency (ms, log)")
    ax.set_ylabel("CDF (%)")
    ax.set_xlim(0.5, 1e6)
    ax.set_ylim(0, 105)
    ax.legend(fontsize=7, loc="lower right")
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "e2e_cdf.pdf"))
    plt.close(fig)


# 9. SysPerf: GC pause vs e2e p99 over codec sweep
def plot_sysperf_gc():
    codecs = ["none", "snappy", "lz4", "gzip", "zstd"]
    gc_pause = []
    e2e_p99 = []
    num_gc = []
    for c in codecs:
        d = load(f"results-comp-{c}.json")
        gc_pause.append(d["sysperf_summary"]["gc_pause_total_ms"])
        e2e_p99.append(d["final"]["e2e_latency_ms"]["P99"])
        num_gc.append(d["sysperf_summary"]["num_gc"])
    x = np.arange(len(codecs))
    fig, ax1 = plt.subplots(figsize=(5.0, 2.4))
    width = 0.4
    b1 = ax1.bar(x - width/2, gc_pause, width, color="#8172b2", label="GC pause total (ms)")
    ax1.set_ylabel("Total GC pause (ms)")
    ax1.set_xticks(x); ax1.set_xticklabels(codecs)
    ax2 = ax1.twinx()
    ax2.plot(x, num_gc, "kD-", label="# GC cycles")
    ax2.set_ylabel("# GC cycles in 20 s run")
    ax2.grid(False)
    for i, n in enumerate(num_gc):
        ax2.text(i, n + 25, f"{n}", ha="center", fontsize=7)
    lines, labels = ax1.get_legend_handles_labels()
    l2, lb2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + l2, labels + lb2, fontsize=7, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(ASSETS, "sysperf_gc.pdf"))
    plt.close(fig)


if __name__ == "__main__":
    plot_baseline_timeline()
    plot_compression_sweep()
    plot_acks_sweep()
    plot_msgsize()
    plot_payload_compression()
    plot_slow_consumer()
    plot_recovery()
    plot_e2e_cdf()
    plot_sysperf_gc()
    print("All plots written to", ASSETS)
