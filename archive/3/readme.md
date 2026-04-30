Results:

```
% go run main.go
📢 Producer started: 1000 msg/sec
🐌 Consumer started: 5ms delay per message
... Consumer reached: 1000/10000 (Lag is growing)
✅ Producer finished sending.
... Consumer reached: 2000/10000 (Lag is growing)
... Consumer reached: 3000/10000 (Lag is growing)
... Consumer reached: 4000/10000 (Lag is growing)
... Consumer reached: 5000/10000 (Lag is growing)
... Consumer reached: 6000/10000 (Lag is growing)
... Consumer reached: 7000/10000 (Lag is growing)
... Consumer reached: 8000/10000 (Lag is growing)
... Consumer reached: 9000/10000 (Lag is growing)
... Consumer reached: 10000/10000 (Lag is growing)

--- 📊 Preliminary Results ---
P50 Latency: 22.063906s
P95 Latency: 42.809045s (Tail)
P99 Latency: 44.653868s (Tail)
Max Latency: 45.113616s
------------------------------
```

---

# Kafka Backpressure & Consumer Lag Framework (Go)

## 1. Problem Statement: The "Slow Consumer" Bottleneck

In a distributed streaming architecture, the **Producer** and **Consumer** rarely operate at the same speed. When a consumer becomes bottlenecked (due to heavy computation, database locks, or network I/O), the system faces a critical choice: **drop data**, **block the producer**, or **buffer**.

Kafka chooses to **buffer**. While this prevents data loss, it introduces **Consumer Lag**, which directly translates into **increased data latency**.

### Experimental Steps Taken:

1. **Controlled Injection:** A Go producer is throttled to exactly **1,000 msg/sec** using a deterministic ticker.
2. **Intentional Bottleneck:** A Go consumer is artificially slowed down with a **5ms per-message delay** (limiting it to 200 msg/sec).
3. **Metadata Capture:** Every message is "timestamped" at the moment of production.
4. **Lag Accumulation:** We observe the delta between the production timestamp and consumption time to measure the "Age of Data."

---

## 2. Kafka & Code Logic: Why it works

### Kafka’s Role as a Durable Buffer

Unlike traditional message brokers that push data and wait for a "Done" signal, Kafka is a **Pull-based** system. The data is written to a persistent append-only log. The consumer moves its "Offset" pointer as it reads. Because the log is stored on disk, the producer can keep writing even if the consumer is offline or slow—until the disk fills up.

### The Go Implementation Architecture

- **The Ticker (Producer):** Uses `time.NewTicker` to ensure we don't saturate the CPU, but rather test the _logic_ of the rate limit.
- **The Synchronous Producer:** We use `SyncProducer` here to ensure that "Message 10" is acknowledged by the broker before "Message 11" is sent, providing a stable baseline for our rate.
- **The Latency Collector:** We use a thread-safe slice (`sync.Mutex`) to collect every single message's travel time. This allows us to calculate **Quantiles** (p50, p95, p99) rather than just a simple (and misleading) average.

---

## 3. Benchmarking Metrics: What are we measuring?

We aren't just measuring speed; we are measuring **System Health**.

| Metric               | Definition                                                                               | Significance in this Project                                                                                    |
| -------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **P50 (Median)**     | The latency experienced by the 50th percentile of messages.                              | Shows the "average" delay as the lag starts to build up.                                                        |
| **P95 / P99 (Tail)** | The latency of the slowest 5% or 1% of messages.                                         | **Crucial.** This represents the "worst case" and shows how much the lag has compounded by the end of the test. |
| **Consumer Lag**     | The distance (in offsets) between the latest message in Kafka and the last read message. | Direct indicator of how "stale" the consumer's view of the world is.                                            |
| **Data Rate**        | The volume of data (MB/s) flowing through the pipe.                                      | Helps identify if the bottleneck is the Network, Disk, or Logic.                                                |

---

## 4. Result Analysis: The "Death Spiral"

Your results showed a **P99 of ~44 seconds**. This is a textbook example of an **unstable system**.

### The "Age of Data" Problem

In a real-time system (like fraud detection or GPS tracking), a 44-second delay makes the data useless. The "Age of Data" is the time a message spends sitting in the Kafka log waiting to be processed.

### The Death Spiral Explained

When the **Consumption Rate < Production Rate**:

1. **Lag builds linearly:** Every second, the consumer falls 800 messages further behind.
2. **Recovery becomes harder:** Even if the producer stops, the consumer still has a massive "backlog" to chew through.
3. **Resource Exhaustion:** If the lag grows too large, the broker might start deleting old data (retention policy) before the consumer even sees it.

**Is this expected?** Yes. For your specific parameters (1000 in vs 200 out), a 44-second P99 is mathematically certain. If the system were "healthy," the P99 would stay below **10-20ms**.

---

## 5. Next Steps: Recovery & Dynamic Testing

To turn this into a comprehensive research framework, we need to test **System Elasticity**.

### A. The Recovery Test (The "Catch-Up" Phase)

Modify the code to change the `consumerDelay` halfway through.

- **Step 1:** Run 5,000 messages with a 5ms delay (Build Lag).
- **Step 2:** Run the next 5,000 messages with **0ms delay** (Clear Lag).
- **Measure:** How long does it take for the P99 latency to return to normal? This is your **Recovery Time Objective (RTO)**.

### B. Dynamic Bottlenecks

Instead of a constant `5ms` sleep, try:

- **Jitter:** Use a random delay `[1ms to 10ms]` to simulate unpredictable network performance.
- **Burstiness:** Send 5,000 messages instantly, then wait 5 seconds. This tests how Kafka handles "Micro-bursts."

### C. Different Parameter Testing

- **Batching:** Change `config.Producer.Flush.Messages`. Larger batches increase throughput but can increase **Tail Latency** (p99) because messages wait longer to be "shipped."
- **Concurrency:** Use multiple consumers (Consumer Groups) to see if horizontal scaling solves the lag issues.

---
