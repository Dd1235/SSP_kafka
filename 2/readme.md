```
(base) .. % go run main.go
🚀 Starting benchmark: 100000 messages...
... Progress: 98548/100000 (98.5%)

---

## Metric : Value

Total Msgs : 100000
Msg Size : 1024 bytes
Duration : 1.01 sec
Throughput : 99181.82 msg/sec
Data Rate : 96.86 MB/sec

---
```

# Kafka Performance Benchmark (Go + Sarama)

This project benchmarks the producer throughput of a local Kafka cluster running in **KRaft mode** using the **IBM/Sarama** Go library.

## Quick Results

On a **MacBook Air (M-Series)**, the benchmark achieved:

- **Throughput:** ~99,180 messages/sec
- **Data Rate:** ~96.86 MB/sec
- **Payload:** 100,000 messages at 1KB each

---

## What We Benchmarked

We specifically tested **Producer Throughput** using an **Asynchronous Pipeline**.

In Kafka, there are two ways to send data:

1. **SyncProducer:** Waits for an acknowledgment (ACK) from Kafka for every single message before sending the next. This is safe but slow (limited by network round-trip time).
2. **AsyncProducer (Used here):** Batches messages in memory and sends them in "chunks." This maximizes the utilization of the CPU and Network.

### Key Configuration used:

- **Acks (RequiredAcks):** Set to `WaitForLocal`. Kafka acknowledges the message as soon as the leader writes it to its local log, without waiting for replicas.
- **Compression:** Snappy (balances CPU overhead vs. bandwidth).
- **Batching:** The producer was configured to flush every 1,000 messages, reducing the number of I/O system calls.

---

## Under the Hood: Threads & Concurrency

### 1. Go Routines (The "Producer" Side)

The Go code utilizes two main execution paths:

- **The Spawner:** A goroutine that floods the Sarama `Input()` channel with 100,000 messages as fast as possible.
- **The Success Listener:** A loop that drains the `Successes()` channel. This is critical—if this loop stops, the internal buffer fills up, and the whole application "hangs" due to backpressure.

### 2. CPU & Context Switching

Because we are using **AsyncProducer**, the Sarama library manages its own background goroutines to handle connection pooling and message serialization. On your MacBook, these goroutines are distributed across your efficiency and performance cores.

- **Serialization:** Converting your Go structs/bytes into the Kafka Wire Protocol.
- **Checksums:** Calculating CRC32 for message integrity.

### 3. Kafka (The "Broker" Side)

Inside the Docker container, Kafka is running in **KRaft mode**. Unlike the older ZooKeeper architecture, KRaft handles metadata internally, which significantly reduces the "cold start" latency you see when running benchmarks locally.

---

## Result Interpretation

| Result              | Meaning                                                                                                                                     |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **High Throughput** | Your CPU is efficiently serializing data and the Go scheduler is managing goroutines without excessive context switching.                   |
| **1.01s Duration**  | This suggests that the bottleneck is no longer the code, but likely the **Docker Virtual Filesystem (VirtioFS)** writing to your Mac's SSD. |
| **~97 MB/sec**      | You are saturating a significant portion of the virtual network bandwidth between your Host Mac and the Docker Linux VM.                    |

---

## How to Reproduce

1. **Start Kafka:** `docker compose up -d`
2. **Run Benchmark:** `go run main.go`
3. **Clean up:** `docker compose down`

---
