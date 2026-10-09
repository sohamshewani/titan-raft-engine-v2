# titan-raft-engine
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/titan-raft-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/titan-raft-engine/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/titan-raft-engine) [![Stars](https://img.shields.io/github/stars/sohamshewani/titan-raft-engine?style=flat)](https://github.com/sohamshewani/titan-raft-engine)

## Executive Summary & Problem Space

`titan-raft-engine` is an ultra-low-latency, zero-allocation-on-hot-path consensus engine written in modern C++ (C++20). Engineered for mission-critical distributed systems, it implements the Raft consensus algorithm with a relentless focus on hardware efficiency, predictable tail latencies (p99.99), and strict memory safety.

Distributed key-value stores and transaction coordinators frequently suffer from systemic bottlenecks:
- **Lock Contention:** Heavy mutex locking across consensus state transitions and log appends limits multi-core scalability.
- **Garbage Collection Pressure:** Dynamic allocations (`std::string`, heap-allocated log entries) inside the consensus loop trigger frequent allocator locks and memory fragmentation.
- **I/O Latency Spikes:** Synchronous WAL (Write-Ahead Log) flushing directly on the RPC critical path bounds throughput.
- **Unbounded Memory Growth:** Lack of strict resource quotas and memory recycling leads to OOM terminations under network partitions.

`titan-raft-engine` resolves these challenges through a custom arena-based memory architecture, lock-free ring buffers for message passing (`include/raft/concurrency.hpp`), vectorized RPC dispatch (`include/raft/rpc.hpp`), and an optimized storage engine (`include/raft/storage.hpp`) that decouples state machine execution from disk synchronization.

---

## Structural ASCII Architecture / Dataflow Diagram

The system is organized into decoupled, highly cohesive architectural layers ensuring strict separation of concerns between network I/O, consensus logic, and persistent storage.

```
 +-----------------------------------------------------------------------+
 |                         RPC & Network Layer                           |
 |                (include/raft/rpc.hpp / Vectorized IO)                 |
 +-----------------------------------+-----------------------------------+
                                     | (Raw Byte Streams / Encoded RPCs)
                                     v
 +-----------------------------------------------------------------------+
 |                        Concurrency & Dispatch                         |
 |           (include/raft/concurrency.hpp / Lock-Free Rings)            |
 +-----------------------------------+-----------------------------------+
                                     | (Dispatched Raft Messages)
                                     v
 +-----------------------------------------------------------------------+
 |                          Engine Core Layer                            |
 |                   (src/engine.cpp / State Machine)                    |
 +-----------------+-------------------------------+-----------------+
                   |                               |                 |
                   v (Log Append / Read)           v (State Sync)    v (Allocation)
 +-----------------------+       +-------------------+       +-----------+
 |     Storage Engine    |       |   Leader Election |       | Arena/    |
 | (include/raft/storage)|       | & AppendEntries   |       | Allocator |
 +-----------------------+       +-------------------+       +-----------+
```

### Detailed Subsystem Dataflow

```
[ Client Request / Network ]
           |
           v
   (include/raft/rpc.hpp) 
           |
           +---> Zero-Copy Deserialization
           |
           v
   (include/raft/concurrency.hpp)
           |
           +---> Lock-Free MPMC Ring Buffer 
           |
           v
     [ src/engine.cpp ] <-----> (include/raft/allocator.hpp) [ Arena Blocks ]
           |
           +---> Consensus State Machine (Leader / Follower / Candidate)
           |
           +---> (include/raft/storage.hpp) WAL & Snapshot Disk Persist
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
`titan-raft-engine` rejects traditional thread-per-connection and coarse-grained locking models. 
- **Actor-Based Event Loop:** Core state transitions inside `src/engine.cpp` operate on a single-writer principle, eliminating data races across the primary state machine without locks.
- **Lock-Free Synchronization:** Cross-thread communication (e.g., submitting client mutations from worker threads to the consensus engine) utilizes lock-free multi-producer, multi-consumer (MPMC) ring buffers (`include/raft/concurrency.hpp`), bounded by atomic memory barriers (`std::memory_order_acquire` / `std::memory_order_release`).

### Memory Ownership & Arena Allocation
To eliminate heap fragmentation and latency spikes caused by standard allocators (`malloc`/`free`):
- **Region-Based Allocation:** `include/raft/allocator.hpp` implements a block-based arena allocator. Log entries and transient RPC contexts are carved out of pre-allocated memory pools.
- **RAII Lifetimes:** Persistent structures manage explicit ownership via custom deleters, while ephemeral message buffers are reset in $O(1)$ time by advancing arena reset pointers upon batch transaction completion.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation / Path | Algorithmic Complexity | Target Latency (p99) | Notes |
|---|---|---|---|
| **Local Log Append** | $O(1)$ amortized | $< 5\mu s$ | Uses arena memory pooling; zero heap allocations. |
| **Client Read (Linearizable)** | $O(1)$ | $< 10\mu s$ | Verified via read index / lease read optimization. |
| **AppendEntries RPC Handling** | $O(K)$ ($K = \text{entries}$) | $< 25\mu s$ | Vectorized batch validation and disk commit. |
| **Snapshot Compaction** | $O(N)$ ($N = \text{log size}$) | Background | Offloaded to asynchronous worker threads; zero pause time. |
| **Leader Election Recovery** | $O(M)$ ($M = \text{peers}$) | $< 50ms$ | Randomized backoff prevents split-vote live-locks. |

---

## Security Architecture & Threat Model

`titan-raft-engine` is hardened against malicious network payloads, fault injection, and memory corruption attacks.

### Input Boundaries & Sanitization
- **Strict Wire-Format Validation:** All incoming RPCs (`include/raft/rpc.hpp`) undergo rigorous structural and boundary validation before deserialization. Oversized payloads or malformed protobuf/binary frames are rejected immediately, preventing integer overflows and buffer over-reads.
- **Checksum Verification:** Persistent log entries (`include/raft/storage.hpp`) contain cryptographic or CRC32 checksums, safeguarding against silent data corruption on disk.

### Memory Corruption & Sanitization Protections
- **No Raw Pointers on Hot Path:** Strict adherence to smart pointers (`std::unique_ptr`, `std::shared_ptr`) and safe references.
- **Bounds-Checked Containers:** Custom span and view abstractions enforce compile-time and runtime bounds checks.
- **Sanitizer Integration:** The build pipeline mandates clean execution under AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and ThreadSanitizer (TSan).

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
- **Compiler:** GCC 11+ or Clang 13+ with full C++20 support.
- **Build System:** Make / GNU Make.

### 1. Build Production Binaries & Tests
Compiles the core engine, test suite, and benchmarking harness with aggressive optimizations (`-O3 -march=native`).

```bash
make clean
make all
```

### 2. Run Invariant & Concurrency Verification Tests
Executes the comprehensive test suite (`tests/test_invariants.cpp`), validating Raft safety properties, cluster partition recovery, and memory safety invariants.

```bash
make test
./bin/test_invariants
```

### 3. Execute Performance Benchmarks
Runs the high-throughput benchmarking suite (`benchmarks/bench_engine.cpp`) to measure log append latency, throughput under load, and concurrency scaling.

```bash
make benchmark
./bin/bench_engine --iterations=1000000 --threads=8