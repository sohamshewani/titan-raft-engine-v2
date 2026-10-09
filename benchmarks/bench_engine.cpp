#include <iostream>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <thread>
#include <atomic>
#include <filesystem>

// Include Titan Raft Engine headers
// Assuming standard installation paths for titan-raft-engine
#include <titan/raft_engine.h>

class LatencyTracker {
public:
    explicit LatencyTracker(size_t capacity) {
        latencies_.reserve(capacity);
    }

    inline void record(uint64_t nanos) {
        latencies_.push_back(nanos);
    }

    void finalize() {
        std::sort(latencies_.begin(), latencies_.end());
    }

    double get_p(double percentile) const {
        if (latencies_.empty()) return 0.0;
        size_t index = static_cast<size_t>(percentile / 100.0 * (latencies_.size() - 1));
        return static_cast<double>(latencies_[index]);
    }

    size_t size() const { return latencies_.size(); }

private:
    std::vector<uint64_t> latencies_;
};

int main() {
    // Configuration
    const size_t num_operations = 50000;
    const std::string db_path = "./titan_bench_data";

    // Clean up any existing test data
    std::filesystem::remove_all(db_path);

    // Initialize Titan Raft Engine
    titan::RaftEngineConfig config;
    config.dir = db_path;
    
    titan::RaftEngine* engine = nullptr;
    titan::Status status = titan::RaftEngine::Open(config, &engine);
    if (!status.ok() || engine == nullptr) {
        std::cerr << "Failed to open Titan Raft Engine: " << status.ToString() << std::endl;
        return 1;
    }

    LatencyTracker append_latencies(num_operations);
    std::string test_key = "bench_key";
    std::string test_value(256, 'x'); // 256 bytes payload

    // Warm-up phase
    for (size_t i = 0; i < 1000; ++i) {
        uint64_t index = i;
        titan::Entry entry;
        entry.set_index(index);
        entry.set_term(1);
        entry.set_data(test_value);
        engine->Append(entry);
    }
    engine->Flush();

    // Benchmark Phase: Append Operations
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < num_operations; ++i) {
        uint64_t index = 1000 + i;
        titan::Entry entry;
        entry.set_index(index);
        entry.set_term(1);
        entry.set_data(test_value);

        auto op_start = std::chrono::high_resolution_clock::now();
        status = engine->Append(entry);
        auto op_end = std::chrono::high_resolution_clock::now();

        if (!status.ok()) {
            std::cerr << "Append failed at index " << index << ": " << status.ToString() << std::endl;
            break;
        }

        uint64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
        append_latencies.record(ns);
    }

    // Ensure durability
    engine->Flush();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    append_latencies.finalize();

    // Calculate throughput
    double seconds = static_cast<double>(total_duration) / 1000.0;
    double ops_sec = static_cast<double>(append_latencies.size()) / seconds;

    // Output formatted metrics table to stdout
    std::cout << "\n==================================================\n";
    std::cout << "      Titan Raft Engine Microbenchmark Results    \n";
    std::cout << "==================================================\n";
    std::cout << "Operation Type   : Append (256 bytes payload)\n";
    std::cout << "Total Operations : " << append_latencies.size() << "\n";
    std::cout << "Total Time (ms)  : " << total_duration << "\n";
    std::cout << "--------------------------------------------------\n";
    std::cout << "Throughput       : " << static_cast<int>(ops_sec) << " ops/sec\n";
    std::cout << "--------------------------------------------------\n";
    std::cout << "Latency Percentiles (microseconds):\n";
    std::cout << "  p50            : " << append_latencies.get_p(50.0) / 1000.0 << " us\n";
    std::cout << "  p95            : " << append_latencies.get_p(95.0) / 1000.0 << " us\n";
    std::cout << "  p99            : " << append_latencies.get_p(99.0) / 1000.0 << " us\n";
    std::cout << "  p99.9          : " << append_latencies.get_p(99.9) / 1000.0 << " us\n";
    std::cout << "==================================================\n";

    // Cleanup
    delete engine;
    std::filesystem::remove_all(db_path);

    return 0;
}