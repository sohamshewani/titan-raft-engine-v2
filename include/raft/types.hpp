#ifndef TITAN_RAFT_TYPES_HPP
#define TITAN_RAFT_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>
#include <system_error>
#include <cstring>
#include <memory>
#include <array>
#include <algorithm>

namespace titan::raft {

using TermId = uint64_t;
using LogIndex = uint64_t;
using NodeId = uint64_t;
using Crc32cHash = uint32_t;

enum class EntryType : uint8_t {
    DATA = 0x01,
    CONFIG_CHANGE = 0x02,
    NO_OP = 0x03
};

enum class StateRole : uint8_t {
    FOLLOWER = 0,
    CANDIDATE = 1,
    LEADER = 2
};

struct alignas(64) LogHeader {
    LogIndex index;
    TermId term;
    Crc32cHash checksum;
    uint32_t payload_size;
    EntryType type;
    uint8_t reserved[3];
};

static_assert(sizeof(LogHeader) % 8 == 0, "LogHeader must be 8-byte aligned");
static_assert(sizeof(LogHeader) == 32, "LogHeader size must be exactly 32 bytes for strict packing");

class alignas(64) LogEntry {
private:
    LogHeader header_;
    std::vector<uint8_t> payload_;

public:
    LogEntry() noexcept 
        : header_{0, 0, 0, 0, EntryType::NO_OP, {0, 0, 0}}, payload_() {}

    LogEntry(LogIndex idx, TermId t, EntryType et, const uint8_t* data, size_t size)
        : header_{idx, t, 0, static_cast<uint32_t>(size), et, {0, 0, 0}},
          payload_(data, data + size) {
        header_.checksum = compute_checksum(data, size);
    }

    LogEntry(LogIndex idx, TermId t, EntryType et, std::vector<uint8_t>&& data)
        : header_{idx, t, 0, static_cast<uint32_t>(data.size()), et, {0, 0, 0}},
          payload_(std::move(data)) {
        header_.checksum = compute_checksum(payload_.data(), payload_.size());
    }

    ~LogEntry() noexcept = default;

    LogEntry(const LogEntry&) = delete;
    LogEntry& operator=(const LogEntry&) = delete;

    LogEntry(LogEntry&& other) noexcept
        : header_(other.header_), payload_(std::move(other.payload_)) {
        other.header_ = LogHeader{0, 0, 0, 0, EntryType::NO_OP, {0, 0, 0}};
    }

    LogEntry& operator=(LogEntry&& other) noexcept {
        if (this != &other) {
            header_ = other.header_;
            payload_ = std::move(other.payload_);
            other.header_ = LogHeader{0, 0, 0, 0, EntryType::NO_OP, {0, 0, 0}};
        }
        return *this;
    }

    [[nodiscard]] LogIndex index() const noexcept { return header_.index; }
    [[nodiscard]] TermId term() const noexcept { return header_.term; }
    [[nodiscard]] Crc32cHash checksum() const noexcept { return header_.checksum; }
    [[nodiscard]] EntryType type() const noexcept { return header_.type; }
    [[nodiscard]] size_t size() const noexcept { return payload_.size(); }
    [[nodiscard]] const uint8_t* data() const noexcept { return payload_.data(); }
    
    [[nodiscard]] std::vector<uint8_t> extract_payload() noexcept {
        return std::move(payload_);
    }

    [[nodiscard]] bool verify_integrity() const noexcept {
        Crc32cHash calculated = compute_checksum(payload_.data(), payload_.size());
        return calculated == header_.checksum;
    }

    static Crc32cHash compute_checksum(const uint8_t* data, size_t length) noexcept {
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < length; ++i) {
            crc ^= data[i];
            for (int j = 0; j < 8; ++j) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ 0x82F63B78;
                } else {
                    crc >>= 1;
                }
            }
        }
        return ~crc;
    }
};

struct alignas(64) PersistentState {
    TermId current_term;
    NodeId voted_for;
    LogIndex commit_index;
    LogIndex last_applied;

    PersistentState() noexcept
        : current_term(0), voted_for(0), commit_index(0), last_applied(0) {}
        
    PersistentState(TermId term, NodeId vote, LogIndex commit, LogIndex applied) noexcept
        : current_term(term), voted_for(vote), commit_index(commit), last_applied(applied) {}
};

struct alignas(64) NodeClusterConfig {
    std::vector<NodeId> peers;
    NodeId self_id;

    NodeClusterConfig() : peers(), self_id(0) {}
    NodeClusterConfig(std::vector<NodeId> p, NodeId s) : peers(std::move(p)), self_id(s) {}
};

enum class RaftErrorCode : int {
    SUCCESS = 0,
    WAL_CORRUPTED_ENTRY = 1,
    WAL_IO_ERROR = 2,
    OUT_OF_BOUNDS_INDEX = 3,
    TERM_MISMATCH = 4,
    LEADER_STEP_DOWN = 5,
    MUTATION_REJECTED = 6
};

class RaftErrorCategory : public std::error_category {
public:
    [[nodiscard]] const char* name() const noexcept override {
        return "titan.raft";
    }

    [[nodiscard]] std::string message(int ev) const override {
        switch (static_cast<RaftErrorCode>(ev)) {
            case RaftErrorCode::SUCCESS: return "Success";
            case RaftErrorCode::WAL_CORRUPTED_ENTRY: return "WAL sector verification failed via CRC32C";
            case RaftErrorCode::WAL_IO_ERROR: return "Low-level system I/O failure on memory-mapped log ring";
            case RaftErrorCode::OUT_OF_BOUNDS_INDEX: return "Requested log index exceeds bounds or truncated";
            case RaftErrorCode::TERM_MISMATCH: return "Term identifier discrepancy during consensus validation";
            case RaftErrorCode::LEADER_STEP_DOWN: return "Leader forced to step down due to higher term discovered";
            case RaftErrorCode::MUTATION_REJECTED: return "Command submission rejected by non-leader node";
            default: return "Unknown Titan-Raft Error";
        }
    }
};

inline const std::error_category& raft_error_category() {
    static RaftErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(RaftErrorCode e) noexcept {
    return {static_cast<int>(e), raft_error_category()};
}

} // namespace titan::raft

namespace std {
template <>
struct is_error_code_enum<titan::raft::RaftErrorCode> : true_type {};
}

#endif // TITAN_RAFT_TYPES_HPP