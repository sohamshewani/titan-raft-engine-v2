#ifndef TITAN_RAFT_RPC_HPP
#define TITAN_RAFT_RPC_HPP

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <vector>
#include <memory>
#include <system_error>
#include <chrono>
#include <span>
#include <array>
#include <new>

namespace titan::raft {

enum class RpcType : uint8_t {
    AppendEntriesRequest = 0x01,
    AppendEntriesResponse = 0x02,
    RequestVoteRequest = 0x03,
    RequestVoteResponse = 0x04,
    InstallSnapshotRequest = 0x05,
    InstallSnapshotResponse = 0x06
};

struct alignas(64) RpcHeader {
    RpcType type;
    uint8_t version;
    uint16_t flags;
    uint32_t payload_length;
    uint64_t term;
    uint64_t sender_id;
    uint64_t receiver_id;
    uint32_t crc32c;
    uint32_t reserved;
};

static_assert(sizeof(RpcHeader) == 48, "RpcHeader must be precisely 48 bytes with zero padding");

struct alignas(64) AppendEntriesArgs {
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    uint32_t entry_count;
    uint32_t reserved;
};

static_assert(sizeof(AppendEntriesArgs) == 32, "AppendEntriesArgs must be 32 bytes");

struct alignas(64) AppendEntriesReply {
    uint64_t match_index;
    uint64_t conflict_index;
    uint64_t conflict_term;
    uint8_t success;
    uint8_t padding[7];
};

static_assert(sizeof(AppendEntriesReply) == 32, "AppendEntriesReply must be 32 bytes");

struct alignas(64) RequestVoteArgs {
    uint64_t last_log_index;
    uint64_t last_log_term;
    uint8_t pre_vote;
    uint8_t padding[7];
};

static_assert(sizeof(RequestVoteArgs) == 24, "RequestVoteArgs must be 24 bytes");

struct alignas(64) RequestVoteReply {
    uint64_t vote_granted;
    uint64_t term;
};

static_assert(sizeof(RequestVoteReply) == 16, "RequestVoteReply must be 16 bytes");

struct alignas(64) InstallSnapshotArgs {
    uint64_t last_included_index;
    uint64_t last_included_term;
    uint64_t offset;
    uint64_t data_length;
    uint8_t done;
    uint8_t padding[7];
};

static_assert(sizeof(InstallSnapshotArgs) == 40, "InstallSnapshotArgs must be 40 bytes");

struct alignas(64) InstallSnapshotReply {
    uint64_t term;
    uint64_t bytes_stored;
};

static_assert(sizeof(InstallSnapshotReply) == 16, "InstallSnapshotReply must be 16 bytes");

class RpcMessage {
private:
    RpcHeader header_;
    std::vector<uint8_t> payload_;

    static uint32_t calculate_crc32c(const uint8_t* data, size_t length) noexcept {
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < length; ++i) {
            crc ^= data[i];
            for (int j = 0; j < 8; ++j) {
                uint32_t mask = -(crc & 1);
                crc = (crc >> 1) ^ (0x8ED84611 & mask);
            }
        }
        return ~crc;
    }

public:
    RpcMessage() noexcept {
        std::memset(&header_, 0, sizeof(RpcHeader));
    }

    explicit RpcMessage(RpcType t, uint64_t term, uint64_t sender, uint64_t receiver) noexcept {
        std::memset(&header_, 0, sizeof(RpcHeader));
        header_.type = t;
        header_.version = 1;
        header_.term = term;
        header_.sender_id = sender;
        header_.receiver_id = receiver;
    }

    RpcMessage(const RpcMessage&) = default;
    RpcMessage& operator=(const RpcMessage&) = default;
    RpcMessage(RpcMessage&&) noexcept = default;
    RpcMessage& operator=(RpcMessage&&) noexcept = default;
    ~RpcMessage() = default;

    const RpcHeader& header() const noexcept { return header_; }
    std::span<const uint8_t> payload() const noexcept { return payload_; }

    void set_payload(std::vector<uint8_t>&& data) {
        payload_ = std::move(data);
        header_.payload_length = static_cast<uint32_t>(payload_.size());
    }

    void set_payload(const uint8_t* data, size_t size) {
        payload_.assign(data, data + size);
        header_.payload_length = static_cast<uint32_t>(payload_.size());
    }

    void finalize() noexcept {
        header_.payload_length = static_cast<uint32_t>(payload_.size());
        header_.crc32c = 0;
        
        uint32_t crc = calculate_crc32c(reinterpret_cast<const uint8_t*>(&header_), sizeof(RpcHeader));
        if (!payload_.empty()) {
            // Compute rolling CRC32C over payload
            uint32_t payload_crc = calculate_crc32c(payload_.data(), payload_.size());
            crc ^= payload_crc;
        }
        header_.crc32c = crc;
    }

    bool verify() const noexcept {
        RpcHeader temp = header_;
        temp.crc32c = 0;
        
        uint32_t crc = calculate_crc32c(reinterpret_cast<const uint8_t*>(&temp), sizeof(RpcHeader));
        if (!payload_.empty()) {
            uint32_t payload_crc = calculate_crc32c(payload_.data(), payload_.size());
            crc ^= payload_crc;
        }
        return header_.crc32c == crc;
    }

    std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> buffer;
        buffer.resize(sizeof(RpcHeader) + payload_.size());
        std::memcpy(buffer.data(), &header_, sizeof(RpcHeader));
        if (!payload_.empty()) {
            std::memcpy(buffer.data() + sizeof(RpcHeader), payload_.data(), payload_.size());
        }
        return buffer;
    }

    static bool deserialize(std::span<const uint8_t> raw_data, RpcMessage& out_msg) noexcept {
        if (raw_data.size() < sizeof(RpcHeader)) {
            return false;
        }

        RpcHeader hdr;
        std::memcpy(&hdr, raw_data.data(), sizeof(RpcHeader));

        if (raw_data.size() < sizeof(RpcHeader) + hdr.payload_length) {
            return false;
        }

        out_msg.header_ = hdr;
        if (hdr.payload_length > 0) {
            out_msg.payload_.resize(hdr.payload_length);
            std::memcpy(out_msg.payload_.data(), raw_data.data() + sizeof(RpcHeader), hdr.payload_length);
        } else {
            out_msg.payload_.clear();
        }

        return out_msg.verify();
    }
};

class alignas(64) RpcCodec {
public:
    RpcCodec() = default;
    ~RpcCodec() = default;

    static RpcMessage encode_append_entries(uint64_t term, uint64_t sender, uint64_t receiver,
                                          const AppendEntriesArgs& args, 
                                          std::span<const uint8_t> entries_payload) {
        RpcMessage msg(RpcType::AppendEntriesRequest, term, sender, receiver);
        
        std::vector<uint8_t> payload;
        payload.resize(sizeof(AppendEntriesArgs) + entries_payload.size());
        
        std::memcpy(payload.data(), &args, sizeof(AppendEntriesArgs));
        if (!entries_payload.empty()) {
            std::memcpy(payload.data() + sizeof(AppendEntriesArgs), entries_payload.data(), entries_payload.size());
        }
        
        msg.set_payload(std::move(payload));
        msg.finalize();
        return msg;
    }

    static bool decode_append_entries(const RpcMessage& msg, AppendEntriesArgs& out_args, std::span<const uint8_t>& out_entries) noexcept {
        if (msg.header().type != RpcType::AppendEntriesRequest) {
            return false;
        }
        auto payload = msg.payload();
        if (payload.size() < sizeof(AppendEntriesArgs)) {
            return false;
        }

        std::memcpy(&out_args, payload.data(), sizeof(AppendEntriesArgs));
        out_entries = payload.subspan(sizeof(AppendEntriesArgs));
        return true;
    }

    static RpcMessage encode_append_entries_reply(uint64_t term, uint64_t sender, uint64_t receiver,
                                                const AppendEntriesReply& reply) {
        RpcMessage msg(RpcType::AppendEntriesResponse, term, sender, receiver);
        std::vector<uint8_t> payload(sizeof(AppendEntriesReply));
        std::memcpy(payload.data(), &reply, sizeof(AppendEntriesReply));
        msg.set_payload(std::move(payload));
        msg.finalize();
        return msg;
    }

    static bool decode_append_entries_reply(const RpcMessage& msg, AppendEntriesReply& out_reply) noexcept {
        if (msg.header().type != RpcType::AppendEntriesResponse) {
            return false;
        }
        auto payload = msg.payload();
        if (payload.size() < sizeof(AppendEntriesReply)) {
            return false;
        }
        std::memcpy(&out_reply, payload.data(), sizeof(AppendEntriesReply));
        return true;
    }

    static RpcMessage encode_request_vote(uint64_t term, uint64_t sender, uint64_t receiver,
                                        const RequestVoteArgs& args) {
        RpcMessage msg(RpcType::RequestVoteRequest, term, sender, receiver);
        std::vector<uint8_t> payload(sizeof(RequestVoteArgs));
        std::memcpy(payload.data(), &args, sizeof(RequestVoteArgs));
        msg.set_payload(std::move(payload));
        msg.finalize();
        return msg;
    }

    static bool decode_request_vote(const RpcMessage& msg, RequestVoteArgs& out_args) noexcept {
        if (msg.header().type != RpcType::RequestVoteRequest) {
            return false;
        }
        auto payload = msg.payload();
        if (payload.size() < sizeof(RequestVoteArgs)) {
            return false;
        }
        std::memcpy(&out_args, payload.data(), sizeof(RequestVoteArgs));
        return true;
    }

    static RpcMessage encode_request_vote_reply(uint64_t term, uint64_t sender, uint64_t receiver,
                                              const RequestVoteReply& reply) {
        RpcMessage msg(RpcType::RequestVoteResponse, term, sender, receiver);
        std::vector<uint8_t> payload(sizeof(RequestVoteReply));
        std::memcpy(payload.data(), &reply, sizeof(RequestVoteReply));
        msg.set_payload(std::move(payload));
        msg.finalize();
        return msg;
    }

    static bool decode_request_vote_reply(const RpcMessage& msg, RequestVoteReply& out_reply) noexcept {
        if (msg.header().type != RpcType::RequestVoteResponse) {
            return false;
        }
        auto payload = msg.payload();
        if (payload.size() < sizeof(RequestVoteReply)) {
            return false;
        }
        std::memcpy(&out_reply, payload.data(), sizeof(RequestVoteReply));
        return true;
    }

    static RpcMessage encode_install_snapshot(uint64_t term, uint64_t sender, uint64_t receiver,
                                            const InstallSnapshotArgs& args,
                                            std::span<const uint8_t> chunk_data) {
        RpcMessage msg(RpcType::InstallSnapshotRequest, term, sender, receiver);
        std::vector<uint8_t> payload;
        payload.resize(sizeof(InstallSnapshotArgs) + chunk_data.size());
        std::memcpy(payload.data(), &args, sizeof(InstallSnapshotArgs));
        if (!chunk_data.empty()) {
            std::memcpy(payload.data() + sizeof(InstallSnapshotArgs), chunk_data.data(), chunk_data.size());
        }
        msg.set_payload(std::move(payload));
        msg.finalize();
        return msg;
    }

    static bool decode_install_snapshot(const RpcMessage& msg, InstallSnapshotArgs& out_args, std::span<const uint8_t>& out_chunk) noexcept {
        if (msg.header().type != RpcType::InstallSnapshotRequest) {
            return false;
        }
        auto payload = msg.payload();
        if (payload.size() < sizeof(InstallSnapshotArgs)) {
            return false;
        }
        std::memcpy(&out_args, payload.data(), sizeof(InstallSnapshotArgs));
        out_chunk = payload.subspan(sizeof(InstallSnapshotArgs));
        return true;
    }

    static RpcMessage encode_install_snapshot_reply(uint64_t term, uint64_t sender, uint64_t receiver,
                                                  const InstallSnapshotReply& reply) {
        RpcMessage msg(RpcType::InstallSnapshotResponse, term, sender, receiver);
        std::vector<uint8_t> payload(sizeof(InstallSnapshotReply));
        std::memcpy(payload.data(), &reply, sizeof(InstallSnapshotReply));
        msg.set_payload(std::move(payload));
        msg.finalize();
        return msg;
    }

    static bool decode_install_snapshot_reply(const RpcMessage& msg, InstallSnapshotReply& out_reply) noexcept {
        if (msg.header().type != RpcType::InstallSnapshotResponse) {
            return false;
        }
        auto payload = msg.payload();
        if (payload.size() < sizeof(InstallSnapshotReply)) {
            return false;
        }
        std::memcpy(&out_reply, payload.data(), sizeof(InstallSnapshotReply));
        return true;
    }
};

} // namespace titan::raft

#endif // TITAN_RAFT_RPC_HPP