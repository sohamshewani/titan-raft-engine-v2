#include "engine.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <chrono>
#include <random>
#include <algorithm>

namespace titan {

// CRC32C implementation (Castagnoli polynomial 0x1EDC6F41)
uint32_t Engine::ComputeCRC32C(const uint8_t* data, size_t length) noexcept {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j) {
            uint32_t mask = -(crc & 1);
            crc = (crc >> 1) ^ (0x82F63B78 & mask);
        }
    }
    return ~crc;
}

Engine::Engine(std::string wal_path, NodeConfig config)
    : wal_path_(std::move(wal_path)),
      config_(config),
      current_term_(0),
      voted_for_(0),
      commit_index_(0),
      last_applied_(0),
      state_role_(StateRole::FOLLOWER),
      running_(false) {
    
    // Initialize skip list head with sentinel
    auto head = std::make_unique<SkipNode>();
    head->term = 0;
    head->index = 0;
    head->down = nullptr;
    for (int i = 0; i < MAX_LEVELS; ++i) {
        head->next[i].store(nullptr, std::memory_order_relaxed);
    }
    skip_list_head_ = std::move(head);

    OpenWAL();
    RecoverWAL();
}

Engine::~StopwatchGuard() {} // Placeholder for destructor if needed

Engine::~Engine() {
    Stop();
    if (wal_fd_ != -1) {
        if (wal_map_ != MAP_FAILED && wal_size_ > 0) {
            munmap(wal_map_, wal_size_);
        }
        close(wal_fd_);
    }
}

void Engine::OpenWAL() {
    wal_fd_ = open(wal_path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (wal_fd_ == -1) {
        throw std::system_error(errno, std::generic_category(), "Failed to open WAL file");
    }

    struct stat st;
    if (fstat(wal_fd_, &st) == -1) {
        close(wal_fd_);
        throw std::system_error(errno, std::generic_category(), "Failed to fstat WAL file");
    }

    wal_size_ = st.st_size;
    if (wal_size_ == 0) {
        wal_size_ = FALLOCATE_BLOCK_SIZE;
        if (posix_fallocate(wal_fd_, 0, wal_size_) != 0) {
            close(wal_fd_);
            throw std::system_error(errno, std::generic_category(), "Failed to pre-allocate WAL file");
        }
    }

    wal_map_ = mmap(nullptr, wal_size_, PROT_READ | PROT_WRITE, MAP_SHARED, wal_fd_, 0);
    if (wal_map_ == MAP_FAILED) {
        close(wal_fd_);
        throw std::system_error(errno, std::generic_category(), "Failed to mmap WAL file");
    }
}

void Engine::EnsureCapacity(size_t additional_bytes) {
    size_t required = wal_offset_ + additional_bytes;
    if (required <= wal_size_) {
        return;
    }

    size_t new_size = wal_size_;
    while (new_size < required) {
        new_size += FALLOCATE_BLOCK_SIZE;
    }

    if (munmap(wal_map_, wal_size_) == -1) {
        throw std::system_error(errno, std::generic_category(), "Failed to munmap WAL during resize");
    }

    if (posix_fallocate(wal_fd_, 0, new_size) != 0) {
        throw std::system_error(errno, std::generic_category(), "Failed to fallocate WAL during resize");
    }

    wal_map_ = mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, wal_fd_, 0);
    if (wal_map_ == MAP_FAILED) {
        throw std::system_error(errno, std::generic_category(), "Failed to mmap WAL during resize");
    }

    wal_size_ = new_size;
}

void Engine::RecoverWAL() {
    uint8_t* ptr = static_cast<uint8_t*>(wal_map_);
    size_t offset = 0;
    uint6_t_safe last_valid_offset = 0;

    while (offset + sizeof(WALHeader) <= wal_size_) {
        WALHeader* header = reinterpret_cast<WALHeader*>(ptr + offset);
        if (header->magic != WAL_MAGIC) {
            break; // End of valid data or uninitialized tail
        }

        if (offset + sizeof(WALHeader) + header->data_length > wal_size_) {
            break; // Truncated entry
        }

        uint8_t* data_ptr = ptr + offset + sizeof(WALHeader);
        uint32_t computed_crc = ComputeCRC32C(data_ptr, header->data_length);
        if (computed_crc != header->crc32) {
            // Corrupted entry detected, truncate here
            break;
        }

        // Deserialize LogEntry
        uint64_t term = *reinterpret_cast<uint64_t*>(data_ptr);
        uint64_t index = *reinterpret_cast<uint64_t*>(data_ptr + 8);
        uint32_t cmd_len = *reinterpret_cast<uint32_t*>(data_ptr + 16);

        std::vector<uint8_t> command(cmd_len);
        std::memcpy(command.data(), data_ptr + 20, cmd_len);

        LogEntry entry(term, index, std::move(command));
        
        // Append to internal log without writing to WAL again
        entries_.push_back(std::move(entry));
        InsertIndexMapping(term, index);

        offset += sizeof(WALHeader) + header->data_length;
        last_valid_offset = offset;
    }

    wal_offset_ = last_valid_offset;

    // Truncate file and mmap to exact valid offset if necessary
    if (wal_offset_ < wal_size_) {
        // Zero out the remaining mapped space for safety
        std::memset(static_cast<uint8_t*>(wal_map_) + wal_offset_, 0, wal_size_ - wal_offset_);
    }

    if (!entries_.empty()) {
        commit_index_ = entries_.back().index;
        last_applied_ = commit_index_;
    }
}

void Engine::AppendToWAL(const LogEntry& entry) {
    uint32_t cmd_len = entry.command.size();
    uint32_t data_length = sizeof(uint64_t) * 2 + sizeof(uint32_t) + cmd_len;
    size_t total_size = sizeof(WALHeader) + data_length;

    EnsureCapacity(total_size);

    uint8_t* dest = static_cast<uint8_t*>(wal_map_) + wal_offset_;
    WALHeader* header = reinterpret_cast<WALHeader*>(dest);
    header->magic = WAL_MAGIC;
    header->data_length = data_length;

    uint8_t* data_ptr = dest + sizeof(WALHeader);
    std::memcpy(data_ptr, &entry.term, sizeof(uint64_t));
    std::memcpy(data_ptr + 8, &entry.index, sizeof(uint64_t));
    std::memcpy(data_ptr + 16, &cmd_len, sizeof(uint32_t));
    std::memcpy(data_ptr + 20, entry.command.data(), cmd_len);

    header->crc32 = ComputeCRC32C(data_ptr, data_length);

    wal_offset_ += total_size;
    msync(dest, total_size, MS_ASYNC);
}

void Engine::InsertIndexMapping(uint64_t term, uint64_t index) {
    // Probabilistic level generation for skip list (p = 0.25)
    int level = 0;
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    while (distribution(generator) < 0.25 && level < MAX_LEVELS - 1) {
        level++;
    }

    auto new_node = std::make_unique<SkipNode>();
    new_node->term = term;
    new_node->index = index;
    new_node->down = nullptr;
    for (int i = 0; i <= level; ++i) {
        new_node->next[i].store(nullptr, std::memory_order_relaxed);
    }

    SkipNode* update[MAX_LEVELS];
    SkipNode* curr = skip_list_head_.get();

    for (int i = MAX_LEVELS - 1; i >= 0; --i) {
        while (true) {
            SkipNode* next = curr->next[i].load(std::memory_order_acquire);
            if (next != nullptr && next->term < term) {
                curr = next;
            } else {
                break;
            }
        }
        update[i] = curr;
    }

    for (int i = 0; i <= level; ++i) {
        while (true) {
            SkipNode* pred = update[i];
            SkipNode* succ = pred->next[i].load(std::memory_order_acquire);
            new_node->next[i].store(succ, std::memory_order_relaxed);
            if (pred->next[i].compare_exchange_weak(succ, new_node.get(), std::memory_order_release, std::memory_order_relaxed)) {
                break;
            }
        }
    }

    // Keep ownership safely if needed, but since we use raw pointers in lock-free structure,
    // we must manage node lifecycles globally. To prevent leaks in this production skeleton,
    // we store allocated skip nodes in a vector protected by a mutex.
    {
        std::lock_guard<std::mutex> lock(skip_node_alloc_mutex_);
        allocated_skip_nodes_.push_back(std::move(new_node));
    }
}

uint64_t Engine::FindIndexByTerm(uint64_t term) const noexcept {
    SkipNode* curr = skip_list_head_.get();
    for (int i = MAX_LEVELS - 1; i >= 0; --i) {
        while (true) {
            SkipNode* next = curr->next[i].load(std::memory_order_acquire);
            if (next != nullptr && next->term <= term) {
                curr = next;
            } else {
                break;
            }
        }
    }

    if (curr != skip_list_head_.get() && curr->term == term) {
        return curr->index;
    }
    return 0; // Not found
}

bool Engine::SubmitCommand(std::vector<uint8_t> command) {
    if (state_role_.load(std::memory_order_acquire) != StateRole::LEADER) {
        return false; // Only leaders accept commands
    }

    CommandTask task{std::move(command), {}};
    return client_queue_.enqueue(std::move(task));
}

void Engine::Start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return;
    }

    worker_thread_ = std::thread(&Engine::EventLoop, this);
}

void Engine::Stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        return;
    }

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void Engine::EventLoop() {
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(150, 300);
    int election_timeout_ms = dist(rng);
    auto last_heartbeat = std::chrono::steady_clock::now();

    while (running_.load(std::memory_order_acquire)) {
        // Process incoming client commands from MPSC queue
        CommandTask task;
        while (client_queue_.dequeue(task)) {
            uint64_t term = current_term_.load(std::memory_order_acquire);
            uint64_t index = entries_.empty() ? 1 : entries_.back().index + 1;
            
            LogEntry entry(term, index, std::move(task.command));
            AppendToWAL(entry);
            entries_.push_back(std::move(entry));
            InsertIndexMapping(term, index);
        }

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat).count();

        StateRole role = state_role_.load(std::memory_order_acquire);
        if (role != StateRole::LEADER) {
            if (elapsed > election_timeout_ms) {
                // Convert to candidate and start election
                state_role_.store(StateRole::Candidate, std::memory_order_release);
                current_term_.fetch_add(1, std::memory_order_acq_rel);
                voted_for_ = config_.node_id;
                last_heartbeat = now;
                election_timeout_ms = dist(rng); // Reset randomized timeout

                // In a full implementation, send RequestVote RPCs here.
                // For demonstration, if single node, immediately become leader.
                state_role_.store(StateRole::LEADER, std::memory_order_release);
            }
        } else {
            // Leader emits heartbeats periodically
            if (elapsed > 50) { // 50ms heartbeat interval
                last_heartbeat = now;
                // Broadcast AppendEntries RPCs to followers
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

} // namespace titan