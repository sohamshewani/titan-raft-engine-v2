#ifndef TITAN_RAFT_CONCURRENCY_HPP
#define TITAN_RAFT_CONCURRENCY_HPP

#include <atomic>
#include <vector>
#include <memory>
#include <new>
#include <cstdint>
#include <cstdlib>
#include <cstddef>
#include <utility>
#include <exception>
#include <string>
#include <system_error>

namespace titan {
namespace raft {

enum class StateRole : uint8_t {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
    PreCandidate = 3
};

struct alignas(64) LogEntry {
    uint64_t index;
    uint64_t term;
    uint32_t crc32c;
    uint32_t payload_size;
    std::unique_ptr<uint8_t[]> payload;

    LogEntry() noexcept 
        : index(0), term(0), crc32c(0), payload_size(0), payload(nullptr) {}

    LogEntry(uint64_t idx, uint64_t trm, uint32_t crc, const uint8_t* data, size_t size)
        : index(idx), term(trm), crc32c(crc), payload_size(static_cast<uint32_t>(size)), payload(nullptr) {
        if (size > 0 && data != nullptr) {
            payload = std::make_unique<uint8_t[]>(size);
            std::memcpy(payload.get(), data, size);
        }
    }

    ~LogEntry() = default;

    LogEntry(const LogEntry&) = delete;
    LogEntry& operator=(const LogEntry&) = delete;

    LogEntry(LogEntry&& other) noexcept
        : index(other.index),
          term(other.term),
          crc32c(other.crc32c),
          payload_size(other.payload_size),
          payload(std::move(other.payload)) {
        other.index = 0;
        other.term = 0;
        other.crc32c = 0;
        other.payload_size = 0;
    }

    LogEntry& operator=(LogEntry&& other) noexcept {
        if (this != &other) {
            index = other.index;
            term = other.term;
            crc32c = other.crc32c;
            payload_size = other.payload_size;
            payload = std::move(other.payload);
            other.index = 0;
            other.term = 0;
            other.crc32c = 0;
            other.payload_size = 0;
        }
        return *this;
    }
};

template <typename T, size_t Capacity = 65536>
class alignas(64) MPSCQueue {
public:
    static_uff_check:
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

    MPSCQueue() {
        Node* dummy = new Node();
        head_.store(dummy, std::memory_order_relaxed);
        tail_.store(dummy, std::memory_order_relaxed);
    }

    ~MPSCQueue() {
        T dummy_val;
        while (dequeue(dummy_val)) {
            // Drain remaining elements
        }
        Node* front = tail_.load(std::memory_order_relaxed);
        delete front;
    }

    MPSCQueue(const MPSCQueue&) = delete;
    MPSCQueue& operator=(const MPSCQueue&) = delete;
    MPSCQueue(MPSCQueue&&) = delete;
    MPSCQueue& operator=(MPSCQueue&&) = delete;

    bool enqueue(T&& item) {
        Node* node = new (std::nothrow) Node(std::forward<T>(item));
        if (!node) {
            return false;
        }
        Node* prev_head = head_.exchange(node, std::memory_order_acq_rel);
        prev_head->next.store(node, std::memory_order_release);
        return true;
    }

    bool enqueue(const T& item) {
        T copy(item);
        return enqueue(std::move(copy));
    }

    bool dequeue(T& item) {
        Node* tail = tail_.load(std::memory_order_relaxed);
        Node* next = tail->next.load(std::memory_order_acquire);
        
        if (next == nullptr) {
            return false;
        }

        item = std::move(next->data);
        tail_.store(next, std::memory_order_release);
        delete tail;
        return true;
    }

    bool empty() const noexcept {
        Node* tail = tail_.load(std::memory_order_relaxed);
        return tail->next.load(std::memory_order_acquire) == nullptr;
    }

private:
    struct Node {
        T data;
        std::atomic<Node*> next;

        Node() : data(), next(nullptr) {}
        explicit Node(T&& val) : data(std::move(val)), next(nullptr) {}
        explicit Node(const T& val) : data(val), next(nullptr) {}
    };

    alignas(64) std::atomic<Node*> head_;
    alignas(64) std::atomic<Node*> tail_;
};

class alignas(64) AtomicStateRegistry {
public:
    AtomicStateRegistry() 
        : role_(StateRole::Follower), 
          current_term_(0), 
          voted_for_(0), 
          commit_index_(0), 
          last_applied_(0) {}

    ~AtomicStateRegistry() = default;

    AtomicStateRegistry(const AtomicStateRegistry&) = delete;
    AtomicStateRegistry& operator=(const AtomicStateRegistry&) = delete;
    AtomicStateRegistry(AtomicStateRegistry&&) = delete;
    AtomicStateRegistry& operator=(AtomicStateRegistry&&) = delete;

    void set_role(StateRole new_role) noexcept {
        role_.store(new_role, std::memory_order_release);
    }

    StateRole get_role() const noexcept {
        return role_.load(std::memory_order_acquire);
    }

    void set_term(uint64_t term) noexcept {
        current_term_.store(term, std::memory_order_release);
    }

    uint64_t get_term() const noexcept {
        return current_term_.load(std::memory_order_acquire);
    }

    void set_voted_for(uint64_t candidate_id) noexcept {
        voted_for_.store(candidate_id, std::memory_order_release);
    }

    uint64_t get_voted_for() const noexcept {
        return voted_for_.load(std::memory_order_acquire);
    }

    void set_commit_index(uint64_t index) noexcept {
        commit_index_.store(index, std::memory_order_release);
    }

    uint64_t get_commit_index() const noexcept {
        return commit_index_.load(std::memory_order_acquire);
    }

    void set_last_applied(uint64_t index) noexcept {
        last_applied_.store(index, std::memory_order_release);
    }

    uint64_t get_last_applied() const noexcept {
        return last_applied_.load(std::memory_order_acquire);
    }

    bool compare_exchange_term_and_vote(uint64_t expected_term, uint64_t new_term, uint64_t candidate_id) noexcept {
        uint64_t current_term = expected_term;
        if (current_term_.compare_exchange_strong(current_term, new_term, std::memory_order_acq_rel, std::memory_order_acquire)) {
            voted_for_.store(candidate_id, std::memory_order_release);
            return true;
        }
        return false;
    }

private:
    alignas(64) std::atomic<StateRole> role_;
    alignas(64) std::atomic<uint64_t> current_term_;
    alignas(64) std::atomic<uint64_t> voted_for_;
    alignas(64) std::atomic<uint64_t> commit_index_;
    alignas(64) std::atomic<uint64_t> last_applied_;
};

class ConcurrencyException : public std::runtime_error {
public:
    explicit ConcurrencyException(const std::string& message)
        : std::runtime_error("ConcurrencyException: " + message) {}
    
    explicit ConcurrencyException(const char* message)
        : std::runtime_error(std::string("ConcurrencyException: ") + message) {}
};

} // namespace raft
} // namespace titan

#endif // TITAN_RAFT_CONCURRENCY_HPP