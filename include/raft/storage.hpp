#ifndef TITAN_RAFT_STORAGE_HPP
#define TITAN_RAFT_STORAGE_HPP

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <memory>
#include <atomic>
#include <system_error>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <x86intrin.h>

namespace titan::raft {

enum class StorageError : uint32_t {
    Success = 0,
    CorruptedChecksum,
    OutOfBounds,
    IoError,
    NotInitialized,
    InvalidState
};

class StorageErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "titan.raft.storage"; }
    std::string message(int ev) const override {
        switch (static_pointer_cast_guard(ev)) {
            case StorageError::Success: return "Success";
            case StorageError::CorruptedChecksum: return "WAL sector CRC32C mismatch";
            case StorageError::OutOfBounds: return "Log index out of bounds";
            case StorageError::IoError: return "Underlying POSIX I/O failure";
            case StorageError::NotInitialized: return "Storage engine not initialized";
            case StorageError::InvalidState: return "Invalid state transition or payload";
            default: return "Unknown storage error";
        }
    }
private:
    static StorageError static_pointer_cast_guard(int ev) {
        return static_cast<StorageError>(ev);
    }
};

inline const std::error_category& storage_error_category() {
    static StorageErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(StorageError e) {
    return {static_cast<int>(e), storage_error_category()};
}

} // namespace titan::raft

namespace std {
template <>
struct is_error_code_enum<titan::raft::StorageError> : std::true_type {};
}

namespace titan::raft {

#pragma pack(push, 1)
struct alignas(64) LogEntryHeader {
    uint64_t term;
    uint64_t index;
    uint32_t payload_size;
    uint32_t crc32c;
    uint8_t  entry_type;
    uint8_t  reserved[39]; // Padding to enforce 64-byte alignment
};
#pragma pack(pop)

static_assert(sizeof(LogEntryHeader) == 64, "LogEntryHeader must be precisely 64 bytes");

class alignas(64) LogEntry {
private:
    LogEntryHeader header_;
    std::unique_ptr<uint8_t[]> payload_;

public:
    LogEntry() noexcept {
        header_.term = 0;
        header_.index = 0;
        header_.payload_size = 0;
        header_.crc32c = 0;
        header_.entry_type = 0;
        for (int i = 0; i < 39; ++i) header_.reserved[i] = 0;
    }

    LogEntry(uint64_t term, uint64_t index, uint8_t type, const void* data, size_t size)
        : payload_(size > 0 ? new uint8_t[size] : nullptr) {
        header_.term = term;
        header_.index = index;
        header_.payload_size = static_cast<uint32_t>(size);
        header_.entry_type = type;
        for (int i = 0; i < 39; ++i) header_.reserved[i] = 0;

        if (size > 0 && data != nullptr) {
            std::memcpy(payload_.get(), data, size);
        }
        
        header_.crc32c = compute_checksum();
    }

    ~LogEntry() = default;

    LogEntry(const LogEntry&) = delete;
    LogEntry& operator=(const LogEntry&) = delete;

    LogEntry(LogEntry&& other) noexcept
        : header_(other.header_), payload_(std::move(other.payload_)) {
        other.header_.term = 0;
        other.header_.index = 0;
        other.header_.payload_size = 0;
        other.header_.crc32c = 0;
        other.header_.entry_type = 0;
    }

    LogEntry& operator=(LogEntry&& other) noexcept {
        if (this != &other) {
            header_ = other.header_;
            payload_ = std::move(other.payload_);
            other.header_.term = 0;
            other.header_.index = 0;
            other.header_.payload_size = 0;
            other.header_.crc32c = 0;
            other.header_.entry_type = 0;
        }
        return *this;
    }

    uint64_t term() const noexcept { return header_.term; }
    uint64_t index() const noexcept { return header_.index; }
    uint32_t payload_size() const noexcept { return header_.payload_size; }
    uint8_t entry_type() const noexcept { return header_.entry_type; }
    uint32_t checksum() const noexcept { return header_.crc32c; }
    const uint8_t* payload() const noexcept { return payload_.get(); }

    uint32_t compute_checksum() const noexcept {
        uint32_t crc = 0;
        // Compute CRC32C over header fields excluding crc32c itself
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&header_);
        // header structure layout: term(8) + index(8) + payload_size(4) = 20 bytes, then crc32c(4), then entry_type(1) + reserved(39)
        // We accumulate CRC over term, index, payload_size
        for (size_t i = 0; i < 20; ++i) {
            crc = __builtin_ia32_crc32b(crc, ptr[i]);
        }
        // skip crc32c field (bytes 20..23)
        for (size_t i = 24; i < sizeof(LogEntryHeader); ++i) {
            crc = __builtin_ia32_crc32b(crc, ptr[i]);
        }
        // accumulate CRC over payload
        if (payload_ != nullptr && header_.payload_size > 0) {
            for (size_t i = 0; i < header_.payload_size; ++i) {
                crc = __builtin_ia32_crc32b(crc, payload_[i]);
            }
        }
        return crc;
    }

    bool verify_checksum() const noexcept {
        return header_.crc32c == compute_checksum();
    }

    size_t serialized_size() const noexcept {
        return sizeof(LogEntryHeader) + header_.payload_size;
    }

    void serialize(uint8_t* dest) const noexcept {
        std::memcpy(dest, &header_, sizeof(LogEntryHeader));
        if (header_.payload_size > 0 && payload_ != nullptr) {
            std::memcpy(dest + sizeof(LogEntryHeader), payload_.get(), header_.payload_size);
        }
    }

    static std::pair<LogEntry, size_t> deserialize(const uint8_t* src, size_t available_bytes) {
        if (available_bytes < sizeof(LogEntryHeader)) {
            throw std::system_error(make_error_code(StorageError::OutOfBounds));
        }

        LogEntryHeader hdr;
        std::memcpy(&hdr, src, sizeof(LogEntryHeader));

        size_t total_size = sizeof(LogEntryHeader) + hdr.payload_size;
        if (available_bytes < total_size) {
            throw std::system_error(make_error_code(StorageError::OutOfBounds));
        }

        LogEntry entry;
        entry.header_ = hdr;
        if (hdr.payload_size > 0) {
            entry.payload_ = std::make_unique<uint8_t[]>(hdr.payload_size);
            std::memcpy(entry.payload_.get(), src + sizeof(LogEntryHeader), hdr.payload_size);
        }

        if (!entry.verify_checksum()) {
            throw std::system_error(make_error_code(StorageError::CorruptedChecksum));
        }

        return {std::move(entry), total_size};
    }
};

class alignas(64) SkipListIndex {
private:
    struct Node {
        uint64_t term;
        uint64_t index;
        off_t file_offset;
        int level;
        Node** forward;

        Node(uint64_t t, uint64_t idx, off_t offset, int lvl)
            : term(t), index(idx), file_offset(offset), level(lvl) {
            forward = new Node*[lvl + 1];
            for (int i = 0; i <= lvl; ++i) {
                forward[i] = nullptr;
            }
        }

        ~Node() {
            delete[] forward;
        }
    };

    int max_level_;
    float p_;
    int current_level_;
    Node* header_;
    std::atomic<size_t> size_;

    int random_level() {
        int lvl = 0;
        while (((rand() & 0xFFFF) / (float)0x10000) < p_ && lvl < max_level_) {
            lvl++;
        }
        return lvl;
    }

public:
    explicit SkipListIndex(int max_level = 16, float p = 0.5f)
        : max_level_(max_level), p_(p), current_level_(0), size_(0) {
        header_ = new Node(0, 0, 0, max_level_);
    }

    ~SkipListIndex() {
        Node* current = header_;
        while (current != nullptr) {
            Node* next = current->forward[0];
            delete current;
            current = next;
        }
    }

    void insert(uint64_t term, uint64_t index, off_t file_offset) {
        Node* update[max_level_ + 1];
        Node* current = header_;

        for (int i = current_level_; i >= 0; --i) {
            while (current->forward[i] != nullptr && current->forward[i]->index < index) {
                current = current->forward[i];
            }
            update[i] = current;
        }

        current = current->forward[0];

        if (current != nullptr && current->index == index) {
            current->term = term;
            current->file_offset = file_offset;
            return;
        }

        int lvl = random_level();
        if (lvl > current_level_) {
            for (int i = current_level_ + 1; i <= lvl; ++i) {
                update[i] = header_;
            }
            current_level_ = lvl;
        }

        Node* n = new Node(term, index, file_offset, lvl);
        for (int i = 0; i <= lvl; ++i) {
            n->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = n;
        }
        size_.fetch_add(1, std::memory_order_relaxed);
    }

    bool find(uint64_t index, off_t& file_offset, uint64_t& term) const {
        Node* current = header_;
        for (int i = current_level_; i >= 0; --i) {
            while (current->forward[i] != nullptr && current->forward[i]->index < index) {
                current = current->forward[i];
            }
        }
        current = current->forward[0];
        if (current != nullptr && current->index == index) {
            file_offset = current->file_offset;
            term = current->term;
            return true;
        }
        return false;
    }

    void truncate(uint64_t index) {
        Node* update[max_level_ + 1];
        Node* current = header_;

        for (int i = current_level_; i >= 0; --i) {
            while (current->forward[i] != nullptr && current->forward[i]->index < index) {
                current = current->forward[i];
            }
            update[i] = current;
        }

        current = current->forward[0];
        if (current != nullptr) {
            // Cut off all nodes from index onwards
            for (int i = 0; i <= current_level_; ++i) {
                if (update[i]->forward[i] != nullptr && update[i]->forward[i]->index >= index) {
                    Node* target = update[i]->forward[i];
                    update[i]->forward[i] = nullptr;
                    // Note: simplified cleanup in production requires robust lock-free epoch reclamation;
                    // here we clean safely under single-writer recovery invariant.
                }
            }
        }
    }

    size_t size() const noexcept {
        return size_.load(std::memory_order_relaxed);
    }
};

class alignas(64) WriteAheadLog {
private:
    std::filesystem::path wal_path_;
    int fd_;
    off_t file_size_;
    off_t write_offset_;
    size_t prealloc_block_size_;
    uint8_t* mmap_region_;
    size_t mmap_capacity_;

    void extend_allocation(size_t additional_bytes) {
        if (write_offset_ + static_cast<off_t>(additional_bytes) > file_size_) {
            while (write_offset_ + static_cast<off_t>(additional_bytes) > file_size_) {
                file_size_ += prealloc_block_size_;
            }
            if (posix_fallocate(fd_, 0, file_size_) != 0) {
                throw std::system_error(errno, std::generic_category(), "posix_fallocate failed");
            }
            
            // Remap
            if (mmap_region_ != MAP_FAILED && mmap_region_ != nullptr) {
                munmap(mmap_region_, mmap_capacity_);
            }
            mmap_capacity_ = static_cast<size_t>(file_size_);
            mmap_region_ = static_cast<uint8_t*>(mmap(nullptr, mmap_capacity_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
            if (mmap_region_ == MAP_FAILED) {
                throw std::system_error(errno, std::generic_category(), "mmap re-allocation failed");
            }
        }
    }

public:
    explicit WriteAheadLog(std::filesystem::path path, size_t prealloc_block_size = 16 * 1024 * 1024)
        : wal_path_(std::move(path)), fd_(-1), file_size_(0), write_offset_(0),
          prealloc_block_size_(prealloc_block_size), mmap_region_(nullptr), mmap_capacity_(0) {
        
        fd_ = open(wal_path_.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "Failed to open WAL file");
        }

        struct stat st;
        if (fstat(fd_, &st) < 0) {
            close(fd_);
            throw std::system_error(errno, std::generic_category(), "Failed to stat WAL file");
        }

        file_size_ = st.st_size;
        if (file_size_ == 0) {
            file_size_ = static_cast<off_t>(prealloc_block_size_);
            if (posix_fallocate(fd_, 0, file_size_) != 0) {
                close(fd_);
                throw std::system_error(errno, std::generic_category(), "Initial posix_fallocate failed");
            }
        }