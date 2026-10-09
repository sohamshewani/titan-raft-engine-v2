#ifndef TITAN_RAFT_ALLOCATOR_HPP
#define TITAN_RAFT_ALLOCATOR_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <memory>
#include <utility>
#include <exception>
#include <system_error>

namespace titan {
namespace raft {

class AllocationException : public std::bad_alloc {
public:
    explicit AllocationException(const char* message) : msg_(message) {}
    const char* what() const noexcept override { return msg_; }
private:
    const char* msg_;
};

class CacheAlignedAllocator {
public:
    static constexpr std::size_t kCacheLineSize = 64;

    template <typename T>
    static T* allocate_array(std::size_t count) {
        if (count == 0) {
            return nullptr;
        }
        if (count > SIZE_MAX / sizeof(T)) {
            throw AllocationException("Allocation size overflow");
        }
        std::size_t size = count * sizeof(T);
        std::size_t remainder = size % kCacheLineSize;
        if (remainder != 0) {
            size += (kCacheLineSize - remainder);
        }

        void* ptr = nullptr;
        int result = posix_memalign(&ptr, kCacheLineSize, size);
        if (result != 0 || ptr == nullptr) {
            throw AllocationException("posix_memalign failed");
        }
        return static_cast<T*>(ptr);
    }

    static void deallocate(void* ptr) noexcept {
        std::free(ptr);
    }

    template <typename T, typename... Args>
    static T* create(Args&&... args) {
        void* raw = allocate_array<T>(1);
        try {
            return ::new (raw) T(std::forward<Args>(args)...);
        } catch (...) {
            deallocate(raw);
            throw;
        }
    }

    template <typename T>
    static void destroy(T* ptr) noexcept {
        if (ptr != nullptr) {
            ptr->~T();
            deallocate(ptr);
        }
    }
};

template <typename T>
class TitanAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = TitanAllocator<U>;
    };

    TitanAllocator() noexcept = default;
    template <typename U>
    TitanAllocator(const TitanAllocator<U>&) noexcept {}

    pointer allocate(size_type n) {
        return CacheAlignedAllocator::allocate_array<T>(n);
    }

    void deallocate(pointer p, size_type) noexcept {
        CacheAlignedAllocator::deallocate(p);
    }

    template <typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* p) noexcept {
        p->~U();
    }

    size_type max_size() const noexcept {
        return SIZE_MAX / sizeof(T);
    }

    bool operator==(const TitanAllocator&) const noexcept { return true; }
    bool operator!=(const TitanAllocator&) const noexcept { return false; }
};

} // namespace raft
} // namespace titan

#endif // TITAN_RAFT_ALLOCATOR_HPP