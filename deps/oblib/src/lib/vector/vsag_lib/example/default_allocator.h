#ifndef DEFAULT_ALLOCATOR_H
#define DEFAULT_ALLOCATOR_H
#include "vsag/allocator.h"

class DefaultAllocator : public vsag::Allocator {
public:
    DefaultAllocator() = default;
    virtual ~DefaultAllocator() = default;

    DefaultAllocator(const DefaultAllocator&) = delete;
    DefaultAllocator(DefaultAllocator&&) = delete;

public:
    std::string
    Name() override;

    void*
    Allocate(size_t size) override;

    void
    Deallocate(void* p) override;

    void*
    Reallocate(void* p, size_t size) override;
};
#endif // DEFAULT_ALLOCATOR_H
