

// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <map>
#include <memory>
#include <unordered_set>
#include <vector>

#include "logger.h"
#include "vsag/allocator.h"

namespace vsag {

class DefaultAllocator : public Allocator {
public:
    static DefaultAllocator*
    Instance() {
        static DefaultAllocator s_instance;
        return &s_instance;
    }

public:
    DefaultAllocator() = default;
    ~DefaultAllocator() override {
#ifndef NDEBUG
        if (not allocated_ptrs_.empty()) {
            logger::error(fmt::format("There is a memory leak in {}.", Name()));
            abort();
        }
#endif
    }

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

private:
#ifndef NDEBUG
    std::unordered_set<void*> allocated_ptrs_;
    std::mutex set_mutex_;
#endif
};

template <class T>
class AllocatorWrapper {
public:
    using value_type = T;
    using pointer = T*;
    using void_pointer = void*;
    using const_void_pointer = const void*;
    using size_type = size_t;
    using difference_type = ptrdiff_t;

    AllocatorWrapper(Allocator* allocator) {
        this->allocator_ = allocator;
    }

    template <class U>
    AllocatorWrapper(const AllocatorWrapper<U>& other) : allocator_(other.allocator_) {
    }

    bool
    operator==(const AllocatorWrapper& other) const noexcept {
        return allocator_ == other.allocator_;
    }

    pointer
    allocate(size_type n, const_void_pointer hint = 0) {
        return static_cast<pointer>(allocator_->Allocate(n * sizeof(value_type)));
    }

    void
    deallocate(pointer p, size_type n) {
        allocator_->Deallocate(static_cast<void_pointer>(p));
    }

    template <class U, class... Args>
    void
    construct(U* p, Args&&... args) {
        ::new ((void_pointer)p) U(std::forward<Args>(args)...);
    }

    template <class U>
    void
    destroy(U* p) {
        p->~U();
    }

    template <class U>
    struct rebind {
        using other = AllocatorWrapper<U>;
    };

    Allocator* allocator_{};
};

}  // namespace vsag
