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

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "vsag/allocator.h"
#include "vsag/logger.h"

namespace vsag {

class Options {
public:
    static Options&
    Instance();

public:
    // Gets the number of threads with memory order acquire for thread safety. In a non-isolated
    // resource environment, limit the number of threads used for disk index IO during the search
    // process; it is recommended to set this to one to two times the number of available CPU cores
    // in the system. The size of num_threads is limited to between 1 and 200.
    inline size_t
    num_threads_io() const {
        return num_threads_io_.load(std::memory_order_acquire);
    }

    inline size_t
    num_threads_building() const {
        return num_threads_building_.load(std::memory_order_acquire);
    }

    void
    set_num_threads_io(size_t num_threads);

    void
    set_num_threads_building(size_t num_threads);

    // Gets the limit of block size with memory order acquire for thread safety. The setting of
    // block size should be greater than 2M.
    inline size_t
    block_size_limit() const {
        return block_size_limit_.load(std::memory_order_acquire);
    }

    void
    set_block_size_limit(size_t size);

    Logger*
    logger();

    inline bool
    set_logger(Logger* logger) {
        logger_ = logger;
        return true;
    }

private:
    Options() = default;
    ~Options() = default;

    // Deleted copy constructor and assignment operator to prevent copies
    Options(const Options&) = delete;
    Options(const Options&&) = delete;
    Options&
    operator=(const Options&) = delete;

private:
    // The size of the thread pool for single index I/O during searches.
    std::atomic<size_t> num_threads_io_{8};

    // The number of threads used for building a single index.
    std::atomic<size_t> num_threads_building_{4};

    // The size of the maximum memory allocated each time (default is 128MB)
    std::atomic<size_t> block_size_limit_{128 * 1024 * 1024};

    Logger* logger_ = nullptr;
};

using Option = Options;  // for compatibility

}  // namespace vsag
