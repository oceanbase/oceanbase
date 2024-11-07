
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

#include "vsag/options.h"

#include <utility>

#include "default_allocator.h"
#include "default_logger.h"
#include "logger.h"

namespace vsag {

Options&
Options::Instance() {
    static Options s_instance;
    return s_instance;
}

Logger*
Options::logger() {
    static std::shared_ptr<DefaultLogger> s_default_logger = std::make_shared<DefaultLogger>();
    if (not logger_) {
        this->set_logger(s_default_logger.get());
    }
    return logger_;
}

void
Options::set_block_size_limit(size_t size) {
    if (size < 2 * 1024 * 1024) {
        throw std::runtime_error(fmt::format("size ({}) should be greater than 2M.", size));
    }
    block_size_limit_.store(size, std::memory_order_release);
}

void
Options::set_num_threads_io(size_t num_threads) {
    if (num_threads < 1 || num_threads > 200) {
        throw std::runtime_error(
            fmt::format("num_threads must be set between 1 and 200, but found {}.", num_threads));
    }
    num_threads_io_.store(num_threads, std::memory_order_release);
}

void
Options::set_num_threads_building(size_t num_threads) {
    if (num_threads < 1 || num_threads > 200) {
        throw std::runtime_error(
            fmt::format("num_threads must be set between 1 and 200, but found {}.", num_threads));
    }
    num_threads_building_.store(num_threads, std::memory_order_release);
}

}  // namespace vsag
