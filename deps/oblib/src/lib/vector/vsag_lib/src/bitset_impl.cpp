

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

#include "./bitset_impl.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <random>
#include <sstream>

namespace vsag {

BitsetPtr
Bitset::Random(int64_t length) {
    auto bitset = std::make_shared<BitsetImpl>();
    static auto gen =
        std::bind(std::uniform_int_distribution<>(0, 1), std::default_random_engine());
    for (int64_t i = 0; i < length; ++i) {
        bitset->Set(i, gen());
    }
    return bitset;
}

BitsetPtr
Bitset::Make() {
    return std::make_shared<BitsetImpl>();
}

void
BitsetImpl::Set(int64_t pos, bool value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (value) {
        r_.add(pos);
    } else {
        r_.remove(pos);
    }
}

bool
BitsetImpl::Test(int64_t pos) {
    std::lock_guard<std::mutex> lock(mutex_);
    return r_.contains(pos);
}

uint64_t
BitsetImpl::Count() {
    return r_.cardinality();
}

std::string
BitsetImpl::Dump() {
    return r_.toString();
}

}  // namespace vsag
