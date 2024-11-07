
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

#include <cstring>
#include <deque>
#include <functional>
#include <mutex>

#include "../../default_allocator.h"
#include "stream_reader.h"
#include "stream_writer.h"

namespace hnswlib {

class BlockManager {
public:
    BlockManager(size_t size_data_per_element, size_t block_size_limit, vsag::Allocator* allocator);

    ~BlockManager();

    char*
    GetElementPtr(size_t index, size_t offset);

    bool
    Resize(size_t new_max_elements);

    bool
    Serialize(char*& buffer, size_t cur_element_count);

    bool
    Serialize(std::ostream& ofs, size_t cur_element_count);

    bool
    Deserialize(std::function<void(uint64_t, uint64_t, void*)> read_func,
                uint64_t cursor,
                size_t cur_element_count);

    bool
    Deserialize(std::istream& ifs, size_t cur_element_count);

    inline size_t
    GetSize() const {
        return max_elements_ * size_data_per_element_;
    }

    bool
    SerializeImpl(StreamWriter& writer, uint64_t cur_element_count);

    bool
    DeserializeImpl(StreamReader& reader, uint64_t cur_element_count);

private:
    std::vector<char*> blocks_ = {};
    size_t data_num_per_block_ = 0;
    size_t block_size_ = 0;
    size_t size_data_per_element_ = 0;
    size_t max_elements_ = 0;
    std::vector<size_t> block_lens_ = {};
    vsag::Allocator* const allocator_ = nullptr;
};
}  // namespace hnswlib
