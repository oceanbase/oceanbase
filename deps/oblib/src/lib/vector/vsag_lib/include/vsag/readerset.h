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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace vsag {

enum class IOErrorCode { IO_SUCCESS = 0, IO_ERROR = 1, IO_TIMEOUT = 2 };

using CallBack = std::function<void(IOErrorCode code, const std::string& message)>;

class Reader {
public:
    Reader() = default;
    ~Reader() = default;

public:
    // Read len bytes from file/memory to the memory pointed to by dest.
    // thread-safe
    virtual void
    Read(uint64_t offset, uint64_t len, void* dest) = 0;

    virtual void
    AsyncRead(uint64_t offset, uint64_t len, void* dest, CallBack callback) = 0;

    virtual uint64_t
    Size() const = 0;
};

class ReaderSet {
public:
    ReaderSet() = default;
    ~ReaderSet() = default;

    void
    Set(const std::string& name, std::shared_ptr<Reader> reader) {
        data_[name] = reader;
    }

    std::shared_ptr<Reader>
    Get(const std::string& name) const {
        if (data_.find(name) == data_.end()) {
            return nullptr;
        }
        return data_.at(name);
    }

    std::vector<std::string>
    GetKeys() const {
        std::vector<std::string> keys;
        keys.resize(data_.size());
        transform(data_.begin(),
                  data_.end(),
                  keys.begin(),
                  [](std::pair<std::string, std::shared_ptr<Reader>> pair) { return pair.first; });
        return keys;
    }

    bool
    Contains(const std::string& key) const {
        return data_.find(key) != data_.end();
    }

private:
    std::unordered_map<std::string, std::shared_ptr<Reader>> data_;
};

}  // namespace vsag
