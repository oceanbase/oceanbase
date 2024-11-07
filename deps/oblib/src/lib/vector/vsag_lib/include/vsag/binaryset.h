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
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace vsag {

struct Binary {
    std::shared_ptr<int8_t[]> data;
    size_t size = 0;
};

class BinarySet {
public:
    BinarySet() = default;

    ~BinarySet() = default;

    void
    Set(const std::string& name, Binary binary) {
        data_[name] = binary;
    }

    Binary
    Get(const std::string& name) const {
        if (data_.find(name) == data_.end()) {
            return Binary();
        }
        return data_.at(name);
    }

    std::vector<std::string>
    GetKeys() const {
        std::vector<std::string> keys;
        keys.resize(data_.size());
        transform(
            data_.begin(), data_.end(), keys.begin(), [](std::pair<std::string, Binary> pair) {
                return pair.first;
            });
        return keys;
    }

    bool
    Contains(const std::string& key) const {
        return data_.find(key) != data_.end();
    }

private:
    std::unordered_map<std::string, Binary> data_;
};

}  // namespace vsag
