
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

#include <nlohmann/json.hpp>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "../footer.h"
#include "../logger.h"
#include "vsag/index.h"

namespace vsag {

static const int64_t LOOK_AT_K = 20;

class ConjugateGraph {
public:
    ConjugateGraph();

    tl::expected<bool, Error>
    AddNeighbor(int64_t from_tag_id, int64_t to_tag_id);

    tl::expected<uint32_t, Error>
    EnhanceResult(std::priority_queue<std::pair<float, size_t>>& results,
                  const std::function<float(int64_t)>& distance_of_tag) const;

public:
    tl::expected<Binary, Error>
    Serialize() const;

    tl::expected<void, Error>
    Serialize(std::ostream& out_stream) const;

    tl::expected<void, Error>
    Deserialize(const Binary& binary);

    tl::expected<void, Error>
    Deserialize(std::istream& in_stream);

    size_t
    GetMemoryUsage() const;

private:
    const std::unordered_set<int64_t>&
    get_neighbors(int64_t from_tag_id) const;

    void
    clear();

private:
    uint32_t memory_usage_;

    std::unordered_map<int64_t, std::unordered_set<int64_t>> conjugate_graph_;

    SerializationFooter footer_;
};

}  // namespace vsag
