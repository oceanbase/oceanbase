
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

#include <memory>
#include <string>

#include "../algorithm/hnswlib/hnswlib.h"

namespace vsag {

struct CreateHnswParameters {
public:
    static CreateHnswParameters
    FromJson(const std::string& json_string);

public:
    // required vars
    std::shared_ptr<hnswlib::SpaceInterface> space;
    int64_t max_degree;
    int64_t ef_construction;
    bool use_conjugate_graph;
    bool use_static;
    bool normalize = false;

protected:
    CreateHnswParameters() = default;
};

struct CreateFreshHnswParameters : public CreateHnswParameters {
public:
    static CreateFreshHnswParameters
    FromJson(const std::string& json_string);

public:
    // required vars
    bool use_reversed_edges;

private:
    CreateFreshHnswParameters() = default;
};

struct HnswSearchParameters {
public:
    static HnswSearchParameters
    FromJson(const std::string& json_string);

public:
    // required vars
    int64_t ef_search;
    bool use_conjugate_graph_search;

private:
    HnswSearchParameters() = default;
};

}  // namespace vsag
