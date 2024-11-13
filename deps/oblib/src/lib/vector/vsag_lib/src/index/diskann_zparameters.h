
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

#include <string>

#include "diskann.h"

namespace vsag {

struct CreateDiskannParameters {
public:
    static CreateDiskannParameters
    FromJson(const std::string& json_string);

public:
    // require vars
    int64_t dim;
    std::string dtype;
    diskann::Metric metric;
    int64_t max_degree;
    int64_t ef_construction;
    int64_t pq_dims;
    float pq_sample_rate;

    // optional vars with default value
    bool use_preload = false;
    bool use_reference = true;
    bool use_opq = false;
    bool use_bsa = false;
    bool use_async_io = false;

private:
    CreateDiskannParameters() = default;
};

struct DiskannSearchParameters {
public:
    static DiskannSearchParameters
    FromJson(const std::string& json_string);

public:
    // required vars
    int64_t ef_search;
    uint64_t beam_search;
    int64_t io_limit;

    // optional vars with default value
    bool use_reorder = false;

private:
    DiskannSearchParameters() = default;
};

}  // namespace vsag
