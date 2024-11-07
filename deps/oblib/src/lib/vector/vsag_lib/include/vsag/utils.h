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

#include <cstddef>
#include <random>
#include <string>
#include <vector>

#include "bitset.h"
#include "vsag/errors.h"
#include "vsag/expected.hpp"

namespace vsag {

float
kmeans_clustering(
    size_t d, size_t n, size_t k, const float* x, float* centroids, const std::string& dis_type);

BitsetPtr
l2_and_filtering(int64_t dim, int64_t nb, const float* base, const float* query, float threshold);

float
knn_search_recall(const float* base,
                  const int64_t* id_map,
                  int64_t base_num,
                  const float* query,
                  int64_t data_dim,
                  const int64_t* result_ids,
                  int64_t result_size);

float
range_search_recall(const float* base,
                    const int64_t* base_ids,
                    int64_t num_base,
                    const float* query,
                    int64_t dim,
                    const int64_t* result_ids,
                    int64_t result_size,
                    float threshold);

}  // namespace vsag
