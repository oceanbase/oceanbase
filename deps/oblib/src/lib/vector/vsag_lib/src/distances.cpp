
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

#include <assert.h>

#include <cstdint>
#include <vector>

#include "vsag/utils.h"

namespace vsag {

float
l2sqr(const void* vec1, const void* vec2, int64_t dim) {
    float* v1 = (float*)vec1;
    float* v2 = (float*)vec2;

    float res = 0;
    for (int64_t i = 0; i < dim; i++) {
        float t = *v1 - *v2;
        v1++;
        v2++;
        res += t * t;
    }

    return res;
}

BitsetPtr
l2_and_filtering(int64_t dim, int64_t nb, const float* base, const float* query, float threshold) {
    BitsetPtr bp = Bitset::Make();

    for (int64_t i = 0; i < nb; ++i) {
        const float dist = l2sqr(base + i * dim, query, dim);
        if (dist <= threshold) {
            bp->Set(i, true);
        }
    }

    return bp;
}

float
knn_search_recall(const float* base,
                  const int64_t* id_map,
                  int64_t base_num,
                  const float* query,
                  int64_t data_dim,
                  const int64_t* result_ids,
                  int64_t result_size) {
    int64_t nearest_index = 0;
    float nearest_dis = std::numeric_limits<float>::max();
    for (int64_t i = 0; i < base_num; ++i) {
        float dis = l2sqr(base + i * data_dim, query, data_dim);
        if (nearest_dis > dis) {
            nearest_index = i;
            nearest_dis = dis;
        }
    }
    for (int64_t i = 0; i < result_size; ++i) {
        if (result_ids[i] == id_map[nearest_index]) {
            return 1;
        }
    }
    return 0;
}

float
range_search_recall(const float* base,
                    const int64_t* base_ids,
                    int64_t num_base,
                    const float* query,
                    int64_t dim,
                    const int64_t* result_ids,
                    int64_t result_size,
                    float threshold) {
    BitsetPtr groundtruth = l2_and_filtering(dim, num_base, base, query, threshold);
    if (groundtruth->Count() == 0) {
        return 1;
    }
    return (float)(result_size) / groundtruth->Count();
}

}  // namespace vsag
