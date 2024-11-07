

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

#include <cpuinfo.h>

#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <random>

#include "vsag/vsag.h"

namespace vsag {

typedef float (*DistanceFunc)(const void* pVect1, const void* pVect2, const void* qty_ptr);
extern DistanceFunc
GetInnerProductDistanceFunc(size_t dim);
extern DistanceFunc
GetL2DistanceFunc(size_t dim);

}  // namespace vsag

float
L2Sqr(const void* pVect1v, const void* pVect2v, const void* qty_ptr) {
    float* pVect1 = (float*)pVect1v;
    float* pVect2 = (float*)pVect2v;
    size_t qty = *((size_t*)qty_ptr);

    float res = 0;
    for (size_t i = 0; i < qty; i++) {
        float t = *pVect1 - *pVect2;
        pVect1++;
        pVect2++;
        res += t * t;
    }
    return res;
}

float
InnerProductDistance(const void* pVect1, const void* pVect2, const void* qty_ptr) {
    size_t qty = *((size_t*)qty_ptr);
    float res = 0;
    for (unsigned i = 0; i < qty; i++) {
        res += ((float*)pVect1)[i] * ((float*)pVect2)[i];
    }
    return 1 - res;
}

TEST_CASE("test ip instructions", "[ut][simd]") {
    std::random_device rd;
    std::mt19937 rng(rd());
    for (size_t dim = 1; dim < 1026; dim++) {
        std::uniform_real_distribution<> distrib_real;
        float vector1[dim];
        float vector2[dim];
        for (int j = 0; j < dim; j++) {
            vector1[j] = distrib_real(rng);
            vector2[j] = distrib_real(rng);
        }
        auto distance_func = vsag::GetInnerProductDistanceFunc(dim);
        bool equal = (std::abs(InnerProductDistance(vector1, vector2, &dim) -
                               distance_func(vector1, vector2, &dim)) < 0.001);
        REQUIRE(equal);
    }
}

TEST_CASE("test l2 instructions", "[ut][simd]") {
    std::random_device rd;
    std::mt19937 rng(rd());
    for (size_t dim = 1; dim < 1026; dim++) {
        std::uniform_real_distribution<> distrib_real;
        float vector1[dim];
        float vector2[dim];
        for (int j = 0; j < dim; j++) {
            vector1[j] = distrib_real(rng);
            vector2[j] = distrib_real(rng);
        }
        auto distance_func = vsag::GetL2DistanceFunc(dim);
        bool equal = (std::abs(L2Sqr(vector1, vector2, &dim) -
                               distance_func(vector1, vector2, &dim)) < 0.001);
        REQUIRE(equal);
    }
}
