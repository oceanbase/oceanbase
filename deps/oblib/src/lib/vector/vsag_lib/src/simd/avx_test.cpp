

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

#include <catch2/catch_test_macros.hpp>
#include <cstdint>

#include "./simd.h"
#include "catch2/catch_approx.hpp"
#include "cpuinfo.h"
#include "fixtures.h"

TEST_CASE("avx l2 simd16", "[ut][simd][avx]") {
#if defined(ENABLE_AVX)
    if (cpuinfo_has_x86_sse()) {
        size_t dim = 16;
        auto vectors = fixtures::generate_vectors(2, dim);

        fixtures::dist_t distance =
            vsag::L2SqrSIMD16ExtAVX(vectors.data(), vectors.data() + dim, &dim);
        fixtures::dist_t expected_distance =
            vsag::L2Sqr(vectors.data(), vectors.data() + dim, &dim);
        REQUIRE(distance == expected_distance);
    }
#endif
}

TEST_CASE("avx ip simd16", "[ut][simd][avx]") {
#if defined(ENABLE_AVX)
    if (cpuinfo_has_x86_sse()) {
        size_t dim = 16;
        auto vectors = fixtures::generate_vectors(2, dim);

        fixtures::dist_t distance =
            vsag::InnerProductSIMD16ExtAVX(vectors.data(), vectors.data() + dim, &dim);
        fixtures::dist_t expected_distance =
            vsag::InnerProduct(vectors.data(), vectors.data() + dim, &dim);
        REQUIRE(distance == expected_distance);
    }
#endif
}

TEST_CASE("avx pq calculation", "[ut][simd][avx]") {
#if defined(ENABLE_AVX)
    if (cpuinfo_has_x86_avx2()) {
        size_t dim = 256;
        float single_dim_value = 0.571;
        float results_expected[256]{0.0f};
        float results[256]{0.0f};
        auto vectors = fixtures::generate_vectors(1, dim);

        vsag::PQDistanceAVXFloat256(vectors.data(), single_dim_value, results);
        vsag::PQDistanceFloat256(vectors.data(), single_dim_value, results_expected);

        for (int i = 0; i < dim; ++i) {
            REQUIRE(fabs(results_expected[i] - results[i]) < 0.001);
        }
    }
#endif
}
