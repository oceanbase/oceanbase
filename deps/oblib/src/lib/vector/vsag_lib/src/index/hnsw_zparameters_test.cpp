

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

#include "hnsw_zparameters.h"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("create hnsw with invalid metric type", "[ut][hnsw]") {
    auto json_string = R"(
        {
            "dtype": "float32",
            "metric_type": "unknown-metric-type",
            "dim": 512,
            "hnsw": {
                "max_degree": 16,
                "ef_construction": 100
            },
            "diskann": {
                "max_degree": 16,
                "ef_construction": 200,
                "pq_dims": 32,
                "pq_sample_rate": 0.5
            }
        }
        )";

    REQUIRE_THROWS_AS(vsag::CreateHnswParameters::FromJson(json_string), std::invalid_argument);
}
