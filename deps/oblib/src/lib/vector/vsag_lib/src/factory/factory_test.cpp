

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

#include "vsag/factory.h"

#include <spdlog/spdlog-inl.h>

#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

#include "../logger.h"
#include "vsag/errors.h"

TEST_CASE("create index with full parameters", "[factory][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);

    SECTION("hnsw") {
        auto parameters = nlohmann::json::parse(R"(
        {
            "dtype": "float32",
            "metric_type": "l2",
            "dim": 512,
            "hnsw": {
                "max_degree": 16,
                "ef_construction": 100
            }
        }
        )");

        auto index = vsag::Factory::CreateIndex("hnsw", parameters.dump());
        REQUIRE(index.has_value());
    }

    SECTION("diskann") {
        auto parameters = nlohmann::json::parse(R"(
        {
            "dtype": "float32",
            "metric_type": "l2",
            "dim": 256,
            "diskann": {
                "ef_construction": 200,
                "max_degree": 16,
                "pq_dims": 32,
                "pq_sample_rate": 0.5
            }
        }
        )");

        auto index = vsag::Factory::CreateIndex("diskann", parameters.dump());
        REQUIRE(index.has_value());
    }
}

TEST_CASE("create hnsw with incomplete parameters", "[factory][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);

    auto standard_parameters = nlohmann::json::parse(R"(
            {
                "dtype": "float32",
                "metric_type": "l2",
                "dim": 512,
                "hnsw": {
                    "max_degree": 16,
                    "ef_construction": 100
                }
            }
            )");

    SECTION("dtype is not provided") {
        standard_parameters.erase("dtype");
        auto index = vsag::Factory::CreateIndex("hnsw", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("metric_type is not provided") {
        standard_parameters.erase("metric_type");
        auto index = vsag::Factory::CreateIndex("hnsw", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("dim is not provided") {
        standard_parameters.erase("dim");
        auto index = vsag::Factory::CreateIndex("hnsw", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("hnsw is not provided") {
        standard_parameters.erase("hnsw");
        auto index = vsag::Factory::CreateIndex("hnsw", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("max_degree is not provided") {
        standard_parameters["hnsw"].erase("max_degree");
        auto index = vsag::Factory::CreateIndex("hnsw", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("ef_construction is not provided") {
        standard_parameters["hnsw"].erase("ef_construction");
        auto index = vsag::Factory::CreateIndex("hnsw", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }
}

TEST_CASE("create diskann with incomplete parameters", "[factory][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);

    auto standard_parameters = nlohmann::json::parse(R"(
            {
                "dim": 256,
                "dtype": "float32",
                "metric_type": "l2",
                "diskann": {
                    "max_degree": 16,
                    "ef_construction": 200,
                    "pq_dims": 32,
                    "pq_sample_rate": 0.5
                }
            }
            )");

    SECTION("dtype is not provided") {
        standard_parameters.erase("dtype");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("metric_type is not provided") {
        standard_parameters.erase("metric_type");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("dim is not provided") {
        standard_parameters.erase("dim");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("diskann is not provided") {
        standard_parameters.erase("diskann");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("max_degree is not provided") {
        standard_parameters["diskann"].erase("max_degree");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("ef_construction is not provided") {
        standard_parameters["diskann"].erase("ef_construction");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("pq_dims is not provided") {
        standard_parameters["diskann"].erase("pq_dims");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("pq_sample_rate is not provided") {
        standard_parameters["diskann"].erase("pq_sample_rate");
        auto index = vsag::Factory::CreateIndex("diskann", standard_parameters.dump());
        REQUIRE_FALSE(index.has_value());
        REQUIRE(index.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }
}
