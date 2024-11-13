
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

#include "diskann.h"

#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>
#include <tuple>
#include <vector>

#include "../logger.h"
#include "distance.h"
#include "fixtures.h"
#include "vsag/errors.h"

TEST_CASE("build", "[diskann][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;
    auto index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                 "float32",
                                                 ef_construction,
                                                 max_degree,
                                                 pq_sample_rate,
                                                 pq_dims,
                                                 dim,
                                                 false,
                                                 false,
                                                 false);

    int64_t num_elements = 10;
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_elements, dim);

    SECTION("build with incorrect dim") {
        int64_t incorrect_dim = dim - 1;
        auto dataset = vsag::Dataset::Make();
        dataset->Dim(incorrect_dim)
            ->NumElements(num_elements)
            ->Ids(ids.data())
            ->Float32Vectors(vectors.data())
            ->Owner(false);
        auto result = index->Build(dataset);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("build twice") {
        auto dataset = vsag::Dataset::Make();
        dataset->Dim(dim)
            ->NumElements(10)
            ->Ids(ids.data())
            ->Float32Vectors(vectors.data())
            ->Owner(false);
        REQUIRE(index->Build(dataset).has_value());

        auto result = index->Build(dataset);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::BUILD_TWICE);
    }
}

TEST_CASE("build & search empty index", "[diskann][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;
    auto index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                 "float32",
                                                 ef_construction,
                                                 max_degree,
                                                 pq_sample_rate,
                                                 pq_dims,
                                                 dim,
                                                 false,
                                                 false,
                                                 false);

    auto dataset = vsag::Dataset::Make();
    dataset->NumElements(0);
    auto result = index->Build(dataset);
    REQUIRE(result.has_value());

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(1, dim);
    auto one_vector = vsag::Dataset::Make();
    one_vector->NumElements(1)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    auto search_parameters = R"(
    {
        "diskann": {
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": false
        }
    }
    )";

    auto knnsearch = index->KnnSearch(one_vector, 10, search_parameters);
    REQUIRE(knnsearch.has_value());
    REQUIRE(knnsearch.value()->GetNumElements() == 1);
    REQUIRE(knnsearch.value()->GetDim() == 0);

    auto rangesearch = index->RangeSearch(one_vector, 10, search_parameters);
    REQUIRE(rangesearch.has_value());
    REQUIRE(rangesearch.value()->GetNumElements() == 1);
    REQUIRE(rangesearch.value()->GetDim() == 0);
}

TEST_CASE("knn_search", "[diskann][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;
    auto index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                 "float32",
                                                 ef_construction,
                                                 max_degree,
                                                 pq_sample_rate,
                                                 pq_dims,
                                                 dim,
                                                 false,
                                                 false,
                                                 false);

    int64_t num_elements = 100;
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_elements, dim);

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)
        ->NumElements(num_elements)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    auto result = index->Build(dataset);
    REQUIRE(result.has_value());

    auto query = vsag::Dataset::Make();
    query->Dim(dim)->NumElements(1)->Ids(ids.data())->Float32Vectors(vectors.data())->Owner(false);
    int64_t k = 10;
    nlohmann::json params{{"diskann", {{"ef_search", 100}, {"beam_search", 4}, {"io_limit", 200}}}};

    SECTION("index empty") {
        auto empty_index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                           "float32",
                                                           ef_construction,
                                                           max_degree,
                                                           pq_sample_rate,
                                                           pq_dims,
                                                           dim,
                                                           false,
                                                           false,
                                                           false);
        auto result = empty_index->KnnSearch(query, k, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INDEX_EMPTY);
    }

    SECTION("invalid parameters k is 0") {
        auto result = index->KnnSearch(query, 0, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters k less than 0") {
        auto result = index->KnnSearch(query, -1, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("dimension not equal") {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim - 1)->Float32Vectors(vectors.data())->Owner(false);
        auto result = index->KnnSearch(query, k, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters diskann not found") {
        nlohmann::json invalid_params{};
        auto result = index->KnnSearch(query, k, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters beam_search not found") {
        nlohmann::json invalid_params{{"diskann", {{"ef_search", 100}, {"io_limit", 200}}}};

        auto result = index->KnnSearch(query, k, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters io_limit not found") {
        nlohmann::json invalid_params{{"diskann", {{"ef_search", 100}, {"beam_search", 4}}}};

        auto result = index->KnnSearch(query, k, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters ef_search not found") {
        nlohmann::json invalid_params{{"diskann", {{"beam_search", 4}, {"io_limit", 200}}}};

        auto result = index->KnnSearch(query, k, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }
}

TEST_CASE("range_search", "[diskann][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;
    auto index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                 "float32",
                                                 ef_construction,
                                                 max_degree,
                                                 pq_sample_rate,
                                                 pq_dims,
                                                 dim,
                                                 false,
                                                 false,
                                                 false);

    int64_t num_elements = 100;
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_elements, dim);

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)
        ->NumElements(num_elements)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    auto result = index->Build(dataset);
    REQUIRE(result.has_value());

    auto query = vsag::Dataset::Make();
    query->Dim(dim)->NumElements(1)->Ids(ids.data())->Float32Vectors(vectors.data())->Owner(false);
    float radius = 9.9f;
    nlohmann::json params{{"diskann", {{"ef_search", 100}, {"beam_search", 4}, {"io_limit", 200}}}};

    SECTION("successful case with smaller range_search_limit") {
        int64_t range_search_limit = num_elements - 1;
        auto result = index->RangeSearch(query, 1000, params.dump(), range_search_limit);
        REQUIRE(result.has_value());
        REQUIRE((*result)->GetDim() == range_search_limit);
    }

    SECTION("successful case with larger range_search_limit") {
        int64_t range_search_limit = num_elements + 1;
        auto result = index->RangeSearch(query, 1000, params.dump(), range_search_limit);
        REQUIRE(result.has_value());
        REQUIRE((*result)->GetDim() == num_elements);
    }

    SECTION("invalid parameter range_search_limit less than 0") {
        int64_t range_search_limit = -1;
        auto result = index->RangeSearch(query, 1000, params.dump(), range_search_limit);
        REQUIRE(result.has_value());
        REQUIRE((*result)->GetDim() == num_elements);
    }

    SECTION("invalid parameter range_search_limit equals to 0") {
        int64_t range_search_limit = 0;
        auto result = index->RangeSearch(query, 1000, params.dump(), range_search_limit);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("index empty") {
        auto empty_index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                           "float32",
                                                           ef_construction,
                                                           max_degree,
                                                           pq_sample_rate,
                                                           pq_dims,
                                                           dim,
                                                           false,
                                                           false,
                                                           false);
        auto result = empty_index->RangeSearch(query, radius, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INDEX_EMPTY);
    }

    SECTION("invalid parameter radius equals to 0") {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data())->Owner(false);
        auto result = index->RangeSearch(query, 0, params.dump());
        REQUIRE(result.has_value());
    }

    SECTION("invalid parameter radius less than 0") {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data())->Owner(false);
        auto result = index->RangeSearch(query, -1, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("dimension not equal") {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim - 1)->Float32Vectors(vectors.data())->Owner(false);
        auto result = index->RangeSearch(query, radius, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("query length is not 1") {
        auto query = vsag::Dataset::Make();
        query->NumElements(2)->Dim(dim)->Float32Vectors(vectors.data())->Owner(false);
        auto result = index->RangeSearch(query, radius, params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters diskann not found") {
        nlohmann::json invalid_params{};
        auto result = index->RangeSearch(query, radius, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters beam_search not found") {
        nlohmann::json invalid_params{{"diskann", {{"ef_search", 100}, {"io_limit", 200}}}};

        auto result = index->RangeSearch(query, radius, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters io_limit not found") {
        nlohmann::json invalid_params{{"diskann", {{"ef_search", 100}, {"beam_search", 4}}}};

        auto result = index->RangeSearch(query, radius, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }

    SECTION("invalid parameters ef_search not found") {
        nlohmann::json invalid_params{{"diskann", {{"beam_search", 4}, {"io_limit", 200}}}};

        auto result = index->RangeSearch(query, radius, invalid_params.dump());
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }
}

TEST_CASE("serialize empty index", "[diskann][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;
    auto index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                 "float32",
                                                 ef_construction,
                                                 max_degree,
                                                 pq_sample_rate,
                                                 pq_dims,
                                                 dim,
                                                 false,
                                                 false,
                                                 false);

    auto result = index->Serialize();
    REQUIRE(result.has_value());
}

TEST_CASE("deserialize on not empty index", "[diskann][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;
    auto index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                 "float32",
                                                 ef_construction,
                                                 max_degree,
                                                 pq_sample_rate,
                                                 pq_dims,
                                                 dim,
                                                 false,
                                                 false,
                                                 false);

    int64_t num_elements = 100;
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_elements, dim);

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)
        ->NumElements(num_elements)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    auto result = index->Build(dataset);
    REQUIRE(result.has_value());

    auto binary_set = index->Serialize();
    REQUIRE(binary_set.has_value());

    auto voidresult = index->Deserialize(binary_set.value());
    REQUIRE_FALSE(voidresult.has_value());
    REQUIRE(voidresult.error().type == vsag::ErrorType::INDEX_NOT_EMPTY);
}

TEST_CASE("split building process", "[diskann][ut]") {
    vsag::logger::set_level(vsag::logger::level::debug);
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;

    int64_t num_elements = 1000;
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_elements, dim);

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)
        ->NumElements(num_elements)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);

    vsag::Index::Checkpoint checkpoint;
    std::shared_ptr<vsag::DiskANN> partial_index;
    double partial_time = 0;
    {
        vsag::Timer timer(partial_time);
        while (not checkpoint.finish) {
            partial_index = std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                                            "float32",
                                                            ef_construction,
                                                            max_degree,
                                                            pq_sample_rate,
                                                            pq_dims,
                                                            dim,
                                                            false,
                                                            false,
                                                            false);
            checkpoint = partial_index->ContinueBuild(dataset, checkpoint.data).value();
        }
    }

    nlohmann::json parameters{
        {"diskann", {{"ef_search", 10}, {"beam_search", 4}, {"io_limit", 20}}}};
    float correct = 0;
    for (int i = 0; i < num_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data() + i * dim)->Owner(false);
        int64_t k = 2;
        if (auto result = partial_index->KnnSearch(query, k, parameters.dump());
            result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetIds()[0] == i) {
                    correct++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    float recall_partial = correct / 1000;

    double full_time = 0;
    {
        vsag::Timer timer(full_time);
        std::shared_ptr<vsag::DiskANN> full_index =
            std::make_shared<vsag::DiskANN>(diskann::Metric::L2,
                                            "float32",
                                            ef_construction,
                                            max_degree,
                                            pq_sample_rate,
                                            pq_dims,
                                            dim,
                                            false,
                                            false,
                                            false);
        full_index->Build(dataset);
    }
    correct = 0;
    for (int i = 0; i < num_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data() + i * dim)->Owner(false);
        int64_t k = 2;
        if (auto result = partial_index->KnnSearch(query, k, parameters.dump());
            result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetIds()[0] == i) {
                    correct++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    float recall_full = correct / 1000;
    std::cout << "Recall: " << recall_full << std::endl;
    REQUIRE(recall_full == recall_partial);
}
