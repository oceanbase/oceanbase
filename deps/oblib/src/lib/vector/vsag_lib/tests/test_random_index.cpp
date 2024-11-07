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

#include <spdlog/spdlog.h>

#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

#include "vsag/vsag.h"

using namespace std;

TEST_CASE("Random Index Test", "[ft][random]") {
    std::random_device rd;
    std::mt19937 rng(rd());

    std::uniform_int_distribution<int> dim_generate(1, 500);
    std::uniform_int_distribution<int> max_elements_generate(
        2, 1000);  // DiskANN does not allow building a graph with fewer than 2 points.
    std::uniform_int_distribution<int> max_degree_generate(
        5, 64);  // When the number of edges is less than 5, connectivity cannot be guaranteed.
    std::uniform_int_distribution<int> construct_generate(1, 500);
    std::uniform_int_distribution<int> search_generate(1, 500);
    std::uniform_int_distribution<int> k_generate(1, 200);
    std::uniform_int_distribution<int> io_limit_generate(1, 500);
    std::uniform_real_distribution<float> threshold_generate(1, 500);
    std::uniform_real_distribution<float> pq_dims_generate(
        1, 512);  // DiskANN does not allow the number of PQ buckets to be greater than 512.
    std::uniform_real_distribution<float> use_pq_search_generate;
    std::uniform_real_distribution<float> mold_generate(-1000, 1000);

    int dim = dim_generate(rng);
    int max_elements = max_elements_generate(rng);
    int max_degree = max_degree_generate(rng);
    int ef_construction = std::max(max_degree, construct_generate(rng));
    int ef_search = search_generate(rng);
    int64_t k = k_generate(rng);

    int io_limit = io_limit_generate(rng);
    float threshold = threshold_generate(rng);
    int pq_dims = pq_dims_generate(rng);
    bool use_pq_search = use_pq_search_generate(rng) > 0.5;
    float mold = mold_generate(rng);

    std::uniform_int_distribution<int> seed_random;
    int seed = seed_random(rng);
    rng.seed(seed);

    spdlog::info(
        "seed: {}, dim: {}, max_elements: {}, max_degree: {}, ef_construction: {}, ef_search: {}, "
        "k: {}, "
        "io_limit: {}, threshold: {}, pq_dims: {}, use_pq_search: {}, mold: {}",
        seed,
        dim,
        max_elements,
        max_degree,
        ef_construction,
        ef_search,
        k,
        io_limit,
        threshold,
        pq_dims,
        use_pq_search,
        mold);
    float pq_sample_rate = 0.5;
    // Initing index
    nlohmann::json hnsw_parameters{
        {"max_elements", max_elements},
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"ef_search", ef_search},
    };

    nlohmann::json diskann_parameters{{"max_degree", max_degree},
                                      {"ef_construction", ef_construction},
                                      {"pq_sample_rate", pq_sample_rate},
                                      {"pq_dims", pq_dims},
                                      {"use_pq_search", use_pq_search}};
    nlohmann::json index_parameters{{"dtype", "float32"},
                                    {"metric_type", "l2"},
                                    {"dim", dim},
                                    {"diskann", diskann_parameters},
                                    {"hnsw", hnsw_parameters}};

    nlohmann::json parameters{
        {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", io_limit}}},
        {"hnsw", {{"ef_search", ef_search}}}};

    // Generate random data
    std::uniform_real_distribution<> distrib_real;
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int i = 0; i < max_elements; i++) {
        ids[i] = i;
    }
    for (int i = 0; i < dim * max_elements; i++) {
        data[i] = distrib_real(rng) * mold;
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);

    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
    REQUIRE(index.has_value());
    hnsw = index.value();
    hnsw->Build(dataset);

    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        auto knn_result = hnsw->KnnSearch(query, k, parameters.dump());
        REQUIRE(knn_result.has_value());

        REQUIRE(knn_result.value()->GetDim() == std::min(k, (int64_t)max_elements));
        auto range_result = hnsw->RangeSearch(query, threshold, parameters.dump());
        REQUIRE(range_result.has_value());
    }

    std::shared_ptr<vsag::Index> diskann;
    index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    diskann->Build(dataset);

    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        auto knn_result = diskann->KnnSearch(query, k, parameters.dump());
        REQUIRE(knn_result.has_value());
        REQUIRE(knn_result.value()->GetDim() == std::min(k, (int64_t)max_elements));
        auto range_result = diskann->RangeSearch(query, threshold, parameters.dump());
        REQUIRE(range_result.has_value());
    }

    REQUIRE(diskann->GetMemoryUsage() < diskann->GetEstimateBuildMemory(max_elements));
}
