
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
#include <iostream>

#include "vsag/vsag.h"

class RandomAllocator : public vsag::Allocator {
public:
    std::string
    Name() override {
        return "myallocator";
    }
    RandomAllocator() {
        rd_ = std::shared_ptr<std::random_device>(new std::random_device());
        gen_ = std::shared_ptr<std::mt19937>(new std::mt19937((*rd_)()));
        std::uniform_int_distribution<int> seed_random;
        std::uniform_real_distribution<> dis;
        std::uniform_real_distribution<> ratio_range(0.001f, 0.1f);
        int seed = seed_random(*gen_);
        float r = ratio_range(*gen_);
        std::cout << "seed: " << seed << "   error ratio: " << r << std::endl;
        gen_->seed(seed);
        error_ratio_ = r;
    }

    void*
    Allocate(size_t size) override {
        auto number = dis_(*gen_);
        if (number < error_ratio_) {
            return nullptr;
        }
        return malloc(size);
    }

    void
    Deallocate(void* p) override {
        return free(p);
    }

    void*
    Reallocate(void* p, size_t size) override {
        return realloc(p, size);
    }

private:
    std::shared_ptr<std::random_device> rd_;
    std::shared_ptr<std::mt19937> gen_;
    std::uniform_real_distribution<> dis_;
    float error_ratio_ = 0.0f;
};

TEST_CASE("Random Alocator Test", "[ft][hnsw]") {
    RandomAllocator allocator;

    auto paramesters = R"(
    {
        "dtype": "float32",
        "metric_type": "l2",
        "dim": 128,
        "hnsw": {
            "max_degree": 16,
            "ef_construction": 100
        }
    }
    )";
    auto result_index = vsag::Factory::CreateIndex("hnsw", paramesters, &allocator);
    if (not result_index.has_value()) {
        return;
    }
    auto index = result_index.value();

    int64_t num_vectors = 2000;
    int64_t num_querys = 1000;
    int64_t dim = 128;

    // prepare ids and vectors
    auto ids = std::shared_ptr<int64_t[]>(new int64_t[num_vectors]);
    auto vectors = std::shared_ptr<float[]>(new float[num_vectors * dim]);

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int64_t i = 0; i < num_vectors; ++i) {
        ids[i] = i;
    }
    for (int64_t i = 0; i < dim * num_vectors; ++i) {
        vectors[i] = distrib_real(rng);
    }
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors / 2)
        ->Dim(dim)
        ->Ids(ids.get())
        ->Float32Vectors(vectors.get())
        ->Owner(false);
    index->Build(base);

    for (int i = num_vectors / 2; i < num_vectors; ++i) {
        auto single_data = vsag::Dataset::Make();
        single_data->NumElements(1)
            ->Dim(dim)
            ->Ids(ids.get() + i)
            ->Float32Vectors(vectors.get() + i * dim)
            ->Owner(false);
        index->Add(single_data);
    }

    // search on the index
    auto query_vector = std::shared_ptr<float[]>(
        new float[num_querys * dim]);  // memory will be released by query the dataset
    for (int64_t i = 0; i < dim * num_querys; ++i) {
        query_vector[i] = distrib_real(rng);
    }
    auto hnsw_search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        }
    }
    )";
    int64_t topk = 10;
    auto query = vsag::Dataset::Make();
    for (int i = 0; i < num_querys; i++) {
        query->NumElements(1)->Dim(dim)->Float32Vectors(query_vector.get() + dim * i)->Owner(false);
        auto result = index->KnnSearch(query, topk, hnsw_search_parameters);
    }
    index = nullptr;
}
