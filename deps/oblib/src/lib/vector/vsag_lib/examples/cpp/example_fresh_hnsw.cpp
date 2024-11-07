
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

#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <random>

#include "vsag/vsag.h"

int
main() {
    int dim = 128;             // Dimension of the elements
    int max_elements = 10000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;       // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 50;  // Controls index search speed/build speed tradeoff
    int ef_search = 50;
    float threshold = 8.0;

    nlohmann::json hnsw_parameters{
        {"max_degree", max_degree}, {"ef_construction", ef_construction}, {"ef_search", ef_search}};
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};
    std::shared_ptr<vsag::Index> hnsw;
    if (auto index = vsag::Factory::CreateIndex("fresh_hnsw", index_parameters.dump());
        index.has_value()) {
        hnsw = index.value();
    } else {
        std::cout << "Build HNSW Error" << std::endl;
        return -1;
    }
    std::shared_ptr<int64_t[]> ids(new int64_t[max_elements]);
    std::shared_ptr<float[]> data(new float[dim * max_elements]);

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    {
        auto dataset = vsag::Dataset::Make();
        dataset->Dim(dim)
            ->NumElements(max_elements)
            ->Ids(ids.get())
            ->Float32Vectors(data.get())
            ->Owner(false);
        if (const auto num = hnsw->Build(dataset); num.has_value()) {
            std::cout << "After Build(), Index constains: " << hnsw->GetNumElements() << std::endl;
        } else if (num.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "Failed to build index: internalError" << std::endl;
            exit(-1);
        }
    }

    // Test recall
    float correct = 0;
    float recall = 0;
    {
        for (int i = 0; i < max_elements; i++) {
            auto query = vsag::Dataset::Make();
            query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
            // {
            //   "hnsw": {
            //     "ef_search": 200
            //   }
            // }

            nlohmann::json parameters{
                {"hnsw", {{"ef_search", ef_search}}},
            };
            int64_t k = 10;
            if (auto result = hnsw->KnnSearch(query, k, parameters.dump()); result.has_value()) {
                correct += vsag::knn_search_recall(data.get(),
                                                   ids.get(),
                                                   max_elements,
                                                   data.get() + i * dim,
                                                   dim,
                                                   result.value()->GetIds(),
                                                   result.value()->GetDim());
            } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
                std::cerr << "failed to perform knn search on index" << std::endl;
            }
        }
        recall = correct / max_elements;
        std::cout << std::fixed << std::setprecision(3)
                  << "Memory Uasage:" << hnsw->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
        std::cout << "Recall: " << recall << std::endl;
        std::cout << hnsw->GetStats() << std::endl;
    }

    // Remove and re-add
    for (int i = 0; i < max_elements; ++i) {
        hnsw->Remove(i);
    }
    std::cout << "Finish remove" << std::endl;

    for (int i = 0; i < max_elements; ++i) {
        auto incremental = vsag::Dataset::Make();
        incremental->Dim(dim)
            ->NumElements(1)
            ->Ids(ids.get() + i)
            ->Float32Vectors(data.get() + i * dim)
            ->Owner(false);
        hnsw->Add(incremental);
    }

    correct = 0;
    recall = 0;
    {
        for (int i = 0; i < max_elements; i++) {
            auto query = vsag::Dataset::Make();
            query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
            // {
            //   "hnsw": {
            //     "ef_search": 200
            //   }
            // }

            nlohmann::json parameters{
                {"hnsw", {{"ef_search", ef_search}}},
            };
            int64_t k = 10;
            if (auto result = hnsw->KnnSearch(query, k, parameters.dump()); result.has_value()) {
                correct += vsag::knn_search_recall(data.get(),
                                                   ids.get(),
                                                   max_elements,
                                                   data.get() + i * dim,
                                                   dim,
                                                   result.value()->GetIds(),
                                                   result.value()->GetDim());
            } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
                std::cerr << "failed to perform knn search on index" << std::endl;
            }
        }
        recall = correct / max_elements;
        std::cout << std::fixed << std::setprecision(3)
                  << "Memory Uasage:" << hnsw->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
        std::cout << "Recall after remove and re-add: " << recall << std::endl;
        std::cout << hnsw->GetStats() << std::endl;
    }
}
