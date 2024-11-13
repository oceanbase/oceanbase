
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

#include <iostream>
#include <nlohmann/json.hpp>

#include "vsag/vsag.h"

void
float_hnsw_conjugate() {
    // parameters
    int dim = 128;
    int base_elements = 10000;
    int query_elements = 1000;
    int max_degree = 16;
    int ef_construction = 100;
    int ef_search = 10;
    int64_t k = 10;

    // generate data (use base[0: query_num] as query)
    auto base = vsag::Dataset::Make();
    std::shared_ptr<int64_t[]> base_ids(new int64_t[base_elements]);
    std::shared_ptr<float[]> base_data(new float[dim * base_elements]);
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distribution_real(-1, 1);
    for (int i = 0; i < base_elements; i++) {
        base_ids[i] = i;

        for (int d = 0; d < dim; d++) {
            base_data[d + i * dim] = distribution_real(rng);
        }
    }
    base->Dim(dim)
        ->NumElements(base_elements)
        ->Ids(base_ids.get())
        ->Float32Vectors(base_data.get())
        ->Owner(false);

    // create index
    nlohmann::json hnsw_parameters{{"max_degree", max_degree},
                                   {"ef_construction", ef_construction},
                                   {"ef_search", ef_search},
                                   {"use_conjugate_graph", true}};
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};
    std::shared_ptr<vsag::Index> hnsw;
    if (auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
        index.has_value()) {
        hnsw = index.value();
    } else {
        std::cout << "Build HNSW Error" << std::endl;
        return;
    }

    // Build index
    {
        if (const auto num = hnsw->Build(base); num.has_value()) {
            std::cout << "After Build(), Index constains: " << hnsw->GetNumElements() << std::endl;
        } else if (num.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "Failed to build index: internalError" << std::endl;
            exit(-1);
        }
    }

    // Search without conjugate graph
    std::set<std::pair<int, int64_t>> failed_queries;
    {
        nlohmann::json search_parameters{
            {"hnsw", {{"ef_search", ef_search}, {"use_conjugate_graph_search", false}}},
        };
        int correct = 0;
        std::cout << "====Search Stage====" << std::endl;
        std::cout << std::fixed << std::setprecision(3)
                  << "Memory Usage:" << hnsw->GetMemoryUsage() / 1024.0 << " KB" << std::endl;

        for (int i = 0; i < query_elements; i++) {
            auto query = vsag::Dataset::Make();
            query->Dim(dim)
                ->Float32Vectors(base_data.get() + i * dim)
                ->NumElements(1)
                ->Owner(false);

            auto result = hnsw->KnnSearch(query, k, search_parameters.dump());
            int64_t global_optimum = i;  // global optimum is itself
            int64_t local_optimum = result.value()->GetIds()[0];

            if (local_optimum != global_optimum) {
                failed_queries.emplace(i, global_optimum);
            }

            if (local_optimum == global_optimum) {
                correct++;
            }
        }
        std::cout << "Recall: " << correct / (1.0 * query_elements) << std::endl;
    }

    // Feedback
    {
        nlohmann::json search_parameters{
            {"hnsw", {{"ef_search", ef_search}, {"use_conjugate_graph_search", false}}},
        };
        int error_fixed = 0;
        std::cout << "====Feedback Stage====" << std::endl;
        for (auto item : failed_queries) {
            auto query = vsag::Dataset::Make();
            query->Dim(dim)
                ->Float32Vectors(base_data.get() + item.first * dim)
                ->NumElements(1)
                ->Owner(false);
            error_fixed += *hnsw->Feedback(query, 1, search_parameters.dump(), item.second);
        }
        std::cout << "Fixed queries num: " << error_fixed << std::endl;
    }

    // Enhanced search
    {
        nlohmann::json search_parameters{
            {"hnsw", {{"ef_search", ef_search}, {"use_conjugate_graph_search", true}}},
        };
        int correct = 0;
        std::cout << "====Enhanced Search Stage====" << std::endl;
        std::cout << std::fixed << std::setprecision(3)
                  << "Memory Usage:" << hnsw->GetMemoryUsage() / 1024.0 << " KB" << std::endl;

        for (int i = 0; i < query_elements; i++) {
            auto query = vsag::Dataset::Make();
            query->Dim(dim)
                ->Float32Vectors(base_data.get() + i * dim)
                ->NumElements(1)
                ->Owner(false);

            auto result = hnsw->KnnSearch(query, k, search_parameters.dump());
            int64_t global_optimum = i;  // global optimum is itself
            int64_t local_optimum = result.value()->GetIds()[0];

            if (local_optimum == global_optimum) {
                correct++;
            }
        }
        std::cout << "Enhanced Recall: " << correct / (1.0 * query_elements) << std::endl;
    }
}

int
main() {
    float_hnsw_conjugate();
}
