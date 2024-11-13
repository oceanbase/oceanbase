
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
#include <future>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>
#include <type_traits>

#include "vsag/options.h"
#include "vsag/vsag.h"

class ThreadPool {
public:
    ThreadPool(size_t threads) : stop(false) {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock,
                                             [this] { return this->stop || !this->tasks.empty(); });
                        if (this->stop && this->tasks.empty())
                            return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    template <class F, class... Args>
    auto
    enqueue(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F, Args...>::type> {
        using return_type = typename std::invoke_result<F, Args...>::type;

        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));

        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (stop)
                throw std::runtime_error("enqueue on stopped ThreadPool");
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread& worker : workers) worker.join();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

float
query_knn(std::shared_ptr<vsag::Index> index,
          const vsag::DatasetPtr& query,
          int64_t id,
          int64_t k,
          const std::string& parameters,
          vsag::BitsetPtr invalid) {
    if (auto result = index->KnnSearch(query, k, parameters, invalid); result.has_value()) {
        if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == id) {
            return 1.0;
        } else {
            std::cout << result.value()->GetDim() << " " << result.value()->GetIds()[0] << " " << id
                      << std::endl;
        }
    } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
        std::cerr << "failed to perform knn search on index" << std::endl;
    }
    return 0.0;
}

TEST_CASE("DiskAnn Multi-threading", "[ft][diskann]") {
    int dim = 65;             // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 100;
    int io_limit = 200;
    float threshold = 8.0;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 9;  // pq_dims represents the dimensionality of the compressed vector.
    nlohmann::json diskann_parameters{{"max_degree", max_degree},
                                      {"ef_construction", ef_construction},
                                      {"pq_sample_rate", pq_sample_rate},
                                      {"pq_dims", pq_dims}};
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump()).value();

    std::shared_ptr<int64_t[]> ids(new int64_t[max_elements]);
    std::shared_ptr<float[]> data(new float[dim * max_elements]);

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)
        ->NumElements(max_elements)
        ->Ids(ids.get())
        ->Float32Vectors(data.get())
        ->Owner(false);
    auto result = index->Build(dataset);
    REQUIRE(result.has_value());

    ThreadPool pool(10);
    std::vector<std::future<float>> future_results;
    float correct = 0;
    nlohmann::json parameters{
        {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", io_limit}}}};
    std::string str_parameters = parameters.dump();

    vsag::BitsetPtr zeros = vsag::Bitset::Make();
    for (int i = 0; i < max_elements; i++) {
        int64_t k = 2;
        future_results.push_back(
            pool.enqueue([&index, &ids, dim, &data, i, k, &str_parameters, &zeros]() -> float {
                auto query = vsag::Dataset::Make();
                query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
                return query_knn(index, query, *(ids.get() + i), k, str_parameters, zeros);
            }));
    }
    for (int i = 0; i < future_results.size(); ++i) {
        correct += future_results[i].get();
    }

    float recall = correct / max_elements;
    std::cout << index->GetStats() << std::endl;
    REQUIRE(recall >= 0.99);
}

TEST_CASE("HNSW Multi-threading", "[ft][hnsw]") {
    int dim = 16;             // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 100;
    float threshold = 8.0;
    nlohmann::json hnsw_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"ef_search", ef_search},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump()).value();
    std::shared_ptr<int64_t[]> ids(new int64_t[max_elements]);
    std::shared_ptr<float[]> data(new float[dim * max_elements]);

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    ThreadPool pool(10);
    std::vector<std::future<uint64_t>> insert_results;
    for (int64_t i = 0; i < max_elements; ++i) {
        insert_results.push_back(pool.enqueue([&ids, &data, &index, dim, i]() -> uint64_t {
            auto dataset = vsag::Dataset::Make();
            dataset->Dim(dim)
                ->NumElements(1)
                ->Ids(ids.get() + i)
                ->Float32Vectors(data.get() + i * dim)
                ->Owner(false);
            auto add_res = index->Add(dataset);
            REQUIRE(add_res.has_value());
            return add_res.value().size();
        }));
    }
    for (auto& res : insert_results) {
        REQUIRE(res.get() == 0);
    }

    std::vector<std::future<float>> future_results;
    float correct = 0;
    nlohmann::json parameters{
        {"hnsw", {{"ef_search", ef_search}}},
    };
    std::string str_parameters = parameters.dump();
    vsag::BitsetPtr zeros = vsag::Bitset::Make();
    for (int i = 0; i < max_elements; i++) {
        int64_t k = 2;
        future_results.push_back(
            pool.enqueue([&index, &ids, dim, &data, i, k, &str_parameters, &zeros]() -> float {
                auto query = vsag::Dataset::Make();
                query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
                return query_knn(index, query, *(ids.get() + i), k, str_parameters, zeros);
            }));
    }
    for (int i = 0; i < future_results.size(); ++i) {
        correct += future_results[i].get();
    }

    float recall = correct / max_elements;
    std::cout << index->GetStats() << std::endl;
    REQUIRE(recall == 1);
}

TEST_CASE("multi-threading read-write test", "[ft][hnsw]") {
    // avoid too much slow task logs
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kWARN);

    int dim = 16;
    int max_elements = 5000;
    int max_degree = 16;
    int ef_construction = 200;
    int ef_search = 100;
    nlohmann::json hnsw_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"ef_search", ef_search},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump()).value();
    std::shared_ptr<int64_t[]> ids(new int64_t[max_elements]);
    std::shared_ptr<float[]> data(new float[dim * max_elements]);

    ThreadPool pool(16);

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);
    nlohmann::json parameters{
        {"hnsw", {{"ef_search", ef_search}}},
    };
    std::string str_parameters = parameters.dump();

    std::vector<std::future<uint64_t>> insert_results;
    std::vector<std::future<bool>> search_results;
    for (int64_t i = 0; i < max_elements; ++i) {
        // insert
        insert_results.push_back(pool.enqueue([&ids, &data, &index, dim, i]() -> uint64_t {
            auto dataset = vsag::Dataset::Make();
            dataset->Dim(dim)
                ->NumElements(1)
                ->Ids(ids.get() + i)
                ->Float32Vectors(data.get() + i * dim)
                ->Owner(false);
            auto add_res = index->Add(dataset);
            REQUIRE(add_res.has_value());
            return add_res.value().size();
        }));

        // search
        search_results.push_back(
            pool.enqueue([&index, &ids, dim, &data, i, &str_parameters]() -> bool {
                auto query = vsag::Dataset::Make();
                query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
                auto result = index->KnnSearch(query, 2, str_parameters);
                return result.has_value();
            }));
    }
    for (auto& res : insert_results) {
        REQUIRE(res.get() == 0);
    }

    for (int i = 0; i < search_results.size(); ++i) {
        REQUIRE(search_results[i].get());
    }
}
