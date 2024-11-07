
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
#include <catch2/generators/catch_generators.hpp>
#include <fstream>
#include <iostream>
#include <limits>
#include <nlohmann/json.hpp>
#include <numeric>
#include <random>

#include "vsag/errors.h"
#include "vsag/vsag.h"

using namespace std;

template <typename T>
static void
writeBinaryPOD(std::ostream& out, const T& podRef) {
    out.write((char*)&podRef, sizeof(T));
}

template <typename T>
static void
readBinaryPOD(std::istream& in, T& podRef) {
    in.read((char*)&podRef, sizeof(T));
}

const std::string tmp_dir = "/tmp/";

TEST_CASE("HNSW range search", "[ft][hnsw]") {
    spdlog::set_level(spdlog::level::debug);

    int dim = 71;
    int max_elements = 10000;
    int max_degree = 16;
    int ef_construction = 100;
    int ef_search = 100;
    // Initing index
    nlohmann::json hnsw_parameters{
        {"max_elements", max_elements},
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"ef_search", ef_search},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};

    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Generate random data
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_real_distribution<> distrib_real;
    std::uniform_int_distribution<int> seed_random;
    int seed = seed_random(rng);
    std::cout << "seed: " << seed << std::endl;
    rng.seed(seed);
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int64_t i = 0; i < max_elements; i++) {
        ids[i] = i;
    }
    for (int64_t i = 0; i < dim * max_elements; ++i) {
        data[i] = distrib_real(rng);
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    hnsw->Build(dataset);

    REQUIRE(hnsw->GetNumElements() == max_elements);

    float radius = 12.0f;
    float* query_data = new float[dim];
    for (int64_t i = 0; i < dim; ++i) {
        query_data[i] = distrib_real(rng);
    }
    auto query = vsag::Dataset::Make();
    query->Dim(dim)->NumElements(1)->Float32Vectors(query_data);
    nlohmann::json parameters{
        {"hnsw", {{"ef_search", ef_search}}},
    };
    auto result = hnsw->RangeSearch(query, radius, parameters.dump());
    REQUIRE(result.has_value());
    REQUIRE(result.value()->GetNumElements() == 1);

    auto expected = vsag::l2_and_filtering(dim, max_elements, data, query_data, radius);
    if (expected->Count() != result.value()->GetDim()) {
        std::cout << "not 100% recall: expect " << expected->Count() << " return "
                  << result.value()->GetDim() << std::endl;
    }

    // check no false recall
    for (int64_t i = 0; i < result.value()->GetDim(); ++i) {
        auto offset = result.value()->GetIds()[i];
        CHECK(expected->Test(offset));
    }

    // recall > 99%
    CHECK((expected->Count() - result.value()->GetDim()) * 100 < max_elements);
}

TEST_CASE("HNSW Filtering Test", "[ft][hnsw]") {
    spdlog::set_level(spdlog::level::debug);

    int dim = 17;
    int max_elements = 1000;
    int max_degree = 16;
    int ef_construction = 100;
    int ef_search = 1000;
    // Initing index
    nlohmann::json hnsw_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"ef_search", ef_search},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};

    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Generate random data
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_real_distribution<> distrib_real;
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int64_t i = 0; i < max_elements; i++) {
        ids[i] = max_elements - i - 1;
    }
    for (int64_t i = 0; i < dim * max_elements; ++i) {
        data[i] = distrib_real(rng);
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    hnsw->Build(dataset);

    REQUIRE(hnsw->GetNumElements() == max_elements);

    // Query the elements for themselves and measure recall 1@1
    float correct_knn = 0.0f;
    float recall_knn = 0.0f;
    float correct_range = 0.0f;
    float recall_range = 0.0f;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"hnsw", {{"ef_search", ef_search}}},
        };
        float radius = 9.87f;
        int64_t k = 10;

        vsag::BitsetPtr filter = vsag::Bitset::Random(max_elements);
        int64_t num_deleted = filter->Count();

        if (auto result = hnsw->RangeSearch(query, radius, parameters.dump(), filter);
            result.has_value()) {
            REQUIRE(result.value()->GetDim() == max_elements - num_deleted);
            for (int64_t j = 0; j < result.value()->GetDim(); ++j) {
                // deleted ids NOT in result
                REQUIRE(filter->Test(result.value()->GetIds()[j]) == false);
            }
        } else {
            std::cerr << "failed to range search on index: internalError" << std::endl;
            exit(-1);
        }

        if (auto result = hnsw->KnnSearch(query, k, parameters.dump(), filter);
            result.has_value()) {
            REQUIRE(result.has_value());
            for (int64_t j = 0; j < result.value()->GetDim(); ++j) {
                // deleted ids NOT in result
                REQUIRE(filter->Test(result.value()->GetIds()[j]) == false);
            }
        } else {
            std::cerr << "failed to knn search on index: internalError" << std::endl;
            exit(-1);
        }

        vsag::BitsetPtr ones = vsag::Bitset::Make();
        for (int64_t i = 0; i < max_elements; ++i) {
            ones->Set(i, true);
        }
        if (auto result = hnsw->RangeSearch(query, radius, parameters.dump(), ones);
            result.has_value()) {
            REQUIRE(result.value()->GetDim() == 0);
            REQUIRE(result.value()->GetDistances() == nullptr);
            REQUIRE(result.value()->GetIds() == nullptr);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to range search on index: internalError" << std::endl;
            exit(-1);
        }

        if (auto result = hnsw->KnnSearch(query, k, parameters.dump(), ones); result.has_value()) {
            REQUIRE(result.value()->GetDim() == 0);
            REQUIRE(result.value()->GetDistances() == nullptr);
            REQUIRE(result.value()->GetIds() == nullptr);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to knn search on index: internalError" << std::endl;
            exit(-1);
        }

        vsag::BitsetPtr zeros = vsag::Bitset::Make();

        if (auto result = hnsw->KnnSearch(query, k, parameters.dump(), zeros); result.has_value()) {
            correct_knn += vsag::knn_search_recall(data,
                                                   ids,
                                                   max_elements,
                                                   data + i * dim,
                                                   dim,
                                                   result.value()->GetIds(),
                                                   result.value()->GetDim());
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to knn search on index: internalError" << std::endl;
            exit(-1);
        }

        if (auto result = hnsw->RangeSearch(query, radius, parameters.dump(), zeros);
            result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == ids[i]) {
                    correct_range++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to range search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    recall_knn = correct_knn / max_elements;
    recall_range = correct_range / max_elements;

    REQUIRE(recall_range == 1);
    REQUIRE(recall_knn == 1);
}

TEST_CASE("HNSW small dimension", "[ft][hnsw]") {
    spdlog::set_level(spdlog::level::debug);

    int dim = 3;
    int max_elements = 1000;
    int max_degree = 24;
    int ef_construction = 100;
    int ef_search = 100;
    // Initing index
    nlohmann::json hnsw_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"ef_search", ef_search},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};

    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int i = 0; i < max_elements; i++) {
        ids[i] = i;
    }
    for (int i = 0; i < dim * max_elements; i++) {
        data[i] = distrib_real(rng);
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    hnsw->Add(dataset);
    return;

    // Query the elements for themselves and measure recall 1@1
    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"hnsw", {{"ef_search", ef_search}}},
        };
        int64_t k = 10;
        if (auto result = hnsw->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetIds()[0] == i) {
                correct++;
            }
            REQUIRE(result.value()->GetDim() == k);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    float recall = correct / max_elements;

    REQUIRE(recall == 1);
}

TEST_CASE("HNSW Random Id", "[ft][hnsw]") {
    spdlog::set_level(spdlog::level::debug);

    int dim = 128;
    int max_elements = 1000;
    int max_degree = 64;
    int ef_construction = 200;
    int ef_search = 200;
    // Initing index
    nlohmann::json hnsw_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"ef_search", ef_search},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};

    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Generate random data
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib_real;
    std::uniform_int_distribution<> ids_random(0, max_elements - 1);
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int i = 0; i < max_elements; i++) {
        ids[i] = ids_random(rng);
        if (i == 1 || i == 2) {
            ids[i] = std::numeric_limits<int64_t>::max();
        } else if (i == 3 || i == 4) {
            ids[i] = std::numeric_limits<int64_t>::min();
        } else if (i == 5 || i == 6) {
            ids[i] = 1;
        } else if (i == 7 || i == 8) {
            ids[i] = -1;
        } else if (i == 9 || i == 10) {
            ids[i] = 0;
        }
    }
    for (int i = 0; i < dim * max_elements; i++) {
        data[i] = distrib_real(rng);
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    auto failed_ids = hnsw->Build(dataset);

    float rate = hnsw->GetNumElements() / (float)max_elements;
    // 1 - 1 / e
    REQUIRE((rate > 0.60 && rate < 0.65));

    REQUIRE(failed_ids->size() + hnsw->GetNumElements() == max_elements);

    // Query the elements for themselves and measure recall 1@1
    float correct = 0;
    std::set<int64_t> unique_ids;
    for (int i = 0; i < max_elements; i++) {
        if (unique_ids.find(ids[i]) != unique_ids.end()) {
            continue;
        }
        unique_ids.insert(ids[i]);
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"hnsw", {{"ef_search", ef_search}}},
        };
        int64_t k = 10;
        if (auto result = hnsw->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetIds()[0] == ids[i]) {
                correct++;
            }
            REQUIRE(result.value()->GetDim() == k);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    float recall = correct / hnsw->GetNumElements();
    REQUIRE(recall == 1);
}

TEST_CASE("pq infer knn search time recall", "[ft][hnsw]") {
    spdlog::set_level(spdlog::level::debug);

    int dim = 128;
    int max_elements = 1000;
    int max_degree = 64;
    int ef_construction = 200;
    int ef_search = 200;
    // Initing index
    nlohmann::json hnsw_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"use_static", true},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};

    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int i = 0; i < max_elements; i++) {
        ids[i] = i;
    }
    for (int i = 0; i < dim * max_elements; i++) {
        data[i] = distrib_real(rng);
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    hnsw->Build(dataset);

    // Query the elements for themselves and measure recall 1@1
    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"hnsw", {{"ef_search", ef_search}}},
        };
        int64_t k = 10;
        if (auto result = hnsw->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetIds()[0] == i) {
                correct++;
            }
            REQUIRE(result.value()->GetDim() == k);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    float recall = correct / max_elements;

    REQUIRE(recall == 1);
}

TEST_CASE("hnsw serialize", "[ft][hnsw]") {
    spdlog::set_level(spdlog::level::debug);

    int dim = 128;
    int max_elements = 1000;
    int max_degree = 64;
    int ef_construction = 200;
    int ef_search = 200;
    auto metric_type = GENERATE("cosine", "l2");
    auto use_static = GENERATE(true, false);
    // Initing index
    nlohmann::json hnsw_parameters{{"max_degree", max_degree},
                                   {"ef_construction", ef_construction},
                                   {"use_static", use_static},
                                   {"use_conjugate_graph", true}};
    nlohmann::json index_parameters{{"dtype", "float32"},
                                    {"metric_type", metric_type},
                                    {"dim", dim},
                                    {"hnsw", hnsw_parameters}};

    if (metric_type == std::string("cosine") && use_static) {
        return;  // static hnsw only support the metric type of l2.
    }
    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int i = 0; i < max_elements; i++) {
        ids[i] = i;
    }
    for (int i = 0; i < dim * max_elements; i++) {
        data[i] = distrib_real(rng);
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    hnsw->Build(dataset);

    // Serialize(single-file)
    {
        if (auto bs = hnsw->Serialize(); bs.has_value()) {
            hnsw = nullptr;
            auto keys = bs->GetKeys();
            std::vector<uint64_t> offsets;

            std::ofstream file(tmp_dir + "hnsw.index", std::ios::binary);
            uint64_t offset = 0;
            for (auto key : keys) {
                // [len][data...][len][data...]...
                vsag::Binary b = bs->Get(key);
                writeBinaryPOD(file, b.size);
                file.write((const char*)b.data.get(), b.size);
                offsets.push_back(offset);
                offset += sizeof(b.size) + b.size;
            }
            // footer
            for (uint64_t i = 0; i < keys.size(); ++i) {
                // [len][key...][offset][len][key...][offset]...
                const auto& key = keys[i];
                int64_t len = key.length();
                writeBinaryPOD(file, len);
                file.write(key.c_str(), key.length());
                writeBinaryPOD(file, offsets[i]);
            }
            // [num_keys][footer_offset]$
            writeBinaryPOD(file, keys.size());
            writeBinaryPOD(file, offset);
            file.close();
        } else if (bs.error().type == vsag::ErrorType::NO_ENOUGH_MEMORY) {
            std::cerr << "no enough memory to serialize index" << std::endl;
        }
    }

    // Deserialize(binaryset)
    {
        std::ifstream file(tmp_dir + "hnsw.index", std::ios::in);
        file.seekg(-sizeof(uint64_t) * 2, std::ios::end);
        uint64_t num_keys, footer_offset;
        readBinaryPOD(file, num_keys);
        readBinaryPOD(file, footer_offset);
        // std::cout << "num_keys: " << num_keys << std::endl;
        // std::cout << "footer_offset: " << footer_offset << std::endl;
        file.seekg(footer_offset, std::ios::beg);

        std::vector<std::string> keys;
        std::vector<uint64_t> offsets;
        for (uint64_t i = 0; i < num_keys; ++i) {
            int64_t key_len;
            readBinaryPOD(file, key_len);
            // std::cout << "key_len: " << key_len << std::endl;
            char key_buf[key_len + 1];
            memset(key_buf, 0, key_len + 1);
            file.read(key_buf, key_len);
            // std::cout << "key: " << key_buf << std::endl;
            keys.push_back(key_buf);

            uint64_t offset;
            readBinaryPOD(file, offset);
            // std::cout << "offset: " << offset << std::endl;
            offsets.push_back(offset);
        }

        vsag::BinarySet bs;
        for (uint64_t i = 0; i < num_keys; ++i) {
            file.seekg(offsets[i], std::ios::beg);
            vsag::Binary b;
            readBinaryPOD(file, b.size);
            // std::cout << "len: " << b.size << std::endl;
            b.data.reset(new int8_t[b.size]);
            file.read((char*)b.data.get(), b.size);
            bs.Set(keys[i], b);
        }

        if (auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
            index.has_value()) {
            hnsw = index.value();
        } else {
            std::cout << "Build HNSW Error" << std::endl;
            return;
        }
        hnsw->Deserialize(bs);
    }

    // Deserialize(readerset)
    {
        std::ifstream file(tmp_dir + "hnsw.index", std::ios::in);
        file.seekg(-sizeof(uint64_t) * 2, std::ios::end);
        uint64_t num_keys, footer_offset;
        readBinaryPOD(file, num_keys);
        readBinaryPOD(file, footer_offset);
        // std::cout << "num_keys: " << num_keys << std::endl;
        // std::cout << "footer_offset: " << footer_offset << std::endl;
        file.seekg(footer_offset, std::ios::beg);

        std::vector<std::string> keys;
        std::vector<uint64_t> offsets;
        for (uint64_t i = 0; i < num_keys; ++i) {
            int64_t key_len;
            readBinaryPOD(file, key_len);
            // std::cout << "key_len: " << key_len << std::endl;
            char key_buf[key_len + 1];
            memset(key_buf, 0, key_len + 1);
            file.read(key_buf, key_len);
            // std::cout << "key: " << key_buf << std::endl;
            keys.push_back(key_buf);

            uint64_t offset;
            readBinaryPOD(file, offset);
            // std::cout << "offset: " << offset << std::endl;
            offsets.push_back(offset);
        }

        vsag::ReaderSet rs;
        for (uint64_t i = 0; i < num_keys; ++i) {
            int64_t size = 0;
            if (i + 1 == num_keys) {
                size = footer_offset;
            } else {
                size = offsets[i + 1];
            }
            size -= (offsets[i] + sizeof(uint64_t));
            auto file_reader = vsag::Factory::CreateLocalFileReader(
                tmp_dir + "hnsw.index", offsets[i] + sizeof(uint64_t), size);
            rs.Set(keys[i], file_reader);
        }

        if (auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump());
            index.has_value()) {
            hnsw = index.value();
        } else {
            std::cout << "Build HNSW Error" << std::endl;
            return;
        }
        hnsw->Deserialize(rs);
    }

    // Query the elements for themselves and measure recall 1@10
    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"hnsw", {{"ef_search", ef_search}}},
        };
        int64_t k = 10;
        if (auto result = hnsw->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            correct += vsag::knn_search_recall(data,
                                               ids,
                                               max_elements,
                                               data + i * dim,
                                               dim,
                                               result.value()->GetIds(),
                                               result.value()->GetDim());
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform search on index" << std::endl;
        }
    }
    float recall = correct / max_elements;

    REQUIRE(recall == 1);
}
