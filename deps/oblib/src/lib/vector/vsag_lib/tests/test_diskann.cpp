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
#include <catch2/generators/catch_generators.hpp>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

#include "vsag/errors.h"
#include "vsag/vsag.h"

const std::string tmp_dir = "/tmp/";

TEST_CASE("DiskAnn Float Recall", "[ft][diskann]") {
    int dim = 128;            // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 8;  // pq_dims represents the dimensionality of the compressed vector.
    std::string disk_layout_file = "index.out";
    // Initing index
    nlohmann::json diskann_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"pq_sample_rate", pq_sample_rate},
        {"pq_dims", pq_dims},
        {"use_reference", false},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };
    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    diskann->Build(dataset);

    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
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
    float recall = correct / max_elements;
    std::cout << "Stard Recall: " << recall << std::endl;

    REQUIRE(recall > 0.85);
    // Serialize
    {
        auto bs = diskann->Serialize();
        REQUIRE(bs.has_value());
        diskann = nullptr;

        vsag::Binary pq_b = bs->Get(vsag::DISKANN_PQ);
        std::ofstream pq(tmp_dir + "diskann_pq.index", std::ios::binary);
        pq.write((const char*)pq_b.data.get(), pq_b.size);
        pq.close();

        vsag::Binary compressed_vector_b = bs->Get(vsag::DISKANN_COMPRESSED_VECTOR);
        std::ofstream compressed(tmp_dir + "diskann_compressed_vector.index", std::ios::binary);
        compressed.write((const char*)compressed_vector_b.data.get(), compressed_vector_b.size);
        compressed.close();

        vsag::Binary tag_b = bs->Get(vsag::DISKANN_TAG_FILE);
        std::ofstream tag(tmp_dir + "diskann_tag.index", std::ios::binary);
        tag.write((const char*)tag_b.data.get(), tag_b.size);
        tag.close();

        vsag::Binary layout_file_b = bs->Get(vsag::DISKANN_LAYOUT_FILE);
        std::ofstream layout(tmp_dir + disk_layout_file, std::ios::binary);
        layout.write((const char*)layout_file_b.data.get(), layout_file_b.size);
        layout.close();
    }

    size_t pq_len = 0;
    size_t compressed_len = 0;
    size_t tag_len = 0;
    size_t disk_layout_len = 0;

    // Deserialize
    {
        vsag::BinarySet bs;

        std::ifstream pq(tmp_dir + "diskann_pq.index", std::ios::binary);
        pq.seekg(0, std::ios::end);
        size_t size = pq.tellg();
        pq.seekg(0, std::ios::beg);
        std::shared_ptr<int8_t[]> buff(new int8_t[size]);
        pq.read(reinterpret_cast<char*>(buff.get()), size);
        pq_len = size;
        vsag::Binary pq_b{
            .data = buff,
            .size = size,
        };
        bs.Set(vsag::DISKANN_PQ, pq_b);

        std::ifstream compressed(tmp_dir + "diskann_compressed_vector.index", std::ios::binary);
        compressed.seekg(0, std::ios::end);
        size = compressed.tellg();
        compressed.seekg(0, std::ios::beg);
        buff.reset(new int8_t[size]);
        compressed.read(reinterpret_cast<char*>(buff.get()), size);
        compressed_len = size;
        vsag::Binary compressed_vector_b{
            .data = buff,
            .size = size,
        };
        bs.Set(vsag::DISKANN_COMPRESSED_VECTOR, compressed_vector_b);

        std::ifstream tag(tmp_dir + "diskann_tag.index", std::ios::binary);
        tag.seekg(0, std::ios::end);
        size = tag.tellg();
        tag.seekg(0, std::ios::beg);
        buff.reset(new int8_t[size]);
        tag.read(reinterpret_cast<char*>(buff.get()), size);
        tag_len = size;
        vsag::Binary tag_b{
            .data = buff,
            .size = size,
        };
        bs.Set(vsag::DISKANN_TAG_FILE, tag_b);

        std::ifstream disk_layout(tmp_dir + disk_layout_file, std::ios::binary);
        disk_layout.seekg(0, std::ios::end);
        size = disk_layout.tellg();
        disk_layout.seekg(0, std::ios::beg);
        buff.reset(new int8_t[size]);
        disk_layout.read(reinterpret_cast<char*>(buff.get()), size);
        disk_layout_len = size;
        vsag::Binary disk_layout_b{
            .data = buff,
            .size = size,
        };
        bs.Set(vsag::DISKANN_LAYOUT_FILE, disk_layout_b);

        diskann = nullptr;
        index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
        REQUIRE(index.has_value());
        diskann = index.value();

        diskann->Deserialize(bs);
    }
    // Query the elements for themselves and measure recall 1@2
    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetIds()[0] == i) {
                    correct++;
                }
            } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
                std::cerr << "failed to search on index: internalError" << std::endl;
                exit(-1);
            }
        }
    }
    recall = correct / max_elements;
    std::cout << "BS Recall: " << recall << std::endl;
    REQUIRE(recall > 0.85);

    //     Deserialize
    {
        vsag::ReaderSet rs;
        auto pq_reader =
            vsag::Factory::CreateLocalFileReader(tmp_dir + "diskann_pq.index", 0, pq_len);
        auto compressed_vector_reader = vsag::Factory::CreateLocalFileReader(
            tmp_dir + "diskann_compressed_vector.index", 0, compressed_len);
        auto tag_reader =
            vsag::Factory::CreateLocalFileReader(tmp_dir + "diskann_tag.index", 0, tag_len);
        auto disk_layout_reader =
            vsag::Factory::CreateLocalFileReader(tmp_dir + disk_layout_file, 0, disk_layout_len);
        rs.Set(vsag::DISKANN_PQ, pq_reader);
        rs.Set(vsag::DISKANN_COMPRESSED_VECTOR, compressed_vector_reader);
        rs.Set(vsag::DISKANN_LAYOUT_FILE, disk_layout_reader);
        rs.Set(vsag::DISKANN_TAG_FILE, tag_reader);

        diskann = nullptr;
        index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
        REQUIRE(index.has_value());
        diskann = index.value();

        diskann->Deserialize(rs);
    }

    // Query the elements for themselves and measure recall 1@2
    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
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
    recall = correct / max_elements;
    std::cout << "RS Recall: " << recall << std::endl;
    REQUIRE(recall > 0.85);
}

TEST_CASE("DiskAnn IP Search", "[ft][diskann]") {
    int dim = 128;            // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 8;  // pq_dims represents the dimensionality of the compressed vector.
    std::string disk_layout_file = "index.out";
    // Initing index
    nlohmann::json diskann_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"pq_sample_rate", pq_sample_rate},
        {"pq_dims", pq_dims},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "ip"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    diskann->Build(dataset);

    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
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
    float recall = correct / max_elements;
    std::cout << "Stard Recall: " << recall << std::endl;

    REQUIRE(recall > 0.70);
}

/* FIXME: segmentation fault on some platform
TEST_CASE("DiskAnn OPQ", "[ft][diskann]") {
    int dim = 128;            // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 8;  // pq_dims represents the dimensionality of the compressed vector.
    std::string disk_layout_file = "index.out";
    // Initing index
    nlohmann::json diskann_parameters{{"max_degree", max_degree},
                                      {"ef_construction", ef_construction},
                                      {"pq_sample_rate", pq_sample_rate},
                                      {"pq_dims", pq_dims},
                                      {"use_opq", true}};
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    diskann->Build(dataset);

    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetIds()[0] == i) {
                    correct++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internal error" << std::endl;
            exit(-1);
        }
    }
    float recall = correct / max_elements;
    std::cout << "Stard Recall: " << recall << std::endl;

    REQUIRE(recall > 0.99);
}
*/

TEST_CASE("DiskAnn Range Query", "[ft][diskann]") {
    int dim = 256;            // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 32;  // pq_dims represents the dimensionality of the compressed vector.
    float threshold = 8.0;
    // Initing index
    nlohmann::json diskann_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"pq_sample_rate", pq_sample_rate},
        {"pq_dims", pq_dims},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    diskann->Build(dataset);

    float correct = 0;
    float true_result = 0;
    float return_result = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        auto range_result =
            vsag::l2_and_filtering(dim, max_elements, data, data + i * dim, threshold);
        if (auto result = diskann->RangeSearch(query, threshold, parameters.dump());
            result.has_value()) {
            if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == i) {
                correct++;
            }
            for (int j = 0; j < result.value()->GetDim(); ++j) {
                REQUIRE(range_result->Test(result.value()->GetIds()[j]));
            }
            true_result += range_result->Count();
            return_result += result.value()->GetDim();
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    float recall = correct / max_elements;
    std::cout << "Stard Recall: " << recall << std::endl;

    REQUIRE(recall >= 0.99);

    REQUIRE((true_result - return_result) / true_result < 0.01);
}

TEST_CASE("DiskAnn Preload Graph", "[ft][diskann]") {
    int dim = 65;             // Dimension of the elements
    int max_elements = 2000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 9;  // pq_dims represents the dimensionality of the compressed vector.
    int64_t k = GENERATE(2, 700);
    float threshold = 8.0;
    // Initing index
    nlohmann::json diskann_parameters{{"max_degree", max_degree},
                                      {"ef_construction", ef_construction},
                                      {"pq_sample_rate", pq_sample_rate},
                                      {"pq_dims", pq_dims},
                                      {"use_reference", false},
                                      {"use_pq_search", true}};
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int i = 0; i < max_elements; i++) ids[i] = i;
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    diskann->Build(dataset);

    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == i) {
                    correct++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    float recall = correct / max_elements;

    REQUIRE(recall >= 0.99);

    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{{"diskann",
                                   {{"ef_search", ef_search},
                                    {"beam_search", 4},
                                    {"io_limit", 200},
                                    {"use_reorder", true}}}};
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == i) {
                    correct++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    recall = correct / max_elements;

    REQUIRE(recall >= 0.99);

    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{{"diskann",
                                   {{"ef_search", ef_search},
                                    {"beam_search", 4},
                                    {"io_limit", 200},
                                    {"use_reorder", true}}}};
        float threshold = 0.1;
        if (auto result = diskann->RangeSearch(query, threshold, parameters.dump());
            result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == i) {
                    correct++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    recall = correct / max_elements;

    REQUIRE(recall >= 0.99);
}

TEST_CASE("DiskAnn Filter Test", "[ft][diskann]") {
    int dim = 65;             // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int label_num = 100;      // Total number of labels
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 9;  // pq_dims represents the dimensionality of the compressed vector.
    float threshold = 8.0;
    int64_t k = 10;
    // Initing index
    nlohmann::json diskann_parameters{{"max_degree", max_degree},
                                      {"ef_construction", ef_construction},
                                      {"pq_sample_rate", pq_sample_rate},
                                      {"pq_dims", pq_dims},
                                      {"use_pq_search", true}};
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    std::uniform_int_distribution<> ids_random(0, max_elements - 1);

    for (int64_t i = 0; i < max_elements; i++) {
        int64_t array_id = i / label_num;
        int64_t label = i % label_num;
        ids[i] = label | (array_id << 32);
    }
    for (int i = 0; i < dim * max_elements; i++) data[i] = distrib_real(rng);

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    diskann->Build(dataset);

    REQUIRE(max_elements == diskann->GetNumElements());

    float correct_knn = 0.0f;
    float recall_knn = 0.0f;
    float correct_range = 0.0f;
    float recall_range = 0.0f;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

        vsag::BitsetPtr invalid = vsag::Bitset::Random(label_num);
        int64_t num_deleted = invalid->Count();
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        if (auto result = diskann->KnnSearch(query, k, parameters.dump(), invalid);
            result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                if (result.value()->GetDim() != 0 && result.value()->GetNumElements() == 1) {
                    REQUIRE(invalid->Test(ids[i] & 0xFFFFFFFFLL) ^
                            (ids[i] == result.value()->GetIds()[0]));
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to knn search on index: internalError" << std::endl;
            exit(-1);
        }

        if (auto result = diskann->RangeSearch(query, threshold, parameters.dump(), invalid);
            result.has_value()) {
            if (result.value()->GetDim() != 0 && result.value()->GetNumElements() == 1) {
                REQUIRE(invalid->Test(ids[i] & 0xFFFFFFFFLL) ^
                        (ids[i] == result.value()->GetIds()[0]));
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to range search on index: internalError" << std::endl;
            exit(-1);
        }
        vsag::BitsetPtr ones = vsag::Bitset::Make();
        for (int64_t i = 0; i < label_num; ++i) {
            ones->Set(i, true);
        }
        if (auto result = diskann->RangeSearch(query, threshold, parameters.dump(), ones);
            result.has_value()) {
            REQUIRE(result.value()->GetDim() == 0);
            REQUIRE(result.value()->GetDistances() == nullptr);
            REQUIRE(result.value()->GetIds() == nullptr);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to range search on index: internalError" << std::endl;
            exit(-1);
        }

        if (auto result = diskann->KnnSearch(query, k, parameters.dump(), ones);
            result.has_value()) {
            REQUIRE(result.value()->GetDim() == 0);
            REQUIRE(result.value()->GetDistances() == nullptr);
            REQUIRE(result.value()->GetIds() == nullptr);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to range search on index: internalError" << std::endl;
            exit(-1);
        }

        vsag::BitsetPtr zeros = vsag::Bitset::Make();

        if (auto result = diskann->KnnSearch(query, k, parameters.dump(), zeros);
            result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == ids[i]) {
                    correct_knn++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to knn search on index: internalError" << std::endl;
            exit(-1);
        }

        if (auto result = diskann->RangeSearch(query, threshold, parameters.dump(), zeros);
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

TEST_CASE("DiskAnn Random Id", "[ft][diskann]") {
    int dim = 65;             // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 9;  // pq_dims represents the dimensionality of the compressed vector.
    float threshold = 8.0;
    // Initing index
    nlohmann::json diskann_parameters{{"max_degree", max_degree},
                                      {"ef_construction", ef_construction},
                                      {"pq_sample_rate", pq_sample_rate},
                                      {"pq_dims", pq_dims},
                                      {"use_pq_search", true}};
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    // Generate random data
    std::mt19937 rng;
    std::uniform_real_distribution<> distrib_real;
    std::uniform_int_distribution<> ids_rand(0, max_elements - 1);
    for (int i = 0; i < max_elements; i++) {
        ids[i] = ids_rand(rng);
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
    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    auto failed_ids = diskann->Build(dataset);

    float rate = diskann->GetNumElements() / (float)max_elements;
    std::cout << rate << std::endl;
    // 1 - 1 / e
    REQUIRE((rate > 0.60 && rate < 0.65));
    REQUIRE(failed_ids->size() + diskann->GetNumElements() == max_elements);

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
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            if (result.value()->GetNumElements() == 1) {
                REQUIRE(!std::isinf(result.value()->GetDistances()[0]));
                if (result.value()->GetDim() != 0 && result.value()->GetIds()[0] == ids[i]) {
                    correct++;
                }
            }
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to search on index: internalError" << std::endl;
            exit(-1);
        }
    }
    float recall = correct / diskann->GetNumElements();
    REQUIRE(recall >= 0.99);
}

TEST_CASE("DiskANN small dimension", "[ft][diskann]") {
    int dim = 3;
    int max_elements = 1000;
    int max_degree = 16;
    int ef_construction = 100;
    int ef_search = 100;
    float pq_sample_rate = 0.5;
    int pq_dims = 8;
    // Initing index
    nlohmann::json diskann_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"pq_sample_rate", pq_sample_rate},
        {"pq_dims", pq_dims},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };
    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(index.has_value());
    diskann = index.value();

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

    // Build index
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    diskann->Build(dataset);

    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", 200}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
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
    float recall = correct / max_elements;
    std::cout << "Stard Recall: " << recall << std::endl;

    REQUIRE(recall > 0.85);
}

TEST_CASE("split building process", "[ft][diskann]") {
    int64_t dim = 128;
    int64_t ef_construction = 100;
    int64_t max_degree = 12;
    float pq_sample_rate = 1.0f;
    size_t pq_dims = 16;

    int64_t max_elements = 1000;
    // Initing index
    nlohmann::json diskann_parameters{
        {"max_degree", max_degree},
        {"ef_construction", ef_construction},
        {"pq_sample_rate", pq_sample_rate},
        {"pq_dims", pq_dims},
    };
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

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

    std::shared_ptr<vsag::Index> partial_index;
    auto result_index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
    REQUIRE(result_index.has_value());
    partial_index = result_index.value();

    // Build DiskANN index using a single object continuously.
    vsag::Index::Checkpoint checkpoint1;
    while (not checkpoint1.finish) {
        checkpoint1 = partial_index->ContinueBuild(dataset, checkpoint1.data).value();
    }

    // Build the DiskANN index in multi-object.
    vsag::Index::Checkpoint checkpoint2;
    while (not checkpoint2.finish) {
        auto result_index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
        REQUIRE(result_index.has_value());
        partial_index = result_index.value();
        checkpoint2 = partial_index->ContinueBuild(dataset, checkpoint2.data).value();
    }

    // Verify the consistency of recall rates between segmented index building and complete index building.
    nlohmann::json parameters{
        {"diskann", {{"ef_search", 10}, {"beam_search", 4}, {"io_limit", 20}}}};
    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
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
    float recall_partial = correct / max_elements;

    std::shared_ptr<vsag::Index> full_index;
    {
        auto result_index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
        REQUIRE(result_index.has_value());
        full_index = result_index.value();
        full_index->Build(dataset);
    }
    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
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
    float recall_full = correct / max_elements;
    std::cout << "Recall: " << recall_full << std::endl;
    REQUIRE(recall_full == recall_partial);
}
