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

#include <chrono>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <thread>

#include "vsag/dataset.h"
#include "vsag/vsag.h"

const std::string tmp_dir = "/tmp/";

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

void
float_diskann() {
    int dim = 65;             // Dimension of the elements
    int max_elements = 1000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;      // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 200;
    int io_limit = 200;
    float threshold = 8.0;
    float pq_sample_rate =
        0.5;  // pq_sample_rate represents how much original data is selected during the training of pq compressed vectors.
    int pq_dims = 9;  // pq_dims represents the dimensionality of the compressed vector.
    // Initing index
    // {
    // 	"dim": 256,
    // 	"dtype": "float32",
    // 	"metric_type": "l2",
    // 	"diskann": {
    // 	    "ef_construction": 200,
    // 	    "max_degree": 16,
    // 	    "pq_dims": 32,
    // 	    "pq_sample_rate": 0.5
    // 	}
    // }
    nlohmann::json diskann_parameters{{"max_degree", max_degree},
                                      {"ef_construction", ef_construction},
                                      {"pq_sample_rate", pq_sample_rate},
                                      {"pq_dims", pq_dims},
                                      {"use_pq_search", true},
                                      {"use_async_io", true}};
    nlohmann::json index_parameters{
        {"dtype", "float32"},
        {"metric_type", "l2"},
        {"dim", dim},
        {"diskann", diskann_parameters},
    };

    std::shared_ptr<vsag::Index> diskann;
    if (auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
        index.has_value()) {
        diskann = index.value();
    } else {
        std::cout << "Build DiskANN Error" << std::endl;
        return;
    }

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
    if (const auto num = diskann->Build(dataset); num.has_value()) {
        std::cout << "After Build(), Index constains: " << diskann->GetNumElements() << std::endl;
    } else if (num.error().type == vsag::ErrorType::INTERNAL_ERROR) {
        std::cerr << "Failed to build index: internalError" << std::endl;
        exit(-1);
    }

    float correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        // {
        //  "diskann": {
        //    "ef_search": 200,
        //    "beam_search": 4,
        //    "io_limit": 200
        //  }
        // }
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 1}, {"io_limit", io_limit}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            correct += vsag::knn_search_recall(data,
                                               ids,
                                               max_elements,
                                               data + i * dim,
                                               dim,
                                               result.value()->GetIds(),
                                               result.value()->GetDim());
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    float recall = correct / max_elements;
    std::cout << std::fixed << std::setprecision(3)
              << "Memory Usage:" << diskann->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
    std::cout << "Stard Recall: " << recall << std::endl;

    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        // {
        //  "diskann": {
        //    "ef_search": 200,
        //    "beam_search": 4,
        //    "io_limit": 200
        //  }
        // }
        auto range_result =
            vsag::l2_and_filtering(dim, max_elements, data, data + i * dim, threshold);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", io_limit}}}};
        if (auto result = diskann->RangeSearch(query, threshold, parameters.dump());
            result.has_value()) {
            correct += vsag::range_search_recall(data,
                                                 ids,
                                                 max_elements,
                                                 data + i * dim,
                                                 dim,
                                                 result.value()->GetIds(),
                                                 result.value()->GetDim(),
                                                 threshold);
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    recall = correct / max_elements;
    std::cout << std::fixed << std::setprecision(3)
              << "Memory Usage:" << diskann->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
    std::cout << "Range Query Recall: " << recall << std::endl;

    std::cout << "============================================" << std::endl;
    std::cout << "Query Statistical Information:" << diskann->GetStats() << std::endl;
    std::cout << "============================================" << std::endl;

    // Serialize(multi-file)
    {
        if (auto bs = diskann->Serialize(); bs.has_value()) {
            diskann = nullptr;
            auto keys = bs->GetKeys();
            for (auto key : keys) {
                vsag::Binary b = bs->Get(key);
                std::ofstream file(tmp_dir + "diskann.index." + key, std::ios::binary);
                file.write((const char*)b.data.get(), b.size);
                file.close();
            }
            std::ofstream metafile(tmp_dir + "diskann.index._meta", std::ios::out);
            for (auto key : keys) {
                metafile << key << std::endl;
            }
            metafile.close();
        } else if (bs.error().type == vsag::ErrorType::NO_ENOUGH_MEMORY) {
            std::cerr << "no enough memory to serialize index" << std::endl;
        }
    }
    // Deserialize(binaryset)
    {
        std::ifstream metafile(tmp_dir + "diskann.index._meta", std::ios::in);
        std::vector<std::string> keys;
        std::string line;
        while (std::getline(metafile, line)) {
            keys.push_back(line);
        }
        metafile.close();

        vsag::BinarySet bs;
        for (auto key : keys) {
            std::ifstream file(tmp_dir + "diskann.index." + key, std::ios::in);
            file.seekg(0, std::ios::end);
            vsag::Binary b;
            b.size = file.tellg();
            b.data.reset(new int8_t[b.size]);
            file.seekg(0, std::ios::beg);
            file.read((char*)b.data.get(), b.size);
            bs.Set(key, b);
        }

        if (auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
            index.has_value()) {
            diskann = index.value();
        } else {
            std::cout << "Build DiskANN Error" << std::endl;
            return;
        }
        diskann->Deserialize(bs);
    }

    // Query the elements for themselves and measure recall 1@10
    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", io_limit}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            correct += vsag::knn_search_recall(data,
                                               ids,
                                               max_elements,
                                               data + i * dim,
                                               dim,
                                               result.value()->GetIds(),
                                               result.value()->GetDim());
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    recall = correct / max_elements;

    std::cout << std::fixed << std::setprecision(3)
              << "Memory Usage:" << diskann->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
    std::cout << "BS Recall: " << recall << std::endl;

    // Serialize(single-file)
    {
        if (auto bs = diskann->Serialize(); bs.has_value()) {
            diskann = nullptr;
            auto keys = bs->GetKeys();
            std::vector<uint64_t> offsets;

            std::ofstream file(tmp_dir + "diskann.index", std::ios::binary);
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
        std::ifstream file(tmp_dir + "diskann.index", std::ios::in);
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
        if (auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
            index.has_value()) {
            diskann = index.value();
        } else {
            std::cout << "Build DiskANN Error" << std::endl;
            return;
        }
        diskann->Deserialize(bs);
    }

    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{
            {"diskann", {{"ef_search", ef_search}, {"beam_search", 4}, {"io_limit", io_limit}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            correct += vsag::knn_search_recall(data,
                                               ids,
                                               max_elements,
                                               data + i * dim,
                                               dim,
                                               result.value()->GetIds(),
                                               result.value()->GetDim());
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    recall = correct / max_elements;

    std::cout << std::fixed << std::setprecision(3)
              << "Memory Usage:" << diskann->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
    std::cout << "Single File BS Recall: " << recall << std::endl;

    // Deserialize(readerset)
    {
        std::ifstream file(tmp_dir + "diskann.index", std::ios::in);
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
                tmp_dir + "diskann.index", offsets[i] + sizeof(uint64_t), size);
            rs.Set(keys[i], file_reader);
        }

        if (auto index = vsag::Factory::CreateIndex("diskann", index_parameters.dump());
            index.has_value()) {
            diskann = index.value();
        } else {
            std::cout << "Build DiskANN Error" << std::endl;
            return;
        }
        diskann->Deserialize(rs);
    }

    // Query the elements for themselves and measure recall 1@2

    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);
        nlohmann::json parameters{{"diskann",
                                   {{"ef_search", ef_search},
                                    {"beam_search", 4},
                                    {"io_limit", io_limit},
                                    {"use_reorder", true}}}};
        int64_t k = 2;
        if (auto result = diskann->KnnSearch(query, k, parameters.dump()); result.has_value()) {
            correct += vsag::knn_search_recall(data,
                                               ids,
                                               max_elements,
                                               data + i * dim,
                                               dim,
                                               result.value()->GetIds(),
                                               result.value()->GetDim());
        } else if (result.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "failed to perform knn search on index" << std::endl;
        }
    }
    recall = correct / max_elements;
    std::cout << std::fixed << std::setprecision(3)
              << "Memory Usage:" << diskann->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
    std::cout << "RS Recall: " << recall << std::endl;

    std::cout << "============================================" << std::endl;
    std::cout << "Query Statistical Information:" << diskann->GetStats() << std::endl;
    std::cout << "============================================" << std::endl;
}

int
main() {
    std::cout << "version: " << vsag::version() << std::endl;
    float_diskann();
    std::cout << "version: " << vsag::version() << std::endl;
    //
    //    uint32_t a, b;
    //    uint64_t c, d;
    //    std::ifstream in("/tmp/index.out");
    //    in.read((char *)&a, sizeof(uint32_t));
    //    in.read((char *)&b, sizeof(uint32_t));
    //    in.read((char *)&c, sizeof(uint64_t));
    //    in.read((char *)&d, sizeof(uint64_t));

    return 0;
}
