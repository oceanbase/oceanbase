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

#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <random>

#include "local_file_reader.h"
#include "vsag/errors.h"
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
float_hnsw() {
    int dim = 128;             // Dimension of the elements
    int max_elements = 10000;  // Maximum number of elements, should be known beforehand
    int max_degree = 16;       // Tightly connected with internal dimensionality of the data
    // strongly affects the memory consumption
    int ef_construction = 200;  // Controls index search speed/build speed tradeoff
    int ef_search = 100;
    float threshold = 8.0;

    // Initing index
    // {
    //   "dim": 16,
    //   "dtype": "float32",
    //   "metric_type": "l2",
    //   "hnsw": {
    //     "max_degree": 16,
    //     "ef_construction": 200,
    //     "ef_search": 200,
    //     "max_elements": 1000
    //   }
    // }
    nlohmann::json hnsw_parameters{{"max_degree", max_degree},
                                   {"ef_construction", ef_construction},
                                   {"ef_search", ef_search},
                                   {"use_static", false}};
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
            ->NumElements(max_elements - 1)
            ->Ids(ids.get())
            ->Float32Vectors(data.get())
            ->Owner(false);
        if (const auto num = hnsw->Build(dataset); num.has_value()) {
            std::cout << "After Build(), Index constains: " << hnsw->GetNumElements() << std::endl;
        } else if (num.error().type == vsag::ErrorType::INTERNAL_ERROR) {
            std::cerr << "Failed to build index: internalError" << std::endl;
            exit(-1);
        }

        // Adding data after index built
        auto incremental = vsag::Dataset::Make();
        incremental->Dim(dim)
            ->NumElements(1)
            ->Ids(ids.get() + max_elements - 1)
            ->Float32Vectors(data.get() + (max_elements - 1) * dim)
            ->Owner(false);
        hnsw->Add(incremental);
        std::cout << "After Add(), Index constains: " << hnsw->GetNumElements() << std::endl;
    }

    // Query the elements for themselves and measure recall 1@1
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

    correct = 0;
    {
        for (int i = 0; i < max_elements; i++) {
            auto query = vsag::Dataset::Make();
            query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);

            nlohmann::json parameters{
                {"hnsw", {{"ef_search", ef_search}}},
            };
            if (auto result = hnsw->RangeSearch(query, threshold, parameters.dump());
                result.has_value()) {
                correct += vsag::range_search_recall(data.get(),
                                                     ids.get(),
                                                     max_elements,
                                                     data.get() + i * dim,
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
                  << "Memory Usage:" << hnsw->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
        std::cout << "Range Query Recall: " << recall << std::endl;
    }

    // Serialize(multi-file)
    {
        if (auto bs = hnsw->Serialize(); bs.has_value()) {
            hnsw = nullptr;
            auto keys = bs->GetKeys();
            for (auto key : keys) {
                vsag::Binary b = bs->Get(key);
                std::ofstream file(tmp_dir + "hnsw.index." + key, std::ios::binary);
                file.write((const char*)b.data.get(), b.size);
                file.close();
            }
            std::ofstream metafile(tmp_dir + "hnsw.index._meta", std::ios::out);
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
        std::ifstream metafile(tmp_dir + "hnsw.index._meta", std::ios::in);
        std::vector<std::string> keys;
        std::string line;
        while (std::getline(metafile, line)) {
            keys.push_back(line);
        }
        metafile.close();

        vsag::BinarySet bs;
        for (auto key : keys) {
            std::ifstream file(tmp_dir + "hnsw.index." + key, std::ios::in);
            file.seekg(0, std::ios::end);
            vsag::Binary b;
            b.size = file.tellg();
            b.data.reset(new int8_t[b.size]);
            file.seekg(0, std::ios::beg);
            file.read((char*)b.data.get(), b.size);
            bs.Set(key, b);
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

    // Query the elements for themselves and measure recall 1@10
    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
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

    // Deserialize(readerset)
    {
        std::ifstream metafile(tmp_dir + "hnsw.index._meta", std::ios::in);
        std::vector<std::string> keys;
        std::string line;
        while (std::getline(metafile, line)) {
            keys.push_back(line);
        }
        metafile.close();

        vsag::ReaderSet rs;
        for (auto key : keys) {
            std::ifstream file_(tmp_dir + "hnsw.index." + key);
            file_.seekg(0, std::ios::end);
            int64_t size = file_.tellg();
            file_.close();
            auto file_reader =
                vsag::Factory::CreateLocalFileReader(tmp_dir + "hnsw.index." + key, 0, size);
            rs.Set(key, file_reader);
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
    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
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
            std::cerr << "failed to perform search on index" << std::endl;
        }
    }
    recall = correct / max_elements;
    std::cout << std::fixed << std::setprecision(3)
              << "Memory Uasage:" << hnsw->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
    std::cout << "Recall: " << recall << std::endl;

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
    correct = 0;
    for (int i = 0; i < max_elements; i++) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(data.get() + i * dim)->Owner(false);
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
            std::cerr << "failed to perform search on index" << std::endl;
        }
    }
    recall = correct / max_elements;
    std::cout << std::fixed << std::setprecision(3)
              << "Memory Uasage:" << hnsw->GetMemoryUsage() / 1024.0 << " KB" << std::endl;
    std::cout << "Recall: " << recall << std::endl;
    std::cout << "statstics: " << hnsw->GetStats() << std::endl;
}

int
main() {
    std::cout << "version: " << vsag::version() << std::endl;
    float_hnsw();
    std::cout << "version: " << vsag::version() << std::endl;
    return 0;
}
