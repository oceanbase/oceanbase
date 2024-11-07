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

#pragma once

#include <cstdint>
#include <vector>

#include "vsag/index.h"

namespace vsag {

class SimpleFlat : public Index {
public:
    explicit SimpleFlat(const std::string& metric_type, int64_t dim);

    tl::expected<std::vector<int64_t>, Error>
    Build(const DatasetPtr& base) override;

    virtual tl::expected<std::vector<int64_t>, Error>
    Add(const DatasetPtr& base) override;

    tl::expected<bool, Error>
    Remove(int64_t id) override;

    tl::expected<DatasetPtr, Error>
    KnnSearch(const DatasetPtr& query,
              int64_t k,
              const std::string& parameters,
              BitsetPtr invalid = nullptr) const override;

    tl::expected<DatasetPtr, Error>
    KnnSearch(const DatasetPtr& query,
              int64_t k,
              const std::string& parameters,
              const std::function<bool(int64_t)>& filter) const override;

    tl::expected<DatasetPtr, Error>
    RangeSearch(const DatasetPtr& query,
                float radius,
                const std::string& parameters,
                int64_t limited_size = -1) const override;

    tl::expected<DatasetPtr, Error>
    RangeSearch(const DatasetPtr& query,
                float radius,
                const std::string& parameters,
                BitsetPtr invalid,
                int64_t limited_size = -1) const override;

    tl::expected<DatasetPtr, Error>
    RangeSearch(const DatasetPtr& query,
                float radius,
                const std::string& parameters,
                const std::function<bool(int64_t)>& filter,
                int64_t limited_size = -1) const override;

public:
    tl::expected<BinarySet, Error>
    Serialize() const override;

    tl::expected<void, Error>
    Deserialize(const BinarySet& binary_set) override;

    tl::expected<void, Error>
    Deserialize(const ReaderSet& reader_set) override;

    int64_t
    GetMemoryUsage() const override {
        size_t ids_size = num_elements_ * sizeof(int64_t);
        size_t vector_size = num_elements_ * dim_ * sizeof(float);
        return ids_size + vector_size;
    }

    int64_t
    GetNumElements() const override {
        return num_elements_;
    }

    std::string
    GetStats() const override;

private:
    using rs = std::pair<float, int64_t>;

    std::vector<rs>
    knn_search(const float* query, int64_t k, const std::function<bool(int64_t)>& filter) const;

    std::vector<rs>
    range_search(const float* query,
                 float radius,
                 const std::function<bool(int64_t)>& filter) const;

    static float
    l2(const float* v1, const float* v2, int64_t dim);

    static float
    ip(const float* v1, const float* v2, int64_t dim);

    static float
    cosine(const float* v1, const float* v2, int64_t dim);

private:
    const std::string metric_type_;
    int64_t num_elements_ = 0;
    int64_t dim_ = 0;
    std::vector<int64_t> ids_;
    std::vector<float> data_;
};

}  // namespace vsag
