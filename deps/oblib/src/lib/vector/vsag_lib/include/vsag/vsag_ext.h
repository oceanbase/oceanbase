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

#include "vsag/allocator.h"
#include "vsag/vsag.h"

namespace vsag {
namespace ext {

class DatasetHandler {
    friend class IndexHandler;

public:
    static DatasetHandler*
    Make() {
        auto ret = new DatasetHandler();
        ret->dataset_ = Dataset::Make();
        return ret;
    }

    static DatasetHandler*
    Make(DatasetPtr dataset) {
        auto ret = new DatasetHandler();
        ret->dataset_ = dataset;
        return ret;
    }

public:
    DatasetHandler*
    Owner(bool is_owner, Allocator* allocator = nullptr);

    DatasetHandler*
    NumElements(const int64_t num_elements);

    int64_t
    GetNumElements() const;

    DatasetHandler*
    Dim(const int64_t dim);

    int64_t
    GetDim() const;

    DatasetHandler*
    Ids(const int64_t* ids);

    const int64_t*
    GetIds() const;

    DatasetHandler*
    Distances(const float* dists);

    const float*
    GetDistances() const;

    DatasetHandler*
    Int8Vectors(const int8_t* vectors);

    const int8_t*
    GetInt8Vectors() const;

    DatasetHandler*
    Float32Vectors(const float* vectors);

    const float*
    GetFloat32Vectors() const;

private:
    DatasetHandler() = default;

private:
    DatasetPtr dataset_ = nullptr;
};

class BitsetHandler {
    friend class IndexHandler;

public:
    static BitsetHandler*
    Make() {
        auto ret = new BitsetHandler();
        ret->bitset_ = Bitset::Make();
        return ret;
    }

    void
    Set(int64_t pos, bool value = true);

    bool
    Test(int64_t pos);

    uint64_t
    Count();

private:
    BitsetPtr bitset_ = nullptr;
};

class IndexHandler {
public:
    static tl::expected<IndexHandler*, Error>
    Make(const std::string& name, const std::string& parameters) {
        auto ret = new IndexHandler();
        auto index = Factory::CreateIndex(name, parameters);
        if (index.has_value()) {
            ret->index_ = index.value();
            return ret;
        } else {
            return tl::unexpected(index.error());
        }
    }

    ~IndexHandler() = default;

public:
    tl::expected<std::vector<int64_t>, Error>
    Build(DatasetHandler* base);

    tl::expected<std::vector<int64_t>, Error>
    Add(DatasetHandler* base);

    tl::expected<bool, Error>
    Remove(int64_t id);

    tl::expected<DatasetHandler*, Error>
    KnnSearch(DatasetHandler* query,
              int64_t k,
              const std::string& parameters,
              BitsetHandler* invalid = nullptr) const;

    tl::expected<DatasetHandler*, Error>
    RangeSearch(DatasetHandler* query,
                float radius,
                const std::string& parameters,
                BitsetHandler* invalid = nullptr,
                int64_t limited_size = -1) const;

    tl::expected<BinarySet, Error>
    Serialize() const;

    tl::expected<void, Error>
    Deserialize(const BinarySet& binary_set);

    tl::expected<void, Error>
    Deserialize(const ReaderSet& reader_set);

    tl::expected<void, Error>
    Serialize(std::ostream& out_stream);

    tl::expected<void, Error>
    Deserialize(std::istream& in_stream);

    int64_t
    GetNumElements() const;

    int64_t
    GetMemoryUsage() const;

private:
    IndexHandler() = default;

private:
    vsag::IndexPtr index_ = nullptr;
};

};  // namespace ext
};  // namespace vsag
