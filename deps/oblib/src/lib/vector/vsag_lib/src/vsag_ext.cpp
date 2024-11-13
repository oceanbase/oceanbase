

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

#include "vsag/vsag_ext.h"

namespace vsag {
namespace ext {

DatasetHandler*
DatasetHandler::Owner(bool is_owner, Allocator* allocator) {
    dataset_->Owner(is_owner, allocator);
    return this;
}

DatasetHandler*
DatasetHandler::NumElements(const int64_t num_elements) {
    dataset_->NumElements(num_elements);
    return this;
}

int64_t
DatasetHandler::GetNumElements() const {
    return dataset_->GetNumElements();
}

DatasetHandler*
DatasetHandler::Dim(const int64_t dim) {
    dataset_->Dim(dim);
    return this;
}

int64_t
DatasetHandler::GetDim() const {
    return dataset_->GetDim();
}

DatasetHandler*
DatasetHandler::Ids(const int64_t* ids) {
    dataset_->Ids(ids);
    return this;
}

const int64_t*
DatasetHandler::GetIds() const {
    return dataset_->GetIds();
}

DatasetHandler*
DatasetHandler::Distances(const float* dists) {
    dataset_->Distances(dists);
    return this;
}

const float*
DatasetHandler::GetDistances() const {
    return dataset_->GetDistances();
}

DatasetHandler*
DatasetHandler::Int8Vectors(const int8_t* vectors) {
    dataset_->Int8Vectors(vectors);
    return this;
}

const int8_t*
DatasetHandler::GetInt8Vectors() const {
    return dataset_->GetInt8Vectors();
}

DatasetHandler*
DatasetHandler::Float32Vectors(const float* vectors) {
    dataset_->Float32Vectors(vectors);
    return this;
}

const float*
DatasetHandler::GetFloat32Vectors() const {
    return dataset_->GetFloat32Vectors();
}

void
BitsetHandler::Set(int64_t pos, bool value) {
    bitset_->Set(pos, value);
}

bool
BitsetHandler::Test(int64_t pos) {
    return bitset_->Test(pos);
}

uint64_t
BitsetHandler::Count() {
    return bitset_->Count();
}

tl::expected<std::vector<int64_t>, Error>
IndexHandler::Build(DatasetHandler* base) {
    return index_->Build(base->dataset_);
}

tl::expected<std::vector<int64_t>, Error>
IndexHandler::Add(DatasetHandler* base) {
    return index_->Add(base->dataset_);
}

tl::expected<bool, Error>
IndexHandler::Remove(int64_t id) {
    return index_->Remove(id);
}

tl::expected<DatasetHandler*, Error>
IndexHandler::KnnSearch(DatasetHandler* query,
                        int64_t k,
                        const std::string& parameters,
                        BitsetHandler* invalid) const {
    BitsetPtr invalid_bitset = nullptr;
    if (invalid) {
        invalid_bitset = invalid->bitset_;
    }
    auto ret = index_->KnnSearch(query->dataset_, k, parameters, invalid_bitset);
    if (ret.has_value()) {
        return DatasetHandler::Make(ret.value());
    } else {
        return tl::unexpected(ret.error());
    }
}

tl::expected<DatasetHandler*, Error>
IndexHandler::RangeSearch(DatasetHandler* query,
                          float radius,
                          const std::string& parameters,
                          BitsetHandler* invalid,
                          int64_t limited_size) const {
    BitsetPtr invalid_bitset = nullptr;
    if (invalid) {
        invalid_bitset = invalid->bitset_;
    }
    auto ret =
        index_->RangeSearch(query->dataset_, radius, parameters, invalid_bitset, limited_size);
    if (ret.has_value()) {
        return DatasetHandler::Make(ret.value());
    } else {
        return tl::unexpected(ret.error());
    }
}

tl::expected<BinarySet, Error>
IndexHandler::Serialize() const {
    return index_->Serialize();
}

tl::expected<void, Error>
IndexHandler::Deserialize(const BinarySet& binary_set) {
    return index_->Deserialize(binary_set);
}

tl::expected<void, Error>
IndexHandler::Deserialize(const ReaderSet& reader_set) {
    return index_->Deserialize(reader_set);
}

tl::expected<void, Error>
IndexHandler::Serialize(std::ostream& out_stream) {
    return index_->Serialize(out_stream);
}

tl::expected<void, Error>
IndexHandler::Deserialize(std::istream& in_stream) {
    return index_->Deserialize(in_stream);
}

int64_t
IndexHandler::GetNumElements() const {
    return index_->GetNumElements();
}

int64_t
IndexHandler::GetMemoryUsage() const {
    return index_->GetMemoryUsage();
}

};  // namespace ext
};  // namespace vsag
