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

#include "simpleflat.h"

#include <cmath>
#include <cstring>
#include <nlohmann/json.hpp>

#include "vsag/errors.h"
#include "vsag/expected.hpp"
#include "vsag/readerset.h"

#define ROW_ID_MASK 0xFFFFFFFFLL

namespace vsag {

SimpleFlat::SimpleFlat(const std::string& metric_type, int64_t dim)
    : metric_type_(metric_type), dim_(dim) {
}

tl::expected<std::vector<int64_t>, Error>
SimpleFlat::Build(const DatasetPtr& base) {
    std::vector<int64_t> failed_ids;
    if (not this->data_.empty()) {
        return tl::unexpected(Error(ErrorType::BUILD_TWICE, ""));
    }

    if (this->dim_ != base->GetDim()) {
        return tl::unexpected(Error(ErrorType::DIMENSION_NOT_EQUAL, ""));
    }

    int64_t raw_length = base->GetNumElements();

    this->ids_.resize(raw_length);
    this->data_.resize(raw_length * this->dim_);
    int64_t actual_add_size = 0;
    for (int i = 0; i < raw_length; ++i) {
        auto end = ids_.begin() + actual_add_size;
        if (actual_add_size == 0 || std::find(ids_.begin(), end, base->GetIds()[i]) == end) {
            std::memcpy(this->ids_.data() + actual_add_size, base->GetIds() + i, sizeof(int64_t));

            std::memcpy(this->data_.data() + actual_add_size * this->dim_,
                        base->GetFloat32Vectors() + i * this->dim_,
                        this->dim_ * sizeof(float));
            actual_add_size++;
        } else {
            failed_ids.push_back(ids_[i]);
        }
    }
    this->num_elements_ += actual_add_size;

    this->ids_.resize(this->num_elements_);
    this->data_.resize(this->num_elements_ * this->dim_);

    return std::move(failed_ids);
}

tl::expected<std::vector<int64_t>, Error>
SimpleFlat::Add(const DatasetPtr& base) {
    std::vector<int64_t> failed_ids;
    if (not this->data_.empty()) {
        if (this->dim_ != base->GetDim()) {
            return tl::unexpected(Error(ErrorType::DIMENSION_NOT_EQUAL, ""));
        }
    }

    int64_t num_elements_existed = this->num_elements_;

    this->ids_.resize(num_elements_existed + base->GetNumElements());
    this->data_.resize((num_elements_existed + base->GetNumElements()) * this->dim_);

    int64_t actual_add_size = 0;
    for (int i = 0; i < base->GetNumElements(); ++i) {
        int64_t cur_size = num_elements_existed + actual_add_size;
        if (num_elements_existed + actual_add_size == 0 ||
            std::find(ids_.begin(), ids_.begin() + cur_size, base->GetIds()[i]) ==
                ids_.begin() + cur_size) {
            std::memcpy(this->ids_.data() + cur_size, base->GetIds() + i, sizeof(int64_t));

            std::memcpy(this->data_.data() + cur_size * this->dim_,
                        base->GetFloat32Vectors() + i * this->dim_,
                        this->dim_ * sizeof(float));
            actual_add_size++;
        } else {
            failed_ids.push_back(ids_[i]);
        }
    }
    this->num_elements_ += actual_add_size;

    this->ids_.resize(this->num_elements_);
    this->data_.resize(this->num_elements_ * this->dim_);

    return std::move(failed_ids);
}

tl::expected<DatasetPtr, Error>
SimpleFlat::KnnSearch(const DatasetPtr& query,
                      int64_t k,
                      const std::string& parameters,
                      BitsetPtr invalid) const {
    auto filter = [invalid](int64_t id) -> bool {
        if (invalid) {
            return invalid->Test(id);
        } else {
            return false;
        }
    };
    return this->KnnSearch(query, k, parameters, filter);
}

tl::expected<DatasetPtr, Error>
SimpleFlat::KnnSearch(const DatasetPtr& query,
                      int64_t k,
                      const std::string& parameters,
                      const std::function<bool(int64_t)>& filter) const {
    int64_t dim = query->GetDim();
    k = std::min(k, GetNumElements());
    int64_t num_elements = query->GetNumElements();
    if (num_elements != 1) {
        return tl::unexpected(Error(ErrorType::INTERNAL_ERROR, ""));
    }
    if (this->dim_ != dim) {
        return tl::unexpected(Error(ErrorType::DIMENSION_NOT_EQUAL, ""));
    }

    std::vector<rs> knn_result = knn_search(query->GetFloat32Vectors(), k, filter);

    auto result = Dataset::Make();
    if (knn_result.size() == 0) {
        result->Dim(0)->NumElements(1);
        return result;
    }

    int64_t* ids = new int64_t[knn_result.size()];
    float* dists = new float[knn_result.size()];
    for (int64_t kk = 0; kk < knn_result.size(); ++kk) {
        ids[kk] = knn_result[knn_result.size() - 1 - kk].second;
        dists[kk] = knn_result[knn_result.size() - 1 - kk].first;
    }
    result->NumElements(1)->Dim(knn_result.size())->Ids(ids)->Distances(dists);
    return std::move(result);
}

tl::expected<DatasetPtr, Error>
SimpleFlat::RangeSearch(const DatasetPtr& query,
                        float radius,
                        const std::string& parameters,
                        int64_t limited_size) const {
    return this->RangeSearch(query, radius, parameters, (BitsetPtr) nullptr, limited_size);
}

tl::expected<DatasetPtr, Error>
SimpleFlat::RangeSearch(const DatasetPtr& query,
                        float radius,
                        const std::string& parameters,
                        BitsetPtr invalid,
                        int64_t limited_size) const {
    auto filter = [invalid](int64_t id) -> bool {
        if (invalid) {
            return invalid->Test(id);
        } else {
            return false;
        }
    };
    return this->RangeSearch(query, radius, parameters, filter, limited_size);
}

tl::expected<DatasetPtr, Error>
SimpleFlat::RangeSearch(const DatasetPtr& query,
                        float radius,
                        const std::string& parameters,
                        const std::function<bool(int64_t)>& filter,
                        int64_t limited_size) const {
    int64_t nq = query->GetNumElements();
    int64_t dim = query->GetDim();
    if (this->dim_ != dim) {
        return tl::unexpected(Error(ErrorType::DIMENSION_NOT_EQUAL, ""));
    }

    if (nq != 1) {
        return tl::unexpected(Error(ErrorType::INTERNAL_ERROR, ""));
    }

    auto range_result = range_search(query->GetFloat32Vectors(), radius, filter);

    auto result = Dataset::Make();
    size_t target_size = range_result.size();
    if (range_result.size() == 0) {
        result->Dim(0)->NumElements(1);
        return result;
    }

    if (limited_size >= 1) {
        target_size = std::min((size_t)limited_size, target_size);
    }

    int64_t* ids = new int64_t[target_size];
    float* dists = new float[target_size];
    for (int64_t kk = 0; kk < target_size; ++kk) {
        ids[kk] = range_result[range_result.size() - 1 - kk].second;
        dists[kk] = range_result[range_result.size() - 1 - kk].first;
    }
    result->NumElements(1)->Dim(target_size)->Ids(ids)->Distances(dists);
    return std::move(result);
}

tl::expected<BinarySet, Error>
SimpleFlat::Serialize() const {
    try {
        BinarySet bs;
        size_t ids_size = num_elements_ * sizeof(int64_t);
        size_t vector_size = num_elements_ * dim_ * sizeof(float);

        std::shared_ptr<int8_t[]> ids(new int8_t[sizeof(int64_t) * 2 + ids_size]);
        std::shared_ptr<int8_t[]> vectors(new int8_t[vector_size]);

        int8_t* tmp_ptr = ids.get();
        std::memcpy(tmp_ptr, &num_elements_, sizeof(int64_t));

        tmp_ptr += sizeof(int64_t);
        std::memcpy(tmp_ptr, &dim_, sizeof(int64_t));

        tmp_ptr += sizeof(int64_t);
        std::memcpy(tmp_ptr, ids_.data(), ids_size);

        std::memcpy(vectors.get(), data_.data(), vector_size);

        Binary ids_binary{
            .data = ids,
            .size = sizeof(int64_t) + sizeof(int64_t) + ids_size,
        };
        bs.Set(SIMPLEFLAT_IDS, ids_binary);

        Binary vectors_binary{
            .data = vectors,
            .size = vector_size,
        };
        bs.Set(SIMPLEFLAT_VECTORS, vectors_binary);
        return bs;
    } catch (const std::bad_alloc& e) {
        return tl::unexpected(Error(ErrorType::NO_ENOUGH_MEMORY, ""));
    }
}

tl::expected<void, Error>
SimpleFlat::Deserialize(const BinarySet& binary_set) {
    Binary ids_binary = binary_set.Get(SIMPLEFLAT_IDS);
    Binary data_binary = binary_set.Get(SIMPLEFLAT_VECTORS);

    int8_t* tmp_ptr = ids_binary.data.get();
    std::memcpy(&num_elements_, tmp_ptr, sizeof(int64_t));
    tmp_ptr += sizeof(int64_t);

    int64_t tmp_dim;
    std::memcpy(&tmp_dim, tmp_ptr, sizeof(int64_t));

    if (tmp_dim != dim_) {
        return tl::unexpected(Error(ErrorType::DIMENSION_NOT_EQUAL, ""));
    }

    tmp_ptr += sizeof(int64_t);

    size_t ids_size = num_elements_ * sizeof(int64_t);
    size_t vector_size = num_elements_ * dim_ * sizeof(float);

    if (sizeof(int64_t) + sizeof(int64_t) + ids_size != ids_binary.size ||
        vector_size != data_binary.size) {
        return tl::unexpected(Error(ErrorType::INVALID_BINARY, ""));
    }

    ids_.resize(this->num_elements_);
    std::memcpy(ids_.data(), tmp_ptr, ids_size);

    data_.resize(this->num_elements_ * this->dim_);
    std::memcpy(data_.data(), data_binary.data.get(), vector_size);

    return {};
}

tl::expected<void, Error>
SimpleFlat::Deserialize(const ReaderSet& reader_set) {
    BinarySet bs;

    std::shared_ptr<Reader> vectors_reader = reader_set.Get(SIMPLEFLAT_VECTORS);
    std::shared_ptr<Reader> ids_reader = reader_set.Get(SIMPLEFLAT_IDS);

    std::shared_ptr<int8_t[]> vectors(new int8_t[vectors_reader->Size()]);
    std::shared_ptr<int8_t[]> ids(new int8_t[ids_reader->Size()]);

    vectors_reader->Read(0, vectors_reader->Size(), vectors.get());
    ids_reader->Read(0, ids_reader->Size(), ids.get());

    Binary vectors_binary{
        .data = vectors,
        .size = vectors_reader->Size(),
    };
    bs.Set(SIMPLEFLAT_VECTORS, vectors_binary);

    Binary ids_binary{
        .data = ids,
        .size = ids_reader->Size(),
    };
    bs.Set(SIMPLEFLAT_IDS, ids_binary);

    Deserialize(bs);

    return {};
}
std::vector<SimpleFlat::rs>
SimpleFlat::knn_search(const float* query,
                       int64_t k,
                       const std::function<bool(int64_t)>& filter) const {
    std::priority_queue<SimpleFlat::rs> q;
    for (int64_t i = 0; i < this->num_elements_; ++i) {
        if (filter(ids_[i])) {
            continue;
        }
        const float* base = data_.data() + i * this->dim_;
        float distance = 0.0f;
        if (this->metric_type_ == "l2") {
            distance = l2(base, query, this->dim_);
        } else if (this->metric_type_ == "ip") {
            distance = ip(base, query, this->dim_);
        } else if (this->metric_type_ == "cosine") {
            distance = cosine(base, query, this->dim_);
        } else {
            distance = 0;
        }

        q.push(std::make_pair(distance, this->ids_[i]));
        if (q.size() > k) {
            q.pop();
        }
    }

    std::vector<SimpleFlat::rs> results;
    while (not q.empty()) {
        results.push_back(q.top());
        q.pop();
    }
    return results;
}

std::vector<SimpleFlat::rs>
SimpleFlat::range_search(const float* query,
                         float radius,
                         const std::function<bool(int64_t)>& filter) const {
    std::priority_queue<SimpleFlat::rs> q;
    for (int64_t i = 0; i < this->num_elements_; ++i) {
        if (filter(ids_[i])) {
            continue;
        }
        const float* base = data_.data() + i * this->dim_;
        float distance = 0.0f;
        if (this->metric_type_ == "l2") {
            distance = l2(base, query, this->dim_);
        } else if (this->metric_type_ == "ip") {
            distance = ip(base, query, this->dim_);
        } else if (this->metric_type_ == "cosine") {
            distance = cosine(base, query, this->dim_);
        } else {
            distance = 0;
        }

        if (distance < radius) {
            q.push(std::make_pair(distance, this->ids_[i]));
        }
    }

    std::vector<SimpleFlat::rs> results;
    while (not q.empty()) {
        results.push_back(q.top());
        q.pop();
    }
    return results;
}

float
SimpleFlat::l2(const float* v1, const float* v2, int64_t dim) {
    float dist = 0;
    for (int64_t i = 0; i < dim; ++i) {
        dist += std::pow(v1[i] - v2[i], 2);
    }
    return dist;
}

float
SimpleFlat::ip(const float* v1, const float* v2, int64_t dim) {
    float dist = 0;
    for (int64_t i = 0; i < dim; ++i) {
        dist += v1[i] * v2[i];
    }
    dist = 1 - dist;
    return dist;
}

float
SimpleFlat::cosine(const float* v1, const float* v2, int64_t dim) {
    float dist = 0;
    float mold_v1 = 0;
    float mold_v2 = 0;
    for (int64_t i = 0; i < dim; ++i) {
        mold_v1 += v1[i] * v1[i];
        mold_v2 += v2[i] * v2[i];
        dist += v1[i] * v2[i];
    }
    dist = 1 - dist / std::sqrt(mold_v1 * mold_v2);
    return dist;
}

std::string
SimpleFlat::GetStats() const {
    nlohmann::json j;
    j["num_elements"] = num_elements_;
    j["dim"] = dim_;
    return j.dump();
}

tl::expected<bool, Error>
SimpleFlat::Remove(int64_t id) {
    auto iter = std::find(ids_.begin(), ids_.end(), id);
    if (iter != ids_.end()) {
        int index = iter - ids_.begin();
        num_elements_--;
        ids_[index] = ids_[num_elements_];
        std::memcpy(
            data_.data() + index * dim_, data_.data() + num_elements_ * dim_, dim_ * sizeof(float));
        ids_.resize(num_elements_);
        data_.resize(num_elements_ * dim_);
    } else {
        return false;
    }

    return true;
}

}  // namespace vsag
