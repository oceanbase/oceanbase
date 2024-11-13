

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

#include "hnsw.h"

#include <fmt/format-inl.h>

#include <cstdint>
#include <exception>
#include <new>
#include <nlohmann/json.hpp>
#include <stdexcept>

#include "../algorithm/hnswlib/hnswlib.h"
#include "../common.h"
#include "../logger.h"
#include "../safe_allocator.h"
#include "../utils.h"
#include "./hnsw_zparameters.h"
#include "vsag/binaryset.h"
#include "vsag/constants.h"
#include "vsag/errors.h"
#include "vsag/expected.hpp"

namespace vsag {

const static int64_t EXPANSION_NUM = 1000000;
const static int64_t DEFAULT_MAX_ELEMENT = 1;
const static int MINIMAL_M = 8;
const static int MAXIMAL_M = 64;
const static uint32_t GENERATE_SEARCH_K = 50;
const static uint32_t GENERATE_SEARCH_L = 400;
const static float GENERATE_OMEGA = 0.51;

HNSW::HNSW(std::shared_ptr<hnswlib::SpaceInterface> space_interface,
           int M,
           int ef_construction,
           bool use_static,
           bool use_reversed_edges,
           bool use_conjugate_graph,
           bool normalize,
           Allocator* allocator)
    : space(std::move(space_interface)),
      use_static_(use_static),
      use_conjugate_graph_(use_conjugate_graph),
      use_reversed_edges_(use_reversed_edges) {
    dim_ = *((size_t*)space->get_dist_func_param());

    M = std::min(std::max(M, MINIMAL_M), MAXIMAL_M);

    if (ef_construction <= 0) {
        throw std::runtime_error(MESSAGE_PARAMETER);
    }

    if (use_conjugate_graph) {
        conjugate_graph_ = std::make_shared<ConjugateGraph>();
    }

    if (not allocator) {
        allocator = DefaultAllocator::Instance();
    }
    allocator_ = std::shared_ptr<SafeAllocator>(new SafeAllocator(allocator));

    if (!use_static_) {
        alg_hnsw =
            std::make_shared<hnswlib::HierarchicalNSW>(space.get(),
                                                       DEFAULT_MAX_ELEMENT,
                                                       allocator_.get(),
                                                       M,
                                                       ef_construction,
                                                       use_reversed_edges_,
                                                       normalize,
                                                       Options::Instance().block_size_limit());
    } else {
        if (dim_ % 4 != 0) {
            // FIXME(wxyu): remove throw stmt from construct function
            throw std::runtime_error("cannot build static hnsw while dim % 4 != 0");
        }
        alg_hnsw = std::make_shared<hnswlib::StaticHierarchicalNSW>(
            space.get(),
            DEFAULT_MAX_ELEMENT,
            allocator_.get(),
            M,
            ef_construction,
            Options::Instance().block_size_limit());
    }
}

tl::expected<std::vector<int64_t>, Error>
HNSW::build(const DatasetPtr& base) {
    try {
        if (base->GetNumElements() == 0) {
            empty_index_ = true;
            return std::vector<int64_t>();
        }

        logger::debug("index.dim={}, base.dim={}", this->dim_, base->GetDim());

        auto base_dim = base->GetDim();
        CHECK_ARGUMENT(base_dim == dim_,
                       fmt::format("base.dim({}) must be equal to index.dim({})", base_dim, dim_));

        int64_t num_elements = base->GetNumElements();

        std::unique_lock lock(rw_mutex_);
        if (auto result = init_memory_space(); not result.has_value()) {
            return tl::unexpected(result.error());
        }

        auto ids = base->GetIds();
        auto vectors = base->GetFloat32Vectors();
        std::vector<int64_t> failed_ids;
        {
            SlowTaskTimer t("hnsw graph");
            for (int64_t i = 0; i < num_elements; ++i) {
                // noexcept runtime
                if (!alg_hnsw->addPoint((const void*)(vectors + i * dim_), ids[i])) {
                    logger::debug("duplicate point: {}", ids[i]);
                    failed_ids.emplace_back(ids[i]);
                }
            }
        }

        if (use_static_) {
            SlowTaskTimer t("hnsw pq", 1000);
            auto* hnsw = static_cast<hnswlib::StaticHierarchicalNSW*>(alg_hnsw.get());
            hnsw->encode_hnsw_data();
        }

        return failed_ids;
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(
            ErrorType::INVALID_ARGUMENT, "failed to build(invalid argument): ", e.what());
    }
}

tl::expected<std::vector<int64_t>, Error>
HNSW::add(const DatasetPtr& base) {
    SlowTaskTimer t("hnsw add", 20);

    if (use_static_) {
        LOG_ERROR_AND_RETURNS(ErrorType::UNSUPPORTED_INDEX_OPERATION,
                              "static index does not support add");
    }
    try {
        auto base_dim = base->GetDim();
        CHECK_ARGUMENT(base_dim == dim_,
                       fmt::format("base.dim({}) must be equal to index.dim({})", base_dim, dim_));

        int64_t num_elements = base->GetNumElements();
        auto ids = base->GetIds();
        auto vectors = base->GetFloat32Vectors();
        std::vector<int64_t> failed_ids;

        std::unique_lock lock(rw_mutex_);
        if (auto result = init_memory_space(); not result.has_value()) {
            return tl::unexpected(result.error());
        }
        for (int64_t i = 0; i < num_elements; ++i) {
            // noexcept runtime
            if (!alg_hnsw->addPoint((const void*)(vectors + i * dim_), ids[i])) {
                logger::debug("duplicate point: {}", i);
                failed_ids.push_back(ids[i]);
            }
        }

        return failed_ids;
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(
            ErrorType::INVALID_ARGUMENT, "failed to add(invalid argument): ", e.what());
    }
}

template <typename FilterType>
tl::expected<DatasetPtr, Error>
HNSW::knn_search_internal(const DatasetPtr& query,
                          int64_t k,
                          const std::string& parameters,
                          const FilterType& filter_obj) const {
    if (filter_obj) {
        BitsetOrCallbackFilter filter(filter_obj);
        return this->knn_search(query, k, parameters, &filter);
    } else {
        return this->knn_search(query, k, parameters, nullptr);
    }
};

tl::expected<DatasetPtr, Error>
HNSW::knn_search(const DatasetPtr& query,
                 int64_t k,
                 const std::string& parameters,
                 hnswlib::BaseFilterFunctor* filter_ptr) const {
    SlowTaskTimer t("hnsw knnsearch", 20);

    try {
        // cannot perform search on empty index
        if (empty_index_) {
            auto ret = Dataset::Make();
            ret->Dim(0)->NumElements(1);
            return ret;
        }

        // check query vector
        CHECK_ARGUMENT(query->GetNumElements() == 1, "query dataset should contain 1 vector only");
        auto vector = query->GetFloat32Vectors();
        int64_t query_dim = query->GetDim();
        CHECK_ARGUMENT(
            query_dim == dim_,
            fmt::format("query.dim({}) must be equal to index.dim({})", query_dim, dim_));

        // check k
        CHECK_ARGUMENT(k > 0, fmt::format("k({}) must be greater than 0", k))
        k = std::min(k, GetNumElements());

        std::shared_lock lock(rw_mutex_);

        // check search parameters
        auto params = HnswSearchParameters::FromJson(parameters);

        // perform search
        std::priority_queue<std::pair<float, size_t>> results;
        double time_cost;
        try {
            Timer t(time_cost);
            results = alg_hnsw->searchKnn(
                (const void*)(vector), k, std::max(params.ef_search, k), filter_ptr);
        } catch (const std::runtime_error& e) {
            LOG_ERROR_AND_RETURNS(ErrorType::INTERNAL_ERROR,
                                  "failed to perofrm knn_search(internalError): ",
                                  e.what());
        }

        // update stats
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            result_queues_[STATSTIC_KNN_TIME].Push(time_cost);
        }

        // return result
        auto result = Dataset::Make();
        if (results.size() == 0) {
            result->Dim(0)->NumElements(1);
            return result;
        }

        // perform conjugate graph enhancement
        if (use_conjugate_graph_ and params.use_conjugate_graph_search) {
            std::shared_lock lock(rw_mutex_);
            time_cost = 0;
            Timer t(time_cost);

            auto func = [this, vector](int64_t label) {
                return this->alg_hnsw->getDistanceByLabel(label, vector);
            };
            conjugate_graph_->EnhanceResult(results, func);
        }

        // return result

        result->Dim(results.size())->NumElements(1)->Owner(true, allocator_->GetRawAllocator());

        int64_t* ids = (int64_t*)allocator_->Allocate(sizeof(int64_t) * results.size());
        result->Ids(ids);
        float* dists = (float*)allocator_->Allocate(sizeof(float) * results.size());
        result->Distances(dists);

        for (int64_t j = results.size() - 1; j >= 0; --j) {
            dists[j] = results.top().first;
            ids[j] = results.top().second;
            results.pop();
        }

        return std::move(result);
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::INVALID_ARGUMENT,
                              "failed to perform knn_search(invalid argument): ",
                              e.what());
    } catch (const std::bad_alloc& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::NO_ENOUGH_MEMORY,
                              "failed to perform knn_search(not enough memory): ",
                              e.what());
    }
}

template <typename FilterType>
tl::expected<DatasetPtr, Error>
HNSW::range_search_internal(const DatasetPtr& query,
                            float radius,
                            const std::string& parameters,
                            const FilterType& filter_obj,
                            int64_t limited_size) const {
    if (filter_obj) {
        BitsetOrCallbackFilter filter(filter_obj);
        return this->range_search(query, radius, parameters, &filter, limited_size);
    } else {
        return this->range_search(query, radius, parameters, nullptr, limited_size);
    }
};

tl::expected<DatasetPtr, Error>
HNSW::range_search(const DatasetPtr& query,
                   float radius,
                   const std::string& parameters,
                   hnswlib::BaseFilterFunctor* filter_ptr,
                   int64_t limited_size) const {
    SlowTaskTimer t("hnsw rangesearch", 20);

    try {
        // cannot perform search on empty index
        if (empty_index_) {
            auto ret = Dataset::Make();
            ret->Dim(0)->NumElements(1);
            return ret;
        }

        if (use_static_) {
            LOG_ERROR_AND_RETURNS(ErrorType::UNSUPPORTED_INDEX_OPERATION,
                                  "static index does not support rangesearch");
        }

        // check query vector
        CHECK_ARGUMENT(query->GetNumElements() == 1, "query dataset should contain 1 vector only");
        auto vector = query->GetFloat32Vectors();
        int64_t query_dim = query->GetDim();
        CHECK_ARGUMENT(
            query_dim == dim_,
            fmt::format("query.dim({}) must be equal to index.dim({})", query_dim, dim_));

        // check radius
        CHECK_ARGUMENT(radius >= 0, fmt::format("radius({}) must be greater equal than 0", radius))

        // check limited_size
        CHECK_ARGUMENT(limited_size != 0,
                       fmt::format("limited_size({}) must not be equal to 0", limited_size))

        // check search parameters
        auto params = HnswSearchParameters::FromJson(parameters);

        // perform search
        std::priority_queue<std::pair<float, size_t>> results;
        double time_cost;
        try {
            std::shared_lock lock(rw_mutex_);
            Timer timer(time_cost);
            results =
                alg_hnsw->searchRange((const void*)(vector), radius, params.ef_search, filter_ptr);
        } catch (std::runtime_error& e) {
            LOG_ERROR_AND_RETURNS(ErrorType::INTERNAL_ERROR,
                                  "failed to perofrm range_search(internalError): ",
                                  e.what());
        }

        // update stats
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            result_queues_[STATSTIC_KNN_TIME].Push(time_cost);
        }

        // return result
        auto result = Dataset::Make();
        size_t target_size = results.size();
        if (results.size() == 0) {
            result->Dim(0)->NumElements(1);
            return result;
        }
        if (limited_size >= 1) {
            target_size = std::min((size_t)limited_size, target_size);
        }
        result->Dim(target_size)->NumElements(1)->Owner(true, allocator_->GetRawAllocator());
        int64_t* ids = (int64_t*)allocator_->Allocate(sizeof(int64_t) * target_size);
        result->Ids(ids);
        float* dists = (float*)allocator_->Allocate(sizeof(float) * target_size);
        result->Distances(dists);
        for (int64_t j = results.size() - 1; j >= 0; --j) {
            if (j < target_size) {
                dists[j] = results.top().first;
                ids[j] = results.top().second;
            }
            results.pop();
        }

        return std::move(result);
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::INVALID_ARGUMENT,
                              "failed to perform range_search(invalid argument): ",
                              e.what());
    } catch (const std::bad_alloc& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::NO_ENOUGH_MEMORY,
                              "failed to perform range_search(not enough memory): ",
                              e.what());
    }
}

BinarySet
HNSW::empty_binaryset() const {
    // version 0 pairs:
    // - hnsw_blank: b"EMPTY_HNSW"
    const std::string empty_str = "EMPTY_HNSW";
    size_t num_bytes = empty_str.length();
    std::shared_ptr<int8_t[]> bin(new int8_t[num_bytes]);
    memcpy(bin.get(), empty_str.c_str(), empty_str.length());
    Binary b{
        .data = bin,
        .size = num_bytes,
    };
    BinarySet bs;
    bs.Set(BLANK_INDEX, b);

    return bs;
}

tl::expected<BinarySet, Error>
HNSW::serialize() const {
    if (GetNumElements() == 0) {
        // return a special binaryset means empty
        return empty_binaryset();
    }

    SlowTaskTimer t("hnsw serialize");
    size_t num_bytes = alg_hnsw->calcSerializeSize();
    try {
        std::shared_ptr<int8_t[]> bin(new int8_t[num_bytes]);
        std::shared_lock lock(rw_mutex_);
        alg_hnsw->saveIndex(bin.get());
        Binary b{
            .data = bin,
            .size = num_bytes,
        };
        BinarySet bs;
        bs.Set(HNSW_DATA, b);

        if (use_conjugate_graph_) {
            Binary b_cg = *conjugate_graph_->Serialize();
            bs.Set(CONJUGATE_GRAPH_DATA, b_cg);
        }

        return bs;
    } catch (const std::bad_alloc& e) {
        LOG_ERROR_AND_RETURNS(
            ErrorType::NO_ENOUGH_MEMORY, "failed to serialize(bad alloc): ", e.what());
    }
}

tl::expected<void, Error>
HNSW::serialize(std::ostream& out_stream) {
    if (GetNumElements() == 0) {
        LOG_ERROR_AND_RETURNS(ErrorType::INDEX_EMPTY, "failed to serialize: hnsw index is empty");

        // FIXME(wxyu): cannot support serialize empty index by stream
        // auto bs = empty_binaryset();
        // for (const auto& key : bs.GetKeys()) {
        //     auto b = bs.Get(key);
        //     out_stream.write((char*)b.data.get(), b.size);
        // }
        // return {};
    }

    SlowTaskTimer t("hnsw serialize");

    // no expected exception
    std::shared_lock lock(rw_mutex_);
    alg_hnsw->saveIndex(out_stream);

    if (use_conjugate_graph_) {
        conjugate_graph_->Serialize(out_stream);
    }

    return {};
}

tl::expected<void, Error>
HNSW::deserialize(const BinarySet& binary_set) {
    SlowTaskTimer t("hnsw deserialize");
    if (this->alg_hnsw->getCurrentElementCount() > 0) {
        LOG_ERROR_AND_RETURNS(ErrorType::INDEX_NOT_EMPTY,
                              "failed to deserialize: index is not empty");
    }

    // check if binaryset is a empty index
    if (binary_set.Contains(BLANK_INDEX)) {
        empty_index_ = true;
        return {};
    }

    Binary b = binary_set.Get(HNSW_DATA);
    auto func = [&](uint64_t offset, uint64_t len, void* dest) -> void {
        std::memcpy(dest, b.data.get() + offset, len);
    };

    try {
        std::unique_lock lock(rw_mutex_);
        if (auto result = init_memory_space(); not result.has_value()) {
            return tl::unexpected(result.error());
        }
        alg_hnsw->loadIndex(func, this->space.get());
        if (use_conjugate_graph_) {
            Binary b_cg = binary_set.Get(CONJUGATE_GRAPH_DATA);
            if (not conjugate_graph_->Deserialize(b_cg).has_value()) {
                throw std::runtime_error("error in deserialize conjugate graph");
            }
        }
    } catch (const std::runtime_error& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::READ_ERROR, "failed to deserialize: ", e.what());
    } catch (const std::out_of_range& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::READ_ERROR, "failed to deserialize: ", e.what());
    }

    return {};
}

tl::expected<void, Error>
HNSW::deserialize(const ReaderSet& reader_set) {
    SlowTaskTimer t("hnsw deserialize");
    if (this->alg_hnsw->getCurrentElementCount() > 0) {
        LOG_ERROR_AND_RETURNS(ErrorType::INDEX_NOT_EMPTY,
                              "failed to deserialize: index is not empty");
    }

    // check if readerset is a empty index
    if (reader_set.Contains(BLANK_INDEX)) {
        empty_index_ = true;
        return {};
    }

    auto func = [&](uint64_t offset, uint64_t len, void* dest) -> void {
        reader_set.Get(HNSW_DATA)->Read(offset, len, dest);
    };

    try {
        std::unique_lock lock(rw_mutex_);
        if (auto result = init_memory_space(); not result.has_value()) {
            return tl::unexpected(result.error());
        }
        alg_hnsw->loadIndex(func, this->space.get());
    } catch (const std::runtime_error& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::READ_ERROR, "failed to deserialize: ", e.what());
    }

    return {};
}

tl::expected<void, Error>
HNSW::deserialize(std::istream& in_stream) {
    SlowTaskTimer t("hnsw deserialize");
    if (this->alg_hnsw->getCurrentElementCount() > 0) {
        LOG_ERROR_AND_RETURNS(ErrorType::INDEX_NOT_EMPTY,
                              "failed to deserialize: index is not empty");
    }

    try {
        std::unique_lock lock(rw_mutex_);
        if (auto result = init_memory_space(); not result.has_value()) {
            return tl::unexpected(result.error());
        }
        alg_hnsw->loadIndex(in_stream, this->space.get());
        if (use_conjugate_graph_ and not conjugate_graph_->Deserialize(in_stream).has_value()) {
            throw std::runtime_error("error in deserialize conjugate graph");
        }
    } catch (const std::runtime_error& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::READ_ERROR, "failed to deserialize: ", e.what());
    }

    return {};
}

std::string
HNSW::GetStats() const {
    nlohmann::json j;
    j[STATSTIC_DATA_NUM] = GetNumElements();
    j[STATSTIC_INDEX_NAME] = INDEX_HNSW;
    j[STATSTIC_MEMORY] = GetMemoryUsage();

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        for (auto& item : result_queues_) {
            j[item.first] = item.second.GetAvgResult();
        }
    }
    return j.dump();
}

tl::expected<bool, Error>
HNSW::remove(int64_t id) {
    if (use_static_) {
        LOG_ERROR_AND_RETURNS(ErrorType::UNSUPPORTED_INDEX_OPERATION,
                              "static hnsw does not support remove");
    }

    try {
        std::unique_lock lock(rw_mutex_);
        if (use_reversed_edges_) {
            std::reinterpret_pointer_cast<hnswlib::HierarchicalNSW>(alg_hnsw)->removePoint(id);
        } else {
            std::reinterpret_pointer_cast<hnswlib::HierarchicalNSW>(alg_hnsw)->markDelete(id);
        }
    } catch (const std::runtime_error& e) {
        spdlog::warn("mark delete error for id {}: {}", id, e.what());
        return false;
    }

    return true;
}

tl::expected<uint32_t, Error>
HNSW::feedback(const DatasetPtr& query,
               int64_t k,
               const std::string& parameters,
               int64_t global_optimum_tag_id) {
    if (not use_conjugate_graph_) {
        LOG_ERROR_AND_RETURNS(ErrorType::UNSUPPORTED_INDEX_OPERATION,
                              "no conjugate graph used for feedback");
    }
    if (empty_index_) {
        return 0;
    }

    if (global_optimum_tag_id == std::numeric_limits<int64_t>::max()) {
        auto exact_result = this->brute_force(query, k);
        if (exact_result.has_value()) {
            global_optimum_tag_id = exact_result.value()->GetIds()[0];
        } else {
            LOG_ERROR_AND_RETURNS(ErrorType::INVALID_ARGUMENT,
                                  "failed to feedback(invalid argument): ",
                                  exact_result.error().message);
        }
    }

    auto result = this->knn_search(query, k, parameters, nullptr);
    if (result.has_value()) {
        std::unique_lock lock(rw_mutex_);
        return this->feedback(*result, global_optimum_tag_id, k);
    } else {
        LOG_ERROR_AND_RETURNS(ErrorType::INVALID_ARGUMENT,
                              "failed to feedback(invalid argument): ",
                              result.error().message);
    }
}

tl::expected<uint32_t, Error>
HNSW::feedback(const DatasetPtr& result, int64_t global_optimum_tag_id, int64_t k) {
    if (not alg_hnsw->isValidLabel(global_optimum_tag_id)) {
        LOG_ERROR_AND_RETURNS(
            ErrorType::INVALID_ARGUMENT,
            "failed to feedback(invalid argument): global optimum tag id doesn't belong to index");
    }

    auto tag_ids = result->GetIds();
    k = std::min(k, result->GetDim());
    uint32_t successfully_feedback = 0;

    for (int i = 0; i < k; i++) {
        if (not alg_hnsw->isValidLabel(tag_ids[i])) {
            LOG_ERROR_AND_RETURNS(
                ErrorType::INVALID_ARGUMENT,
                "failed to feedback(invalid argument): input result don't belong to index");
        }
        if (*conjugate_graph_->AddNeighbor(tag_ids[i], global_optimum_tag_id)) {
            successfully_feedback++;
        }
    }

    return successfully_feedback;
}

tl::expected<DatasetPtr, Error>
HNSW::brute_force(const DatasetPtr& query, int64_t k) {
    try {
        CHECK_ARGUMENT(k > 0, fmt::format("k({}) must be greater than 0", k));
        CHECK_ARGUMENT(query->GetNumElements() == 1,
                       fmt::format("query num({}) must equal to 1", query->GetNumElements()));
        CHECK_ARGUMENT(
            query->GetDim() == dim_,
            fmt::format("query.dim({}) must be equal to index.dim({})", query->GetDim(), dim_));

        auto result = Dataset::Make();
        result->NumElements(k)->Owner(true, allocator_->GetRawAllocator());
        int64_t* ids = (int64_t*)allocator_->Allocate(sizeof(int64_t) * k);
        result->Ids(ids);
        float* dists = (float*)allocator_->Allocate(sizeof(float) * k);
        result->Distances(dists);

        auto vector = query->GetFloat32Vectors();
        std::shared_lock lock(rw_mutex_);
        std::priority_queue<std::pair<float, hnswlib::labeltype>> bf_result =
            alg_hnsw->bruteForce((const void*)vector, k);
        result->Dim(std::min(k, (int64_t)bf_result.size()));

        for (int i = result->GetDim() - 1; i >= 0; i--) {
            ids[i] = bf_result.top().second;
            dists[i] = bf_result.top().first;
            bf_result.pop();
        }

        return std::move(result);
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::INVALID_ARGUMENT,
                              "failed to perform brute force search(invalid argument): ",
                              e.what());
    }
}

bool
HNSW::CheckGraphIntegrity() const {
    auto* hnsw = static_cast<hnswlib::HierarchicalNSW*>(alg_hnsw.get());
    return hnsw->checkReverseConnection();
}

tl::expected<uint32_t, Error>
HNSW::pretrain(const std::vector<int64_t>& base_tag_ids,
               uint32_t k,
               const std::string& parameters) {
    if (not use_conjugate_graph_) {
        LOG_ERROR_AND_RETURNS(ErrorType::UNSUPPORTED_INDEX_OPERATION,
                              "no conjugate graph used for pretrain");
    }
    if (empty_index_) {
        return 0;
    }

    uint32_t add_edges = 0;
    int64_t topk_neighbor_tag_id;
    const float* topk_data;
    std::shared_ptr<float[]> generated_data(new float[dim_]);
    auto base = Dataset::Make();
    auto generated_query = Dataset::Make();
    base->Dim(dim_)->NumElements(1)->Owner(false);
    generated_query->Dim(dim_)->NumElements(1)->Float32Vectors(generated_data.get())->Owner(false);

    for (const int64_t& base_tag_id : base_tag_ids) {
        try {
            base->Float32Vectors(this->alg_hnsw->getDataByLabel(base_tag_id));
        } catch (const std::runtime_error& e) {
            LOG_ERROR_AND_RETURNS(
                ErrorType::INVALID_ARGUMENT,
                fmt::format(
                    "failed to pretrain(invalid argument): bas tag id ({}) doesn't belong to index",
                    base_tag_id));
        }

        auto result = this->knn_search(base,
                                       vsag::GENERATE_SEARCH_K,
                                       fmt::format(R"(
                                        {{
                                            "hnsw": {{
                                                "ef_search": {},
                                                "use_conjugate_graph": true
                                            }}
                                        }})",
                                                   vsag::GENERATE_SEARCH_L),
                                       nullptr);

        for (int i = 0; i < result.value()->GetDim(); i++) {
            topk_neighbor_tag_id = result.value()->GetIds()[i];
            if (topk_neighbor_tag_id == base_tag_id) {
                continue;
            }
            topk_data = this->alg_hnsw->getDataByLabel(topk_neighbor_tag_id);

            for (int d = 0; d < dim_; d++) {
                generated_data.get()[d] = vsag::GENERATE_OMEGA * base->GetFloat32Vectors()[d] +
                                          (1 - vsag::GENERATE_OMEGA) * topk_data[d];
            }

            auto feedback_result = this->Feedback(generated_query, k, parameters, base_tag_id);
            if (feedback_result.has_value()) {
                add_edges += *feedback_result;
            } else {
                LOG_ERROR_AND_RETURNS(ErrorType::INVALID_ARGUMENT,
                                      "failed to feedback(invalid argument): ",
                                      feedback_result.error().message);
            }
        }
    }

    return add_edges;
}

tl::expected<bool, Error>
HNSW::init_memory_space() {
    if (is_init_memory_) {
        return true;
    }
    try {
        alg_hnsw->init_memory_space();
    } catch (std::runtime_error& r) {
        LOG_ERROR_AND_RETURNS(ErrorType::NO_ENOUGH_MEMORY, "allocate memory failed:", r.what());
    }
    is_init_memory_ = true;
    return true;
}

}  // namespace vsag
