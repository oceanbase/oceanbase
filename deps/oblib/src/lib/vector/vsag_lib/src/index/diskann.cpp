

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

#include "diskann.h"

#include <ThreadPool.h>
#include <local_file_reader.h>

#include <algorithm>
#include <exception>
#include <functional>
#include <future>
#include <iterator>
#include <new>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <utility>

#include "../common.h"
#include "../logger.h"
#include "../utils.h"
#include "./diskann_zparameters.h"
#include "vsag/constants.h"
#include "vsag/errors.h"
#include "vsag/expected.hpp"
#include "vsag/index.h"
#include "vsag/readerset.h"

namespace vsag {

const static float MACRO_TO_MILLI = 1000;
const static int64_t DATA_LIMIT = 2;
const static size_t MAXIMAL_BEAM_SEARCH = 64;
const static size_t MINIMAL_BEAM_SEARCH = 1;
const static int MINIMAL_R = 8;
const static int MAXIMAL_R = 64;
const static int VECTOR_PER_BLOCK = 1;
const static float GRAPH_SLACK = 1.3 * 1.05;
const static size_t MINIMAL_SECTOR_LEN = 4096;
const static std::string BUILD_STATUS = "status";
const static std::string BUILD_CURRENT_ROUND = "round";
const static std::string BUILD_NODES = "builded_nodes";
const static std::string BUILD_FAILED_LOC = "failed_loc";

template <typename T>
Binary
to_binary(T& value) {
    Binary binary;
    binary.size = sizeof(T);
    binary.data = std::shared_ptr<int8_t[]>(new int8_t[binary.size]);
    std::memcpy(binary.data.get(), &value, binary.size);
    return binary;
}

template <typename T>
T
from_binary(const Binary& binary) {
    T value;
    std::memcpy(&value, binary.data.get(), binary.size);
    return value;
}

class LocalMemoryReader : public Reader {
public:
    LocalMemoryReader(std::stringstream& file, bool support_async_io) {
        if (support_async_io) {
            pool_ = std::make_unique<ThreadPool>(Option::Instance().num_threads_io());
        }
        file_ << file.rdbuf();
        file_.seekg(0, std::ios::end);
        size_ = file_.tellg();
    }

    ~LocalMemoryReader() = default;

    virtual void
    Read(uint64_t offset, uint64_t len, void* dest) override {
        std::lock_guard<std::mutex> lock(mutex_);
        file_.seekg(offset, std::ios::beg);
        file_.read((char*)dest, len);
    }

    virtual void
    AsyncRead(uint64_t offset, uint64_t len, void* dest, CallBack callback) override {
        if (pool_) {
            pool_->enqueue([this, offset, len, dest, callback]() {
                this->Read(offset, len, dest);
                callback(IOErrorCode::IO_SUCCESS, "success");
            });
        } else {
            throw std::runtime_error("LocalMemoryReader does not support AsyncRead");
        }
    }

    virtual uint64_t
    Size() const override {
        return size_;
    }

private:
    std::stringstream file_;
    uint64_t size_;
    std::mutex mutex_;
    std::unique_ptr<ThreadPool> pool_;
};

Binary
convert_stream_to_binary(const std::stringstream& stream) {
    std::streambuf* buf = stream.rdbuf();
    std::streamsize size = buf->pubseekoff(0, stream.end, stream.in);  // get the stream buffer size
    buf->pubseekpos(0, stream.in);                                     // reset pointer pos
    std::shared_ptr<int8_t[]> binary_data(new int8_t[size]);
    buf->sgetn((char*)binary_data.get(), size);
    Binary binary{
        .data = binary_data,
        .size = (size_t)size,
    };
    return std::move(binary);
}

void
convert_binary_to_stream(const Binary& binary, std::stringstream& stream) {
    stream.str("");
    if (binary.data && binary.size > 0) {
        stream.write((const char*)binary.data.get(), binary.size);
    }
}

DiskANN::DiskANN(diskann::Metric metric,
                 std::string data_type,
                 int L,
                 int R,
                 float p_val,
                 size_t disk_pq_dims,
                 int64_t dim,
                 bool preload,
                 bool use_reference,
                 bool use_opq,
                 bool use_bsa,
                 bool use_async_io)
    : metric_(metric),
      L_(L),
      R_(R),
      p_val_(p_val),
      data_type_(data_type),
      disk_pq_dims_(disk_pq_dims),
      dim_(dim),
      preload_(preload),
      use_reference_(use_reference),
      use_opq_(use_opq),
      use_bsa_(use_bsa),
      use_async_io_(use_async_io) {
    if (not use_async_io_) {
        pool_ = std::make_unique<ThreadPool>(Option::Instance().num_threads_io());
    }
    status_ = IndexStatus::EMPTY;
    batch_read_ =
        [&](const std::vector<read_request>& requests, bool async, CallBack callBack) -> void {
        if (async) {
            for (const auto& req : requests) {
                auto [offset, len, dest] = req;
                disk_layout_reader_->AsyncRead(offset, len, dest, callBack);
            }
        } else {
            if (not pool_) {
                for (const auto& req : requests) {
                    auto [offset, len, dest] = req;
                    disk_layout_reader_->Read(offset, len, dest);
                }
            } else {
                std::vector<std::future<void>> futures;
                for (const auto& req : requests) {
                    auto future = pool_->enqueue([&, req]() {
                        auto [offset, len, dest] = req;
                        disk_layout_reader_->Read(offset, len, dest);
                    });
                    futures.push_back(std::move(future));
                }

                for (auto& fut : futures) {
                    fut.get();
                }
            }
        }
    };

    R_ = std::min(MAXIMAL_R, std::max(MINIMAL_R, R_));

    // When the length of the vector is too long, set sector_len_ to the size of storing a vector along with its linkage list.
    sector_len_ =
        std::max(MINIMAL_SECTOR_LEN,
                 (size_t)(dim * sizeof(float) + (R_ * GRAPH_SLACK + 1) * sizeof(uint32_t)) *
                     VECTOR_PER_BLOCK);
}

tl::expected<std::vector<int64_t>, Error>
DiskANN::build(const DatasetPtr& base) {
    try {
        if (base->GetNumElements() == 0) {
            empty_index_ = true;
            return std::vector<int64_t>();
        }

        auto data_dim = base->GetDim();
        CHECK_ARGUMENT(data_dim == dim_,
                       fmt::format("base.dim({}) must be equal to index.dim({})", data_dim, dim_));

        std::unique_lock lock(rw_mutex_);

        if (this->index_) {
            LOG_ERROR_AND_RETURNS(ErrorType::BUILD_TWICE, "failed to build index: build twice");
        }

        auto vectors = base->GetFloat32Vectors();
        auto ids = base->GetIds();
        auto data_num = base->GetNumElements();

        std::vector<size_t> failed_locs;
        {
            SlowTaskTimer t("diskann build full (graph)");
            // build graph
            build_index_ = std::make_shared<diskann::Index<float, int64_t, int64_t>>(
                metric_, data_dim, data_num, false, true, false, false, 0, false);
            std::vector<int64_t> tags(ids, ids + data_num);
            auto index_build_params =
                diskann::IndexWriteParametersBuilder(L_, R_)
                    .with_num_threads(Options::Instance().num_threads_building())
                    .build();
            failed_locs =
                build_index_->build(vectors, data_num, index_build_params, tags, use_reference_);
            build_index_->save(graph_stream_, tag_stream_);
            build_index_.reset();
        }
        {
            SlowTaskTimer t("diskann build full (pq)");
            diskann::generate_disk_quantized_data<float>(vectors,
                                                         data_num,
                                                         data_dim,
                                                         failed_locs,
                                                         pq_pivots_stream_,
                                                         disk_pq_compressed_vectors_,
                                                         metric_,
                                                         p_val_,
                                                         disk_pq_dims_,
                                                         use_opq_,
                                                         use_bsa_);
        }
        {
            SlowTaskTimer t("diskann build full (disk layout)");
            diskann::create_disk_layout<float>(vectors,
                                               data_num,
                                               data_dim,
                                               failed_locs,
                                               graph_stream_,
                                               disk_layout_stream_,
                                               sector_len_,
                                               "");
        }

        std::vector<int64_t> failed_ids;
        std::transform(failed_locs.begin(),
                       failed_locs.end(),
                       std::back_inserter(failed_ids),
                       [&ids](const auto& index) { return ids[index]; });

        disk_layout_reader_ =
            std::make_shared<LocalMemoryReader>(disk_layout_stream_, use_async_io_);
        reader_.reset(new LocalFileReader(batch_read_));
        index_.reset(new diskann::PQFlashIndex<float, int64_t>(
            reader_, metric_, sector_len_, dim_, use_bsa_));
        index_->load_from_separate_paths(
            pq_pivots_stream_, disk_pq_compressed_vectors_, tag_stream_);
        if (preload_) {
            index_->load_graph(graph_stream_);
        } else {
            graph_stream_.clear();
        }
        status_ = IndexStatus::MEMORY;
        return failed_ids;
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(
            ErrorType::INVALID_ARGUMENT, "failed to build(invalid argument): ", e.what());
    }
}

tl::expected<DatasetPtr, Error>
DiskANN::knn_search(const DatasetPtr& query,
                    int64_t k,
                    const std::string& parameters,
                    BitsetPtr invalid) const {
    // check filter
    std::function<bool(int64_t)> filter = nullptr;
    if (invalid != nullptr) {
        filter = [invalid](int64_t offset) -> bool {
            int64_t bit_index = offset & ROW_ID_MASK;
            return invalid->Test(bit_index);
        };
    }
    return this->knn_search(query, k, parameters, filter);
};

tl::expected<DatasetPtr, Error>
DiskANN::knn_search(const DatasetPtr& query,
                    int64_t k,
                    const std::string& parameters,
                    const std::function<bool(int64_t)>& filter) const {
    SlowTaskTimer t("diskann knnsearch", 200);

    // cannot perform search on empty index
    if (empty_index_) {
        auto ret = Dataset::Make();
        ret->Dim(0)->NumElements(1);
        return ret;
    }

    try {
        if (!index_) {
            LOG_ERROR_AND_RETURNS(ErrorType::INDEX_EMPTY,
                                  "failed to search: diskann index is empty");
        }

        // check query vector
        auto query_num = query->GetNumElements();
        auto query_dim = query->GetDim();
        CHECK_ARGUMENT(
            query_dim == dim_,
            fmt::format("query.dim({}) must be equal to index.dim({})", query_dim, dim_));

        // check k
        CHECK_ARGUMENT(k > 0, fmt::format("k({}) must be greater than 0", k))
        k = std::min(k, GetNumElements());

        // check search parameters
        auto params = DiskannSearchParameters::FromJson(parameters);
        int64_t ef_search = params.ef_search;
        size_t beam_search = params.beam_search;
        int64_t io_limit = params.io_limit;
        bool reorder = params.use_reorder;

        // ensure that in the topK scenario, ef_search > io_limit and io_limit > k.
        if (reorder && preload_) {
            ef_search = std::max(2 * k, ef_search);
            io_limit = std::max(2 * k, io_limit);
        } else {
            ef_search = std::max(ef_search, k);
            io_limit = std::min(ef_search, std::max(io_limit, k));
        }
        io_limit = std::min(io_limit, GetNumElements());
        ef_search = std::min(ef_search, GetNumElements());

        beam_search = std::min(beam_search, MAXIMAL_BEAM_SEARCH);
        beam_search = std::max(beam_search, MINIMAL_BEAM_SEARCH);

        uint64_t labels[query_num * k];
        auto distances = new float[query_num * k];
        auto ids = new int64_t[query_num * k];
        diskann::QueryStats query_stats[query_num];
        for (int i = 0; i < query_num; i++) {
            try {
                double time_cost = 0;
                {
                    std::shared_lock lock(rw_mutex_);
                    Timer timer(time_cost);
                    if (preload_) {
                        if (use_async_io_) {
                            k = index_->cached_beam_search_async(
                                query->GetFloat32Vectors() + i * dim_,
                                k,
                                ef_search,
                                labels + i * k,
                                distances + i * k,
                                beam_search,
                                filter,
                                io_limit,
                                reorder,
                                query_stats + i);
                        } else {
                            k = index_->cached_beam_search_memory(
                                query->GetFloat32Vectors() + i * dim_,
                                k,
                                ef_search,
                                labels + i * k,
                                distances + i * k,
                                beam_search,
                                filter,
                                io_limit,
                                reorder,
                                query_stats + i);
                        }
                    } else {
                        k = index_->cached_beam_search(query->GetFloat32Vectors() + i * dim_,
                                                       k,
                                                       ef_search,
                                                       labels + i * k,
                                                       distances + i * k,
                                                       beam_search,
                                                       filter,
                                                       io_limit,
                                                       false,
                                                       query_stats + i);
                    }
                }
                {
                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    result_queues_[STATSTIC_KNN_IO].Push(query_stats[i].n_ios);
                    result_queues_[STATSTIC_KNN_TIME].Push(time_cost);
                    result_queues_[STATSTIC_KNN_IO_TIME].Push(
                        (query_stats[i].io_us / query_stats[i].n_ios) / MACRO_TO_MILLI);
                }

            } catch (const std::runtime_error& e) {
                delete[] distances;
                delete[] ids;
                LOG_ERROR_AND_RETURNS(ErrorType::INTERNAL_ERROR,
                                      "failed to perform knn search on diskann: ",
                                      e.what());
            }
        }

        auto result = Dataset::Make();
        result->NumElements(query->GetNumElements())->Dim(0);

        if (k == 0) {
            delete[] distances;
            delete[] ids;
            return std::move(result);
        }
        for (int i = 0; i < query_num * k; ++i) {
            ids[i] = static_cast<int64_t>(labels[i]);
        }

        result->NumElements(query_num)->Dim(k)->Distances(distances)->Ids(ids);
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

tl::expected<DatasetPtr, Error>
DiskANN::range_search(const DatasetPtr& query,
                      float radius,
                      const std::string& parameters,
                      BitsetPtr invalid,
                      int64_t limited_size) const {
    // check filter
    std::function<bool(int64_t)> filter = nullptr;
    if (invalid != nullptr) {
        filter = [invalid](int64_t offset) -> bool {
            int64_t bit_index = offset & ROW_ID_MASK;
            return invalid->Test(bit_index);
        };
    }
    return this->range_search(query, radius, parameters, filter, limited_size);
};

tl::expected<DatasetPtr, Error>
DiskANN::range_search(const DatasetPtr& query,
                      float radius,
                      const std::string& parameters,
                      const std::function<bool(int64_t)>& filter,
                      int64_t limited_size) const {
    SlowTaskTimer t("diskann rangesearch", 200);

    // cannot perform search on empty index
    if (empty_index_) {
        auto ret = Dataset::Make();
        ret->Dim(0)->NumElements(1);
        return ret;
    }

    try {
        if (!index_) {
            LOG_ERROR_AND_RETURNS(
                ErrorType::INDEX_EMPTY,
                fmt::format("failed to search: {} index is empty", INDEX_DISKANN));
        }

        // check query vector
        int64_t query_num = query->GetNumElements();
        int64_t query_dim = query->GetDim();
        CHECK_ARGUMENT(
            query_dim == dim_,
            fmt::format("query.dim({}) must be equal to index.dim({})", query_dim, dim_));

        // check radius
        CHECK_ARGUMENT(radius >= 0, fmt::format("radius({}) must be greater equal than 0", radius))
        CHECK_ARGUMENT(query_num == 1, fmt::format("query.num({}) must be equal to 1", query_num));

        // check limited_size
        CHECK_ARGUMENT(limited_size != 0,
                       fmt::format("limited_size({}) must not be equal to 0", limited_size))

        // check search parameters
        auto params = DiskannSearchParameters::FromJson(parameters);
        size_t beam_search = params.beam_search;
        int64_t ef_search = params.ef_search;
        CHECK_ARGUMENT(ef_search > 0,
                       fmt::format("ef_search({}) must be greater than 0", ef_search));

        bool reorder = params.use_reorder;
        int64_t io_limit = params.io_limit;

        beam_search = std::min(beam_search, MAXIMAL_BEAM_SEARCH);
        beam_search = std::max(beam_search, MINIMAL_BEAM_SEARCH);

        std::vector<uint64_t> labels;
        std::vector<float> range_distances;
        diskann::QueryStats query_stats;
        try {
            double time_cost = 0;
            {
                std::shared_lock lock(rw_mutex_);
                Timer timer(time_cost);
                index_->range_search(query->GetFloat32Vectors(),
                                     radius,
                                     ef_search,
                                     ef_search * 2,
                                     labels,
                                     range_distances,
                                     beam_search,
                                     io_limit,
                                     reorder,
                                     filter,
                                     preload_,
                                     &query_stats);
            }
            {
                std::lock_guard<std::mutex> lock(stats_mutex_);

                result_queues_[STATSTIC_RANGE_IO].Push(query_stats.n_ios);
                result_queues_[STATSTIC_RANGE_HOP].Push(query_stats.n_hops);
                result_queues_[STATSTIC_RANGE_TIME].Push(time_cost);
                result_queues_[STATSTIC_RANGE_CACHE_HIT].Push(query_stats.n_cache_hits);
                result_queues_[STATSTIC_RANGE_IO_TIME].Push(
                    (query_stats.io_us / query_stats.n_ios) / MACRO_TO_MILLI);
            }
        } catch (const std::runtime_error& e) {
            LOG_ERROR_AND_RETURNS(
                ErrorType::INTERNAL_ERROR, "failed to perform range search on diskann: ", e.what());
        }

        int64_t k = labels.size();
        size_t target_size = k;

        auto result = Dataset::Make();
        if (k == 0) {
            return std::move(result);
        }
        if (limited_size >= 1) {
            target_size = std::min((size_t)limited_size, target_size);
        }

        auto dis = new float[target_size];
        auto ids = new int64_t[target_size];
        for (int i = 0; i < target_size; ++i) {
            ids[i] = static_cast<int64_t>(labels[i]);
            dis[i] = range_distances[i];
        }

        result->NumElements(query_num)->Dim(target_size)->Distances(dis)->Ids(ids);
        return std::move(result);
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::INVALID_ARGUMENT,
                              "falied to perform range_search(invalid argument): ",
                              e.what());
    } catch (const std::bad_alloc& e) {
        LOG_ERROR_AND_RETURNS(ErrorType::NO_ENOUGH_MEMORY,
                              "failed to perform range_search(not enough memory): ",
                              e.what());
    }
}

BinarySet
DiskANN::empty_binaryset() const {
    // version 0 pairs:
    // - hnsw_blank: b"EMPTY_DISKANN"
    const std::string empty_str = "EMPTY_DISKANN";
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
DiskANN::serialize() const {
    if (status_ == IndexStatus::EMPTY) {
        // return a special binaryset means empty
        return empty_binaryset();
    }

    SlowTaskTimer t("diskann serialize");
    try {
        std::shared_lock lock(rw_mutex_);
        BinarySet bs;

        bs.Set(DISKANN_PQ, convert_stream_to_binary(pq_pivots_stream_));
        bs.Set(DISKANN_COMPRESSED_VECTOR, convert_stream_to_binary(disk_pq_compressed_vectors_));
        bs.Set(DISKANN_LAYOUT_FILE, convert_stream_to_binary(disk_layout_stream_));
        bs.Set(DISKANN_TAG_FILE, convert_stream_to_binary(tag_stream_));
        if (preload_) {
            bs.Set(DISKANN_GRAPH, convert_stream_to_binary(graph_stream_));
        }
        return bs;
    } catch (const std::bad_alloc& e) {
        return tl::unexpected(Error(ErrorType::NO_ENOUGH_MEMORY, ""));
    }
}

tl::expected<void, Error>
DiskANN::deserialize(const BinarySet& binary_set) {
    SlowTaskTimer t("diskann deserialize");
    std::unique_lock lock(rw_mutex_);
    if (this->index_) {
        LOG_ERROR_AND_RETURNS(ErrorType::INDEX_NOT_EMPTY,
                              "failed to deserialize: index is not empty")
    }

    // check if binaryset is a empty index
    if (binary_set.Contains(BLANK_INDEX)) {
        empty_index_ = true;
        return {};
    }

    convert_binary_to_stream(binary_set.Get(DISKANN_LAYOUT_FILE), disk_layout_stream_);
    auto graph = binary_set.Get(DISKANN_GRAPH);
    if (preload_) {
        if (graph.data) {
            convert_binary_to_stream(graph, graph_stream_);
        } else {
            LOG_ERROR_AND_RETURNS(
                ErrorType::MISSING_FILE,
                fmt::format("missing file: {} when deserialize diskann index", DISKANN_GRAPH));
        }
    } else {
        if (graph.data) {
            logger::warn("serialize without using file: {} ", DISKANN_GRAPH);
        }
    }
    load_disk_index(binary_set);
    status_ = IndexStatus::MEMORY;

    return {};
}

tl::expected<void, Error>
DiskANN::deserialize(const ReaderSet& reader_set) {
    SlowTaskTimer t("diskann deserialize");

    std::unique_lock lock(rw_mutex_);
    if (this->index_) {
        LOG_ERROR_AND_RETURNS(ErrorType::INDEX_NOT_EMPTY,
                              fmt::format("failed to deserialize: {} is not empty", INDEX_DISKANN));
    }

    // check if readerset is a empty index
    if (reader_set.Contains(BLANK_INDEX)) {
        empty_index_ = true;
        return {};
    }

    std::stringstream pq_pivots_stream, disk_pq_compressed_vectors, graph, tag_stream;

    {
        auto pq_reader = reader_set.Get(DISKANN_PQ);
        auto pq_pivots_data = std::make_unique<char[]>(pq_reader->Size());
        pq_reader->Read(0, pq_reader->Size(), pq_pivots_data.get());
        pq_pivots_stream.write(pq_pivots_data.get(), pq_reader->Size());
        pq_pivots_stream.seekg(0);
    }

    {
        auto compressed_vector_reader = reader_set.Get(DISKANN_COMPRESSED_VECTOR);
        auto compressed_vector_data = std::make_unique<char[]>(compressed_vector_reader->Size());
        compressed_vector_reader->Read(
            0, compressed_vector_reader->Size(), compressed_vector_data.get());
        disk_pq_compressed_vectors.write(compressed_vector_data.get(),
                                         compressed_vector_reader->Size());
        disk_pq_compressed_vectors.seekg(0);
    }

    {
        auto tag_reader = reader_set.Get(DISKANN_TAG_FILE);
        auto tag_data = std::make_unique<char[]>(tag_reader->Size());
        tag_reader->Read(0, tag_reader->Size(), tag_data.get());
        tag_stream.write(tag_data.get(), tag_reader->Size());
        tag_stream.seekg(0);
    }

    disk_layout_reader_ = reader_set.Get(DISKANN_LAYOUT_FILE);
    reader_.reset(new LocalFileReader(batch_read_));
    index_.reset(
        new diskann::PQFlashIndex<float, int64_t>(reader_, metric_, sector_len_, dim_, use_bsa_));
    index_->load_from_separate_paths(pq_pivots_stream, disk_pq_compressed_vectors, tag_stream);

    auto graph_reader = reader_set.Get(DISKANN_GRAPH);
    if (preload_) {
        if (graph_reader) {
            auto graph_data = std::make_unique<char[]>(graph_reader->Size());
            graph_reader->Read(0, graph_reader->Size(), graph_data.get());
            graph.write(graph_data.get(), graph_reader->Size());
            graph.seekg(0);
            index_->load_graph(graph);
        } else {
            LOG_ERROR_AND_RETURNS(
                ErrorType::MISSING_FILE,
                fmt::format("miss file: {} when deserialize diskann index", DISKANN_GRAPH));
        }
    } else {
        if (graph_reader) {
            logger::warn("serialize without using file: {} ", DISKANN_GRAPH);
        }
    }
    status_ = IndexStatus::HYBRID;

    return {};
}

std::string
DiskANN::GetStats() const {
    nlohmann::json j;
    j[STATSTIC_DATA_NUM] = GetNumElements();
    j[STATSTIC_INDEX_NAME] = INDEX_DISKANN;
    j[STATSTIC_MEMORY] = GetMemoryUsage();

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        for (auto& item : result_queues_) {
            j[item.first] = item.second.GetAvgResult();
        }
    }

    return j.dump();
}

int64_t
DiskANN::GetEstimateBuildMemory(const int64_t num_elements) const {
    int64_t estimate_memory_usage = 0;
    // Memory usage of graph (1.365 is the relaxation factor used by DiskANN during graph construction.)
    estimate_memory_usage +=
        (num_elements * R_ * sizeof(uint32_t) + num_elements * (R_ + 1) * sizeof(uint32_t)) *
        GRAPH_SLACK;
    // Memory usage of disk layout
    if (sector_len_ > MINIMAL_SECTOR_LEN) {
        estimate_memory_usage += (num_elements + 1) * sector_len_ * sizeof(uint8_t);
    } else {
        size_t single_node =
            (size_t)(dim_ * sizeof(float) + (R_ * GRAPH_SLACK + 1) * sizeof(uint32_t)) *
            VECTOR_PER_BLOCK;
        size_t node_per_sector = MINIMAL_SECTOR_LEN / single_node;
        size_t sector_size = num_elements / node_per_sector + 1;
        estimate_memory_usage += (sector_size + 1) * sector_len_ * sizeof(uint8_t);
    }
    // Memory usage of the ID mapping.
    estimate_memory_usage += num_elements * sizeof(int64_t) * 2;
    // Memory usage of the compressed PQ vectors.
    estimate_memory_usage += disk_pq_dims_ * num_elements * sizeof(uint8_t) * 2;
    // Memory usage of the PQ centers and chunck offsets.
    estimate_memory_usage += 256 * dim_ * sizeof(float) * (3 + 1) + dim_ * sizeof(uint32_t);
    return estimate_memory_usage;
}

template <typename Container>
Binary
serialize_to_binary(const Container& container) {
    using ValueType = typename Container::value_type;
    size_t total_size = container.size() * sizeof(ValueType);
    std::shared_ptr<int8_t[]> raw_data(new int8_t[total_size], std::default_delete<int8_t[]>());

    int8_t* data_ptr = raw_data.get();
    for (const ValueType& value : container) {
        std::memcpy(data_ptr, &value, sizeof(ValueType));
        data_ptr += sizeof(ValueType);
    }

    Binary binary_data{raw_data, total_size};
    return binary_data;
}

template <typename Container>
Container
deserialize_from_binary(const Binary& binary_data) {
    using ValueType = typename Container::value_type;

    Container deserialized_container;
    const int8_t* data_ptr = binary_data.data.get();
    size_t num_elements = binary_data.size / sizeof(ValueType);

    for (size_t i = 0; i < num_elements; ++i) {
        ValueType value;
        std::memcpy(&value, data_ptr, sizeof(ValueType));
        deserialized_container.insert(deserialized_container.end(), value);
        data_ptr += sizeof(ValueType);
    }

    return std::move(deserialized_container);
}

template <typename T>
Binary
serialize_vector_to_binary(std::vector<T> data) {
    if (data.empty()) {
        return Binary();
    }
    size_t total_size = data.size() * sizeof(T);
    std::shared_ptr<int8_t[]> raw_data(new int8_t[total_size], std::default_delete<int8_t[]>());
    int8_t* data_ptr = raw_data.get();
    std::memcpy(data_ptr, data.data(), total_size);
    Binary binary_data{raw_data, total_size};
    return binary_data;
}

template <typename T>
std::vector<T>
deserialize_vector_from_binary(const Binary& binary_data) {
    std::vector<T> deserialized_container;
    if (binary_data.size == 0) {
        return std::move(deserialized_container);
    }
    const int8_t* data_ptr = binary_data.data.get();
    size_t num_elements = binary_data.size / sizeof(T);
    deserialized_container.resize(num_elements);
    std::memcpy(deserialized_container.data(), data_ptr, num_elements * sizeof(T));
    return std::move(deserialized_container);
}

tl::expected<Index::Checkpoint, Error>
DiskANN::continue_build(const DatasetPtr& base, const BinarySet& binary_set) {
    std::unique_lock lock(rw_mutex_);
    try {
        BuildStatus build_status = BuildStatus::BEGIN;
        if (not binary_set.GetKeys().empty()) {
            Binary status_binary = binary_set.Get(BUILD_STATUS);
            CHECK_ARGUMENT(status_binary.data != nullptr, "missing status while partial building");
            build_status = from_binary<BuildStatus>(status_binary);
        }
        CHECK_ARGUMENT(
            base->GetDim() == dim_,
            fmt::format("base.dim({}) must be equal to index.dim({})", base->GetDim(), dim_));
        CHECK_ARGUMENT(
            base->GetNumElements() >= DATA_LIMIT,
            "number of elements must be greater equal than " + std::to_string(DATA_LIMIT));
        if (this->index_ && status_ != IndexStatus::BUILDING) {
            LOG_ERROR_AND_RETURNS(ErrorType::BUILD_TWICE, "failed to build index: build twice");
        }
        status_ = IndexStatus::BUILDING;
        BinarySet after_binary_set;
        switch (build_status) {
            case BEGIN: {
                int round = 1;
                SlowTaskTimer t(fmt::format("diskann build graph {}/{}", round, build_batch_num_));
                build_status = BuildStatus::GRAPH;
                build_partial_graph(base, binary_set, after_binary_set, round);
                round++;
                after_binary_set.Set(BUILD_CURRENT_ROUND, to_binary<int>(round));
                break;
            }
            case GRAPH: {
                int round = from_binary<int>(binary_set.Get(BUILD_CURRENT_ROUND));
                SlowTaskTimer t(
                    fmt::format("diskann build (graph {}/{})", round, build_batch_num_));
                build_partial_graph(base, binary_set, after_binary_set, round);
                if (round < build_batch_num_) {
                    round++;
                } else {
                    build_status = BuildStatus::EDGE_PRUNE;
                }
                after_binary_set.Set(BUILD_CURRENT_ROUND, to_binary<int>(round));
                break;
            }
            case EDGE_PRUNE: {
                SlowTaskTimer t(fmt::format("diskann build (edge prune)"));
                int round = from_binary<int>(binary_set.Get(BUILD_CURRENT_ROUND));
                build_partial_graph(base, binary_set, after_binary_set, round);
                build_status = BuildStatus::PQ;
                break;
            }
            case PQ: {
                SlowTaskTimer t(fmt::format("diskann build (pq)"));
                auto failed_locs =
                    deserialize_vector_from_binary<size_t>(after_binary_set.Get(BUILD_FAILED_LOC));
                diskann::generate_disk_quantized_data<float>(base->GetFloat32Vectors(),
                                                             base->GetNumElements(),
                                                             dim_,
                                                             failed_locs,
                                                             pq_pivots_stream_,
                                                             disk_pq_compressed_vectors_,
                                                             metric_,
                                                             p_val_,
                                                             disk_pq_dims_,
                                                             use_opq_);
                after_binary_set = binary_set;
                after_binary_set.Set(DISKANN_PQ, convert_stream_to_binary(pq_pivots_stream_));
                after_binary_set.Set(DISKANN_COMPRESSED_VECTOR,
                                     convert_stream_to_binary(disk_pq_compressed_vectors_));
                build_status = BuildStatus::DISK_LAYOUT;
                break;
            }
            case DISK_LAYOUT: {
                SlowTaskTimer t(fmt::format("diskann build (disk layout)"));
                auto failed_locs =
                    deserialize_vector_from_binary<size_t>(after_binary_set.Get(BUILD_FAILED_LOC));
                convert_binary_to_stream(binary_set.Get(DISKANN_GRAPH), graph_stream_);
                diskann::create_disk_layout<float>(base->GetFloat32Vectors(),
                                                   base->GetNumElements(),
                                                   dim_,
                                                   failed_locs,
                                                   graph_stream_,
                                                   disk_layout_stream_,
                                                   sector_len_,
                                                   "");
                load_disk_index(binary_set);
                build_status = BuildStatus::FINISH;
                status_ = IndexStatus::MEMORY;
                break;
            }
            case FINISH:
                logger::warn("build process is finished");
        }
        after_binary_set.Set(BUILD_STATUS, to_binary<BuildStatus>(build_status));
        Checkpoint checkpoint{.data = after_binary_set,
                              .finish = build_status == BuildStatus::FINISH};
        return checkpoint;
    } catch (const std::invalid_argument& e) {
        LOG_ERROR_AND_RETURNS(
            ErrorType::INVALID_ARGUMENT, "failed to build(invalid argument): ", e.what());
    }
}

tl::expected<void, Error>
DiskANN::build_partial_graph(const DatasetPtr& base,
                             const BinarySet& binary_set,
                             BinarySet& after_binary_set,
                             int round) {
    auto vectors = base->GetFloat32Vectors();
    auto ids = base->GetIds();
    auto data_num = base->GetNumElements();
    std::vector<int64_t> tags(ids, ids + data_num);
    {
        // build graph
        build_index_ = std::make_shared<diskann::Index<float, int64_t, int64_t>>(
            metric_, dim_, data_num, false, true, false, false, 0, false);

        std::unordered_set<uint32_t> builded_nodes;
        if (round > 1) {
            std::stringstream graph_stream, tag_stream;
            convert_binary_to_stream(binary_set.Get(DISKANN_GRAPH), graph_stream);
            convert_binary_to_stream(binary_set.Get(DISKANN_TAG_FILE), tag_stream);

            build_index_->load(graph_stream, tag_stream, Options::Instance().num_threads_io(), L_);
            builded_nodes =
                deserialize_from_binary<std::unordered_set<uint32_t>>(binary_set.Get(BUILD_NODES));
        }

        auto index_build_params = diskann::IndexWriteParametersBuilder(L_, R_)
                                      .with_num_threads(Options::Instance().num_threads_building())
                                      .build();
        std::vector<size_t> failed_locs = build_index_->build(vectors,
                                                              dim_,
                                                              index_build_params,
                                                              tags,
                                                              use_reference_,
                                                              round,
                                                              build_batch_num_,
                                                              &builded_nodes);
        build_index_->save(graph_stream_, tag_stream_);
        after_binary_set.Set(BUILD_NODES,
                             serialize_to_binary<std::unordered_set<uint32_t>>(builded_nodes));
        after_binary_set.Set(BUILD_FAILED_LOC, serialize_vector_to_binary<size_t>(failed_locs));
        build_index_.reset();
    }
    after_binary_set.Set(DISKANN_GRAPH, convert_stream_to_binary(graph_stream_));
    after_binary_set.Set(DISKANN_TAG_FILE, convert_stream_to_binary(tag_stream_));
    return tl::expected<void, Error>();
}

tl::expected<void, Error>
DiskANN::load_disk_index(const BinarySet& binary_set) {
    disk_layout_reader_ = std::make_shared<LocalMemoryReader>(disk_layout_stream_, use_async_io_);
    reader_.reset(new LocalFileReader(batch_read_));
    index_.reset(
        new diskann::PQFlashIndex<float, int64_t>(reader_, metric_, sector_len_, dim_, use_bsa_));

    convert_binary_to_stream(binary_set.Get(DISKANN_COMPRESSED_VECTOR),
                             disk_pq_compressed_vectors_);
    convert_binary_to_stream(binary_set.Get(DISKANN_PQ), pq_pivots_stream_);
    convert_binary_to_stream(binary_set.Get(DISKANN_TAG_FILE), tag_stream_);
    index_->load_from_separate_paths(pq_pivots_stream_, disk_pq_compressed_vectors_, tag_stream_);
    if (preload_) {
        index_->load_graph(graph_stream_);
    } else {
        graph_stream_.str("");
    }
    return tl::expected<void, Error>();
}

}  // namespace vsag
