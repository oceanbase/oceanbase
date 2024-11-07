
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

#include <ThreadPool.h>
#include <abstract_index.h>
#include <disk_utils.h>
#include <index.h>
#include <omp.h>
#include <pq_flash_index.h>

#include <functional>
#include <map>
#include <nlohmann/json.hpp>
#include <queue>
#include <shared_mutex>
#include <string>

#include "../common.h"
#include "../logger.h"
#include "../utils.h"
#include "vsag/index.h"
#include "vsag/options.h"

using ThreadPool = progschj::ThreadPool;

namespace vsag {

enum IndexStatus { EMPTY = 0, MEMORY = 1, HYBRID = 2, BUILDING = 3 };

enum BuildStatus { BEGIN = 0, GRAPH = 1, EDGE_PRUNE = 2, PQ = 3, DISK_LAYOUT = 4, FINISH = 5 };

class DiskANN : public Index {
public:
    using rs = std::pair<float, size_t>;

    // offset: uint64, len: uint64, dest: void*
    using read_request = std::tuple<uint64_t, uint64_t, void*>;

    DiskANN(diskann::Metric metric,
            std::string data_type,
            int L,
            int R,
            float p_val,
            size_t disk_pq_dims,
            int64_t dim,
            bool preload,
            bool use_reference = true,
            bool use_opq = false,
            bool use_bsa = false,
            bool use_async_io = false);

    ~DiskANN() = default;

    tl::expected<std::vector<int64_t>, Error>
    Build(const DatasetPtr& base) override {
        SAFE_CALL(return this->build(base));
    }

    tl::expected<Checkpoint, Error>
    ContinueBuild(const DatasetPtr& base, const BinarySet& binary_set) override {
        SAFE_CALL(return this->continue_build(base, binary_set));
    }

    tl::expected<DatasetPtr, Error>
    KnnSearch(const DatasetPtr& query,
              int64_t k,
              const std::string& parameters,
              BitsetPtr invalid = nullptr) const override {
        SAFE_CALL(return this->knn_search(query, k, parameters, invalid));
    }

    tl::expected<DatasetPtr, Error>
    KnnSearch(const DatasetPtr& query,
              int64_t k,
              const std::string& parameters,
              const std::function<bool(int64_t)>& filter) const override {
        SAFE_CALL(return this->knn_search(query, k, parameters, filter));
    }

    tl::expected<DatasetPtr, Error>
    RangeSearch(const DatasetPtr& query,
                float radius,
                const std::string& parameters,
                int64_t limited_size = -1) const override {
        SAFE_CALL(return this->range_search(
            query, radius, parameters, (BitsetPtr) nullptr, limited_size));
    }

    tl::expected<DatasetPtr, Error>
    RangeSearch(const DatasetPtr& query,
                float radius,
                const std::string& parameters,
                const std::function<bool(int64_t)>& filter,
                int64_t limited_size = -1) const override {
        SAFE_CALL(return this->range_search(query, radius, parameters, filter, limited_size));
    }

    tl::expected<DatasetPtr, Error>
    RangeSearch(const DatasetPtr& query,
                float radius,
                const std::string& parameters,
                BitsetPtr invalid,
                int64_t limited_size = -1) const override {
        SAFE_CALL(return this->range_search(query, radius, parameters, invalid, limited_size));
    }

public:
    tl::expected<BinarySet, Error>
    Serialize() const override {
        SAFE_CALL(return this->serialize());
    }

    tl::expected<void, Error>
    Deserialize(const BinarySet& binary_set) override {
        SAFE_CALL(return this->deserialize(binary_set));
    }

    tl::expected<void, Error>
    Deserialize(const ReaderSet& reader_set) override {
        SAFE_CALL(return this->deserialize(reader_set));
    }

public:
    int64_t
    GetNumElements() const override {
        if (status_ == EMPTY)
            return 0;
        return index_->get_data_num();
    }

    int64_t
    GetMemoryUsage() const override {
        if (status_ == MEMORY) {
            return index_->get_memory_usage() + disk_pq_compressed_vectors_.str().size() +
                   pq_pivots_stream_.str().size() + disk_layout_stream_.str().size() +
                   tag_stream_.str().size() + graph_stream_.str().size();
        } else if (status_ == HYBRID) {
            return index_->get_memory_usage();
        }
        return 0;
    }

    int64_t
    GetEstimateBuildMemory(const int64_t num_elements) const override;

    std::string
    GetStats() const override;

private:
    tl::expected<std::vector<int64_t>, Error>
    build(const DatasetPtr& base);

    tl::expected<Checkpoint, Error>
    continue_build(const DatasetPtr& base, const BinarySet& binary_set);

    tl::expected<DatasetPtr, Error>
    knn_search(const DatasetPtr& query,
               int64_t k,
               const std::string& parameters,
               BitsetPtr invalid) const;

    tl::expected<DatasetPtr, Error>
    knn_search(const DatasetPtr& query,
               int64_t k,
               const std::string& parameters,
               const std::function<bool(int64_t)>& filter) const;

    tl::expected<DatasetPtr, Error>
    range_search(const DatasetPtr& query,
                 float radius,
                 const std::string& parameters,
                 const std::function<bool(int64_t)>& filter,
                 int64_t limited_size) const;

    tl::expected<DatasetPtr, Error>
    range_search(const DatasetPtr& query,
                 float radius,
                 const std::string& parameters,
                 BitsetPtr invalid,
                 int64_t limited_size) const;

    tl::expected<BinarySet, Error>
    serialize() const;

    tl::expected<void, Error>
    deserialize(const BinarySet& binary_set);

    tl::expected<void, Error>
    deserialize(const ReaderSet& reader_set);

    tl::expected<void, Error>
    build_partial_graph(const DatasetPtr& base,
                        const BinarySet& binary_set,
                        BinarySet& after_binary_set,
                        int round);

    tl::expected<void, Error>
    load_disk_index(const BinarySet& binary_set);

    BinarySet
    empty_binaryset() const;

private:
    std::shared_ptr<LocalFileReader> reader_;
    std::shared_ptr<diskann::PQFlashIndex<float, int64_t>> index_;
    std::shared_ptr<diskann::Index<float, int64_t, int64_t>> build_index_;
    std::stringstream pq_pivots_stream_;
    std::stringstream disk_pq_compressed_vectors_;
    std::stringstream disk_layout_stream_;
    std::stringstream tag_stream_;
    std::stringstream graph_stream_;

    std::function<void(const std::vector<read_request>&, bool, CallBack)> batch_read_;
    diskann::Metric metric_;
    std::shared_ptr<Reader> disk_layout_reader_;
    std::string data_type_;

    int L_ = 200;
    int R_ = 64;
    float p_val_ = 0.5;
    size_t disk_pq_dims_ = 8;
    size_t sector_len_;

    int64_t build_batch_num_ = 10;

    int64_t dim_;
    bool use_reference_ = true;
    bool use_opq_ = false;
    bool use_bsa_ = false;
    bool use_async_io_ = false;
    bool preload_;
    IndexStatus status_;
    bool empty_index_ = false;

    mutable std::shared_mutex rw_mutex_;

private:  // Request Statistics
    mutable std::mutex stats_mutex_;
    std::unique_ptr<ThreadPool> pool_;

    mutable std::map<std::string, WindowResultQueue> result_queues_;
};

}  // namespace vsag
