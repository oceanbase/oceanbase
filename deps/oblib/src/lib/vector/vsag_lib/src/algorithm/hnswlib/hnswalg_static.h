
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

#include <assert.h>
#include <pq.h>
#include <stdlib.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <list>
#include <random>
#include <stdexcept>
#include <unordered_set>
//#include <Eigen/Dense>
#include "../../default_allocator.h"
#include "hnswlib.h"
#include "visited_list_pool.h"

namespace hnswlib {
typedef unsigned int tableint;
typedef unsigned int linklistsizeint;
class StaticHierarchicalNSW : public AlgorithmInterface<float> {
private:
    static const tableint MAX_LABEL_OPERATION_LOCKS = 65536;
    static const unsigned char DELETE_MARK = 0x01;

    size_t max_elements_{0};
    mutable std::atomic<size_t> cur_element_count_{0};  // current number of elements
    size_t size_data_per_element_{0};
    size_t size_links_per_element_{0};
    mutable std::atomic<size_t> num_deleted_{0};  // number of deleted elements
    size_t M_{0};
    size_t maxM_{0};
    size_t maxM0_{0};
    size_t ef_construction_{0};

    double mult_{0.0}, revSize_{0.0};
    int maxlevel_{0};

    VisitedListPool* visited_list_pool_{nullptr};

    // Locks operations with element by label value
    mutable std::vector<std::mutex> label_op_locks_;

    std::mutex global;
    std::vector<std::mutex> link_list_locks_;

    tableint enterpoint_node_{0};

    size_t size_links_level0_{0};
    size_t offsetData_{0}, offsetLevel0_{0}, label_offset_{0};

    BlockManager* data_level0_memory_{nullptr};
    char** linkLists_{nullptr};
    int* element_levels_;  // keeps level of each element

    size_t data_size_{0};

    size_t data_element_per_block_{0};

    vsag::Allocator* allocator_;
    DISTFUNC fstdistfunc_;
    void* dist_func_param_{nullptr};

    mutable std::mutex label_lookup_lock;  // lock for label_lookup_
    std::unordered_map<labeltype, tableint> label_lookup_;

    std::default_random_engine level_generator_;
    std::default_random_engine update_probability_generator_;

    mutable std::atomic<long> metric_distance_computations{0};
    mutable std::atomic<long> metric_hops{0};

    bool allow_replace_deleted_ =
        false;  // flag to replace deleted elements (marked as deleted) during insertions

    std::mutex deleted_elements_lock;               // lock for deleted_elements
    std::unordered_set<tableint> deleted_elements;  // contains internal ids of deleted elements

    /*
     * PQ data only support float data
     */
    uint8_t* pq_map{nullptr};
    size_t pq_chunk{0}, pq_cluster{0}, pq_sub_dim{0}, pq_dim{0}, pq_train_bound = 65536;
    float* node_cluster_dist_{nullptr};
    float error_quantile = 0.995, err_quantile_value = 0.0;
    typedef std::vector<std::vector<std::vector<float>>> CodeBook;
    CodeBook pq_book;
    //    Eigen::MatrixXf A_;

    bool is_trained_infer = true, use_node_centroid = true, is_trained_pq = false;

public:
    StaticHierarchicalNSW(SpaceInterface* s) {
    }

    StaticHierarchicalNSW(SpaceInterface* s,
                          const std::string& location,
                          bool nmslib = false,
                          size_t max_elements = 0,
                          bool allow_replace_deleted = false)
        : allow_replace_deleted_(allow_replace_deleted) {
        loadIndex(location, s, max_elements);
    }

    StaticHierarchicalNSW(SpaceInterface* s,
                          size_t max_elements,
                          vsag::Allocator* allocator,
                          size_t M = 16,
                          size_t ef_construction = 200,
                          size_t block_size_limit = 128 * 1024 * 1024,
                          size_t random_seed = 100,
                          bool allow_replace_deleted = false)
        : allocator_(allocator),
          link_list_locks_(max_elements),
          label_op_locks_(MAX_LABEL_OPERATION_LOCKS),
          allow_replace_deleted_(allow_replace_deleted) {
        max_elements_ = max_elements;
        num_deleted_ = 0;
        data_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();
        M_ = M;
        maxM_ = M_;
        maxM0_ = M_ * 2;
        ef_construction_ = std::max(ef_construction, M_);

        element_levels_ = (int*)allocator_->Allocate(max_elements * sizeof(int));

        level_generator_.seed(random_seed);
        update_probability_generator_.seed(random_seed + 1);

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        size_data_per_element_ = size_links_level0_ + data_size_ + sizeof(labeltype);
        offsetData_ = size_links_level0_;
        label_offset_ = size_links_level0_ + data_size_;
        offsetLevel0_ = 0;

        data_level0_memory_ =
            new BlockManager(size_data_per_element_, block_size_limit, allocator_);
        data_level0_memory_->Resize(max_elements_);
        data_element_per_block_ = block_size_limit / size_data_per_element_;

        cur_element_count_ = 0;

        visited_list_pool_ = new VisitedListPool(1, max_elements, allocator_);

        // initializations for special treatment of the first node
        enterpoint_node_ = -1;
        maxlevel_ = -1;

        linkLists_ = (char**)allocator_->Allocate(sizeof(void*) * max_elements_);
        if (linkLists_ == nullptr)
            throw std::runtime_error(
                "Not enough memory: HierarchicalNSW failed to allocate linklists");
        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
        mult_ = 1 / log(1.0 * M_);
        revSize_ = 1.0 / mult_;
    }
    bool
    init_memory_space() override {
        return true;
    }
    ~StaticHierarchicalNSW() {
        delete data_level0_memory_;
        for (tableint i = 0; i < cur_element_count_; i++) {
            if (element_levels_[i] > 0)
                allocator_->Deallocate(linkLists_[i]);
        }
        allocator_->Deallocate(element_levels_);
        allocator_->Deallocate(linkLists_);
        delete visited_list_pool_;
        CodeBook().swap(pq_book);
        allocator_->Deallocate(pq_map);
        allocator_->Deallocate(node_cluster_dist_);
    }

    struct CompareByFirst {
        constexpr bool
        operator()(std::pair<float, tableint> const& a,
                   std::pair<float, tableint> const& b) const noexcept {
            return a.first < b.first;
        }
    };

    inline std::mutex&
    getLabelOpMutex(labeltype label) const {
        // calculate hash
        size_t lock_id = label & (MAX_LABEL_OPERATION_LOCKS - 1);
        return label_op_locks_[lock_id];
    }

    inline labeltype
    getExternalLabel(tableint internal_id) const {
        labeltype return_label;
        memcpy(&return_label,
               (data_level0_memory_->GetElementPtr(internal_id, label_offset_)),
               sizeof(labeltype));
        return return_label;
    }

    inline void
    setExternalLabel(tableint internal_id, labeltype label) const {
        memcpy((data_level0_memory_->GetElementPtr(internal_id, label_offset_)),
               &label,
               sizeof(labeltype));
    }

    inline labeltype*
    getExternalLabeLp(tableint internal_id) const {
        return (labeltype*)(data_level0_memory_->GetElementPtr(internal_id, label_offset_));
    }

    inline char*
    getDataByInternalId(tableint internal_id) const {
        return (data_level0_memory_->GetElementPtr(internal_id, offsetData_));
    }

    int
    getRandomLevel(double reverse_size) {
        std::uniform_real_distribution<double> distribution(0.0, 1.0);
        double r = -log(distribution(level_generator_)) * reverse_size;
        return (int)r;
    }

    size_t
    getMaxElements() override {
        return max_elements_;
    }

    size_t
    getCurrentElementCount() override {
        return cur_element_count_;
    }

    size_t
    getDeletedCount() override {
        return num_deleted_;
    }

    float
    getDistanceByLabel(labeltype label, const void* data_point) override {
        std::unique_lock<std::mutex> lock_table(label_lookup_lock);

        auto search = label_lookup_.find(label);
        if (search == label_lookup_.end()) {
            throw std::runtime_error("Label not found");
        }
        tableint internal_id = search->second;
        lock_table.unlock();

        float dist = fstdistfunc_(data_point, getDataByInternalId(internal_id), dist_func_param_);
        return dist;
    }

    bool
    isValidLabel(labeltype label) override {
        std::unique_lock<std::mutex> lock_table(label_lookup_lock);
        bool is_valid = (label_lookup_.find(label) != label_lookup_.end());
        lock_table.unlock();
        return is_valid;
    }

    std::priority_queue<std::pair<float, tableint>,
                        std::vector<std::pair<float, tableint>>,
                        CompareByFirst>
    searchBaseLayer(tableint ep_id, const void* data_point, int layer) {
        std::shared_ptr<VisitedList> vl = visited_list_pool_->getFreeVisitedList();
        vl_type* visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            top_candidates;
        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            candidateSet;

        float lowerBound;
        if (!isMarkedDeleted(ep_id)) {
            float dist = fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
            top_candidates.emplace(dist, ep_id);
            lowerBound = dist;
            candidateSet.emplace(-dist, ep_id);
        } else {
            lowerBound = std::numeric_limits<float>::max();
            candidateSet.emplace(-lowerBound, ep_id);
        }
        visited_array[ep_id] = visited_array_tag;

        while (!candidateSet.empty()) {
            std::pair<float, tableint> curr_el_pair = candidateSet.top();
            if ((-curr_el_pair.first) > lowerBound && top_candidates.size() == ef_construction_) {
                break;
            }
            candidateSet.pop();

            tableint curNodeNum = curr_el_pair.second;

            std::unique_lock<std::mutex> lock(link_list_locks_[curNodeNum]);

            int* data;  // = (int *)(linkList0_ + curNodeNum * size_links_per_element0_);
            if (layer == 0) {
                data = (int*)get_linklist0(curNodeNum);
            } else {
                data = (int*)get_linklist(curNodeNum, layer);
                //                    data = (int *) (linkLists_[curNodeNum] + (layer - 1) * size_links_per_element_);
            }
            size_t size = getListCount((linklistsizeint*)data);
            tableint* datal = (tableint*)(data + 1);
            auto tmp_data_ptr = getDataByInternalId(*datal);
            auto tmp_data_ptr2 = getDataByInternalId(*datal + 1);
#ifdef USE_SSE
            _mm_prefetch((char*)(visited_array + *(data + 1)), _MM_HINT_T0);
            _mm_prefetch((char*)(visited_array + *(data + 1) + 64), _MM_HINT_T0);
            _mm_prefetch(tmp_data_ptr, _MM_HINT_T0);
            _mm_prefetch(tmp_data_ptr2, _MM_HINT_T0);
#endif

            for (size_t j = 0; j < size; j++) {
                tableint candidate_id = *(datal + j);
                //                    if (candidate_id == 0) continue;
                size_t pre_l = std::min(j, size - 2);
                auto tmp_data_ptr3 = getDataByInternalId(*(datal + pre_l + 1));
#ifdef USE_SSE
                _mm_prefetch((char*)(visited_array + *(datal + pre_l + 1)), _MM_HINT_T0);
                _mm_prefetch(tmp_data_ptr3, _MM_HINT_T0);
#endif
                if (visited_array[candidate_id] == visited_array_tag)
                    continue;
                visited_array[candidate_id] = visited_array_tag;
                char* currObj1 = (getDataByInternalId(candidate_id));

                float dist1 = fstdistfunc_(data_point, currObj1, dist_func_param_);
                if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
                    candidateSet.emplace(-dist1, candidate_id);
                    auto tmp_data_ptr4 = getDataByInternalId(candidateSet.top().second);
#ifdef USE_SSE
                    _mm_prefetch(tmp_data_ptr4, _MM_HINT_T0);
#endif

                    if (!isMarkedDeleted(candidate_id))
                        top_candidates.emplace(dist1, candidate_id);

                    if (top_candidates.size() > ef_construction_)
                        top_candidates.pop();

                    if (!top_candidates.empty())
                        lowerBound = top_candidates.top().first;
                }
            }
        }
        visited_list_pool_->releaseVisitedList(vl);

        return top_candidates;
    }

    template <bool has_deletions, bool collect_metrics = false>
    std::priority_queue<std::pair<float, tableint>,
                        std::vector<std::pair<float, tableint>>,
                        CompareByFirst>
    searchBaseLayerPQSIMDinfer(tableint ep_id,
                               const void* data_point,
                               float*& dist_map,
                               size_t ef,
                               size_t k,
                               BaseFilterFunctor* isIdAllowed = nullptr) const {
        std::shared_ptr<VisitedList> vl = visited_list_pool_->getFreeVisitedList();
        vl_type* visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        // answers        - the KNN set R1
        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            answers;
        // top_candidates - the result set R2
        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            top_candidates;
        // candidate_set  - the search set S
        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            candidate_set;

        float lowerBound;
        float lowerBoundcan;
        // Insert the entry point to the result and search set with its exact distance as a key.
        if (!has_deletions || !isMarkedDeleted(ep_id)) {
            float dist = fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
            lowerBound = dist;
            lowerBoundcan = dist;
            answers.emplace(dist, ep_id);
            top_candidates.emplace(dist, ep_id);
            candidate_set.emplace(-dist, ep_id);
        } else {
            lowerBound = std::numeric_limits<float>::max();
            lowerBoundcan = std::numeric_limits<float>::max();
            candidate_set.emplace(-lowerBound, ep_id);
        }

        visited_array[ep_id] = visited_array_tag;

        // Iteratively generate candidates and conduct DCOs to maintain the result set R.
        while (!candidate_set.empty()) {
            std::pair<float, tableint> current_node_pair = candidate_set.top();

            // When the smallest object in S has its distance larger than the largest in R, terminate the algorithm.
            if ((-current_node_pair.first) > top_candidates.top().first &&
                (top_candidates.size() == ef || has_deletions == false)) {
                break;
            }
            candidate_set.pop();

            // Fetch the smallest object in S.
            tableint current_node_id = current_node_pair.second;
            int* data = (int*)get_linklist0(current_node_id);
            size_t size = getListCount((linklistsizeint*)data);
            if (collect_metrics) {
                metric_hops++;
                metric_distance_computations += size;
            }

            // Enumerate all the neighbors of the object and view them as candidates of KNNs.
            std::vector<int> ids;
            std::vector<float> res;
            int not_vis_count = 0;
            for (size_t j = 1; j <= size; j++) {
                int candidate_id = *(data + j);
                if (!(visited_array[candidate_id] == visited_array_tag)) {
                    not_vis_count++;
                    visited_array[candidate_id] = visited_array_tag;
                    ids.push_back(candidate_id);
                }
            }
            while (ids.size() % 4) ids.push_back(0);
            res.resize(ids.size());
            pq_scan(ids.data(), res.data(), dist_map, ids.size());
            for (size_t j = 0; j < not_vis_count; j++) {
                int candidate_id = ids[j];
                // If the result set is not full, then calculate the exact distance. (i.e., assume the distance threshold to be infinity)
                if (answers.size() < k) {
                    char* currObj1 = (getDataByInternalId(candidate_id));
                    float dist = fstdistfunc_(data_point, currObj1, dist_func_param_);
                    if (!has_deletions || !isMarkedDeleted(candidate_id)) {
                        candidate_set.emplace(-dist, candidate_id);
                        top_candidates.emplace(dist, candidate_id);
                        answers.emplace(dist, candidate_id);
                    }
                    if (!answers.empty())
                        lowerBound = answers.top().first;
                    if (!top_candidates.empty())
                        lowerBoundcan = top_candidates.top().first;
                }
                // Otherwise, conduct DCO with Quantization Distance wrt the N_ef th NN.
                else {
                    if (res[j] - node_cluster_dist_[candidate_id] > lowerBound) {
                        if (top_candidates.size() < ef || lowerBoundcan > res[j]) {
                            top_candidates.emplace(res[j], candidate_id);
                            candidate_set.emplace(-res[j], candidate_id);
                        }
                        if (top_candidates.size() > ef) {
                            top_candidates.pop();
                        }
                        if (!top_candidates.empty())
                            lowerBoundcan = top_candidates.top().first;
                    } else {
                        char* currObj1 = (getDataByInternalId(candidate_id));
                        float dist = fstdistfunc_(data_point, currObj1, dist_func_param_);
                        candidate_set.emplace(-dist, candidate_id);
                        if (!has_deletions || !isMarkedDeleted(candidate_id)) {
                            top_candidates.emplace(dist, candidate_id);
                            answers.emplace(dist, candidate_id);
                        }
                        if (top_candidates.size() > ef)
                            top_candidates.pop();
                        if (answers.size() > k)
                            answers.pop();

                        if (!answers.empty())
                            lowerBound = answers.top().first;
                        if (!top_candidates.empty())
                            lowerBoundcan = top_candidates.top().first;
                    }
                }
            }
        }
        visited_list_pool_->releaseVisitedList(vl);
        return answers;
    }

    template <bool has_deletions, bool collect_metrics = false>
    std::priority_queue<std::pair<float, tableint>,
                        std::vector<std::pair<float, tableint>>,
                        CompareByFirst>
    searchBaseLayerPQinfer(tableint ep_id,
                           const void* data_point,
                           float*& dist_map,
                           size_t ef,
                           size_t k,
                           BaseFilterFunctor* isIdAllowed = nullptr) const {
        std::shared_ptr<VisitedList> vl = visited_list_pool_->getFreeVisitedList();
        vl_type* visited_array = vl->mass;
        vl_type visited_array_tag = vl->curV;

        // answers        - the KNN set R1
        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            answers;
        // top_candidates - the result set R2
        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            top_candidates;
        // candidate_set  - the search set S
        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            candidate_set;

        float lowerBound;
        float lowerBoundcan;
        // Insert the entry point to the result and search set with its exact distance as a key.
        if (!has_deletions || !isMarkedDeleted(ep_id)) {
            float dist = fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
            lowerBound = dist;
            lowerBoundcan = dist;
            answers.emplace(dist, ep_id);
            top_candidates.emplace(dist, ep_id);
            candidate_set.emplace(-dist, ep_id);
        } else {
            lowerBound = std::numeric_limits<float>::max();
            lowerBoundcan = std::numeric_limits<float>::max();
            candidate_set.emplace(-lowerBound, ep_id);
        }

        visited_array[ep_id] = visited_array_tag;

        // Iteratively generate candidates and conduct DCOs to maintain the result set R.
        while (!candidate_set.empty()) {
            std::pair<float, tableint> current_node_pair = candidate_set.top();

            // When the smallest object in S has its distance larger than the largest in R, terminate the algorithm.
            if ((-current_node_pair.first) > top_candidates.top().first &&
                (top_candidates.size() == ef || has_deletions == false)) {
                break;
            }
            candidate_set.pop();

            // Fetch the smallest object in S.
            tableint current_node_id = current_node_pair.second;
            int* data = (int*)get_linklist0(current_node_id);
            size_t size = getListCount((linklistsizeint*)data);
            if (collect_metrics) {
                metric_hops++;
                metric_distance_computations += size;
            }

            // Enumerate all the neighbors of the object and view them as candidates of KNNs.
            for (size_t j = 1; j <= size; j++) {
                int candidate_id = *(data + j);
                if (!(visited_array[candidate_id] == visited_array_tag)) {
                    visited_array[candidate_id] = visited_array_tag;

                    // If the KNN set is not full, then calculate the exact distance. (i.e., assume the distance threshold to be infinity)
                    if (answers.size() < k) {
                        char* currObj1 = (getDataByInternalId(candidate_id));
                        float dist = fstdistfunc_(data_point, currObj1, dist_func_param_);

                        if (!has_deletions || !isMarkedDeleted(candidate_id)) {
                            candidate_set.emplace(-dist, candidate_id);
                            top_candidates.emplace(dist, candidate_id);
                            answers.emplace(dist, candidate_id);
                        }
                        if (!answers.empty())
                            lowerBound = answers.top().first;
                        if (!top_candidates.empty())
                            lowerBoundcan = top_candidates.top().first;
                    } else {
                        float app_dist = naive_product_map_dist(candidate_id, dist_map);
                        if (app_dist - node_cluster_dist_[candidate_id] > lowerBound) {
                            if (top_candidates.size() < ef || lowerBoundcan > app_dist) {
                                top_candidates.emplace(app_dist, candidate_id);
                                candidate_set.emplace(-app_dist, candidate_id);
                            }
                            if (top_candidates.size() > ef) {
                                top_candidates.pop();
                            }
                            if (!top_candidates.empty())
                                lowerBoundcan = top_candidates.top().first;
                        } else {
                            char* currObj1 = (getDataByInternalId(candidate_id));
                            float dist = fstdistfunc_(data_point, currObj1, dist_func_param_);
                            candidate_set.emplace(-dist, candidate_id);
                            if (!has_deletions || !isMarkedDeleted(candidate_id)) {
                                top_candidates.emplace(dist, candidate_id);
                                answers.emplace(dist, candidate_id);
                            }
                            if (top_candidates.size() > ef)
                                top_candidates.pop();
                            if (answers.size() > k)
                                answers.pop();

                            if (!answers.empty())
                                lowerBound = answers.top().first;
                            if (!top_candidates.empty())
                                lowerBoundcan = top_candidates.top().first;
                        }
                    }
                }
            }
        }
        visited_list_pool_->releaseVisitedList(vl);
        return answers;
    }

    //    template <bool has_deletions, bool collect_metrics = false>
    //    std::priority_queue<std::pair<float, tableint>,
    //                        std::vector<std::pair<float, tableint>>,
    //                        CompareByFirst>
    //    searchBaseLayerST(tableint ep_id,
    //                      const void* data_point,
    //                      float radius,
    //                      BaseFilterFunctor* isIdAllowed = nullptr) const {
    //        VisitedList* vl = visited_list_pool_->getFreeVisitedList();
    //        vl_type* visited_array = vl->mass;
    //        vl_type visited_array_tag = vl->curV;
    //
    //        std::priority_queue<std::pair<float, tableint>,
    //                            std::vector<std::pair<float, tableint>>,
    //                            CompareByFirst>
    //            top_candidates;
    //        std::priority_queue<std::pair<float, tableint>,
    //                            std::vector<std::pair<float, tableint>>,
    //                            CompareByFirst>
    //            candidate_set;
    //
    //        float lowerBound;
    //        if ((!has_deletions || !isMarkedDeleted(ep_id)) &&
    //            ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(ep_id)))) {
    //            float dist = fstdistfunc_(data_point, getDataByInternalId(ep_id), dist_func_param_);
    //            lowerBound = dist;
    //            if (dist < radius)
    //                top_candidates.emplace(dist, ep_id);
    //            candidate_set.emplace(-dist, ep_id);
    //        } else {
    //            lowerBound = std::numeric_limits<float>::max();
    //            candidate_set.emplace(-lowerBound, ep_id);
    //        }
    //
    //        visited_array[ep_id] = visited_array_tag;
    //        uint64_t visited_count = 0;
    //
    //        while (!candidate_set.empty()) {
    //            std::pair<float, tableint> current_node_pair = candidate_set.top();
    //
    //            candidate_set.pop();
    //
    //            tableint current_node_id = current_node_pair.second;
    //            int* data = (int*)get_linklist0(current_node_id);
    //            size_t size = getListCount((linklistsizeint*)data);
    //            //                bool cur_node_deleted = isMarkedDeleted(current_node_id);
    //            if (collect_metrics) {
    //                metric_hops_++;
    //                metric_distance_computations_ += size;
    //            }
    //
    //#ifdef USE_SSE
    //            _mm_prefetch((char*)(visited_array + *(data + 1)), _MM_HINT_T0);
    //            _mm_prefetch((char*)(visited_array + *(data + 1) + 64), _MM_HINT_T0);
    //            _mm_prefetch(data_level0_memory_ + (*(data + 1)) * size_data_per_element_ + offsetData_,
    //                         _MM_HINT_T0);
    //            _mm_prefetch((char*)(data + 2), _MM_HINT_T0);
    //#endif
    //
    //            for (size_t j = 1; j <= size; j++) {
    //                int candidate_id = *(data + j);
    ////                    if (candidate_id == 0) continue;
    //#ifdef USE_SSE
    //                _mm_prefetch((char*)(visited_array + *(data + j + 1)), _MM_HINT_T0);
    //                _mm_prefetch(
    //                    data_level0_memory_ + (*(data + j + 1)) * size_data_per_element_ + offsetData_,
    //                    _MM_HINT_T0);  ////////////
    //#endif
    //                if (!(visited_array[candidate_id] == visited_array_tag)) {
    //                    visited_array[candidate_id] = visited_array_tag;
    //                    ++visited_count;
    //
    //                    char* currObj1 = (getDataByInternalId(candidate_id));
    //                    float dist = fstdistfunc_(data_point, currObj1, dist_func_param_);
    //
    //                    if (visited_count < ef_ || dist < radius || lowerBound > dist) {
    //                        candidate_set.emplace(-dist, candidate_id);
    //#ifdef USE_SSE
    //                        _mm_prefetch(data_level0_memory_ +
    //                                         candidate_set.top().second * size_data_per_element_ +
    //                                         offsetLevel0_,  ///////////
    //                                     _MM_HINT_T0);       ////////////////////////
    //#endif
    //
    //                        if ((!has_deletions || !isMarkedDeleted(candidate_id)) &&
    //                            ((!isIdAllowed) || (*isIdAllowed)(getExternalLabel(candidate_id))))
    //                            if (dist < radius)
    //                                top_candidates.emplace(dist, candidate_id);
    //
    //                        if (!top_candidates.empty())
    //                            lowerBound = top_candidates.top().first;
    //                    }
    //                }
    //            }
    //        }
    //
    //        visited_list_pool_->releaseVisitedList(vl);
    //        return top_candidates;
    //    }

    void
    getNeighborsByHeuristic2(std::priority_queue<std::pair<float, tableint>,
                                                 std::vector<std::pair<float, tableint>>,
                                                 CompareByFirst>& top_candidates,
                             const size_t M) {
        if (top_candidates.size() < M) {
            return;
        }

        std::priority_queue<std::pair<float, tableint>> queue_closest;
        std::vector<std::pair<float, tableint>> return_list;
        while (top_candidates.size() > 0) {
            queue_closest.emplace(-top_candidates.top().first, top_candidates.top().second);
            top_candidates.pop();
        }

        while (queue_closest.size()) {
            if (return_list.size() >= M)
                break;
            std::pair<float, tableint> curent_pair = queue_closest.top();
            float floato_query = -curent_pair.first;
            queue_closest.pop();
            bool good = true;

            for (std::pair<float, tableint> second_pair : return_list) {
                float curdist = fstdistfunc_(getDataByInternalId(second_pair.second),
                                             getDataByInternalId(curent_pair.second),
                                             dist_func_param_);
                if (curdist < floato_query) {
                    good = false;
                    break;
                }
            }
            if (good) {
                return_list.push_back(curent_pair);
            }
        }

        for (std::pair<float, tableint> curent_pair : return_list) {
            top_candidates.emplace(-curent_pair.first, curent_pair.second);
        }
    }

    linklistsizeint*
    get_linklist0(tableint internal_id) const {
        return (linklistsizeint*)(data_level0_memory_->GetElementPtr(internal_id, offsetLevel0_));
    }

    linklistsizeint*
    get_linklist(tableint internal_id, int level) const {
        return (linklistsizeint*)(linkLists_[internal_id] + (level - 1) * size_links_per_element_);
    }

    linklistsizeint*
    get_linklist_at_level(tableint internal_id, int level) const {
        return level == 0 ? get_linklist0(internal_id) : get_linklist(internal_id, level);
    }

    tableint
    mutuallyConnectNewElement(const void* data_point,
                              tableint cur_c,
                              std::priority_queue<std::pair<float, tableint>,
                                                  std::vector<std::pair<float, tableint>>,
                                                  CompareByFirst>& top_candidates,
                              int level,
                              bool isUpdate) {
        size_t Mcurmax = level ? maxM_ : maxM0_;
        getNeighborsByHeuristic2(top_candidates, M_);
        if (top_candidates.size() > M_)
            throw std::runtime_error(
                "Should be not be more than M_ candidates returned by the heuristic");

        std::vector<tableint> selectedNeighbors;
        selectedNeighbors.reserve(M_);
        while (top_candidates.size() > 0) {
            selectedNeighbors.push_back(top_candidates.top().second);
            top_candidates.pop();
        }

        tableint next_closest_entry_point = selectedNeighbors.back();

        {
            // lock only during the update
            // because during the addition the lock for cur_c is already acquired
            std::unique_lock<std::mutex> lock(link_list_locks_[cur_c], std::defer_lock);
            if (isUpdate) {
                lock.lock();
            }
            linklistsizeint* ll_cur;
            if (level == 0)
                ll_cur = get_linklist0(cur_c);
            else
                ll_cur = get_linklist(cur_c, level);

            if (*ll_cur && !isUpdate) {
                throw std::runtime_error("The newly inserted element should have blank link list");
            }
            setListCount(ll_cur, selectedNeighbors.size());
            tableint* data = (tableint*)(ll_cur + 1);
            for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
                if (data[idx] && !isUpdate)
                    throw std::runtime_error("Possible memory corruption");
                if (level > element_levels_[selectedNeighbors[idx]])
                    throw std::runtime_error("Trying to make a link on a non-existent level");

                data[idx] = selectedNeighbors[idx];
            }
        }

        for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
            std::unique_lock<std::mutex> lock(link_list_locks_[selectedNeighbors[idx]]);

            linklistsizeint* ll_other;
            if (level == 0)
                ll_other = get_linklist0(selectedNeighbors[idx]);
            else
                ll_other = get_linklist(selectedNeighbors[idx], level);

            size_t sz_link_list_other = getListCount(ll_other);

            if (sz_link_list_other > Mcurmax)
                throw std::runtime_error("Bad value of sz_link_list_other");
            if (selectedNeighbors[idx] == cur_c)
                throw std::runtime_error("Trying to connect an element to itself");
            if (level > element_levels_[selectedNeighbors[idx]])
                throw std::runtime_error("Trying to make a link on a non-existent level");

            tableint* data = (tableint*)(ll_other + 1);

            bool is_cur_c_present = false;
            if (isUpdate) {
                for (size_t j = 0; j < sz_link_list_other; j++) {
                    if (data[j] == cur_c) {
                        is_cur_c_present = true;
                        break;
                    }
                }
            }

            // If cur_c is already present in the neighboring connections of `selectedNeighbors[idx]` then no need to modify any connections or run the heuristics.
            if (!is_cur_c_present) {
                if (sz_link_list_other < Mcurmax) {
                    data[sz_link_list_other] = cur_c;
                    setListCount(ll_other, sz_link_list_other + 1);
                } else {
                    // finding the "weakest" element to replace it with the new one
                    float d_max = fstdistfunc_(getDataByInternalId(cur_c),
                                               getDataByInternalId(selectedNeighbors[idx]),
                                               dist_func_param_);
                    // Heuristic:
                    std::priority_queue<std::pair<float, tableint>,
                                        std::vector<std::pair<float, tableint>>,
                                        CompareByFirst>
                        candidates;
                    candidates.emplace(d_max, cur_c);

                    for (size_t j = 0; j < sz_link_list_other; j++) {
                        candidates.emplace(fstdistfunc_(getDataByInternalId(data[j]),
                                                        getDataByInternalId(selectedNeighbors[idx]),
                                                        dist_func_param_),
                                           data[j]);
                    }

                    getNeighborsByHeuristic2(candidates, Mcurmax);

                    int indx = 0;
                    while (candidates.size() > 0) {
                        data[indx] = candidates.top().second;
                        candidates.pop();
                        indx++;
                    }

                    setListCount(ll_other, indx);
                    // Nearest K:
                    /*int indx = -1;
                    for (int j = 0; j < sz_link_list_other; j++) {
                        float d = fstdistfunc_(getDataByInternalId(data[j]), getDataByInternalId(rez[idx]), dist_func_param_);
                        if (d > d_max) {
                            indx = j;
                            d_max = d;
                        }
                    }
                    if (indx >= 0) {
                        data[indx] = cur_c;
                    } */
                }
            }
        }

        return next_closest_entry_point;
    }

    void
    resizeIndex(size_t new_max_elements) override {
        if (new_max_elements < cur_element_count_)
            throw std::runtime_error(
                "Cannot Resize, max element is less than the current number of elements");

        delete visited_list_pool_;
        visited_list_pool_ = new VisitedListPool(1, new_max_elements, allocator_);

        element_levels_ =
            (int*)allocator_->Reallocate(element_levels_, new_max_elements * sizeof(int));
        std::vector<std::mutex>(new_max_elements).swap(link_list_locks_);

        // Reallocate base layer
        data_level0_memory_->Resize(new_max_elements);

        // Reallocate all other layers
        char** linkLists_new =
            (char**)allocator_->Reallocate(linkLists_, sizeof(void*) * new_max_elements);
        if (linkLists_new == nullptr)
            throw std::runtime_error(
                "Not enough memory: resizeIndex failed to allocate other layers");
        linkLists_ = linkLists_new;

        max_elements_ = new_max_elements;
    }

    template <typename T>
    static void
    writeVarToMem(char*& dest, const T& ref) {
        std::memcpy(dest, (char*)&ref, sizeof(T));
        dest += sizeof(T);
    }

    static void
    writeBinaryToMem(char*& dest, const char* src, size_t len) {
        std::memcpy(dest, src, len);
        dest += len;
    }

    void
    saveIndex(void* d) override {
        // std::ofstream output(location, std::ios::binary);
        // std::streampos position;
        char* dest = (char*)d;

        // writeBinaryPOD(output, offsetLevel0_);
        writeVarToMem(dest, offsetLevel0_);
        // writeBinaryPOD(output, max_elements_);
        writeVarToMem(dest, max_elements_);
        // writeBinaryPOD(output, cur_element_count_);
        writeVarToMem(dest, cur_element_count_);
        // writeBinaryPOD(output, size_data_per_element_);
        writeVarToMem(dest, size_data_per_element_);
        // writeBinaryPOD(output, label_offset_);
        writeVarToMem(dest, label_offset_);
        // writeBinaryPOD(output, offsetData_);
        writeVarToMem(dest, offsetData_);
        // writeBinaryPOD(output, maxlevel_);
        writeVarToMem(dest, maxlevel_);
        // writeBinaryPOD(output, enterpoint_node_);
        writeVarToMem(dest, enterpoint_node_);
        // writeBinaryPOD(output, maxM_);
        writeVarToMem(dest, maxM_);

        // writeBinaryPOD(output, maxM0_);
        writeVarToMem(dest, maxM0_);
        // writeBinaryPOD(output, M_);
        writeVarToMem(dest, M_);
        // writeBinaryPOD(output, mult_);
        writeVarToMem(dest, mult_);
        // writeBinaryPOD(output, ef_construction_);
        writeVarToMem(dest, ef_construction_);

        writeVarToMem(dest, pq_chunk);

        writeVarToMem(dest, pq_cluster);

        writeVarToMem(dest, pq_sub_dim);

        // output.write(data_level0_memory_, cur_element_count_ * size_data_per_element_);
        data_level0_memory_->Serialize(dest, cur_element_count_);

        for (size_t i = 0; i < cur_element_count_; i++) {
            unsigned int linkListSize =
                element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
            // writeBinaryPOD(output, linkListSize);
            writeVarToMem(dest, linkListSize);
            if (linkListSize) {
                // output.write(linkLists_[i], linkListSize);
                writeBinaryToMem(dest, linkLists_[i], linkListSize);
            }
        }

        writeBinaryToMem(dest, (char*)pq_map, max_elements_ * pq_chunk * sizeof(uint8_t));

        for (auto& chunk : pq_book) {
            for (auto& cluster : chunk) {
                writeBinaryToMem(dest, (char*)cluster.data(), pq_sub_dim * sizeof(float));
            }
        }

        writeBinaryToMem(dest, (char*)node_cluster_dist_, max_elements_ * sizeof(float));
        // output.close();
    }

    size_t
    calcSerializeSize() override {
        // std::ofstream output(location, std::ios::binary);
        // std::streampos position;
        size_t size = 0;

        // writeBinaryPOD(output, offsetLevel0_);
        size += sizeof(offsetLevel0_);
        // writeBinaryPOD(output, max_elements_);
        size += sizeof(max_elements_);
        // writeBinaryPOD(output, cur_element_count_);
        size += sizeof(cur_element_count_);
        // writeBinaryPOD(output, size_data_per_element_);
        size += sizeof(size_data_per_element_);
        // writeBinaryPOD(output, label_offset_);
        size += sizeof(label_offset_);
        // writeBinaryPOD(output, offsetData_);
        size += sizeof(offsetData_);
        // writeBinaryPOD(output, maxlevel_);
        size += sizeof(maxlevel_);
        // writeBinaryPOD(output, enterpoint_node_);
        size += sizeof(enterpoint_node_);
        // writeBinaryPOD(output, maxM_);
        size += sizeof(maxM_);

        // writeBinaryPOD(output, maxM0_);
        size += sizeof(maxM0_);
        // writeBinaryPOD(output, M_);
        size += sizeof(M_);
        // writeBinaryPOD(output, mult_);
        size += sizeof(mult_);
        // writeBinaryPOD(output, ef_construction_);
        size += sizeof(ef_construction_);

        size += sizeof(pq_chunk);

        size += sizeof(pq_cluster);

        size += sizeof(pq_sub_dim);

        // output.write(data_level0_memory_, cur_element_count_ * size_data_per_element_);
        size += data_level0_memory_->GetSize();

        for (size_t i = 0; i < cur_element_count_; i++) {
            unsigned int linkListSize =
                element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
            // writeBinaryPOD(output, linkListSize);
            size += sizeof(linkListSize);
            if (linkListSize) {
                // output.write(linkLists_[i], linkListSize);
                size += linkListSize;
            }
        }

        size += max_elements_ * pq_chunk * sizeof(uint8_t);

        size += pq_chunk * pq_cluster * pq_sub_dim * sizeof(float);

        size += max_elements_ * sizeof(float);
        // output.close();
        return size;
    }

    // save index to a file stream
    void
    saveIndex(std::ostream& out_stream) override {
        writeBinaryPOD(out_stream, offsetLevel0_);
        writeBinaryPOD(out_stream, max_elements_);
        writeBinaryPOD(out_stream, cur_element_count_);
        writeBinaryPOD(out_stream, size_data_per_element_);
        writeBinaryPOD(out_stream, label_offset_);
        writeBinaryPOD(out_stream, offsetData_);
        writeBinaryPOD(out_stream, maxlevel_);
        writeBinaryPOD(out_stream, enterpoint_node_);
        writeBinaryPOD(out_stream, maxM_);

        writeBinaryPOD(out_stream, maxM0_);
        writeBinaryPOD(out_stream, M_);
        writeBinaryPOD(out_stream, mult_);
        writeBinaryPOD(out_stream, ef_construction_);

        writeBinaryPOD(out_stream, pq_chunk);
        writeBinaryPOD(out_stream, pq_cluster);
        writeBinaryPOD(out_stream, pq_sub_dim);

        data_level0_memory_->Serialize(out_stream, cur_element_count_);

        for (size_t i = 0; i < cur_element_count_; i++) {
            unsigned int linkListSize =
                element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
            writeBinaryPOD(out_stream, linkListSize);
            if (linkListSize) {
                out_stream.write(linkLists_[i], linkListSize);
            }
        }

        out_stream.write((char*)pq_map, max_elements_ * pq_chunk * sizeof(uint8_t));

        for (auto& chunk : pq_book) {
            for (auto& cluster : chunk) {
                out_stream.write((char*)cluster.data(), pq_sub_dim * sizeof(float));
            }
        }
        out_stream.write((char*)node_cluster_dist_, max_elements_ * sizeof(float));
    }

    void
    saveIndex(const std::string& location) override {
        throw std::runtime_error("static hnsw does not support save index");
        //        std::ofstream output(location, std::ios::binary);
        //        std::streampos position;
        //
        //        writeBinaryPOD(output, offsetLevel0_);
        //        writeBinaryPOD(output, max_elements_);
        //        writeBinaryPOD(output, cur_element_count_);
        //        writeBinaryPOD(output, size_data_per_element_);
        //        writeBinaryPOD(output, label_offset_);
        //        writeBinaryPOD(output, offsetData_);
        //        writeBinaryPOD(output, maxlevel_);
        //        writeBinaryPOD(output, enterpoint_node_);
        //        writeBinaryPOD(output, maxM_);
        //
        //        writeBinaryPOD(output, maxM0_);
        //        writeBinaryPOD(output, M_);
        //        writeBinaryPOD(output, mult_);
        //        writeBinaryPOD(output, ef_construction_);
        //
        //        output.write(data_level0_memory_, cur_element_count_ * size_data_per_element_);
        //
        //        for (size_t i = 0; i < cur_element_count_; i++) {
        //            unsigned int linkListSize =
        //                element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i] : 0;
        //            writeBinaryPOD(output, linkListSize);
        //            if (linkListSize)
        //                output.write(linkLists_[i], linkListSize);
        //        }
        //        output.close();
    }

    template <typename T>
    void
    readFromReader(std::function<void(uint64_t, uint64_t, void*)> read_func,
                   uint64_t& cursor,
                   T& var) {
        read_func(cursor, sizeof(T), &var);
        cursor += sizeof(T);
    }

    // load using reader
    void
    loadIndex(std::function<void(uint64_t, uint64_t, void*)> read_func,
              SpaceInterface* s,
              size_t max_elements_i = 0) override {
        // std::ifstream input(location, std::ios::binary);

        // if (!input.is_open())
        //     throw std::runtime_error("Cannot open file");

        // get file size:
        // input.seekg(0, input.end);
        // std::streampos total_filesize = input.tellg();
        // input.seekg(0, input.beg);

        uint64_t cursor = 0;

        // readBinaryPOD(input, offsetLevel0_);
        readFromReader(read_func, cursor, offsetLevel0_);
        // readBinaryPOD(input, max_elements_);
        readFromReader(read_func, cursor, max_elements_);
        // readBinaryPOD(input, cur_element_count_);
        readFromReader(read_func, cursor, cur_element_count_);

        size_t max_elements = max_elements_i;
        if (max_elements < cur_element_count_)
            max_elements = max_elements_;
        max_elements_ = max_elements;
        // readBinaryPOD(input, size_data_per_element_);
        readFromReader(read_func, cursor, size_data_per_element_);
        // readBinaryPOD(input, label_offset_);
        readFromReader(read_func, cursor, label_offset_);
        // readBinaryPOD(input, offsetData_);
        readFromReader(read_func, cursor, offsetData_);
        // readBinaryPOD(input, maxlevel_);
        readFromReader(read_func, cursor, maxlevel_);
        // readBinaryPOD(input, enterpoint_node_);
        readFromReader(read_func, cursor, enterpoint_node_);

        // readBinaryPOD(input, maxM_);
        readFromReader(read_func, cursor, maxM_);
        // readBinaryPOD(input, maxM0_);
        readFromReader(read_func, cursor, maxM0_);
        // readBinaryPOD(input, M_);
        readFromReader(read_func, cursor, M_);
        // readBinaryPOD(input, mult_);
        readFromReader(read_func, cursor, mult_);
        // readBinaryPOD(input, ef_construction_);
        readFromReader(read_func, cursor, ef_construction_);

        readFromReader(read_func, cursor, pq_chunk);

        readFromReader(read_func, cursor, pq_cluster);

        readFromReader(read_func, cursor, pq_sub_dim);

        data_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();

        // auto pos = input.tellg();

        /// Optional - check if index is ok:
        // input.seekg(cur_element_count_ * size_data_per_element_, input.cur);
        // for (size_t i = 0; i < cur_element_count_; i++) {
        //     if (input.tellg() < 0 || input.tellg() >= total_filesize) {
        //         throw std::runtime_error("Index seems to be corrupted or unsupported");
        //     }

        //     unsigned int linkListSize;
        //     readBinaryPOD(input, linkListSize);
        //     if (linkListSize != 0) {
        //         input.seekg(linkListSize, input.cur);
        //     }
        // }

        // throw exception if it either corrupted or old index
        // if (input.tellg() != total_filesize)
        //     throw std::runtime_error("Index seems to be corrupted or unsupported");

        // input.clear();
        /// Optional check end

        // input.seekg(pos, input.beg);
        resizeIndex(max_elements);

        data_level0_memory_->Deserialize(read_func, cursor, cur_element_count_);
        cursor += cur_element_count_ * size_data_per_element_;

        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        std::vector<std::mutex>(max_elements).swap(link_list_locks_);
        std::vector<std::mutex>(MAX_LABEL_OPERATION_LOCKS).swap(label_op_locks_);

        if (linkLists_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklists");

        revSize_ = 1.0 / mult_;
        for (size_t i = 0; i < cur_element_count_; i++) {
            label_lookup_[getExternalLabel(i)] = i;
            unsigned int linkListSize;
            // readBinaryPOD(input, linkListSize);
            readFromReader(read_func, cursor, linkListSize);
            if (linkListSize == 0) {
                element_levels_[i] = 0;
                linkLists_[i] = nullptr;
            } else {
                element_levels_[i] = linkListSize / size_links_per_element_;
                linkLists_[i] = (char*)allocator_->Allocate(linkListSize);
                if (linkLists_[i] == nullptr)
                    throw std::runtime_error(
                        "Not enough memory: loadIndex failed to allocate linklist");
                // input.read(linkLists_[i], linkListSize);
                read_func(cursor, linkListSize, linkLists_[i]);
                cursor += linkListSize;
            }
        }

        for (size_t i = 0; i < cur_element_count_; i++) {
            if (isMarkedDeleted(i)) {
                num_deleted_ += 1;
                if (allow_replace_deleted_)
                    deleted_elements.insert(i);
            }
        }
        pq_map = (uint8_t*)allocator_->Allocate(max_elements_ * pq_chunk * sizeof(uint8_t));
        read_func(cursor, max_elements_ * pq_chunk * sizeof(uint8_t), (char*)pq_map);
        cursor += max_elements_ * pq_chunk * sizeof(uint8_t);

        pq_book.resize(pq_chunk);
        for (auto& chunk : pq_book) {
            chunk.resize(pq_cluster);
            for (auto& cluster : chunk) {
                cluster.resize(pq_sub_dim);
                read_func(cursor, pq_sub_dim * sizeof(float), (char*)cluster.data());
                cursor += pq_sub_dim * sizeof(float);
            }
        }

        node_cluster_dist_ = (float*)allocator_->Allocate(max_elements_ * sizeof(float));
        read_func(cursor, max_elements_ * sizeof(float), (char*)node_cluster_dist_);
        cursor += max_elements_ * sizeof(float);
        return;
    }

    // load index from a file stream
    void
    loadIndex(std::istream& in_stream, SpaceInterface* s, size_t max_elements_i = 0) override {
        auto beg_pos = in_stream.tellg();

        readBinaryPOD(in_stream, offsetLevel0_);
        readBinaryPOD(in_stream, max_elements_);
        readBinaryPOD(in_stream, cur_element_count_);

        size_t max_elements = max_elements_i;
        if (max_elements < cur_element_count_)
            max_elements = max_elements_;
        max_elements_ = max_elements;
        readBinaryPOD(in_stream, size_data_per_element_);
        readBinaryPOD(in_stream, label_offset_);
        readBinaryPOD(in_stream, offsetData_);
        readBinaryPOD(in_stream, maxlevel_);
        readBinaryPOD(in_stream, enterpoint_node_);

        readBinaryPOD(in_stream, maxM_);
        readBinaryPOD(in_stream, maxM0_);
        readBinaryPOD(in_stream, M_);
        readBinaryPOD(in_stream, mult_);
        readBinaryPOD(in_stream, ef_construction_);

        readBinaryPOD(in_stream, pq_chunk);
        readBinaryPOD(in_stream, pq_cluster);
        readBinaryPOD(in_stream, pq_sub_dim);

        data_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();

        auto pos = in_stream.tellg();

        /// Optional - check if index is ok:
        /*
        in_stream.seekg(cur_element_count_ * size_data_per_element_, in_stream.cur);
        for (size_t i = 0; i < cur_element_count_; i++) {
            if (in_stream.tellg() < 0 || in_stream.tellg() >= beg_pos + length) {
                throw std::runtime_error("Index seems to be corrupted or unsupported");
            }

            unsigned int linkListSize;
            readBinaryPOD(in_stream, linkListSize);
            if (linkListSize != 0) {
                in_stream.seekg(linkListSize, in_stream.cur);
            }
        }

        // throw exception if it either corrupted or old index
        if (in_stream.tellg() != beg_pos + length)
            throw std::runtime_error("Index seems to be corrupted or unsupported");

        in_stream.clear();
        */
        /// Optional check end

        resizeIndex(max_elements);
        in_stream.seekg(pos, in_stream.beg);
        data_level0_memory_->Deserialize(in_stream, cur_element_count_);

        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        std::vector<std::mutex>(max_elements).swap(link_list_locks_);
        std::vector<std::mutex>(MAX_LABEL_OPERATION_LOCKS).swap(label_op_locks_);

        revSize_ = 1.0 / mult_;
        for (size_t i = 0; i < cur_element_count_; i++) {
            label_lookup_[getExternalLabel(i)] = i;
            unsigned int linkListSize;
            readBinaryPOD(in_stream, linkListSize);
            if (linkListSize == 0) {
                element_levels_[i] = 0;
                linkLists_[i] = nullptr;
            } else {
                element_levels_[i] = linkListSize / size_links_per_element_;
                linkLists_[i] = (char*)allocator_->Allocate(linkListSize);
                if (linkLists_[i] == nullptr)
                    throw std::runtime_error(
                        "Not enough memory: loadIndex failed to allocate linklist");
                in_stream.read(linkLists_[i], linkListSize);
            }
        }

        for (size_t i = 0; i < cur_element_count_; i++) {
            if (isMarkedDeleted(i)) {
                num_deleted_ += 1;
                if (allow_replace_deleted_)
                    deleted_elements.insert(i);
            }
        }
        pq_map = (uint8_t*)allocator_->Allocate(max_elements_ * pq_chunk * sizeof(uint8_t));
        ;
        in_stream.read((char*)pq_map, max_elements_ * pq_chunk * sizeof(uint8_t));

        pq_book.resize(pq_chunk);
        for (auto& chunk : pq_book) {
            chunk.resize(pq_cluster);
            for (auto& cluster : chunk) {
                cluster.resize(pq_sub_dim);
                in_stream.read((char*)cluster.data(), pq_sub_dim * sizeof(float));
            }
        }

        node_cluster_dist_ = (float*)allocator_->Allocate(max_elements_ * sizeof(float));
        in_stream.read((char*)node_cluster_dist_, max_elements_ * sizeof(float));

        return;
    }

    // origin load function
    void
    loadIndex(const std::string& location, SpaceInterface* s, size_t max_elements_i = 0) {
        std::ifstream input(location, std::ios::binary);

        if (!input.is_open())
            throw std::runtime_error("Cannot open file");

        // get file size:
        input.seekg(0, input.end);
        std::streampos total_filesize = input.tellg();
        input.seekg(0, input.beg);

        readBinaryPOD(input, offsetLevel0_);
        readBinaryPOD(input, max_elements_);
        readBinaryPOD(input, cur_element_count_);

        size_t max_elements = max_elements_i;
        if (max_elements < cur_element_count_)
            max_elements = max_elements_;
        max_elements_ = max_elements;
        readBinaryPOD(input, size_data_per_element_);
        readBinaryPOD(input, label_offset_);
        readBinaryPOD(input, offsetData_);
        readBinaryPOD(input, maxlevel_);
        readBinaryPOD(input, enterpoint_node_);

        readBinaryPOD(input, maxM_);
        readBinaryPOD(input, maxM0_);
        readBinaryPOD(input, M_);
        readBinaryPOD(input, mult_);
        readBinaryPOD(input, ef_construction_);

        data_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();

        auto pos = input.tellg();

        /// Optional - check if index is ok:
        input.seekg(cur_element_count_ * size_data_per_element_, input.cur);
        for (size_t i = 0; i < cur_element_count_; i++) {
            if (input.tellg() < 0 || input.tellg() >= total_filesize) {
                throw std::runtime_error("Index seems to be corrupted or unsupported");
            }

            unsigned int linkListSize;
            readBinaryPOD(input, linkListSize);
            if (linkListSize != 0) {
                input.seekg(linkListSize, input.cur);
            }
        }

        // throw exception if it either corrupted or old index
        if (input.tellg() != total_filesize)
            throw std::runtime_error("Index seems to be corrupted or unsupported");

        input.clear();
        /// Optional check end

        input.seekg(pos, input.beg);

        data_level0_memory_->Deserialize(input, cur_element_count_);

        size_links_per_element_ = maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

        size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
        std::vector<std::mutex>(max_elements).swap(link_list_locks_);
        std::vector<std::mutex>(MAX_LABEL_OPERATION_LOCKS).swap(label_op_locks_);

        delete visited_list_pool_;
        visited_list_pool_ = new VisitedListPool(1, max_elements, allocator_);

        free(linkLists_);
        linkLists_ = (char**)malloc(sizeof(void*) * max_elements);
        if (linkLists_ == nullptr)
            throw std::runtime_error("Not enough memory: loadIndex failed to allocate linklists");

        revSize_ = 1.0 / mult_;
        for (size_t i = 0; i < cur_element_count_; i++) {
            label_lookup_[getExternalLabel(i)] = i;
            unsigned int linkListSize;
            readBinaryPOD(input, linkListSize);
            if (linkListSize == 0) {
                element_levels_[i] = 0;
                linkLists_[i] = nullptr;
            } else {
                element_levels_[i] = linkListSize / size_links_per_element_;
                linkLists_[i] = (char*)malloc(linkListSize);
                if (linkLists_[i] == nullptr)
                    throw std::runtime_error(
                        "Not enough memory: loadIndex failed to allocate linklist");
                input.read(linkLists_[i], linkListSize);
            }
        }

        for (size_t i = 0; i < cur_element_count_; i++) {
            if (isMarkedDeleted(i)) {
                num_deleted_ += 1;
                if (allow_replace_deleted_)
                    deleted_elements.insert(i);
            }
        }

        input.close();

        return;
    }

    const float*
    getDataByLabel(labeltype label) const override {
        std::lock_guard<std::mutex> lock_label(getLabelOpMutex(label));

        std::unique_lock<std::mutex> lock_table(label_lookup_lock);
        auto search = label_lookup_.find(label);
        if (search == label_lookup_.end() || isMarkedDeleted(search->second)) {
            throw std::runtime_error("Label not found");
        }
        tableint internalId = search->second;
        lock_table.unlock();

        char* data_ptrv = getDataByInternalId(internalId);
        float* data_ptr = (float*)data_ptrv;

        return data_ptr;
    }

    /*
    * Checks the first 16 bits of the memory to see if the element is marked deleted.
    */
    bool
    isMarkedDeleted(tableint internalId) const {
        unsigned char* ll_cur = ((unsigned char*)get_linklist0(internalId)) + 2;
        return *ll_cur & DELETE_MARK;
    }

    unsigned short int
    getListCount(linklistsizeint* ptr) const {
        return *((unsigned short int*)ptr);
    }

    void
    setListCount(linklistsizeint* ptr, unsigned short int size) const {
        *((unsigned short int*)(ptr)) = *((unsigned short int*)&size);
    }

    /*
    * Adds point.
    */
    bool
    addPoint(const void* data_point, labeltype label) override {
        if (addPoint(data_point, label, -1) == -1) {
            return false;
        }
        return true;
    }

    tableint
    addPoint(const void* data_point, labeltype label, int level) {
        tableint cur_c = 0;
        {
            // Checking if the element with the same label already exists
            // if so, updating it *instead* of creating a new element.
            std::unique_lock<std::mutex> lock_table(label_lookup_lock);
            auto search = label_lookup_.find(label);
            if (search != label_lookup_.end()) {
                return -1;
            }

            if (cur_element_count_ >= max_elements_) {
                resizeIndex(max_elements_ + data_element_per_block_);
            }

            cur_c = cur_element_count_;
            cur_element_count_++;
            label_lookup_[label] = cur_c;
        }

        std::unique_lock<std::mutex> lock_el(link_list_locks_[cur_c]);
        int curlevel = getRandomLevel(mult_);
        if (level > 0)
            curlevel = level;

        element_levels_[cur_c] = curlevel;

        std::unique_lock<std::mutex> templock(global);
        int maxlevelcopy = maxlevel_;
        if (curlevel <= maxlevelcopy)
            templock.unlock();
        tableint currObj = enterpoint_node_;
        tableint enterpoint_copy = enterpoint_node_;

        memset(data_level0_memory_->GetElementPtr(cur_c, offsetLevel0_), 0, size_data_per_element_);

        // Initialisation of the data and label
        memcpy(getExternalLabeLp(cur_c), &label, sizeof(labeltype));
        memcpy(getDataByInternalId(cur_c), data_point, data_size_);

        if (curlevel) {
            linkLists_[cur_c] = (char*)allocator_->Allocate(size_links_per_element_ * curlevel + 1);
            if (linkLists_[cur_c] == nullptr)
                throw std::runtime_error("Not enough memory: addPoint failed to allocate linklist");
            memset(linkLists_[cur_c], 0, size_links_per_element_ * curlevel + 1);
        }

        if ((signed)currObj != -1) {
            if (curlevel < maxlevelcopy) {
                float curdist =
                    fstdistfunc_(data_point, getDataByInternalId(currObj), dist_func_param_);
                for (int level = maxlevelcopy; level > curlevel; level--) {
                    bool changed = true;
                    while (changed) {
                        changed = false;
                        unsigned int* data;
                        std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
                        data = get_linklist(currObj, level);
                        int size = getListCount(data);

                        tableint* datal = (tableint*)(data + 1);
                        for (int i = 0; i < size; i++) {
                            tableint cand = datal[i];
                            if (cand < 0 || cand > max_elements_)
                                throw std::runtime_error("cand error");
                            float d = fstdistfunc_(
                                data_point, getDataByInternalId(cand), dist_func_param_);
                            if (d < curdist) {
                                curdist = d;
                                currObj = cand;
                                changed = true;
                            }
                        }
                    }
                }
            }

            bool epDeleted = isMarkedDeleted(enterpoint_copy);
            for (int level = std::min(curlevel, maxlevelcopy); level >= 0; level--) {
                if (level > maxlevelcopy || level < 0)  // possible?
                    throw std::runtime_error("Level error");

                std::priority_queue<std::pair<float, tableint>,
                                    std::vector<std::pair<float, tableint>>,
                                    CompareByFirst>
                    top_candidates = searchBaseLayer(currObj, data_point, level);
                if (epDeleted) {
                    top_candidates.emplace(
                        fstdistfunc_(
                            data_point, getDataByInternalId(enterpoint_copy), dist_func_param_),
                        enterpoint_copy);
                    if (top_candidates.size() > ef_construction_)
                        top_candidates.pop();
                }
                currObj =
                    mutuallyConnectNewElement(data_point, cur_c, top_candidates, level, false);
            }
        } else {
            // Do nothing for the first element
            enterpoint_node_ = 0;
            maxlevel_ = curlevel;
        }

        // Releasing lock for the maximum level
        if (curlevel > maxlevelcopy) {
            enterpoint_node_ = cur_c;
            maxlevel_ = curlevel;
        }
        return cur_c;
    }

    std::priority_queue<std::pair<float, labeltype>>
    searchKnn(const void* query_data,
              size_t k,
              uint64_t ef,
              BaseFilterFunctor* isIdAllowed = nullptr) const override {
        std::priority_queue<std::pair<float, labeltype>> result;
        if (cur_element_count_ == 0)
            return result;
        float* dist_map = nullptr;
        if (is_trained_infer) {
            dist_map = new float[pq_chunk * pq_cluster * pq_sub_dim];
            calc_dist_map((float*)query_data, dist_map);
        }

        tableint currObj = enterpoint_node_;
        float curdist =
            fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), dist_func_param_);

        for (int level = maxlevel_; level > 0; level--) {
            bool changed = true;
            while (changed) {
                changed = false;
                unsigned int* data;

                data = (unsigned int*)get_linklist(currObj, level);
                int size = getListCount(data);
                metric_hops++;
                metric_distance_computations += size;

                tableint* datal = (tableint*)(data + 1);
                for (int i = 0; i < size; i++) {
                    tableint cand = datal[i];
                    if (cand < 0 || cand > max_elements_)
                        throw std::runtime_error("cand error");
                    float d = fstdistfunc_(query_data, getDataByInternalId(cand), dist_func_param_);
                    if (d < curdist) {
                        curdist = d;
                        currObj = cand;
                        changed = true;
                    }
                }
            }
        }

        std::priority_queue<std::pair<float, tableint>,
                            std::vector<std::pair<float, tableint>>,
                            CompareByFirst>
            top_candidates;
        if (num_deleted_) {
            //            if (!is_trained_infer)
            //                top_candidates = searchBaseLayerST<true, true>(
            //                    currObj, query_data, std::max(ef_, k), isIdAllowed);
            //            else
            top_candidates = searchBaseLayerPQSIMDinfer<true, true>(
                currObj, query_data, dist_map, std::max(ef, k), k, isIdAllowed);
        } else {
            //            if (!is_trained_infer)
            //                top_candidates = searchBaseLayerST<false, true>(
            //                    currObj, query_data, std::max(ef, k), isIdAllowed);
            //            else
            top_candidates = searchBaseLayerPQSIMDinfer<true, true>(
                currObj, query_data, dist_map, std::max(ef, k), k, isIdAllowed);
        }

        while (top_candidates.size() > k) {
            top_candidates.pop();
        }
        while (top_candidates.size() > 0) {
            std::pair<float, tableint> rez = top_candidates.top();
            result.push(std::pair<float, labeltype>(rez.first, getExternalLabel(rez.second)));
            top_candidates.pop();
        }
        delete[] dist_map;
        return result;
    }

    std::priority_queue<std::pair<float, labeltype>>
    searchRange(const void* query_data,
                float radius,
                uint64_t ef,
                BaseFilterFunctor* isIdAllowed = nullptr) const override {
        std::runtime_error("static hnsw does not support range search");
        //        std::priority_queue<std::pair<float, labeltype>> result;
        //        if (cur_element_count_ == 0)
        //            return result;
        //
        //        tableint currObj = enterpoint_node_;
        //        float curdist =
        //            fstdistfunc_(query_data, getDataByInternalId(enterpoint_node_), dist_func_param_);
        //
        //        for (int level = maxlevel_; level > 0; level--) {
        //            bool changed = true;
        //            while (changed) {
        //                changed = false;
        //                unsigned int* data;
        //
        //                data = (unsigned int*)get_linklist(currObj, level);
        //                int size = getListCount(data);
        //                metric_hops_++;
        //                metric_distance_computations_ += size;
        //
        //                tableint* datal = (tableint*)(data + 1);
        //                for (int i = 0; i < size; i++) {
        //                    tableint cand = datal[i];
        //                    if (cand < 0 || cand > max_elements_)
        //                        throw std::runtime_error("cand error");
        //                    float d = fstdistfunc_(query_data, getDataByInternalId(cand), dist_func_param_);
        //
        //                    if (d < curdist) {
        //                        curdist = d;
        //                        currObj = cand;
        //                        changed = true;
        //                    }
        //                }
        //            }
        //        }
        //
        //        std::priority_queue<std::pair<float, tableint>,
        //                            std::vector<std::pair<float, tableint>>,
        //                            CompareByFirst>
        //            top_candidates;
        //        if (num_deleted_) {
        //            throw std::runtime_error(
        //                "not support perform range search on a index that deleted some vectors");
        //        } else {
        //            top_candidates =
        //                searchBaseLayerST<false, true>(currObj, query_data, radius, isIdAllowed);
        //            // std::cout << "top_candidates.size(): " << top_candidates.size() << std::endl;
        //        }
        //
        //        // while (top_candidates.size() > k) {
        //        //     top_candidates.pop();
        //        // }
        //        while (top_candidates.size() > 0) {
        //            std::pair<float, tableint> rez = top_candidates.top();
        //            result.push(std::pair<float, labeltype>(rez.first, getExternalLabel(rez.second)));
        //            top_candidates.pop();
        //        }
        //
        //        // std::cout << "hnswalg::result.size(): " << result.size() << std::endl;
        //        return result;
        return {};
    }

    std::priority_queue<std::pair<float, labeltype>>
    bruteForce(const void* data_point, int64_t k) override {
        std::priority_queue<std::pair<float, labeltype>> results;
        for (uint32_t i = 0; i < cur_element_count_; i++) {
            float dist = fstdistfunc_(data_point, getDataByInternalId(i), dist_func_param_);
            if (results.size() < k) {
                results.push({dist, *getExternalLabeLp(i)});
            } else {
                float current_max_dist = results.top().first;
                if (dist < current_max_dist) {
                    results.pop();
                    results.push({dist, *getExternalLabeLp(i)});
                }
            }
        }
        return results;
    }

    void
    checkIntegrity() {
        int connections_checked = 0;
        std::vector<int> inbound_connections_num(cur_element_count_, 0);
        for (int i = 0; i < cur_element_count_; i++) {
            for (int l = 0; l <= element_levels_[i]; l++) {
                linklistsizeint* ll_cur = get_linklist_at_level(i, l);
                int size = getListCount(ll_cur);
                tableint* data = (tableint*)(ll_cur + 1);
                std::unordered_set<tableint> s;
                for (int j = 0; j < size; j++) {
                    assert(data[j] > 0);
                    assert(data[j] < cur_element_count_);
                    assert(data[j] != i);
                    inbound_connections_num[data[j]]++;
                    s.insert(data[j]);
                    connections_checked++;
                }
                assert(s.size() == size);
            }
        }
        if (cur_element_count_ > 1) {
            int min1 = inbound_connections_num[0], max1 = inbound_connections_num[0];
            for (int i = 0; i < cur_element_count_; i++) {
                assert(inbound_connections_num[i] > 0);
                min1 = std::min(inbound_connections_num[i], min1);
                max1 = std::max(inbound_connections_num[i], max1);
            }
            std::cout << "Min inbound: " << min1 << ", Max inbound:" << max1 << "\n";
        }
        std::cout << "integrity ok, checked " << connections_checked << " connections\n";
    }

    void
    load_float_data(const char* filename,
                    float*& data,
                    unsigned& num,
                    unsigned& dim) {  // load data with sift10K pattern
        std::ifstream in(filename, std::ios::binary);
        if (!in.is_open()) {
            std::cout << "open file error" << std::endl;
            exit(-1);
        }
        in.read((char*)&dim, 4);
        std::cout << "data dimension: " << dim << std::endl;
        in.seekg(0, std::ios::end);
        std::ios::pos_type ss = in.tellg();
        size_t fsize = (size_t)ss;
        num = (unsigned)(fsize / (dim + 1) / 4);
        data = new float[num * dim * sizeof(float)];

        in.seekg(0, std::ios::beg);
        for (size_t i = 0; i < num; i++) {
            in.seekg(4, std::ios::cur);
            in.read((char*)(data + i * dim), dim * 4);
        }
        in.close();
    }

    //    void load_project_matrix(const char *filename) {
    //        float *raw_data;
    //        unsigned origin_dim, project_dim;
    //        load_float_data(filename, raw_data, origin_dim, project_dim);
    //        A_ = Eigen::MatrixXf(origin_dim, project_dim);
    //        for (int i = 0; i < origin_dim; i++) {
    //            for (int j = 0; j < project_dim; j++) {
    //                A_(i, j) = raw_data[i * project_dim + j]; // load the matrix
    //            }
    //        }
    //    }

    void
    load_product_codebook(const char* filename) {
        std::ifstream in(filename, std::ios::binary);
        in.read((char*)&pq_chunk, sizeof(unsigned));
        in.read((char*)&pq_cluster, sizeof(unsigned));
        in.read((char*)&pq_sub_dim, sizeof(unsigned));
        std::cerr << "sub vec:: " << pq_chunk << " sub cluster:: " << pq_cluster
                  << " sub dim:: " << pq_sub_dim << std::endl;
        pq_book.resize(pq_chunk);
        for (int i = 0; i < pq_chunk; i++) {
            pq_book[i].resize(pq_cluster);
            for (int j = 0; j < pq_cluster; j++) {
                pq_book[i][j].resize(pq_sub_dim);
                in.read((char*)pq_book[i][j].data(), sizeof(float) * pq_sub_dim);
            }
        }
        is_trained_pq = true;
        pq_dim = pq_chunk * pq_sub_dim;
    }

    //    void project_vector(float *raw_data, unsigned num) const {
    //        Eigen::MatrixXf Q(num, pq_dim);
    //        for (int i = 0; i < num; i++) {
    //            for (int j = 0; j < pq_dim; j++) {
    //                Q(i, j) = raw_data[i * pq_dim + j];
    //            }
    //        }
    //        Q = Q * A_;
    //        for (int i = 0; i < num; i++) {
    //            for (int j = 0; j < pq_dim; j++) {
    //                raw_data[i * pq_dim + j] = Q(i, j);
    //            }
    //        }
    //    }

    void
    get_knn_error_quantile(
        float* train_data, size_t dim, size_t num, size_t k = 20, float quantile = 0.995) {
        error_quantile = quantile;
        std::cout << "get: " << k << " NN  quantile: " << quantile << " dim: " << dim
                  << " num: " << num << std::endl;
        std::vector<std::pair<float, tableint>> train_knn(num * k);
        std::vector<float> error_distribution(num * k);
#pragma omp parallel for
        for (int i = 0; i < num; i++) {
            std::priority_queue<std::pair<float, tableint>> result_queue;
            for (int j = 0; j < cur_element_count_; j++) {
                float dist =
                    naive_l2_dist_calc(train_data + i * dim, (float*)getDataByInternalId(j), dim);
                if (result_queue.size() < k)
                    result_queue.emplace(dist, j);
                else if (result_queue.top().first > dist) {
                    result_queue.pop();
                    result_queue.emplace(dist, j);
                }
            }
            int cnt = 0;
            while (!result_queue.empty()) {
                train_knn[i * k + cnt] = result_queue.top();
                result_queue.pop();
                cnt++;
            }
        }
#pragma omp parallel for
        for (int i = 0; i < num; i++) {
            for (int j = 0; j < k; j++) {
                float app_dist =
                    naive_product_dist(train_knn[i * k + j].second, train_data + i * dim);
                error_distribution[i * k + j] = app_dist - train_knn[i * k + j].first;
            }
        }
        std::sort(error_distribution.begin(), error_distribution.end());
        err_quantile_value = error_distribution[num * k * error_quantile];
        std::cout << k << " NN error quantile:: " << err_quantile_value << std::endl;
    }

    void
    encode_hnsw_data_with_codebook(int left_range, int right_range) {
        assert(is_trained_pq);
        is_trained_infer = true;
        pq_map = (uint8_t*)allocator_->Allocate(max_elements_ * pq_chunk * sizeof(uint8_t));
        if (use_node_centroid)
            node_cluster_dist_ = (float*)allocator_->Allocate(max_elements_ * sizeof(float));
        double ave_encode_loss = 0.0;
#pragma omp parallel for
        for (int i = left_range; i < right_range; i++) {
            float dist_to_centroid = 0.0;
            for (int j = 0; j < pq_chunk; j++) {
                uint8_t belong = 0;
                float dist = naive_l2_dist_calc((float*)getDataByInternalId(i) + j * pq_sub_dim,
                                                pq_book[j][0].data(),
                                                pq_sub_dim);
                for (int k = 1; k < pq_cluster; k++) {
                    float new_dist =
                        naive_l2_dist_calc((float*)getDataByInternalId(i) + j * pq_sub_dim,
                                           pq_book[j][k].data(),
                                           pq_sub_dim);
                    if (new_dist < dist) {
                        belong = k;
                        dist = new_dist;
                    }
                }
                dist_to_centroid += dist;
                pq_map[i * pq_chunk + j] = belong;
            }
#pragma omp critical
            ave_encode_loss += dist_to_centroid;
            if (use_node_centroid)
                node_cluster_dist_[i] = dist_to_centroid;
        }
        std::cout << "encode HNSW finished with ave encode loss:: "
                  << ave_encode_loss / (float)(right_range - left_range) << std::endl;
    }

    void
    encode_hnsw_data(size_t chunk_size = 0, size_t cluster_size = 256) {
        size_t vec_dim = *((size_t*)dist_func_param_);
        if (chunk_size == 0) {
            chunk_size = vec_dim;
            if (chunk_size > 512 && chunk_size % 8 == 0) {
                pq_chunk = chunk_size / 8;
                pq_sub_dim = 8;
            } else {
                pq_chunk = chunk_size / 4;
                pq_sub_dim = 4;
            }
        } else {
            pq_chunk = chunk_size;
            pq_sub_dim = vec_dim / pq_chunk;
        }
        pq_dim = vec_dim;
        pq_cluster = cluster_size;
        if (cur_element_count_ < pq_train_bound)
            pq_train_bound = cur_element_count_;
        auto pq_training_data = std::shared_ptr<float[]>(new float[pq_train_bound * vec_dim]);
        for (size_t i = 0; i < pq_train_bound; i++) {
            memcpy(pq_training_data.get() + i * vec_dim, getDataByInternalId(i), data_size_);
        }
        // generate code book;
        pq_book.resize(pq_chunk);
        for (int i = 0; i < pq_chunk; i++) {
            pq_book[i].resize(pq_cluster);
            for (int j = 0; j < pq_cluster; j++) {
                pq_book[i][j].resize(pq_sub_dim);
            }
        }

        diskann::generate_pq_pivots(
            pq_training_data.get(), pq_train_bound, vec_dim, pq_cluster, pq_chunk, 12, pq_book);
        is_trained_pq = true;
        encode_hnsw_data_with_codebook(0, cur_element_count_);
    }

    static __attribute__((always_inline)) inline float
    naive_l2_dist_calc(const float* p, const float* q, const unsigned dim) {
        float ans = 0;
        for (unsigned i = 0; i < dim; i++) {
            ans += (p[i] - q[i]) * (p[i] - q[i]);
        }
        return ans;
    }

    void
    calc_dist_map(float* query, float*& dist_mp) const {
        for (unsigned i = 0; i < pq_chunk; i++) {
            for (unsigned j = 0; j < pq_cluster; j++) {
                dist_mp[i * pq_cluster + j] =
                    naive_l2_dist_calc(query + i * pq_sub_dim, &pq_book[i][j][0], pq_sub_dim);
            }
        }
    }

    float
    naive_product_map_dist(tableint id, const float* dist_mp) const {
        float res = 0;
        for (int i = 0; i < pq_chunk; i++) {
            res += dist_mp[i * pq_cluster + pq_map[id * pq_chunk + i]];
        }
        return res;
    }

    float
    naive_product_dist(tableint id, const float* query) const {
        float res = 0;
        for (int i = 0; i < pq_chunk; i++) {
            res += naive_l2_dist_calc(
                query + i * pq_sub_dim, pq_book[i][pq_map[id * pq_chunk + i]].data(), pq_sub_dim);
        }
        return res;
    }

#ifdef USE_SSE
    __attribute__((always_inline)) inline void
    sse4_product_map_dist(const uint8_t* const pqcode0,
                          const uint8_t* const& pqcode1,
                          const uint8_t* const& pqcode2,
                          const uint8_t* const& pqcode3,
                          float*& dists,
                          __m128& candidates) const {
        candidates =
            _mm_set_ps(dists[pqcode3[0]], dists[pqcode2[0]], dists[pqcode1[0]], dists[pqcode0[0]]);
        // Such perf critical loop. Pls unroll
        for (unsigned j = 1; j < pq_chunk; ++j) {
            const float* const cdist = dists + j * pq_cluster;
            __m128 partial = _mm_set_ps(
                cdist[pqcode3[j]], cdist[pqcode2[j]], cdist[pqcode1[j]], cdist[pqcode0[j]]);
            candidates = _mm_add_ps(candidates, partial);
        }
    }
#endif

#ifdef USE_AVX

    /** Base functions for avx **/
    __attribute__((always_inline)) inline void
    axv8_product_map_dist(const uint8_t* const pqcode0,
                          const uint8_t* const& pqcode1,
                          const uint8_t* const& pqcode2,
                          const uint8_t* const& pqcode3,
                          const uint8_t* const& pqcode4,
                          const uint8_t* const& pqcode5,
                          const uint8_t* const& pqcode6,
                          const uint8_t* const& pqcode7,
                          const float*& dists,
                          __m256& candidates) const {
        candidates = _mm256_set_ps(dists[pqcode7[0]],
                                   dists[pqcode6[0]],
                                   dists[pqcode5[0]],
                                   dists[pqcode4[0]],
                                   dists[pqcode3[0]],
                                   dists[pqcode2[0]],
                                   dists[pqcode1[0]],
                                   dists[pqcode0[0]]);
        // Such perf critical loop. Pls unroll
        for (unsigned j = 1; j < pq_chunk; ++j) {
            const float* const cdist = dists + j * pq_cluster;
            __m256 partial = _mm256_set_ps(cdist[pqcode7[j]],
                                           cdist[pqcode6[j]],
                                           cdist[pqcode5[j]],
                                           cdist[pqcode4[j]],
                                           cdist[pqcode3[j]],
                                           cdist[pqcode2[j]],
                                           cdist[pqcode1[j]],
                                           cdist[pqcode0[j]]);
            candidates = _mm256_add_ps(candidates, partial);
        }
    }
#endif

#ifdef USE_AVX512
    /** Base functions for avx **/
    __attribute__((always_inline)) inline void
    axv16_product_map_dist(const uint8_t* const pqcode0,
                           const uint8_t* const& pqcode1,
                           const uint8_t* const& pqcode2,
                           const uint8_t* const& pqcode3,
                           const uint8_t* const& pqcode4,
                           const uint8_t* const& pqcode5,
                           const uint8_t* const& pqcode6,
                           const uint8_t* const& pqcode7,
                           const uint8_t* const& pqcode8,
                           const uint8_t* const& pqcode9,
                           const uint8_t* const& pqcode10,
                           const uint8_t* const& pqcode11,
                           const uint8_t* const& pqcode12,
                           const uint8_t* const& pqcode13,
                           const uint8_t* const& pqcode14,
                           const uint8_t* const& pqcode15,
                           const float*& dists,
                           __m512& candidates) const {
        candidates = _mm512_set_ps(dists[pqcode15[0]],
                                   dists[pqcode14[0]],
                                   dists[pqcode13[0]],
                                   dists[pqcode12[0]],
                                   dists[pqcode11[0]],
                                   dists[pqcode10[0]],
                                   dists[pqcode9[0]],
                                   dists[pqcode8[0]],
                                   dists[pqcode7[0]],
                                   dists[pqcode6[0]],
                                   dists[pqcode5[0]],
                                   dists[pqcode4[0]],
                                   dists[pqcode3[0]],
                                   dists[pqcode2[0]],
                                   dists[pqcode1[0]],
                                   dists[pqcode0[0]]);
        // Such perf critical loop. Pls unroll
        for (unsigned j = 1; j < pq_chunk; ++j) {
            const float* const cdist = dists + j * pq_cluster;
            __m512 partial = _mm512_set_ps(dists[pqcode15[0]],
                                           dists[pqcode14[0]],
                                           dists[pqcode13[0]],
                                           dists[pqcode12[0]],
                                           dists[pqcode11[0]],
                                           dists[pqcode10[0]],
                                           dists[pqcode9[0]],
                                           dists[pqcode8[0]],
                                           cdist[pqcode7[j]],
                                           cdist[pqcode6[j]],
                                           cdist[pqcode5[j]],
                                           cdist[pqcode4[j]],
                                           cdist[pqcode3[j]],
                                           cdist[pqcode2[j]],
                                           cdist[pqcode1[j]],
                                           cdist[pqcode0[j]]);
            candidates = _mm512_add_ps(candidates, partial);
        }
    }
#endif

    //The default processing length is a multiple of 4, if not, add 0
    void
    pq_scan(const int* id, float* res, float*& dist_mp, unsigned num) const {
        for (int i = 0; i < num; i += 4) {
            //#ifdef USE_AVX512
            //            if (i + 16 < num) {
            //                __m512 candidate_dist;
            //                const uint8_t* const pqcode0 = pq_map + id[i] * pq_chunk;
            //                const uint8_t* const pqcode1 = pq_map + id[i + 1] * pq_chunk;
            //                const uint8_t* const pqcode2 = pq_map + id[i + 2] * pq_chunk;
            //                const uint8_t* const pqcode3 = pq_map + id[i + 3] * pq_chunk;
            //                const uint8_t* const pqcode4 = pq_map + id[i + 4] * pq_chunk;
            //                const uint8_t* const pqcode5 = pq_map + id[i + 5] * pq_chunk;
            //                const uint8_t* const pqcode6 = pq_map + id[i + 6] * pq_chunk;
            //                const uint8_t* const pqcode7 = pq_map + id[i + 7] * pq_chunk;
            //                const uint8_t* const pqcode8 = pq_map + id[i + 8] * pq_chunk;
            //                const uint8_t* const pqcode9 = pq_map + id[i + 9] * pq_chunk;
            //                const uint8_t* const pqcode10 = pq_map + id[i + 10] * pq_chunk;
            //                const uint8_t* const pqcode11 = pq_map + id[i + 11] * pq_chunk;
            //                const uint8_t* const pqcode12 = pq_map + id[i + 12] * pq_chunk;
            //                const uint8_t* const pqcode13 = pq_map + id[i + 13] * pq_chunk;
            //                const uint8_t* const pqcode14 = pq_map + id[i + 14] * pq_chunk;
            //                const uint8_t* const pqcode15 = pq_map + id[i + 15] * pq_chunk;
            //                axv16_product_map_dist(pqcode0,
            //                                       pqcode1,
            //                                       pqcode2,
            //                                       pqcode3,
            //                                       pqcode4,
            //                                       pqcode5,
            //                                       pqcode6,
            //                                       pqcode7,
            //                                       pqcode8,
            //                                       pqcode9,
            //                                       pqcode10,
            //                                       pqcode11,
            //                                       pqcode12,
            //                                       pqcode13,
            //                                       pqcode14,
            //                                       pqcode15,
            //                                       dist_mp,
            //                                       candidate_dist);
            //                _mm512_storeu_ps(res + i, candidate_dist);
            //                i += 16;
            //                continue;
            //            }
            //#endif
            //
            //#ifdef USE_AVX
            //            if (i + 8 < num) {
            //                __m256 candidate_dist;
            //                const uint8_t* const pqcode0 = pq_map + id[i] * pq_chunk;
            //                const uint8_t* const pqcode1 = pq_map + id[i + 1] * pq_chunk;
            //                const uint8_t* const pqcode2 = pq_map + id[i + 2] * pq_chunk;
            //                const uint8_t* const pqcode3 = pq_map + id[i + 3] * pq_chunk;
            //                const uint8_t* const pqcode4 = pq_map + id[i + 4] * pq_chunk;
            //                const uint8_t* const pqcode5 = pq_map + id[i + 5] * pq_chunk;
            //                const uint8_t* const pqcode6 = pq_map + id[i + 6] * pq_chunk;
            //                const uint8_t* const pqcode7 = pq_map + id[i + 7] * pq_chunk;
            //                axv8_product_map_dist(pqcode0,
            //                                      pqcode1,
            //                                      pqcode2,
            //                                      pqcode3,
            //                                      pqcode4,
            //                                      pqcode5,
            //                                      pqcode6,
            //                                      pqcode7,
            //                                      dist_mp,
            //                                      candidate_dist);
            //                _mm256_storeu_ps(res + i, candidate_dist);
            //                i += 8;
            //                continue;
            //            }
            //#endif

#ifdef USE_SSE
            __m128 candidate_dist;
            const uint8_t* const pqcode0 = pq_map + id[i] * pq_chunk;
            const uint8_t* const pqcode1 = pq_map + id[i + 1] * pq_chunk;
            const uint8_t* const pqcode2 = pq_map + id[i + 2] * pq_chunk;
            const uint8_t* const pqcode3 = pq_map + id[i + 3] * pq_chunk;
            sse4_product_map_dist(pqcode0, pqcode1, pqcode2, pqcode3, dist_mp, candidate_dist);
            _mm_storeu_ps(res + i, candidate_dist);
#endif
        }
    }
};
}  // namespace hnswlib
