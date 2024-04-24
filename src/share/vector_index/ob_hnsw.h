/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_HNSW_INDEX_HNSW_H
#define OCEANBASE_SHARE_HNSW_INDEX_HNSW_H

#include "common/object/ob_object.h"
#include "lib/hash/ob_linear_hash_map.h"
#include <stdint.h>

namespace oceanbase {
namespace share {

struct ObHNSWOption {
  typedef int (*DistanceFunc)(const ObTypeVector &, const ObTypeVector &,
                              double &distance);
  static const int HNSW_MAX_LEVEL = 32;
  static const int64_t CACHE_MAX_SKIP = 4;
  ObHNSWOption() : m_(16), ef_construction_(200) { dfunc_ = nullptr; }
  ObHNSWOption(int64_t m, int64_t ef_construction, DistanceFunc dfunc)
      : m_(m), ef_construction_(ef_construction) {
    dfunc_ = dfunc;
  }
  int64_t get_max_m_on_lv(int64_t lv) { return lv > 0 ? m_ : m_ * 2; }
  int64_t m_;               // [5, 48] is recommanded.
  int64_t ef_construction_; // [200, 400] is recomanded.
  int max_level_;
  double ml_;
  DistanceFunc dfunc_;
};

struct ObHNSWCPoolPtr {
  ObHNSWCPoolPtr() {
    n_objs_ = -1;
    objs_ = nullptr;
  }
  bool is_valid() const {
    return n_objs_ != -1 && objs_ != nullptr && nullptr != buf_str_.ptr();
  }
  int64_t n_objs_;
  common::ObObj *objs_;
  ObString buf_str_;
  TO_STRING_KV(K_(n_objs));
};

struct ObHNSWElement;
struct ObHNSWNeighbor {
  enum NeighborStatus {
    ADDED = 0,
    ORIGINAL,
  };
  ObHNSWNeighbor() : status_(ADDED), distance_(0.0), element_(nullptr) {
    skip_cnt_ = -1;
    cache_epoch_ = 0;
  }
  NeighborStatus status_;
  double distance_;        // distance that persists in table
  ObHNSWElement *element_; // fast path, never direct access the field!!
  ObHNSWCPoolPtr pk_ptr_;  //
  // fast path: set with element_
  int skip_cnt_; // *********** TODO
                 // The skip_cnt_ at the time of inserting into cache
                 // if ObHNSWNeighbor::cache_lv_ > ObHNSWElement::cache_lv_ &&
                 // cache_epoch_ != global_cache_epoch_ then element_ ptr is
                 // invalid!
  uint64_t cache_epoch_;
};

struct ObHNSWLevelNeighbors {
  static const int MAX_NEIGHBORS_NUM = 100;
  ObHNSWLevelNeighbors() {
    n_nb_ = 0;
    MEMSET(nbs_, 0, sizeof(nbs_));
  }
  void add_origin_neighbor(double dis, ObHNSWCPoolPtr ptr,
                           ObHNSWNeighbor::NeighborStatus nb_status,
                           ObHNSWElement *element = nullptr, int skip_cnt = -1,
                           uint64_t cache_epoch = 0) {
    OB_ASSERT(n_nb_ < MAX_NEIGHBORS_NUM);
    nbs_[n_nb_].status_ = nb_status;
    nbs_[n_nb_].element_ = element;
    nbs_[n_nb_].distance_ = dis;
    nbs_[n_nb_].pk_ptr_ = ptr;
    nbs_[n_nb_].skip_cnt_ = skip_cnt;
    nbs_[n_nb_].cache_epoch_ = cache_epoch;
    n_nb_++;
  }
  int64_t find_furthest_neighbor() {
    int64_t f_idx = 0;
    for (int64_t i = 0; i < n_nb_; ++i) {
      if (nbs_[i].distance_ > nbs_[f_idx].distance_) {
        f_idx = i;
      }
    }
    return f_idx;
  }
  void set_lv_neighbor(int64_t nb_idx, double distance, ObHNSWCPoolPtr ptr) {
    n_nb_ = nb_idx + 1;
    nbs_[nb_idx].status_ = ObHNSWNeighbor::ORIGINAL;
    nbs_[nb_idx].element_ = nullptr;
    nbs_[nb_idx].distance_ = distance;
    nbs_[nb_idx].pk_ptr_ = ptr;
    nbs_[nb_idx].skip_cnt_ = -1;
    nbs_[nb_idx].cache_epoch_ = 0;
  }
  void replace_neighbor_with_ele(int64_t replace_idx, ObHNSWElement *ele,
                                 ObHNSWCPoolPtr pk_ptr, double dis);
  TO_STRING_KV(K_(n_nb));
  int64_t n_nb_;
  ObHNSWNeighbor nbs_[MAX_NEIGHBORS_NUM + 1];
};

struct ObHNSWElement {
  ObHNSWElement() {
    skip_cnt_ = -1;
    cache_epoch_ = 0;
    MEMSET(neighbors_per_level_, 0, sizeof(neighbors_per_level_));
  }
  bool is_valid() const { return vector_.is_valid() && pk_ptr_.is_valid(); }
  int set_level(int64_t level, ObIAllocator *allocator);
  int deep_copy(const ObHNSWElement &other, ObIAllocator *allocator,
                int skip_cnt = -1, int64_t cache_epoch = 0);
  static int alloc_and_deep_copy_cpool_ptr(ObHNSWCPoolPtr &dest_ptr,
                                           const ObHNSWCPoolPtr &src_ptr,
                                           ObIAllocator *allocator);
  int skip_cnt_; // *********** TODO
                 // The skip_cnt_ at the time of inserting into cache
  uint64_t cache_epoch_;
  ObHNSWCPoolPtr pk_ptr_;
  common::ObTypeVector vector_;
  // neighbors of different level.
  ObHNSWLevelNeighbors *neighbors_per_level_[ObHNSWOption::HNSW_MAX_LEVEL + 1];
};

struct ObHNSWCandidate // user hold a list of ObHNSWCandidates
{
  bool operator<(const ObHNSWCandidate &cd) const {
    return candi_distance_ < cd.candi_distance_;
  }

  bool operator>(const ObHNSWCandidate &cd) const {
    return candi_distance_ > cd.candi_distance_;
  }
  TO_STRING_KV(K_(candi_distance));
  ObHNSWElement *element_;
  double candi_distance_;
  int skip_cnt_; // *********** TODO
};

inline void ObHNSWLevelNeighbors::replace_neighbor_with_ele(
    int64_t replace_idx, ObHNSWElement *ele, ObHNSWCPoolPtr pk_ptr,
    double dis) {
  nbs_[replace_idx].status_ = ObHNSWNeighbor::ADDED;
  nbs_[replace_idx].element_ = ele;
  nbs_[replace_idx].distance_ = dis;
  nbs_[replace_idx].pk_ptr_ = pk_ptr;
  nbs_[replace_idx].cache_epoch_ = ele->cache_epoch_;
  nbs_[replace_idx].skip_cnt_ = ele->skip_cnt_;
}

} // namespace share
} // namespace oceanbase

#endif