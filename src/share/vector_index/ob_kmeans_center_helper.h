/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_KMEANS_CENTER_HELPER_H_
#define SRC_SHARE_VECTOR_INDEX_OB_KMEANS_CENTER_HELPER_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/vector/ob_vector.h"
#include "share/vector_index/ob_ivf_index_sample_cache.h"
#include <sys/stat.h>

namespace oceanbase {
namespace share {
enum ObIvfKMeansStatus {
  PREPARE_CENTERS,
  INIT_CENTERS,
  RUNNING_KMEANS,
  FINISH
};

// use KMeans++ and ElkanKmeans to opt
class ObKMeansCenterHelper {
public:
  ObKMeansCenterHelper()
      : basic_label_(), allocator_label_(), arena_label_(), is_init_(false), tenant_id_(OB_INVALID_TENANT_ID),
        lists_(OB_DEFAULT_VECTOR_IVF_LISTS), init_lists_(OB_DEFAULT_VECTOR_IVF_LISTS),
        cur_idx_(0), total_cnt_(0), failed_times_(0), iterate_times_(0),
        max_iterate_times_(MAX_ITERATE_TIMES), use_elkan_kmeans_(false),
        distance_type_(INVALID_DISTANCE_TYPE), status_(PREPARE_CENTERS),
        center_vectors_(), allocator_(), arena_allocator_(), select_sql_str_(nullptr),
        nearest_centers_(nullptr), lower_bounds_(nullptr), upper_bounds_(nullptr),
        weight_(nullptr), cache_(nullptr)
  {
    MEMSET(basic_label_, '\0', sizeof(basic_label_));
    MEMSET(allocator_label_, '\0', sizeof(allocator_label_));
    MEMSET(arena_label_, '\0', sizeof(arena_label_));
  }
  virtual ~ObKMeansCenterHelper() { destroy(); }
  virtual int init(const int64_t tenant_id, const int64_t lists,
                   const ObVectorDistanceType distance_type,
                   const int64_t max_iterate_times, const bool use_elkan_kmeans);
  bool is_inited() const { return is_init_; }
  void destroy();
  void reuse();

  int build();
  bool is_finish() const { return FINISH == status_; }
  bool skip_insert() const { return is_finish() && 0 == total_cnt_; }
  ObIvfKMeansStatus get_status() const { return status_; }
  int64_t get_lists() const { return lists_; }
  bool is_using_elkan_kmeans() const { return use_elkan_kmeans_; }
  ObVectorDistanceType get_distance_type() const { return distance_type_; }
  int64_t get_max_iterate_times() const { return max_iterate_times_; }
  common::ObSEArray<ObTypeVector *, OB_DEFAULT_VECTOR_IVF_LISTS> &get_center_vectors() { return center_vectors_[cur_idx_]; }
  int set_select_sql_str(ObSqlString &sql_str) { return select_sql_str_.assign(sql_str); }
  void set_cache(ObIndexSampleCache *cache) {
    cache_ = cache;
    total_cnt_ = cache_->get_sample_cnt();
  }
  ObIndexSampleCache *get_cache() const { return cache_; }
  void set_label(const char* label) {
    (void)snprintf(basic_label_, sizeof(basic_label_), "%s", label);
    (void)snprintf(allocator_label_, sizeof(allocator_label_), "%s%s", basic_label_, "KMAlloc");
    (void)snprintf(arena_label_, sizeof(arena_label_), "%s%s", basic_label_, "KMArena");
  }
  int get_nearest_center(const ObTypeVector &vector,
                         int64_t &nearest_center_idx, double &min_distance);

  DECLARE_TO_STRING;

private:
  int init_first_center();
  int init_centers();
  int kmeans();

  int init_first_center_elkan();
  int init_centers_elkan();
  int elkan_kmeans();
  int get_vector_by_sql(const int64_t offset, ObTypeVector *&next_vector);
  int64_t get_next_idx() { return 1L - cur_idx_; }
  void switch_cur_idx() { cur_idx_ = get_next_idx(); }
  int quick_init_centers();

private:
  static constexpr double L2_DISTANCE_ERROR_THRESHOLD = 0.0001;
  static constexpr double L2_SQUARE_ERROR_THRESHOLD = 0.01;
  static const int64_t PAGE_SIZE = (1 << 12); // 4KB
  static const int64_t MAX_ITERATE_TIMES = 100;
  static const int64_t MAX_RETRY_CNT = 0;

private:
  char basic_label_[lib::AOBJECT_LABEL_SIZE + 1];
  char allocator_label_[lib::AOBJECT_LABEL_SIZE + 1];
  char arena_label_[lib::AOBJECT_LABEL_SIZE + 1];
  bool is_init_;      // keep
  int64_t tenant_id_; // keep
  int64_t lists_;     // keep
  int64_t init_lists_;
  int64_t cur_idx_;                    // reset to 0
  int64_t total_cnt_;                  // sample vector counts // reset to 0
  int64_t failed_times_;               // reset to 0
  int64_t iterate_times_;              // iterate times // reset to 0
  int64_t max_iterate_times_;          // keep
  bool use_elkan_kmeans_;              // keep
  ObVectorDistanceType distance_type_; // keep
  ObIvfKMeansStatus status_;        // reset to PREPARE_CENTERS
  common::ObSEArray<ObTypeVector *, OB_DEFAULT_VECTOR_IVF_LISTS>
      center_vectors_[2]; // reset & reserve
  common::ObFIFOAllocator allocator_; // now it is the same as Arena // reset
  common::ObArenaAllocator arena_allocator_; // reset
  // elkan kmeans
  // if 2D(x, c1) <= D(c1, c2), then D(x, c1) < D(x, c2)
  // 因为三角不等式 D(x, c1) + D(x, c2) > D(c1, c2) , 推出 D(x, c2) > D(c1, c2)
  // - D(x, c1) 2D(x, c1) <= D(c1, c2), 代入得到 D(x, c2) > D(x, c1)
  ObSqlString select_sql_str_; // reset
  int32_t *nearest_centers_;   // 每个向量一个结果
  float *lower_bounds_; // 每个向量对于每个聚簇中心有一个结果
  float *upper_bounds_; // 每个向量一个结果
  float *weight_;       // for kmeans++
  // sample cache
  ObIndexSampleCache *cache_; // no need to reset
};

} // namespace share
} // namespace oceanbase

#endif
