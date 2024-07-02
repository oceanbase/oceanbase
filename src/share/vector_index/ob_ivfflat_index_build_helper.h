/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVFFLAT_INDEX_BUILD_HELPER_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVFFLAT_INDEX_BUILD_HELPER_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/vector/ob_vector.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_ivfflat_index_sample_cache.h"

namespace oceanbase {
namespace sql {
class ObTableScanOp;
}
namespace share {

class ObIvfflatTmpFileMgr {
public:
private:
  int64_t dir_;
  int64_t samples_file_fd_; //
};

enum ObIvfflatBuildStatus {
  PREPARE_CENTERS,
  INIT_CENTERS,
  RUNNING_KMEANS,
  FINISH
};

// use KMeans++ and ElkanKmeans to opt
class ObIvfflatIndexBuildHelper {
public:
  struct IvfflatCentersMemCtx {
    common::ObSEArray<int64_t, OB_DEFAULT_VECTOR_IVFFLAT_LISTS> center_cnt_per_part_;
    int64_t cur_centers_offsets_;  // float

    IvfflatCentersMemCtx(): cur_centers_offsets_(0) {}
    void reset() {
      center_cnt_per_part_.reset();
      cur_centers_offsets_ = 0;
    }
    int64_t get_cur_total_size() { return cur_centers_offsets_; }
    int64_t get_partition_num() { return center_cnt_per_part_.count(); }
    // Only be called when add_new_centers finished.
    common::ObIArray<int64_t>* get_center_cnt_per_part() {
      for (int64_t i = 1; i < center_cnt_per_part_.count(); ++i) {
        center_cnt_per_part_.at(i) += center_cnt_per_part_.at(i - 1);
      }
      return &center_cnt_per_part_;
    }
    void add_new_centers(int64_t center_cnt, int64_t dims) {
      center_cnt_per_part_.push_back(center_cnt);
      cur_centers_offsets_ += (center_cnt * dims);
    }
  };

  ObIvfflatIndexBuildHelper()
      : is_init_(false), tenant_id_(OB_INVALID_TENANT_ID),
        lists_(OB_DEFAULT_VECTOR_IVFFLAT_LISTS),
        init_lists_(OB_DEFAULT_VECTOR_IVFFLAT_LISTS), cur_idx_(0),
        total_cnt_(0), failed_times_(0), iterate_times_(0),
        max_iterate_times_(MAX_ITERATE_TIMES), elkan_kmeans_(false),
        distance_type_(INVALID_DISTANCE_TYPE), status_(PREPARE_CENTERS),
        center_vectors_(), allocator_(), arena_allocator_(), select_sql_str_(),
        nearest_centers_(nullptr), lower_bounds_(nullptr),
        upper_bounds_(nullptr), weight_(nullptr), cache_(nullptr),
        partition_name_(), partition_idx_(-1), partition_num_(0), centers_(),
        vector_dim_(0)
  {}
  virtual ~ObIvfflatIndexBuildHelper() { destroy(); }
  int init(const int64_t tenant_id, const int64_t lists,
           const ObVectorDistanceType distance_type, const int64_t partition_num,
           uint64_t vector_dim = 0);
  bool is_inited() const { return is_init_; }
  void destroy();
  void reuse();

  int build();
  bool is_finish() const { return FINISH == status_; }
  bool skip_insert() const { return FINISH == status_ && 0 == total_cnt_; }
  int64_t get_lists() const { return lists_; }
  ObVectorArray &get_centers() { return centers_; }
  int get_serialized_centers(char *&buf, int64_t &size, ObIAllocator &allocator);

  int set_sample_cache(ObIvfflatFixSampleCache *cache);
  int set_center_cache(const int64_t table_id);

  int set_partition_name(common::ObTabletID &tablet_id, uint64_t base_table_id);
  int construct_batch_insert_container_sql_simple(
      common::ObSqlString &container_string, const int64_t dest_table_id,
      const int64_t container_table_id);
  int construct_batch_insert_index_sql(sqlclient::ObMySQLResult &result,
                                       common::ObSqlString &index_string,
                                       int64_t &row_count, int64_t &idx);
  int construct_batch_insert_index_sql_simple(common::ObSqlString &index_string,
                                              const int64_t dest_table_id);
  int construct_select_sql_string_simple(common::ObSqlString &select_string,
                                         const int64_t dest_table_id,
                                         const int64_t data_table_id);
  int construct_part_select_sql_string(share::schema::ObSchemaGetterGuard &schema_guard,
                                       common::ObSqlString &select_string,
                                       ObTableSchema &data_schema,
                                       const int64_t partition_index,
                                       const int64_t index_column_id);
  int init_center_dummy_pkeys_array();
  int get_patch_pkeys_for_center_dummy_pkeys_array(ObString &patch_str);
  int copy_centers(ObIAllocator &allocator);
  void reset_centers();
  DECLARE_TO_STRING;

private:
  int init_first_center();
  int init_centers();
  int ivfflat_kmeans();

  int init_first_center_elkan();
  int init_centers_elkan();
  int ivfflat_elkan_kmeans();
  int get_vector_by_sql(const int64_t offset, ObTypeVector *&next_vector);
  int get_nearest_center(const ObTypeVector &vector,
                         int64_t &nearest_center_idx, double &min_distance);

  int64_t get_next_idx() { return 1L - cur_idx_; }
  void switch_cur_idx() { cur_idx_ = get_next_idx(); }
  int alloc_and_copy_vector(const ObTypeVector &other, ObTypeVector *&vector);
  int alloc_random_vector(ObTypeVector *&vector, const int64_t vector_size);
  int alloc_vector(ObTypeVector *&vector, const int64_t vector_size);
  void destory_vector(ObTypeVector *&vector);
  void reuse_array(ObIArray<ObTypeVector *> &array);
  int quick_init_centers();

private:
  static constexpr double L2_DISTANCE_ERROR_THRESHOLD = 0.0001;
  static constexpr double L2_SQUARE_ERROR_THRESHOLD = 0.01;
  static const int64_t MAX_ITERATE_TIMES = 100;
  static const int64_t PAGE_SIZE = (1 << 12); // 4KB
  static const int64_t MAX_MEMORY_SIZE =
      (1 << 30); // 1GB // TODO(@jingshui) limit the max memory size
  static const int64_t MAX_RETRY_CNT = 0;
  static const int64_t BATCH_INSERT_SIZE = 1000;

private:
  bool is_init_;      // keep
  int64_t tenant_id_; // keep
  int64_t lists_;     // keep
  int64_t init_lists_;
  int64_t cur_idx_;                    // reset to 0
  int64_t total_cnt_;                  // sample vector counts // reset to 0
  int64_t failed_times_;               // reset to 0
  int64_t iterate_times_;              // iterate times // reset to 0
  int64_t max_iterate_times_;          // keep
  bool elkan_kmeans_;                  // keep
  ObVectorDistanceType distance_type_; // keep
  ObIvfflatBuildStatus status_;        // reset to PREPARE_CENTERS
  common::ObSEArray<ObTypeVector *, OB_DEFAULT_VECTOR_IVFFLAT_LISTS>
      center_vectors_[2]; // reset & reserve
  common::ObSEArray<ObString, OB_DEFAULT_VECTOR_IVFFLAT_LISTS>
      center_dummy_pkeys_;            // no need to reset while reusing
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
  ObIvfflatFixSampleCache *cache_; // no need to reset
  ObString partition_name_;
  int64_t partition_idx_;
  common::ObArenaAllocator allocator_for_partition_name_;
  int64_t partition_num_;
  ObVectorArray centers_; // TODO(@jingshui): use raw ptr to opt
  uint64_t vector_dim_;
  IvfflatCentersMemCtx centers_mem_ctx_;
};

} // namespace share
} // namespace oceanbase

#endif