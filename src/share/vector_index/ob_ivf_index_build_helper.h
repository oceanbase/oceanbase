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

#ifndef SRC_SHARE_VECTOR_INDEX_OB_IVF_INDEX_BUILD_HELPER_H_
#define SRC_SHARE_VECTOR_INDEX_OB_IVF_INDEX_BUILD_HELPER_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_ivf_index_sample_cache.h"
#include "share/vector_index/ob_kmeans_center_helper.h"
#include <string>
#include <sys/stat.h>

namespace oceanbase {
namespace sql {
class ObTableScanOp;
}
namespace share {

// use KMeans++ and ElkanKmeans to opt
class ObIvfIndexBuildHelper {
public:
  ObIvfIndexBuildHelper(const char* label)
      : basic_label_(), build_label_(), is_init_(false), tenant_id_(OB_INVALID_TENANT_ID),
        allocator_(), partition_name_(), partition_idx_(-1)
  {
    MEMSET(basic_label_, '\0', sizeof(basic_label_));
    MEMSET(build_label_, '\0', sizeof(build_label_));
    (void)snprintf(basic_label_, sizeof(basic_label_), "%s", label);
    (void)snprintf(build_label_, sizeof(build_label_), "%s%s", basic_label_, "Build");
  }
  virtual ~ObIvfIndexBuildHelper() { destroy(); }
  virtual int init(const int64_t tenant_id, const int64_t lists,
           const ObVectorDistanceType distance_type);
  bool is_inited() const { return is_init_; }
  virtual void destroy();
  virtual void reuse();

  virtual int build();
  virtual bool is_finish() const { return center_helper_.is_finish(); }
  virtual bool skip_insert() const { return center_helper_.skip_insert(); }
  int64_t get_lists() const { return center_helper_.get_lists(); }

  int set_sample_cache(ObIndexSampleCache *cache);
  virtual int set_center_cache(const int64_t table_id);

  virtual int set_partition_name(common::ObTabletID &tablet_id, uint64_t base_table_id);
  virtual int construct_batch_insert_second_container_sql(
      common::ObSqlString &container_string, const int64_t dest_table_id,
      const int64_t second_container_table_id) { return OB_NOT_SUPPORTED; }
  int construct_batch_insert_container_sql(
      common::ObSqlString &container_string, const int64_t dest_table_id,
      const int64_t container_table_id);
  virtual int construct_batch_insert_index_sql(sqlclient::ObMySQLResult &result,
                                       common::ObSqlString &index_string,
                                       int64_t &row_count, int64_t &idx);
  int construct_batch_insert_index_sql_simple(common::ObSqlString &index_string,
                                              const int64_t dest_table_id);
  int construct_select_sql_string(common::ObSqlString &select_string,
                                         const int64_t dest_table_id,
                                         const int64_t data_table_id);
  int init_center_dummy_pkeys_array();
  DECLARE_TO_STRING;

protected:
  int construct_batch_insert_container_sql_simple(
    common::ObSqlString &container_string, const int64_t dest_table_id,
    const int64_t container_table_id, bool need_seg_idx = false);
  int pre_fill_data_insert_index_sql(
    sqlclient::ObMySQLResult &result, ObTypeVector &vector, common::ObSqlString &index_string,
    int64_t &column_cnt, double &min_distance, int64_t &nearest_center_idx, bool &first_row);
  int post_fill_data_insert_index_sql(
    ObCastCtx &cast_ctx, sqlclient::ObMySQLResult &result, common::ObSqlString &index_string,
    const ObString &vector_str, int64_t &column_cnt, const int64_t nearest_center_idx);
  int get_patch_pkeys_for_center_dummy_pkeys_array(ObString &patch_str);
  static const int64_t PAGE_SIZE = (1 << 12); // 4KB
  static const int64_t BATCH_INSERT_SIZE = 1000;

protected:
  char basic_label_[lib::AOBJECT_LABEL_SIZE + 1];
  char build_label_[lib::AOBJECT_LABEL_SIZE + 1];
  bool is_init_;      // keep
  int64_t tenant_id_; // keep
  ObKMeansCenterHelper center_helper_; // no need to reset while reusing (call reuse)
  common::ObSEArray<ObString, OB_DEFAULT_VECTOR_IVF_LISTS>
      center_dummy_pkeys_;            // no need to reset while reusing
  common::ObFIFOAllocator allocator_; // now it is the same as Arena // reset
  ObString partition_name_;
  int64_t partition_idx_;
  common::ObArenaAllocator allocator_for_partition_name_;
};

} // namespace share
} // namespace oceanbase

#endif
