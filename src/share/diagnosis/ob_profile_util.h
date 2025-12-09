/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"
#include "share/diagnosis/ob_runtime_metrics.h"

namespace oceanbase
{
namespace common
{
template<typename MetricType>
class ObOpProfile;
typedef ObOpProfile<ObMetric> ObProfile;
typedef ObOpProfile<ObMergeMetric> ObMergedProfile;

namespace sqlclient
{
class ObMySQLResult;
} // namespace sqlclient

struct ObProfileItem
{
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    return 0;
  };
  ObAddr addr_;
  int64_t thread_id_{0};
  int64_t op_id_{0};
  int64_t plan_depth_{0};
  ObString op_name_;
  int64_t open_time_{0};
  int64_t close_time_{0};
  int64_t first_row_time_{0};
  int64_t last_row_time_{0};
  int64_t output_batches_{0};
  int64_t skipped_rows_{0};
  int64_t rescan_times_{0};
  int64_t output_rows_{0};
  int64_t db_time_{0};
  int64_t io_time_{0};
  int64_t workarea_mem_{0};
  int64_t workarea_max_mem_{0};
  int64_t workarea_tempseg_{0};
  int64_t workarea_max_tempseg_{0};
  ObString sql_id_;
  uint64_t plan_hash_value_{0};
  int64_t other_1_id_{0};
  int64_t other_1_value_{0};
  int64_t other_2_id_{0};
  int64_t other_2_value_{0};
  int64_t other_3_id_{0};
  int64_t other_3_value_{0};
  int64_t other_4_id_{0};
  int64_t other_4_value_{0};
  int64_t other_5_id_{0};
  int64_t other_5_value_{0};
  int64_t other_6_id_{0};
  int64_t other_6_value_{0};
  int64_t other_7_id_{0};
  int64_t other_7_value_{0};
  int64_t other_8_id_{0};
  int64_t other_8_value_{0};
  int64_t other_9_id_{0};
  int64_t other_9_value_{0};
  int64_t other_10_id_{0};
  int64_t other_10_value_{0};
  ObProfile *profile_{nullptr};
};


struct ObMergedProfileItem
{
  static bool cmp_by_db_time(const ObMergedProfileItem &a, const ObMergedProfileItem &b)
  {
    return a.max_db_time_ < b.max_db_time_;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    return 0;
  };
  int init_from(ObIAllocator *alloc, const ObProfileItem &profile_item);
  int64_t op_id_{0};
  int64_t plan_depth_{0};
  uint64_t max_db_time_{0};
  int64_t plan_hash_value_{0};
  ObString sql_id_;
  int64_t parallel_{0};
  ObMergedProfile *profile_{nullptr};
};

struct ExecutionBound
{
  TO_STRING_KV(K_(start_idx), K_(end_idx), K_(execution_count));
  int64_t start_idx_;
  int64_t end_idx_;
  int64_t execution_count_;
};

class ObProfileUtil
{
public:
  // session_tenant_id used to launch inner sql
  // param_tenant_id used to fetch sql plan monitor in where condition
  static int get_profile_by_id(ObIAllocator *alloc, int64_t session_tenant_id,
                               const ObString &trace_id, const ObString &svr_ip, int64_t svr_port,
                               int64_t param_tenant_id, bool fetch_all_op, int64_t op_id,
                               ObIArray<ObProfileItem> &profile_items);
  static int get_merged_profiles(ObIAllocator *alloc, const ObIArray<ObProfileItem> &profile_items,
                                 ObIArray<ObMergedProfileItem> &merged_profile_items,
                                 ObIArray<ExecutionBound> &execution_bounds);
  static int merge_profile(ObMergedProfile &merged_profile, const ObProfile *piece_profile,
                           ObIAllocator *alloc);

private:
  static int inner_get_profile(ObIAllocator *alloc, int64_t tenant_id, const ObSqlString &sql,
                               ObIArray<ObProfileItem> &profile_items);

  static int read_profile_from_result(ObIAllocator *alloc,
                                      const common::sqlclient::ObMySQLResult &mysql_result,
                                      ObProfileItem &profile_item);
  static int fill_metrics_into_profile(ObProfileItem &profile_item);
};

} // namespace common
} // namespace oceanbase
