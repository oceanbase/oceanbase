/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_INCREMENTAL_STAT_ESTIMATOR_H
#define OB_INCREMENTAL_STAT_ESTIMATOR_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"
#include "share/stat/ob_opt_column_stat_cache.h"

namespace oceanbase {
using namespace sql;
namespace common {

class ObIncrementalStatEstimator
{
public:

  static int try_derive_global_stat(ObExecContext &ctx,
                                    const ObTableStatParam &param,
                                    ObExtraParam &extra,
                                    ObIArray<ObOptStat> &approx_part_opt_stats,
                                    ObIArray<ObOptStat> &opt_stats);

  static bool is_part_can_incremental_gather(const ObTableStatParam &param,
                                             int64_t part_id,
                                             int64_t subpart_cnt,
                                             bool is_gather_part);

  static int derive_global_stat_by_direct_load(ObExecContext &ctx, const uint64_t table_id);
private:

  static int derive_global_stat_from_part_stats(ObExecContext &ctx,
                                               const ObTableStatParam &param,
                                               const ObIArray<ObOptStat> &approx_part_opt_stats,
                                               ObIArray<ObOptStat> &opt_stats);

  static int derive_part_stats_from_subpart_stats(ObExecContext &ctx,
                                                 const ObTableStatParam &param,
                                                 const ObIArray<ObOptStat> &gather_opt_stats,
                                                 ObIArray<ObOptStat> &approx_part_opt_stats);

  static int do_derive_part_stats_from_subpart_stats(
    ObExecContext &ctx,
    ObIAllocator &alloc,
    const ObTableStatParam &param,
    const ObIArray<ObOptStat> &no_regather_subpart_opt_stats,
    const ObIArray<ObOptStat> &gather_opt_stats,
    ObIArray<ObOptStat> &approx_part_opt_stats);

  static int get_table_and_column_stats(ObOptStat &src_opt_stat,
                                        const ObTableStatParam &param,
                                        ObIArray<ObOptTableStat> &table_stats,
                                        ObIArray<ObOptColumnStatHandle> &col_handles);

  static int get_part_ids_and_column_ids_info(ObOptStat &src_opt_stat,
                                              const ObTableStatParam &param,
                                              ObIArray<int64_t> &part_ids,
                                              ObIArray<uint64_t> &column_ids);

  static int generate_all_opt_stat(ObIArray<ObOptTableStat> &table_stats,
                                   const ObIArray<ObOptColumnStatHandle> &col_handles,
                                   int64_t col_cnt,
                                   ObIArray<ObOptStat> &all_opt_stats);

  static int do_derive_global_stat(ObExecContext &ctx,
                                   ObIAllocator &alloc,
                                   const ObTableStatParam &param,
                                   ObIArray<ObOptStat> &part_opt_stats,
                                   bool need_derive_hist,
                                   const StatLevel &approx_level,
                                   const int64_t partition_id,
                                   bool &need_gather_hybrid_hist,
                                   ObOptStat &global_opt_stat);

  static int derive_global_tbl_stat(ObExecContext &ctx,
                                    ObIAllocator &alloc,
                                    const ObTableStatParam &param,
                                    const StatLevel &approx_level,
                                    const int64_t partition_id,
                                    ObIArray<ObOptStat> &part_opt_stats,
                                    ObOptStat &global_opt_stat);

  static int derive_global_col_stat(ObExecContext &ctx,
                                    ObIAllocator &alloc,
                                    const ObTableStatParam &param,
                                    ObIArray<ObOptStat> &part_opt_stats,
                                    bool need_derive_hist,
                                    const StatLevel &approx_level,
                                    const int64_t partition_id,
                                    bool &need_gather_hybrid_hist,
                                    ObOptStat &global_opt_stat);

  static int derive_global_histogram(ObIArray<ObHistogram> &all_part_histogram,
                                     common::ObIAllocator &allocator,
                                     int64_t max_bucket_num,
                                     int64_t total_row_count,
                                     int64_t not_null_count,
                                     int64_t num_distinct,
                                     ObHistogram &histogram,
                                     bool &need_gather_hybrid_hist);

  static int get_no_regather_partition_stats(const uint64_t tenant_id,
                                             const uint64_t table_id,
                                             const ObIArray<uint64_t> &column_ids,
                                             const ObIArray<int64_t> &no_regather_partition_ids,
                                             ObIArray<ObOptTableStat> &no_regather_table_stats,
                                             ObIArray<ObOptColumnStatHandle> &no_regather_col_handles,
                                             ObIArray<ObOptStat> &part_opt_stats);

  static int get_column_ids(const ObIArray<ObColumnStatParam> &column_params,
                            ObIArray<uint64_t> &column_ids);

  static int gen_part_param(const ObTableStatParam &param,
                            const ObIArray<ObOptStat> &need_hybrid_hist_opt_stats,
                            ObTableStatParam &part_param);

  static int get_no_regather_subpart_stats(const ObTableStatParam &param,
                                           ObIArray<ObOptTableStat> &no_regather_table_stats,
                                           ObIArray<ObOptColumnStatHandle> &no_regather_col_handles,
                                           ObIArray<ObOptStat> &subpart_opt_stats);

  static int gen_opt_stat_param_by_direct_load(ObExecContext &ctx,
                                               ObIAllocator &alloc,
                                               const uint64_t table_id,
                                               ObTableStatParam &param);

  static int get_all_part_opt_stat_by_direct_load(const ObTableStatParam param,
                                                  ObIArray<ObOptTableStat> &part_tab_stats,
                                                  ObIArray<ObOptColumnStatHandle> &part_col_handles,
                                                  ObIArray<ObOptStat> &part_opt_stats);

};

} // namespace common
} // namespace oceanbase

#endif //OB_INCREMENTAL_STAT_ESTIMATOR_H