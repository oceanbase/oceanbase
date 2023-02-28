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

#ifndef OB_DBMS_STATS_UTILS_H
#define OB_DBMS_STATS_UTILS_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_table_stat_cache.h"
#include "share/stat/ob_opt_column_stat_cache.h"

namespace oceanbase {
namespace common {

class ObDbmsStatsUtils
{
public:
  static int get_part_info(const ObTableStatParam &param,
                           const ObExtraParam &extra,
                           PartInfo &part_info);

  static int init_col_stats(ObIAllocator &allocator,
                            int64_t col_cnt,
                            ObIArray<ObOptColumnStat *> &col_stats);

  static int check_range_skew(ObHistType hist_type,
                              const ObHistogram::Buckets &bkts,
                              int64_t standard_cnt,
                              bool &is_even_distributed);

  static int split_batch_write(sql::ObExecContext &ctx,
                               ObIArray<ObOptTableStat*> &table_stats,
                               ObIArray<ObOptColumnStat*> &column_stats,
                               const bool is_index_stat = false,
                               const bool is_history_stat = false);

  static int batch_write_history_stats(sql::ObExecContext &ctx,
                                       ObIArray<ObOptTableStatHandle> &history_tab_handles,
                                       ObIArray<ObOptColumnStatHandle> &history_col_handles);

  static int cast_number_to_double(const number::ObNumber &src_val, double &dst_val);

  static int check_table_read_write_valid(const uint64_t tenant_id, bool &is_valid);

  static int check_is_stat_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                 const uint64_t tenant_id,
                                 const int64_t table_id,
                                 bool &is_valid);

  static int check_is_sys_table(share::schema::ObSchemaGetterGuard &schema_guard,
                                   const uint64_t tenant_id,
                                   const int64_t table_id,
                                   bool &is_valid);

  static bool is_no_stat_virtual_table(const int64_t table_id);

  static bool is_virtual_index_table(const int64_t table_id);

  static int parse_granularity(const ObString &granularity,
                               bool &need_global,
                               bool &need_approx_global,
                               bool &need_part,
                               bool &need_subpart);

  static bool is_subpart_id(const ObIArray<PartInfo> &partition_infos,
                            const int64_t partition_id,
                            int64_t &part_id);
  
  static int get_valid_duration_time(const int64_t start_time,
                                     const int64_t max_duration_time,
                                     int64_t &valid_duration_time);

  static int get_dst_partition_by_tablet_id(sql::ObExecContext &ctx,
                                            const uint64_t tablet_id,
                                            const ObIArray<PartInfo> &partition_infos,
                                            int64_t &partition_id);
private:
  static int batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                         const uint64_t tenant_id,
                         ObIArray<ObOptTableStat *> &table_stats,
                         ObIArray<ObOptColumnStat*> &column_stats,
                         const int64_t current_time,
                         const bool is_index_stat,
                         const bool is_history_stat);

};

}
}

#endif // OB_DBMS_STATS_UTILS_H
