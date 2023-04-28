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

#ifndef OB_DBMS_STATS_EXECUTOR_H
#define OB_DBMS_STATS_EXECUTOR_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase {
using namespace sql;
namespace common {

class ObDbmsStatsExecutor
{
public:
  ObDbmsStatsExecutor();

  static int gather_table_stats(ObExecContext &ctx,
                                const ObTableStatParam &param);

  static int gather_index_stats(ObExecContext &ctx,
                                const ObTableStatParam &param);

  static int set_table_stats(ObExecContext &ctx,
                             const ObSetTableStatParam &param);

  static int set_column_stats(ObExecContext &ctx,
                              const ObSetColumnStatParam &param);

  static int delete_table_stats(ObExecContext &ctx,
                                const ObTableStatParam &param,
                                const bool cascade_columns);

  static int delete_column_stats(ObExecContext &ctx,
                                 const ObTableStatParam &param,
                                 const bool only_histogram);

  static int update_online_stat(ObExecContext &ctx,
                                ObTableStatParam &param,
                                share::schema::ObSchemaGetterGuard *schema_guard,
                                const TabStatIndMap &online_table_stats,
                                const ColStatIndMap &online_column_stats);
private:

  static int do_gather_stats(ObExecContext &ctx,
                             const ObTableStatParam &param,
                             ObExtraParam &extra,
                             ObIArray<ObOptStat> &approx_part_opt_stats,
                             ObIArray<ObOptStat> &opt_stats);

  static int do_set_table_stats(const ObSetTableStatParam &param,
                                ObOptTableStat *table_stat);

  static int do_set_column_stats(const ObSetColumnStatParam &param,
                                 ObOptColumnStat *&column_stat);

  static int init_opt_stats(ObIAllocator &allocator,
                            const ObTableStatParam &param,
                            ObExtraParam &extra,
                            ObIArray<ObOptStat> &opt_stats);

  static int init_opt_stat(ObIAllocator &allocator,
                           const ObTableStatParam &param,
                           const ObExtraParam &extra,
                           ObOptStat &stat);

  static int prepare_opt_stat(ObIArray<ObOptStat> &all_stats, ObOptStat *&opt_stat);

  static int check_all_cols_range_skew(const ObTableStatParam &param,
                                       ObIArray<ObOptStat> &opt_stats);

  static int reset_table_locked_state(ObExecContext &ctx,
                                      const ObTableStatParam &param,
                                      const ObIArray<int64_t> &no_stats_partition_ids,
                                      const ObIArray<uint64_t> &part_stattypes);

  static int do_gather_index_stats(ObExecContext &ctx,
                                   const ObTableStatParam &param,
                                   ObExtraParam &extra,
                                   ObIArray<ObOptTableStat *> &all_index_stats);

};

} // end of sql
} // end of namespace

#endif // OB_DBMS_STATS_EXECUTOR_H
