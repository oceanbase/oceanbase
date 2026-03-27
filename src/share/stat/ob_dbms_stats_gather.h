/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_STATS_GATHER_H
#define OB_DBMS_STATS_GATHER_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase {
using namespace sql;
namespace common {

class ObDbmsStatsGather
{
public:
  ObDbmsStatsGather();

  static int gather_stats(ObExecContext &ctx,
                          const ObOptStatGatherParam &param,
                          ObOptStatGatherAudit &audit,
                          ObIArray<ObOptStat> &opt_stats);

  static int gather_index_stats(ObExecContext &ctx,
                                const ObOptStatGatherParam &param,
                                ObIArray<ObOptStat> &opt_stats,
                                ObIArray<ObOptTableStat *> &all_index_stats,
                                ObIArray<ObOptColumnStat *> &all_column_stats);
private:

  static int init_opt_stats(ObIAllocator &allocator,
                            const ObOptStatGatherParam &param,
                            ObIArray<ObOptStat> &opt_stats);

  static int init_opt_stat(ObIAllocator &allocator,
                           const ObOptStatGatherParam &param,
                           const int64_t part_id,
                           const int64_t part_stattype,
                           ObOptStat &stat);

  static int classfy_column_histogram(const ObOptStatGatherParam &param,
                                      ObOptStat &opt_stat);

  static int adjust_sample_param(const ObIArray<ObOptStat> &opt_stats, ObOptStatGatherParam &param);
};

} // end of sql
} // end of namespace

#endif // OB_DBMS_STATS_GATHER_H
