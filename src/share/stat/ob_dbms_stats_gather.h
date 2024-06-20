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
                          ObIArray<ObOptStat> &opt_stats);

  static int gather_index_stats(ObExecContext &ctx,
                                const ObOptStatGatherParam &param,
                                ObIArray<ObOptTableStat *> &all_index_stats);
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

};

} // end of sql
} // end of namespace

#endif // OB_DBMS_STATS_GATHER_H
