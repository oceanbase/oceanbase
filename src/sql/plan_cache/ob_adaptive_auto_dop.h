/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_ADAPTIVE_AUTO_DOP_
#define OCEANBASE_SQL_PLAN_CACHE_OB_ADAPTIVE_AUTO_DOP_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/ob_physical_plan.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{
class ObBatchEstTasks;
class ObCostTableScanSimpleInfo;
class ObDASTabletLoc;
typedef common::hash::ObHashMap<int64_t, int64_t,
        common::hash::NoPthreadDefendMode> AutoDopHashMap;

class ObAdaptiveAutoDop
{
public:
  ObAdaptiveAutoDop(ObExecContext &exec_ctx)
    : ctx_(exec_ctx)
  {

  }
  int calculate_table_auto_dop(const ObPhysicalPlan &plan,
                               AutoDopHashMap &map,
                               bool &is_single_part);

  VIRTUAL_TO_STRING_KV(K(&ctx_));

private:
  int inner_calculate_table_auto_dop(const ObOpSpec &spec, AutoDopHashMap &map, int64_t &table_dop,
                                     bool &is_single_part);
  int calculate_tsc_auto_dop(const ObOpSpec &spec, int64_t &table_dop, bool &is_single_part);
  int build_storage_estimation_tasks(const ObTableScanSpec &tsc_spec,
                                     const ObCostTableScanSimpleInfo &cost_tsc_info,
                                     ObQueryRangeArray &ranges, ObIArray<ObBatchEstTasks *> &tasks,
                                     bool &is_single_part, int64_t &part_cnt);
  int add_estimation_tasks(const ObTableScanSpec &tsc_spec,
                           const ObCostTableScanSimpleInfo &cost_tsc_info,
                           const int64_t schema_version, ObDASTabletLoc *tablet_loc,
                           ObQueryRangeArray &ranges, ObIArray<ObBatchEstTasks *> &tasks);
  int construct_scan_range_batch(ObIAllocator &allocator, const ObQueryRangeArray &scan_ranges,
                                 ObSimpleBatch &batch);
  int do_storage_estimation(ObBatchEstTasks &tasks);
  int do_storage_estimation(ObIArray<ObBatchEstTasks *> &tasks, bool &res_reliable);
  int calculate_tsc_auto_dop(const ObIArray<ObBatchEstTasks *> &tasks,
                             const ObCostTableScanSimpleInfo &cost_tsc_info, int64_t part_cnt,
                             int64_t &table_dop);
  int get_task(ObIArray<ObBatchEstTasks *> &tasks, const ObAddr &addr, ObBatchEstTasks *&task);
  int choose_storage_estimation_partitions(const int64_t partition_limit,
                                           const DASTabletLocSEArray &tablet_locs,
                                           DASTabletLocSEArray &chosen_tablet_locs);

private:
  ObExecContext &ctx_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_ADAPTIVE_AUTO_DOP_
