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

#ifndef OB_LOOKUP_TASK_BUILDER_H_
#define OB_LOOKUP_TASK_BUILDER_H_
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_task_info.h"
#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/table/ob_table_partition_ranges.h"

namespace oceanbase {
namespace sql {
class ObPartitionScanRanges;
class ObLookupTaskBuilder {
public:
  explicit ObLookupTaskBuilder(common::ObIAllocator& allocator)
      : allocator_(allocator),
        exec_ctx_(NULL),
        my_plan_(NULL),
        root_op_(NULL),
        root_spec_(NULL),
        lookup_task_list_(),
        lookup_taskinfo_list_(),
        lookup_info_(),
        partitions_already_in_trans_(),
        ob_job_id_(),
        new_task_id_(0),
        weak_read_(false)
  {}
  virtual ~ObLookupTaskBuilder()
  {
    reset();
  }
  void reset();
  int init(const ObLookupInfo& info, const ObPhysicalPlan* my_plan, const ObJobID& ob_job_id, ObExecContext* exec_ctx,
      ObPhyOperator* root_op, ObOpSpec* root_spec = NULL);
  /**
   * build all tasks and task_infos,
   * tasks are grouped by server, their physical plans are same,
   * task_infos are used to store partition and range info.
   */
  int build_lookup_tasks(
      ObExecContext& ctx, ObMultiPartitionsRangesWarpper& partitions_ranges, uint64_t table_id, uint64_t ref_table_id);
  int rebuild_overflow_task(const ObMiniTaskRetryInfo& retry_info);
  inline common::ObIArray<ObMiniTask>& get_lookup_task_list()
  {
    return lookup_task_list_;
  }
  inline common::ObIArray<ObTaskInfo*>& get_lookup_taskinfo_list()
  {
    return lookup_taskinfo_list_;
  }

private:
  int get_partition_server(int64_t partition_id, ObAddr& run_server);
  int add_range_to_taskinfo(int64_t partition_id, ObIArray<common::ObNewRange>& ranges, const ObAddr& run_server);
  int create_op_input(ObExecContext& ctx, const ObPhyOperator& root_op);
  int get_new_part_ranges(const common::ObIArray<ObTaskInfo::ObPartLoc>& parts_loc, int64_t& succ_range_count,
      common::ObIArray<ObTaskInfo::ObPartLoc>& new_parts_loc, ObMiniTask& new_task);
  /**
   * lookup by row to be compatible with previous small version.
   */
  int rebuild_task_by_single_range(const ObMiniTask& failed_task, const ObTaskInfo& failed_task_info,
      common::ObIArray<ObMiniTask>& retry_task_list, common::ObIArray<ObTaskInfo*>& retry_task_info_list);
  int rebuild_task_by_all_failed_ranges(const ObMiniTask& failed_task, const ObTaskInfo& failed_task_info,
      int64_t& succ_range_count, common::ObIArray<ObMiniTask>& retry_task_list,
      common::ObIArray<ObTaskInfo*>& retry_task_info_list);

private:
  common::ObIAllocator& allocator_;
  ObExecContext* exec_ctx_;
  const ObPhysicalPlan* my_plan_;
  ObPhyOperator* root_op_;
  ObOpSpec* root_spec_;
  common::ObSEArray<ObMiniTask, 16> lookup_task_list_;
  common::ObSEArray<ObTaskInfo*, 16> lookup_taskinfo_list_;
  ObLookupInfo lookup_info_;
  common::ObSEArray<int64_t, 16> partitions_already_in_trans_;
  ObJobID ob_job_id_;
  uint64_t new_task_id_;
  bool weak_read_;

private:
  static const int64_t LOCAL_TASK_POS = 0;
};

}  // namespace sql
}  // namespace oceanbase

#endif
