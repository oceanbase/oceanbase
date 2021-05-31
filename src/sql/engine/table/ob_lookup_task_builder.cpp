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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_lookup_task_builder.h"
#include "sql/engine/table/ob_table_scan.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase {
namespace sql {
int ObLookupTaskBuilder::init(const ObLookupInfo& info, const ObPhysicalPlan* my_plan, const ObJobID& ob_job_id,
    ObExecContext* exec_ctx, ObPhyOperator* root_op, ObOpSpec* root_spec /*= NULL*/)
{
  int ret = OB_SUCCESS;
  const ObPhysicalPlanCtx* phy_plan_ctx = nullptr;
  ObConsistencyLevel consistency = ObConsistencyLevel::INVALID_CONSISTENCY;
  root_op_ = root_op;
  root_spec_ = root_spec;
  if (OB_ISNULL(exec_ctx_ = exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the exec context can not be null", K(ret));
  } else if (OB_ISNULL(my_plan_ = my_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the plan can not be null", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the plan ctx can not be null", K(ret));
  } else if (ObConsistencyLevel::INVALID_CONSISTENCY == (consistency = phy_plan_ctx->get_consistency_level())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the consistency is invalid", K(ret));
  } else if ((OB_ISNULL(root_op_) && OB_ISNULL(root_spec_)) || ((OB_NOT_NULL(root_op_) && OB_NOT_NULL(root_spec_)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(root_op_), KP(root_spec_), K(ret));
  } else if ((NULL != root_op) && OB_FAIL(create_op_input(*exec_ctx, *root_op))) {
    LOG_WARN("create op input failed", K(ret));
  } else if ((NULL != root_spec) && OB_FAIL(root_spec->create_op_input(*exec_ctx))) {
    LOG_WARN("create op input failed", K(ret));
  } else if (OB_FAIL(lookup_info_.assign(info))) {
    LOG_WARN("lookup init failed", K(ret));
  } else {
    ob_job_id_ = ob_job_id;
    weak_read_ = (common::ObConsistencyLevel::WEAK == consistency || common::ObConsistencyLevel::FROZEN == consistency);
  }
  return ret;
}

int ObLookupTaskBuilder::build_lookup_tasks(
    ObExecContext& ctx, ObMultiPartitionsRangesWarpper& partitions_ranges, uint64_t table_id, uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  // set local task to pos at 0
  ObMiniTask local_task;
  ObTaskInfo* empty_task_info = NULL;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  // partition should be add to transaction.
  ObSEArray<int64_t, OB_PARTITION_COUNT_PRE_SQL> partition_ids;
  if (0 != lookup_task_list_.count() || 0 != lookup_taskinfo_list_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the mem should be clean after last execution",
        K(ret),
        K(lookup_task_list_.count()),
        K(lookup_taskinfo_list_.count()));
  } else if (OB_ISNULL(exec_ctx_) || OB_ISNULL(my_session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the exec context can not be null", K(ret));
  } else {
    if (NULL != root_op_) {
      local_task.set_serialize_param(*exec_ctx_, *root_op_, *my_plan_);
    } else if (NULL != root_spec_) {
      local_task.set_serialize_param(exec_ctx_, root_spec_, my_plan_);
    }
    local_task.set_ctrl_server(exec_ctx_->get_addr());
    local_task.set_runner_server(exec_ctx_->get_addr());
    ObTaskID task_id;
    task_id.set_ob_job_id(ob_job_id_);
    task_id.set_task_id(new_task_id_++);
    local_task.set_ob_task_id(task_id);

    if (OB_FAIL(lookup_task_list_.push_back(local_task))) {
      LOG_WARN("push back local task failed", K(ret));
    } else if (OB_FAIL(lookup_taskinfo_list_.push_back(empty_task_info))) {
      LOG_WARN("push back local task info failed", K(ret));
    } else {
      ObAddr run_server;
      ObPartitionScanRanges* partition_ranges = nullptr;
      // find the partition which still not in this transaction.
      for (int64_t i = 0; i < partitions_ranges.count() && OB_SUCC(ret); ++i) {
        // add new partition
        if (OB_FAIL(partitions_ranges.get_partition_ranges_by_idx(i, partition_ranges))) {
          LOG_WARN("Failed to get partition range", K(ret));
        } else if (partition_ranges->partition_id_ == -1 || partition_ranges->ranges_.count() == 0) {
          // do nothing
        } else if (OB_FAIL(partition_ids.push_back(partition_ranges->partition_id_))) {
          LOG_WARN("fail to push back to partition_ids", K(ret));
        }
      }

      // add the partitions to transaction and location.
      if (OB_SUCC(ret) && !partition_ids.empty()) {
        if (OB_FAIL(ObSQLUtils::extend_checker_stmt(ctx, table_id, ref_table_id, partition_ids, weak_read_))) {
          LOG_WARN("fail to extend stmt", K(ret));
        }
        TransResult& trans_result = my_session->get_trans_result();
        ObPartitionKey pkey;
        ObPartitionArray pkeys;
        for (int64_t i = 0; i < partition_ids.count() && OB_SUCC(ret); ++i) {
          if (OB_FAIL(pkey.init(lookup_info_.ref_table_id_, partition_ids.at(i), lookup_info_.partition_cnt_))) {
            LOG_WARN("failed to init partition key", K(ret));
          } else if (OB_FAIL(pkeys.push_back(pkey))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(trans_result.merge_total_partitions(pkeys))) {
          LOG_WARN("fail to merge partitions", K(ret), K(pkeys));
        }
      }

      // find the partition's server.
      for (int64_t i = 0; i < partitions_ranges.count() && OB_SUCC(ret); ++i) {
        run_server.reset();
        if (OB_FAIL(partitions_ranges.get_partition_ranges_by_idx(i, partition_ranges))) {
          LOG_WARN("Failed to get partition range", K(ret));
        } else if (partition_ranges->partition_id_ == -1 || partition_ranges->ranges_.count() == 0) {
          LOG_DEBUG("Empty partition ranges, just skip", K(ret));
        } else if (OB_FAIL(get_partition_server(partition_ranges->partition_id_, run_server))) {
          LOG_WARN("failed to get partition server", K(ret));
        } else if (OB_FAIL(
                       add_range_to_taskinfo(partition_ranges->partition_id_, partition_ranges->ranges_, run_server))) {
          LOG_WARN("add ranges to task info failed", K(ret));
        } else {
          LOG_DEBUG("Success to add range",
              K(ret),
              K(partition_ranges->partition_id_),
              K(partition_ranges->ranges_),
              K(run_server));
        }
      }
    }
  }

  return ret;
}

int ObLookupTaskBuilder::get_partition_server(int64_t part_id, ObAddr& runner_server)
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* phy_location = NULL;
  const share::ObPartitionReplicaLocation* part_replica = NULL;
  if (OB_ISNULL(exec_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec_ctx is null", K(ret));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 *exec_ctx_, lookup_info_.table_id_, lookup_info_.ref_table_id_, phy_location))) {
    LOG_WARN("get physical table location failed", K(ret));
  } else if (OB_ISNULL(phy_location)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy table location is null", K(ret), K(lookup_info_.ref_table_id_));
  } else if (OB_ISNULL(part_replica = phy_location->get_part_replic_by_part_id(part_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy part replica is null", K(ret), K(part_id));
  } else {
    runner_server = part_replica->get_replica_location().server_;
  }
  return ret;
}

int ObLookupTaskBuilder::add_range_to_taskinfo(
    int64_t partition_id, ObIArray<ObNewRange>& ranges, const ObAddr& run_server)
{
  int ret = OB_SUCCESS;
  bool add_ranges = false;
  int64_t pos = OB_INVALID_INDEX_INT64;
  char* buf = NULL;
  for (int64_t i = 0; i < lookup_task_list_.count() && OB_SUCC(ret); ++i) {
    if (lookup_task_list_.at(i).get_runner_server() == run_server) {
      if (OB_ISNULL(lookup_taskinfo_list_.at(i))) {
        pos = i;
      } else {
        ObTaskInfo::ObPartLoc part_loc;
        if (OB_FAIL(
                part_loc.partition_key_.init(lookup_info_.ref_table_id_, partition_id, lookup_info_.partition_cnt_))) {
          LOG_WARN("init the partition key failed", K(ret));
        } else if (OB_FAIL(part_loc.scan_ranges_.assign(ranges))) {
          LOG_WARN("assign ranges to loc ranges failed", K(ret));
        } else if (OB_FAIL(lookup_taskinfo_list_.at(i)->get_range_location().part_locs_.push_back(part_loc))) {
          LOG_WARN("push back range location failed", K(ret));
        } else if (OB_FAIL(lookup_task_list_.at(i).add_partition_key(part_loc.partition_key_))) {
          LOG_WARN("add pkey failed", K(ret));
        }
        add_ranges = true;
        LOG_TRACE("Range added", K(i), K(partition_id), K(part_loc.partition_key_), K(ranges), K(run_server));
      }
      break;
    }
  }
  if (OB_SUCC(ret) && (pos != LOCAL_TASK_POS && pos != OB_INVALID_INDEX_INT64)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret), K(pos));
  }
  if (OB_SUCC(ret) && !add_ranges) {
    // add new task
    if (pos != LOCAL_TASK_POS) {
      // new remote task
      ObMiniTask new_task;
      new_task.set_runner_server(run_server);
      new_task.set_ctrl_server(exec_ctx_->get_addr());
      if (NULL != root_op_) {
        new_task.set_serialize_param(*exec_ctx_, *root_op_, *my_plan_);
      } else if (NULL != root_spec_) {
        new_task.set_serialize_param(exec_ctx_, root_spec_, my_plan_);
      }
      new_task.set_location_idx(partition_id);
      ObTaskID task_id;
      task_id.set_ob_job_id(ob_job_id_);
      task_id.set_task_id(new_task_id_++);
      //    task_id.set_task_id(lookup_task_list_.count());
      new_task.set_ob_task_id(task_id);
      common::ObPartitionKey pkey;
      if (OB_FAIL(pkey.init(lookup_info_.ref_table_id_, partition_id, lookup_info_.partition_cnt_))) {
        LOG_WARN("init partition key failed", K(ret));
      } else if (OB_FAIL(new_task.add_partition_key(pkey))) {
        LOG_WARN("push back pkey failed", K(ret));
      } else if (OB_FAIL(lookup_task_list_.push_back(new_task))) {
        LOG_WARN("push back new taks failed", K(ret));
      } else {
        LOG_TRACE(
            "Remote range added", K(lookup_task_list_.count()), K(partition_id), K(pkey), K(ranges), K(run_server));
      }
    } else {
      // local task
      // pos must be 0
      common::ObPartitionKey pkey;
      if (OB_FAIL(pkey.init(lookup_info_.ref_table_id_, partition_id, lookup_info_.partition_cnt_))) {
        LOG_WARN("init partition key failed", K(ret));
      } else if (OB_FAIL(lookup_task_list_.at(LOCAL_TASK_POS).add_partition_key(pkey))) {
        LOG_WARN("push back pkey failed", K(ret));
      } else {
        LOG_TRACE(
            "Local range added", K(lookup_task_list_.count()), K(partition_id), K(pkey), K(ranges), K(run_server));
      }
    }
    // add new task info
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObTaskInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate mem failed", K(ret));
    } else {
      ObTaskInfo* task_info = new (buf) ObTaskInfo(allocator_);
      ObTaskInfo::ObPartLoc part_loc;
      if (OB_FAIL(
              part_loc.partition_key_.init(lookup_info_.ref_table_id_, partition_id, lookup_info_.partition_cnt_))) {
        LOG_WARN("init partition key failed", K(ret));
      } else if (OB_FAIL(part_loc.scan_ranges_.assign(ranges))) {
        LOG_WARN("assign ranges to loc ranges failed", K(ret));
      } else if (OB_FAIL(task_info->get_range_location().part_locs_.init(lookup_info_.partition_num_))) {
        LOG_WARN("init fixed array failed", K(ret));
      } else if (OB_FAIL(task_info->get_range_location().part_locs_.push_back(part_loc))) {
        LOG_WARN("push back range location failed", K(ret));
      } else if (LOCAL_TASK_POS == pos) {
        lookup_taskinfo_list_.at(LOCAL_TASK_POS) = task_info;
        LOG_TRACE("Local task_info added", K(lookup_task_list_.count()), K(partition_id), K(part_loc.partition_key_));
      } else if (OB_FAIL(lookup_taskinfo_list_.push_back(task_info))) {
        LOG_WARN("push back task info failed", K(ret));
      } else {
        LOG_TRACE("Remote task_info added",
            K(lookup_task_list_.count()),
            K(partition_id),
            K(part_loc.partition_key_),
            K(ranges),
            K(run_server));
      }
    }
  }
  return ret;
}

void ObLookupTaskBuilder::reset()
{
  for (int64_t i = 0; i < lookup_taskinfo_list_.count(); ++i) {
    ObTaskInfo* task_info = lookup_taskinfo_list_.at(i);
    if (OB_ISNULL(task_info)) {
      // do nothing
    } else {
      task_info->~ObTaskInfo();
      allocator_.free(task_info);
      task_info = NULL;
    }
  }
  lookup_taskinfo_list_.reset();
  lookup_task_list_.reset();
  lookup_info_.reset();
  partitions_already_in_trans_.reset();
  // ob_job_id_.reset();
  // new_task_id_ = 0;
}

int ObLookupTaskBuilder::create_op_input(ObExecContext& ctx, const ObPhyOperator& root_op)
{
  int ret = OB_SUCCESS;
  const ObPhyOperator* child_op = NULL;
  ObIPhyOperatorInput* op_input = NULL;
  if (OB_FAIL(root_op.create_operator_input(ctx))) {
    LOG_WARN("create operator input failed", K(ret));
  } else if (OB_ISNULL(op_input = GET_PHY_OP_INPUT(ObIPhyOperatorInput, ctx, root_op.get_id()))) {
    LOG_WARN("the op input can not be null", K(ret), K(root_op.get_type()));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < root_op.get_child_num(); ++i) {
    if (OB_ISNULL(child_op = root_op.get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null", K(ret));
    } else if (OB_FAIL(create_op_input(ctx, *child_op))) {
      LOG_WARN("fail to build child op input", K(ret), K(child_op->get_id()));
    }
  }
  return ret;
}

int ObLookupTaskBuilder::rebuild_overflow_task(const ObMiniTaskRetryInfo& retry_info)
{
  int ret = OB_SUCCESS;
  const ObIArray<std::pair<int64_t, int64_t> >& task_list = retry_info.get_task_list();
  ObSEArray<ObMiniTask, 16> retry_lookup_task_list;
  ObSEArray<ObTaskInfo*, 16> retry_lookup_taskinfo_list;
  ObMiniTask empty_minitask;
  int64_t task_id = 0;
  int64_t succ_range_count = 0;
  if (OB_FAIL(retry_lookup_task_list.push_back(empty_minitask))) {
    LOG_WARN("Failed to push back empty minitask", K(ret));
  } else if (OB_FAIL(retry_lookup_taskinfo_list.push_back(nullptr))) {
    LOG_WARN("Failed to push back local task info", K(ret));
  }
  ARRAY_FOREACH(task_list, i)
  {
    task_id = task_list.at(i).first;
    succ_range_count = task_list.at(i).second;
    // task_id == task idx
    ObMiniTask& mini_task = lookup_task_list_.at(task_id);
    ObTaskInfo* task_info = lookup_taskinfo_list_.at(task_id);

    if (OB_ISNULL(task_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected nullptr", K(ret));
    } else if (retry_info.retry_by_single_range()) {
      if (OB_FAIL(rebuild_task_by_single_range(
              mini_task, *task_info, retry_lookup_task_list, retry_lookup_taskinfo_list))) {
        LOG_WARN("Failed to rebuild task by single range", K(ret));
      }
    } else if (OB_FAIL(rebuild_task_by_all_failed_ranges(
                   mini_task, *task_info, succ_range_count, retry_lookup_task_list, retry_lookup_taskinfo_list))) {
      LOG_WARN("Failed to rebuild task by all failed ranges", K(ret));
    } else {
    }
    if (OB_SUCC(ret) && 0 != succ_range_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected succ range count value, it should be zero!", K(ret), K(succ_range_count));
    }
  }
  for (int64_t i = 0; i < lookup_taskinfo_list_.count(); ++i) {
    ObTaskInfo* task_info = lookup_taskinfo_list_.at(i);
    if (OB_ISNULL(task_info)) {
      // do nothing
    } else {
      task_info->~ObTaskInfo();
      allocator_.free(task_info);
      task_info = NULL;
    }
  }
  lookup_taskinfo_list_.reset();
  lookup_task_list_.reset();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(lookup_task_list_.assign(retry_lookup_task_list))) {
      LOG_WARN("Failed to assign retry lookup task list", K(ret));
    } else if (OB_FAIL(lookup_taskinfo_list_.assign(retry_lookup_taskinfo_list))) {
      LOG_WARN("Failed to assign retry lookup taskinto list", K(ret));
    }
  }
  LOG_TRACE("Rebuild failed task",
      K(ret),
      K(retry_info.retry_by_single_range()),
      K(lookup_task_list_.count()),
      K(lookup_taskinfo_list_.count()));
  return ret;
}

int ObLookupTaskBuilder::get_new_part_ranges(const common::ObIArray<ObTaskInfo::ObPartLoc>& parts_loc,
    int64_t& succ_range_count, common::ObIArray<ObTaskInfo::ObPartLoc>& new_parts_loc, ObMiniTask& new_task)
{
  int ret = OB_SUCCESS;
  int64_t retry_range_count = 0;
  ObTaskInfo::ObPartLoc new_part_loc;
  ARRAY_FOREACH(parts_loc, j)
  {
    if (succ_range_count > parts_loc.at(j).scan_ranges_.count()) {
      succ_range_count -= parts_loc.at(j).scan_ranges_.count();
    } else {
      const ObTaskInfo::ObPartLoc& part_loc = parts_loc.at(j);
      new_part_loc = part_loc;
      new_part_loc.scan_ranges_.reuse();
      retry_range_count = 0;
      ARRAY_FOREACH(part_loc.scan_ranges_, m)
      {
        if (succ_range_count > 0) {
          --succ_range_count;
        } else if (OB_FAIL(new_part_loc.scan_ranges_.push_back(part_loc.scan_ranges_.at(m)))) {
          LOG_WARN("Failed to push back scan range", K(ret));
        } else {
          ++retry_range_count;
        }
      }
      if (OB_SUCC(ret) && !new_part_loc.scan_ranges_.empty()) {
        if (OB_FAIL(new_parts_loc.push_back(new_part_loc))) {
          LOG_WARN("Failed to push back retry part loc", K(ret));
        } else if (OB_FAIL(new_task.add_partition_key(part_loc.partition_key_))) {
          LOG_WARN("Failed to add partition key", K(ret));
        }
      }
      LOG_TRACE("Retry range",
          K(part_loc.scan_ranges_.count()),
          K(part_loc.partition_key_),
          K(succ_range_count),
          K(retry_range_count));
    }
  }
  return ret;
}

int ObLookupTaskBuilder::rebuild_task_by_all_failed_ranges(const ObMiniTask& failed_task,
    const ObTaskInfo& failed_task_info, int64_t& succ_range_count, common::ObIArray<ObMiniTask>& retry_lookup_task_list,
    common::ObIArray<ObTaskInfo*>& retry_lookup_taskinfo_list)
{
  int ret = OB_SUCCESS;
  ObMiniTask new_task;
  ObTaskInfo* task_info = nullptr;
  void* buf = nullptr;
  const common::ObIArray<ObTaskInfo::ObPartLoc>& parts = failed_task_info.get_range_location().part_locs_;
  common::ObSEArray<ObTaskInfo::ObPartLoc, 16> new_parts;

  ObTaskID task_id;
  task_id.set_ob_job_id(ob_job_id_);
  task_id.set_task_id(new_task_id_++);
  // task_id.set_task_id(retry_lookup_task_list.count());
  new_task.set_runner_server(failed_task.get_runner_server());
  new_task.set_ctrl_server(exec_ctx_->get_addr());
  if (NULL != root_op_) {
    new_task.set_serialize_param(*exec_ctx_, *root_op_, *my_plan_);
  } else if (NULL != root_spec_) {
    new_task.set_serialize_param(exec_ctx_, root_spec_, my_plan_);
  }
  new_task.set_ob_task_id(task_id);

  LOG_TRACE(
      "Rebuild lookup task", K(task_id), K(succ_range_count), K(failed_task.get_partition_keys()), K(parts.count()));

  if (OB_FAIL(get_new_part_ranges(parts, succ_range_count, new_parts, new_task))) {
    LOG_WARN("Failed to get new parts loc", K(ret));
  } else if (new_parts.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("No scan ranges need retry", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObTaskInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate mem", K(ret));
  } else {
    task_info = new (buf) ObTaskInfo(allocator_);
    if (OB_FAIL(task_info->get_range_location().part_locs_.init(lookup_info_.partition_num_))) {
      LOG_WARN("Init fixed array failed", K(ret));
    } else if (OB_FAIL(task_info->get_range_location().part_locs_.assign(new_parts))) {
      LOG_WARN("Failed to assign range location", K(ret));
    } else if (OB_FAIL(retry_lookup_task_list.push_back(new_task))) {
      LOG_WARN("Failed to push back minitask", K(ret));
    } else if (OB_FAIL(retry_lookup_taskinfo_list.push_back(task_info))) {
      LOG_WARN("Failed to push back taskinfo", K(ret));
    } else {
      LOG_TRACE("Remote task_info added",
          K(retry_lookup_task_list.count()),
          K(retry_lookup_taskinfo_list.count()),
          K(new_parts.count()));
    }
  }
  return ret;
}

int ObLookupTaskBuilder::rebuild_task_by_single_range(const ObMiniTask& failed_task, const ObTaskInfo& failed_task_info,
    common::ObIArray<ObMiniTask>& retry_lookup_task_list, common::ObIArray<ObTaskInfo*>& retry_lookup_taskinfo_list)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObTaskInfo::ObPartLoc>& parts = failed_task_info.get_range_location().part_locs_;
  ARRAY_FOREACH(parts, j)
  {
    const ObTaskInfo::ObPartLoc& part_loc = parts.at(j);
    ARRAY_FOREACH(part_loc.scan_ranges_, m)
    {
      void* buf = nullptr;

      ObMiniTask new_task;
      ObTaskID task_id;
      task_id.set_ob_job_id(ob_job_id_);
      task_id.set_task_id(new_task_id_++);
      //    task_id.set_task_id(retry_lookup_task_list.count());
      new_task.set_runner_server(failed_task.get_runner_server());
      new_task.set_ctrl_server(exec_ctx_->get_addr());
      if (NULL != root_op_) {
        new_task.set_serialize_param(*exec_ctx_, *root_op_, *my_plan_);
      } else if (NULL != root_spec_) {
        new_task.set_serialize_param(exec_ctx_, root_spec_, my_plan_);
      }
      new_task.set_ob_task_id(task_id);

      if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObTaskInfo))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate mem", K(ret));
      } else if (OB_FAIL(new_task.add_partition_key(part_loc.partition_key_))) {
        LOG_WARN("Failed to add partition key", K(ret));
      } else {
        ObTaskInfo* task_info = new (buf) ObTaskInfo(allocator_);
        ObTaskInfo::ObPartLoc new_part_loc = part_loc;
        new_part_loc.scan_ranges_.reset();
        if (OB_FAIL(task_info->get_range_location().part_locs_.init(lookup_info_.partition_num_))) {
          LOG_WARN("Init fixed array failed", K(ret));
        } else if (OB_FAIL(new_part_loc.scan_ranges_.push_back(part_loc.scan_ranges_.at(m)))) {
          LOG_WARN("Failed to push back scan range", K(ret));
        } else if (OB_FAIL(task_info->get_range_location().part_locs_.push_back(new_part_loc))) {
          LOG_WARN("Failed to push back range location", K(ret));
        } else if (OB_FAIL(retry_lookup_task_list.push_back(new_task))) {
          LOG_WARN("Failed to push back minitask", K(ret));
        } else if (OB_FAIL(retry_lookup_taskinfo_list.push_back(task_info))) {
          LOG_WARN("Failed to push back task", K(ret));
        } else {
        }
      }
    }
    LOG_TRACE("Remote task_info added",
        K(part_loc.partition_key_),
        K(retry_lookup_task_list.count()),
        K(retry_lookup_taskinfo_list.count()));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
