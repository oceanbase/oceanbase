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

#define USING_LOG_PREFIX SQL_EXE

#include "observer/ob_server.h"
#include "lib/hash/ob_iteratable_hashset.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/executor/ob_multiscan_task_spliter.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_mv_table_scan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/executor/ob_job_conf.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {
ObIntraPartitionTaskSpliter::ObIntraPartitionTaskSpliter()
    : table_loc_(NULL),
      part_rep_loc_list_(NULL),
      splitted_ranges_list_(NULL),
      next_task_id_(0),
      part_idx_(0),
      range_idx_(0),
      prepare_done_(false)
{}

ObIntraPartitionTaskSpliter::~ObIntraPartitionTaskSpliter()
{}

int ObIntraPartitionTaskSpliter::get_next_task(ObTaskInfo*& task)
{
  int ret = OB_SUCCESS;
  const ObPartitionReplicaLocation* part_rep_loc = NULL;
  const ObSplittedRanges* splitted_ranges = NULL;
  ObTaskInfo::ObPartLoc part_loc;
  ObTaskInfo::ObRangeLocation* range_loc = NULL;
  if (OB_UNLIKELY(!prepare_done_) && OB_FAIL(prepare())) {
    LOG_WARN("fail to prepare", K(ret));
  } else if (OB_FAIL(get_part_and_ranges(part_rep_loc, splitted_ranges))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to check part idx and range idx", K(ret));
    }
  } else if (OB_ISNULL(part_rep_loc) || OB_ISNULL(splitted_ranges)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part rep loc or splitted ranges is NULL", K(ret), K(part_rep_loc), K(splitted_ranges));
  } else if (OB_FAIL(part_rep_loc->get_partition_key(part_loc.partition_key_))) {
    LOG_WARN("fail to get partition key", K(ret));
  } else if (FALSE_IT(part_loc.renew_time_ = part_rep_loc->get_renew_time())) {
    // nothing.
  } else if (OB_FAIL(get_scan_ranges(*splitted_ranges, part_loc))) {
    LOG_WARN("fail to get scan ranges", K(ret));
  } else if (OB_FAIL(create_task_info(task))) {
    LOG_WARN("fail to create task info", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task info is NULL", K(ret));
  } else if (FALSE_IT(range_loc = &task->get_range_location())) {
    // nothing.
  } else if (OB_FAIL(range_loc->part_locs_.init(1))) {
    LOG_WARN("fail to init part locs", K(ret));
  } else if (OB_FAIL(range_loc->part_locs_.push_back(part_loc))) {
    LOG_WARN("fail to push back part loc", K(ret));
  } else if (FALSE_IT(range_loc->server_ = part_rep_loc->get_replica_location().server_)) {
    // nothing.
  } else if (OB_UNLIKELY(!range_loc->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("range loc is invalid", K(ret), K(range_loc));
  } else {
    ObTaskID ob_task_id;
    ObTaskLocation task_loc;
    ob_task_id.set_ob_job_id(job_->get_ob_job_id());
    ob_task_id.set_task_id(next_task_id_++);
    task_loc.set_ob_task_id(ob_task_id);
    task_loc.set_server(range_loc->server_);
    task->set_task_location(task_loc);
    task->set_location_idx(part_idx_);
  }
  return ret;
}

int ObIntraPartitionTaskSpliter::prepare()
{
  int ret = OB_SUCCESS;
  ObPhyOperator* root_op = NULL;
  ObSEArray<const ObTableScan*, 2> scan_ops;
  const ObTableScan* scan_op = NULL;
  if (OB_ISNULL(job_) || OB_ISNULL(root_op = job_->get_root_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job or root op is NULL", K(ret), K(job_), K(root_op));
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
    LOG_WARN("fail to find scan ops", K(ret), "root_op_id", root_op->get_id());
  } else if (1 != scan_ops.count()) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("table locations count should be 1 now", K(ret), "table_loc_count", scan_ops.count());
  } else if (OB_ISNULL(scan_op = scan_ops.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scan op is NULL", K(ret));
  } else if (OB_UNLIKELY(PHY_MV_TABLE_SCAN == scan_op->get_type())) {
    ret = OB_NOT_SUPPORTED;
    const ObMVTableScan* mv_scan_op = static_cast<const ObMVTableScan*>(scan_op);
    LOG_ERROR("intra partition task spliter does not support mv table scan",
        K(ret),
        K(*scan_op),
        K(mv_scan_op->get_right_table_param()),
        K(mv_scan_op->get_right_table_location_key()));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                 *exec_ctx_, scan_op->get_table_location_key(), scan_op->get_location_table_id(), table_loc_))) {
    LOG_WARN("fail to get phy table location", K(ret));
  } else if (OB_ISNULL(table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is NULL", K(ret));
  } else {
    part_rep_loc_list_ = &table_loc_->get_partition_location_list();
    splitted_ranges_list_ = &table_loc_->get_splitted_ranges_list();
    prepare_done_ = true;
  }
  return ret;
}

int ObIntraPartitionTaskSpliter::get_part_and_ranges(
    const ObPartitionReplicaLocation*& part_rep_loc, const ObSplittedRanges*& splitted_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_rep_loc_list_) || OB_ISNULL(splitted_ranges_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "part rep loc list or splitted ranges list is NULL", K(ret), K(part_rep_loc_list_), K(splitted_ranges_list_));
  } else if (OB_UNLIKELY(part_rep_loc_list_->count() != splitted_ranges_list_->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part locs count and splitted ranges count is not equal",
        K(ret),
        K(part_rep_loc_list_->count()),
        K(splitted_ranges_list_->count()));
  } else if (OB_UNLIKELY(part_idx_ < 0 || range_idx_ < 0)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("part idx or range idx is out of range", K(ret), K(part_idx_), K(range_idx_));
  } else if (part_idx_ >= splitted_ranges_list_->count()) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(splitted_ranges = &splitted_ranges_list_->at(part_idx_))) {
    // nothing.
  } else if (range_idx_ >= splitted_ranges->get_offsets().count()) {
    range_idx_ = 0;
    part_idx_++;
  }
  if (OB_SUCC(ret)) {
    if (part_idx_ >= splitted_ranges_list_->count()) {
      ret = OB_ITER_END;
    } else {
      part_rep_loc = &part_rep_loc_list_->at(part_idx_);
      splitted_ranges = &splitted_ranges_list_->at(part_idx_);
    }
  }
  return ret;
}

int ObIntraPartitionTaskSpliter::get_scan_ranges(
    const ObSplittedRanges& splitted_ranges, ObTaskInfo::ObPartLoc& part_loc)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObNewRange>& ranges = splitted_ranges.get_ranges();
  const ObIArray<int64_t>& offsets = splitted_ranges.get_offsets();
  if (OB_UNLIKELY(!(0 <= range_idx_ && range_idx_ < offsets.count()))) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("range idx is out of range", K(ret), K(range_idx_), K(offsets.count()));
  } else {
    int64_t start_idx = (0 == range_idx_) ? 0 : offsets.at(range_idx_ - 1) + 1;
    int64_t finish_idx = offsets.at(range_idx_);
    for (int64_t idx = start_idx; OB_SUCC(ret) && idx <= finish_idx; idx++) {
      if (OB_UNLIKELY(!(0 <= idx && idx < ranges.count()))) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        LOG_WARN("range idx is out of range", K(ret), K(ranges), K(offsets), K(range_idx_), K(idx));
      } else if (OB_FAIL(part_loc.scan_ranges_.push_back(ranges.at(idx)))) {
        LOG_WARN("fail to push back ranges", K(ret));
      }
    }
    range_idx_++;
  }
  return ret;
}

ObDistributedTaskSpliter::ObPartComparer::ObPartComparer(
    ObIArray<ObShuffleKeys>& shuffle_keys, bool cmp_part, bool cmp_subpart, int sort_order)
    : shuffle_keys_(shuffle_keys),
      cmp_part_(cmp_part),
      cmp_subpart_(cmp_subpart),
      sort_order_(sort_order),
      ret_(OB_SUCCESS)
{}

ObDistributedTaskSpliter::ObPartComparer::~ObPartComparer()
{}

bool ObDistributedTaskSpliter::ObPartComparer::operator()(int64_t idx1, int64_t idx2)
{
  int& ret = ret_;
  int cmp = 0;
  bool cmp_ret = false;
  if (OB_FAIL(ret)) {
    // nothing.
  } else if (!(0 <= idx1 && idx1 < shuffle_keys_.count() && 0 <= idx2 && idx2 < shuffle_keys_.count())) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("idx1 or idx2 is out of range", K(ret), K(idx1), K(idx2), K(shuffle_keys_.count()));
  } else if (OB_FAIL(shuffle_keys_.at(idx1).compare(shuffle_keys_.at(idx2), cmp_part_, cmp_subpart_, cmp))) {
    LOG_WARN("fail to compare shuffle keys", K(ret));
  } else if ((cmp * sort_order_) < 0) {
    cmp_ret = true;  // asc.
  }
  return cmp_ret;
}

ObDistributedTaskSpliter::ObSliceComparer::ObSliceComparer(bool cmp_part, bool cmp_subpart, int sort_order)
    : cmp_part_(cmp_part), cmp_subpart_(cmp_subpart), sort_order_(sort_order), ret_(OB_SUCCESS)
{}

ObDistributedTaskSpliter::ObSliceComparer::~ObSliceComparer()
{}

bool ObDistributedTaskSpliter::ObSliceComparer::operator()(const ObSliceEvent* slice1, const ObSliceEvent* slice2)
{
  int& ret = ret_;
  int cmp = 0;
  bool cmp_ret = false;
  if (OB_FAIL(ret)) {
    // nothing.
  } else if (OB_ISNULL(slice1) || OB_ISNULL(slice2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slice1 or slice2 is NULL", K(ret), K(slice1), K(slice2));
  } else if (OB_FAIL(slice1->get_shuffle_keys().compare(slice2->get_shuffle_keys(), cmp_part_, cmp_subpart_, cmp))) {
    LOG_WARN("fail to compare shuffle keys", K(ret));
  } else if ((cmp * sort_order_) < 0) {
    cmp_ret = true;  // asc.
  }
  return cmp_ret;
}

ObDistributedTaskSpliter::ObDistributedTaskSpliter()
    : table_locations_(ObModIds::OB_SQL_EXECUTOR_TASK_SPLITER, OB_MALLOC_NORMAL_BLOCK_SIZE),
      part_shuffle_keys_(ObModIds::OB_SQL_EXECUTOR_TASK_SPLITER, OB_MALLOC_NORMAL_BLOCK_SIZE),
      part_idxs_(ObModIds::OB_SQL_EXECUTOR_TASK_SPLITER, OB_MALLOC_NORMAL_BLOCK_SIZE),
      child_slices_(ObModIds::OB_SQL_EXECUTOR_TASK_SPLITER, OB_MALLOC_NORMAL_BLOCK_SIZE),
      match_type_(MT_ONLY_MATCH),
      next_task_id_(0),
      head_part_idx_(0),
      head_slice_idx_(0),
      head_slice_count_(0),
      sort_order_(0),
      head_slice_matched_(false),
      repart_part_(false),
      repart_subpart_(false),
      prepare_done_(false)
{}

ObDistributedTaskSpliter::~ObDistributedTaskSpliter()
{}

int ObDistributedTaskSpliter::get_next_task(ObTaskInfo*& task_info)
{
  int ret = OB_SUCCESS;
  task_info = NULL;
  if (OB_UNLIKELY(!prepare_done_)) {
    if (OB_FAIL(prepare())) {
      LOG_WARN("fail to prepare", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (next_task_id_ >= task_store_.count()) {
      ret = OB_ITER_END;
      LOG_DEBUG("get next task end", K(ret), K_(next_task_id), K(task_store_.count()));
    } else {
      task_info = task_store_.at(next_task_id_);
      ObTaskID ob_task_id;
      ObTaskLocation task_loc;
      ob_task_id.set_ob_job_id(job_->get_ob_job_id());
      ob_task_id.set_task_id(next_task_id_);
      task_info->get_task_location().set_ob_task_id(ob_task_id);
      if (!task_info->get_location_idx_list().empty()) {
        task_info->set_location_idx(task_info->get_location_idx_list().at(0));
      }
      ++next_task_id_;
      LOG_DEBUG("get next task info", KPC(task_info));
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::prepare()
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (prepare_done_) {
    // nothing.
  } else if (OB_ISNULL(job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is NULL", K(ret));
  } else if (OB_FAIL(job_->child_need_repart(repart_part_, repart_subpart_))) {
    LOG_WARN("fail to check child need repart", K(ret));
  } else if (OB_FAIL(init_match_type())) {
    LOG_WARN("fail to init match type", K(ret));
  } else if (OB_FAIL(init_table_locations(job_->get_root_op()))) {
    LOG_WARN("fail to init table locations", K(ret));
  } else if (OB_FAIL(check_table_locations())) {
    LOG_WARN("fail to check table locations", K(ret));
  } else if (OB_FAIL(init_part_shuffle_keys())) {
    LOG_WARN("fail to init part shuffle keys", K(ret));
  } else if (OB_FAIL(sort_part_shuffle_keys())) {
    LOG_WARN("fail to sort part shuffle keys", K(ret));
  } else if (OB_FAIL(init_child_task_results())) {
    LOG_WARN("fail to init child task results", K(ret));
  } else if (OB_FAIL(sort_child_slice_shuffle_keys())) {
    LOG_WARN("fail to sort child slice shuffle keys", K(ret));
  } else if (OB_FAIL(calc_head_slice_count())) {
    LOG_WARN("fail to calc head slice count", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(compare_head_part_slice(cmp))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to compare head part and slice", K(ret));
      } else if (task_store_.empty()) {
        ObTaskInfo* task_info = nullptr;
        if (OB_FAIL(task_add_empty_part(task_info))) {
          LOG_WARN("fail to add empty part", K(ret), K_(server));
        } else if (OB_FAIL(task_add_empty_slice(*task_info))) {
          LOG_WARN("fail to add empty slice", K(ret), K_(server));
        }
      }
    } else if (cmp < 0) {
      if (need_all_part()) {
        ObTaskInfo* task_info = nullptr;
        if (OB_FAIL(task_add_head_part(task_info))) {
          LOG_WARN("fail to add head part", K(ret));
        } else if (OB_FAIL(task_add_empty_slice(*task_info))) {
          LOG_WARN("fail to add empty slice", K(ret));
        }
      }
      head_part_idx_++;
    } else if (cmp > 0) {
      if (need_all_slice() && !head_slice_matched_) {
        ObTaskInfo* task_info = NULL;
        if (OB_FAIL(task_add_empty_part(task_info))) {
          LOG_WARN("fail to add empty part", K(ret), K_(server));
        } else if (OB_FAIL(task_add_head_slices(*task_info))) {
          LOG_WARN("fail to add head slices", K(ret), K_(server));
        }
      }
      if (OB_SUCC(ret)) {
        head_slice_idx_ += head_slice_count_;
        if (OB_FAIL(calc_head_slice_count())) {
          LOG_WARN("fail to calc head slice count", K(ret));
        }
      }
    } else {
      ObTaskInfo* task_info = nullptr;
      if (OB_FAIL(task_add_head_part(task_info))) {
        LOG_WARN("fail to add head part", K(ret));
      } else if (OB_FAIL(task_add_head_slices(*task_info))) {
        LOG_WARN("fail to add head slices", K(ret));
      }
      head_part_idx_++;
      // do not inc head_slice_idx_, because next part may be equal to head slice.
    }
    head_slice_matched_ = (0 == cmp);
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    prepare_done_ = true;
  }
  return ret;
}

int ObDistributedTaskSpliter::init_match_type()
{
  int ret = OB_SUCCESS;
  match_type_ = MT_ONLY_MATCH;
  if (OB_ISNULL(job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is NULL", K(ret));
  } else {
    if (job_->has_scan_op()) {
      if (job_->has_outer_join_child_scan() || !job_->has_child_job()) {
        match_type_ = static_cast<ObMatchType>(match_type_ | MT_ALL_PART);
      }
    }
    if (job_->has_child_job()) {
      if (job_->has_outer_join_child_job() || !job_->has_scan_op()) {
        match_type_ = static_cast<ObMatchType>(match_type_ | MT_ALL_SLICE);
      }
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::init_table_locations(ObPhyOperator* root_op)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableScan*, 2> scan_ops;
  ObPhyTableLoc phy_tbl_loc;
  if (OB_ISNULL(root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is NULL", K(ret));
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
    LOG_WARN("fail to find scan ops", K(ret), "root_op_id", root_op->get_id());
  } else {
    // We do not support table scan does not exist but dml operators exist,
    // since we locate task by table scan.
    if (scan_ops.empty()) {
      bool exist = false;
      if (OB_FAIL(check_dml_exist(exist, *root_op))) {
        LOG_WARN("check dml exist failed", K(ret));
      } else if (exist) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("distributed scheduler do not support task with dml operator but table scan not exist", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_ops.count(); ++i) {
    phy_tbl_loc.reset();
    const ObPhyTableLocation* table_loc = NULL;
    const ObTableScan* scan_op = scan_ops.at(i);
    if (OB_ISNULL(scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan op is NULL", K(ret));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                   *exec_ctx_, scan_op->get_table_location_key(), scan_op->get_location_table_id(), table_loc))) {
      LOG_WARN("fail to get phy table location", K(ret));
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table location is NULL", K(ret));
    } else if (OB_FAIL(phy_tbl_loc.set_table_loc(table_loc))) {
      LOG_WARN("fail to set table loc", K(ret), K(*table_loc));
    } else {
      if (PHY_MV_TABLE_SCAN == scan_op->get_type()) {
        const ObMVTableScan* mv_scan_op = static_cast<const ObMVTableScan*>(scan_op);
        table_loc = NULL;
        if (OB_ISNULL(mv_scan_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("mv scan op is NULL", K(ret), K(scan_op), K(mv_scan_op));
        } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(*exec_ctx_,
                       mv_scan_op->get_right_table_location_key(),
                       mv_scan_op->get_right_table_location_key(),
                       table_loc))) {
          LOG_WARN("fail to get mv right table location", K(ret), K(mv_scan_op->get_right_table_location_key()));
        } else if (OB_ISNULL(table_loc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("table location is NULL", K(ret));
        } else {
          ObPartitionKey part_key;
          table_loc->get_partition_location_list().at(0).get_partition_key(part_key);
          if (OB_FAIL(phy_tbl_loc.add_depend_table_key(part_key))) {
            LOG_WARN("fail to add depend table id", K(ret), K(table_loc->get_ref_table_id()));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_locations_.push_back(phy_tbl_loc))) {
        LOG_WARN("fail to add table loc", K(ret), K(phy_tbl_loc));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<const ObTableModify*, 1> insert_ops;
    phy_tbl_loc.reset();
    const ObPhyTableLocation* table_loc = NULL;
    if (OB_FAIL(find_insert_ops(insert_ops, *root_op))) {
      LOG_WARN("failed to find insert ops", K(ret));
    } else if (insert_ops.count() != 1) {
      // do nothing
    } else if (OB_ISNULL(insert_ops.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert operator is null", K(ret));
    } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                   *exec_ctx_, insert_ops.at(0)->get_table_id(), insert_ops.at(0)->get_index_tid(), table_loc))) {
      LOG_WARN("failed to get phy table locations", K(ret));
    } else if (OB_FAIL(phy_tbl_loc.set_table_loc(table_loc))) {
      LOG_WARN("failed to set table location", K(ret), K(table_loc));
    } else if (OB_FAIL(table_locations_.push_back(phy_tbl_loc))) {
      LOG_WARN("failed to add table location", K(ret));
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::check_table_locations()
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* base_table_loc = NULL;
  const ObPartitionReplicaLocationIArray* base_part_locs = NULL;
  if (table_locations_.count() < 1) {
    // nothing.
  } else if (OB_ISNULL(base_table_loc = table_locations_.at(0).get_table_loc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base table location is NULL", K(ret));
  } else if (FALSE_IT(base_part_locs = &base_table_loc->get_partition_location_list())) {
    // nothing.
  } else if (base_part_locs->count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base part locs count < 1", K(ret));
  } else {
    const ObPartitionReplicaLocationIArray* part_locs = NULL;
    ObPartitionKey base_part_key;
    ObPartitionKey part_key;
    for (int64_t i = 1; OB_SUCC(ret) && i < table_locations_.count(); i++) {
      const ObPhyTableLocation* table_loc = table_locations_.at(i).get_table_loc();
      if (OB_ISNULL(table_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table location is NULL", K(ret), K(i));
      } else if (FALSE_IT(part_locs = &table_loc->get_partition_location_list())) {
        // nothing.
      } else if (OB_UNLIKELY(base_part_locs->count() != part_locs->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part locs count is not equal",
            K(ret),
            K(i),
            K(base_part_locs->count()),
            K(part_locs->count()),
            K(table_locations_.count()),
            K(*base_part_locs),
            K(*part_locs));
      }
      // Open specific partition wise plan.
      // A part remote-located plan, which means a plan with some table(single partition) locate at the same remote
      // server, can be executed just like a partition wise plan. Please see obtest t/join/explain_bug.test for more
      // information.
      for (int64_t j = 0; OB_SUCC(ret) && j < base_part_locs->count(); ++j) {
        base_part_key.reset();
        part_key.reset();
        const ObPartitionReplicaLocation& base_part_loc = base_part_locs->at(j);
        const ObPartitionReplicaLocation& part_loc = part_locs->at(j);
        const ObReplicaLocation& base_rep_loc = base_part_loc.get_replica_location();
        const ObReplicaLocation& rep_loc = part_loc.get_replica_location();
        if (OB_FAIL(base_part_loc.get_partition_key(base_part_key))) {
          LOG_WARN("fail to get base part key", K(ret), K(base_part_loc));
        } else if (OB_FAIL(part_loc.get_partition_key(part_key))) {
          LOG_WARN("fail to get part key", K(ret), K(part_loc));
        } else if (!base_part_key.is_valid() || !part_key.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part key is invalid", K(ret), K(i), K(j), K(base_part_key), K(part_key));
        } else if (!base_rep_loc.is_valid() || !rep_loc.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rep loc is invalid", K(ret), K(base_rep_loc), K(rep_loc));
        } else if (base_rep_loc.server_ != rep_loc.server_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rep loc server is not equal", K(ret), K(rep_loc.server_), K(base_rep_loc.server_));
        }
      }
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::init_part_shuffle_keys()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* table_schema = NULL;
  const ObPhyTableLocation* table_loc = NULL;
  const ObPartitionReplicaLocationIArray* part_locs = NULL;
  part_shuffle_keys_.reset();
  part_idxs_.reset();
  if (table_locations_.count() < 1) {
    // nothing.
  } else if (OB_ISNULL(table_loc = table_locations_.at(0).get_table_loc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is NULL", K(ret));
  } else if (FALSE_IT(part_locs = &table_loc->get_partition_location_list())) {
    // nothing.
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 exec_ctx_->get_my_session()->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("faile to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_loc->get_ref_table_id(), table_schema))) {
    LOG_WARN("faile to get table schema", K(ret), K(table_loc->get_ref_table_id()));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is null", K(ret), K(table_loc->get_ref_table_id()));
  } else {
    ObPartitionKey part_key;
    ObShuffleKeys shuffle_keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_locs->count(); i++) {
      const ObPartitionReplicaLocation& part_loc = part_locs->at(i);
      if (OB_FAIL(part_loc.get_partition_key(part_key))) {
        LOG_WARN("fail to get partition key", K(ret), K(part_loc));
      } else if (OB_FAIL(get_shuffle_keys(*table_schema, part_key, shuffle_keys))) {
        LOG_WARN("fail to get shuffle keys", K(ret));
      } else if (OB_FAIL(part_shuffle_keys_.push_back(shuffle_keys))) {
        LOG_WARN("fail to push back shuffle keys", K(ret), K(shuffle_keys));
      } else if (OB_FAIL(part_idxs_.push_back(i))) {
        LOG_WARN("fail to push back part idx", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && part_shuffle_keys_.count() > 1 && (repart_part_ || repart_subpart_)) {
      int cmp = 0;
      if (OB_FAIL(part_shuffle_keys_.at(0).compare(part_shuffle_keys_.at(1), repart_part_, repart_subpart_, cmp))) {
        LOG_WARN("fail to compare shuffle keys", K(ret));
      } else {
        sort_order_ = (cmp < 0) ? 1 : -1;
      }
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::sort_part_shuffle_keys()
{
  int ret = OB_SUCCESS;
  if (part_idxs_.count() > 1 && (repart_part_ || repart_subpart_)) {
    ObPartComparer part_comparer(part_shuffle_keys_, repart_part_, repart_subpart_, sort_order_);
    int64_t* first_part_idx = &part_idxs_.at(0);
    std::sort(first_part_idx, first_part_idx + part_idxs_.count(), part_comparer);
    if (OB_FAIL(part_comparer.get_ret())) {
      LOG_WARN("fail to compare part with shuffle keys", K(ret));
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::get_shuffle_keys(
    const ObTableSchema& table_schema, const ObPartitionKey& part_key, ObShuffleKeys& shuffle_keys)
{
  int ret = OB_SUCCESS;
  shuffle_keys.reset();
  if (OB_SUCC(ret) && repart_part_) {
    if (OB_FAIL(table_schema.get_part_shuffle_key(
            part_key.get_part_idx(), shuffle_keys.part_key_.get_value0(), shuffle_keys.part_key_.get_value1()))) {
      LOG_WARN("fail to get part shuffle key", K(ret));
    } else if (OB_FAIL(shuffle_keys.part_key_.set_shuffle_type(table_schema))) {
      LOG_WARN("set part shuffle type failed");
    }
  }
  if (OB_SUCC(ret) && repart_subpart_) {
    if (OB_FAIL(table_schema.get_subpart_shuffle_key(part_key.get_part_idx(),
            part_key.get_subpart_idx(),
            shuffle_keys.subpart_key_.get_value0(),
            shuffle_keys.subpart_key_.get_value1()))) {
      LOG_WARN("fail to get subpart shuffle key", K(ret));
    } else if (OB_FAIL(shuffle_keys.subpart_key_.set_sub_shuffle_type(table_schema))) {
      LOG_WARN("set subpart shuffle type failed");
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::init_child_task_results()
{
  int ret = OB_SUCCESS;
  ObJob* child_job = NULL;
  bool skip_empty = false;
  child_slices_.reset();
  for (int64_t i = 0; i < job_->get_child_count(); i++) {
    if (OB_FAIL(job_->get_child_job(i, child_job))) {
      LOG_WARN("fail to get child job", K(ret), K(i));
    } else if (OB_ISNULL(child_job)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child job is NULL", K(ret));
    } else if (OB_FAIL(child_job->need_skip_empty_result(skip_empty))) {
      LOG_WARN("fail to need skip empty result", K(ret));
    } else if (OB_FAIL(child_job->append_finished_slice_events(child_slices_, skip_empty))) {
      LOG_WARN("fail to append child finished slice events", K(ret));
    } else if (child_job->is_outer_join_child_job()) {
      match_type_ = static_cast<ObMatchType>(match_type_ | MT_ALL_SLICE);
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::sort_child_slice_shuffle_keys()
{
  int ret = OB_SUCCESS;
  if (child_slices_.count() > 1) {
    ObSliceComparer slice_comparer(repart_part_, repart_subpart_, sort_order_);
    const ObSliceEvent** first_slice = &child_slices_.at(0);
    std::sort(first_slice, first_slice + child_slices_.count(), slice_comparer);
    if (OB_FAIL(slice_comparer.get_ret())) {
      LOG_WARN("fail to compare slice with shuffle keys", K(ret));
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::compare_head_part_slice(int& cmp)
{
  int ret = OB_SUCCESS;
  const ObShuffleKeys* part_shuffle_keys = NULL;
  const ObShuffleKeys* slice_shuffle_keys = NULL;
  if (OB_SUCC(ret) && 0 <= head_part_idx_ && head_part_idx_ < part_idxs_.count()) {
    int64_t real_part_idx = part_idxs_.at(head_part_idx_);
    if (OB_UNLIKELY(!(0 <= real_part_idx && real_part_idx < part_shuffle_keys_.count()))) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("part idx out of range", K(ret), K(real_part_idx), K(part_shuffle_keys_.count()));
    } else {
      part_shuffle_keys = &part_shuffle_keys_.at(real_part_idx);
    }
  }
  if (OB_SUCC(ret) && 0 <= head_slice_idx_ && head_slice_idx_ < child_slices_.count()) {
    slice_shuffle_keys = &child_slices_.at(head_slice_idx_)->get_shuffle_keys();
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(part_shuffle_keys) && OB_ISNULL(slice_shuffle_keys)) {
      ret = OB_ITER_END;
    } else if (OB_ISNULL(part_shuffle_keys)) {
      cmp = 1;
    } else if (OB_ISNULL(slice_shuffle_keys)) {
      cmp = -1;
    } else if (OB_FAIL(part_shuffle_keys->compare(*slice_shuffle_keys, repart_part_, repart_subpart_, cmp))) {
      LOG_WARN("fail to compare shuffle keys", K(ret));
    } else {
      cmp *= sort_order_;
    }
  }
  return ret;
}

int64_t ObDistributedTaskSpliter::get_total_part_cnt() const
{
  int64_t part_cnt = 0;
  if (table_locations_.count() > 0) {
    const ObPhyTableLocation* phy_table_loc = table_locations_.at(0).get_table_loc();
    if (phy_table_loc != nullptr) {
      part_cnt = table_locations_.count() * phy_table_loc->get_partition_location_list().count();
    }
  }
  return part_cnt;
}

int ObDistributedTaskSpliter::need_split_task_by_partition(bool& by_partition) const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2230) {
    bool has_lgi = false;
    ObPhyOperator* root_op = nullptr;
    if (OB_ISNULL(job_) || OB_ISNULL(root_op = job_->get_root_op())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job is null", K(ret), K(job_), K(root_op));
    } else if (OB_UNLIKELY(!IS_TRANSMIT(root_op->get_type()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root operator is null", K(ret));
    } else {
      by_partition = !static_cast<ObTransmit*>(root_op)->has_lgi();
    }
  } else {
    by_partition = true;
  }
  return ret;
}

int ObDistributedTaskSpliter::get_or_create_task_info(const ObAddr& task_server, ObTaskInfo*& task_info)
{
  int ret = OB_SUCCESS;
  task_info = nullptr;
  bool by_partition = false;
  if (OB_FAIL(need_split_task_by_partition(by_partition))) {
    LOG_WARN("check whther generate task by partition failed", K(ret));
  } else if (!by_partition) {
    for (int64_t i = 0; OB_SUCC(ret) && nullptr == task_info && i < task_store_.count(); ++i) {
      ObTaskInfo* tmp_task = task_store_.at(i);
      if (OB_ISNULL(tmp_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task info is null", K(ret));
      } else if (task_server == tmp_task->get_task_location().get_server()) {
        task_info = tmp_task;
      }
    }
  }

  if (OB_SUCC(ret) && nullptr == task_info) {
    int64_t total_part_cnt = get_total_part_cnt();
    if (OB_FAIL(create_task_info(task_info))) {
      LOG_WARN("create task info failed", K(ret));
    } else {
      task_info->get_range_location().server_ = task_server;
      task_info->get_task_location().set_server(task_server);
      if (total_part_cnt > 0) {
        if (OB_FAIL(task_info->get_range_location().part_locs_.init(total_part_cnt))) {
          LOG_WARN("init task info part locations failed", K(ret), K(total_part_cnt));
        } else if (OB_FAIL(task_info->init_location_idx_array(total_part_cnt))) {
          LOG_WARN("init task info location index array failed", K(ret), K(total_part_cnt));
        } else if (OB_FAIL(task_info->init_sclie_count_array(total_part_cnt))) {
          LOG_WARN("init sclie count array failed", K(ret), K(total_part_cnt));
        }
      }
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::get_task_runner_server(ObAddr& runner_server) const
{
  int ret = OB_SUCCESS;
  const ObPhyTableLocation* table_loc = NULL;
  if (OB_UNLIKELY(!(0 <= head_part_idx_ && head_part_idx_ < part_idxs_.count()))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("head part idx is out of range", K(head_part_idx_), K(part_idxs_.count()));
  } else if (OB_UNLIKELY(table_locations_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is empty", K(ret));
  } else if (OB_ISNULL(table_loc = table_locations_.at(0).get_table_loc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is null", K(ret));
  } else {
    int64_t real_part_idx = part_idxs_.at(head_part_idx_);
    const ObPartitionReplicaLocationIArray& rep_locs = table_loc->get_partition_location_list();
    if (OB_UNLIKELY(real_part_idx < 0) || OB_UNLIKELY(real_part_idx >= rep_locs.count())) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("real idx is out of range", K(real_part_idx), K(rep_locs.count()));
    } else {
      const ObPartitionReplicaLocation& part_rep_loc = rep_locs.at(real_part_idx);
      runner_server = part_rep_loc.get_replica_location().server_;
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::task_add_head_part(ObTaskInfo*& task_info)
{
  int ret = OB_SUCCESS;
  ObAddr runner_server;
  int64_t real_part_idx = 0;
  ObTaskInfo::ObPartLoc part_loc;
  const ObPartitionReplicaLocationIArray* part_rep_locs = NULL;
  const ObPartitionReplicaLocation* part_rep_loc = NULL;
  ObTaskInfo::ObRangeLocation* range_loc = NULL;
  if (OB_UNLIKELY(table_locations_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table location is empty", K(ret));
  } else if (OB_UNLIKELY(!(0 <= head_part_idx_ && head_part_idx_ < part_idxs_.count()))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("head part idx is out of range", K(head_part_idx_), K(part_idxs_.count()));
  } else if (OB_UNLIKELY((real_part_idx = part_idxs_.at(head_part_idx_)) < 0)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("real part idx is out of range", K(real_part_idx));
  } else if (OB_FAIL(get_task_runner_server(runner_server))) {
    LOG_WARN("get task runner server failed", K(ret));
  } else if (OB_FAIL(get_or_create_task_info(runner_server, task_info))) {
    LOG_WARN("get or create task info failed", K(ret), K(runner_server));
  } else if (OB_FAIL(task_info->add_location_idx(real_part_idx))) {
    LOG_WARN("add location index to task info failed", K(ret), K(real_part_idx));
  } else {
    range_loc = &(task_info->get_range_location());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_locations_.count(); i++) {
    part_loc.reset();
    const ObDistributedTaskSpliter::ObPhyTableLoc& phy_tbl_loc = table_locations_.at(i);
    const ObPhyTableLocation* table_loc = phy_tbl_loc.get_table_loc();
    const ObIArray<ObPartitionKey>& depend_table_keys = phy_tbl_loc.get_depend_table_keys();
    for (int64_t j = 0; OB_SUCC(ret) && j < depend_table_keys.count(); ++j) {
      if (OB_FAIL(part_loc.depend_table_keys_.push_back(depend_table_keys.at(j)))) {
        LOG_WARN("fail to push back depend table ids", K(ret), K(j), K(depend_table_keys.at(j)));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table location is NULL", K(ret), K(i));
    } else if (FALSE_IT(part_rep_locs = &table_loc->get_partition_location_list())) {
      // nothing.
    } else if (OB_UNLIKELY(real_part_idx >= part_rep_locs->count())) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("real idx is out of range", K(real_part_idx), K(part_rep_locs->count()));
    } else if (FALSE_IT(part_rep_loc = &part_rep_locs->at(real_part_idx))) {
      // nothing.
    } else if (OB_FAIL(part_rep_loc->get_partition_key(part_loc.partition_key_))) {
      LOG_WARN("fail to get partition key", K(ret));
    } else if (FALSE_IT(part_loc.renew_time_ = part_rep_loc->get_renew_time())) {
      // nothing.
    } else if (OB_FAIL(range_loc->part_locs_.push_back(part_loc))) {
      LOG_WARN("fail to push back part loc", K(ret));
    } else {
      LOG_DEBUG("add partition location to task info", K(ret), K(part_loc), KPC(task_info));
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::task_add_head_slices(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  int64_t head_slice_end = head_slice_idx_ + head_slice_count_;
  const ObSliceEvent* slice = NULL;
  ObTaskResultBuf task_result;
  if (OB_UNLIKELY(!(0 <= head_slice_idx_ && head_slice_idx_ < child_slices_.count()))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("head slice idx is out of range", K(ret), K(head_slice_idx_), K(child_slices_.count()));
  } else if (OB_UNLIKELY(head_slice_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("head slice count is <= 0", K(ret), K(head_slice_count_));
  } else {
    ObIArray<ObTaskResultBuf>& child_task_results = task_info.get_child_task_results();
    for (int64_t i = head_slice_idx_; OB_SUCC(ret) && i < head_slice_end; i++) {
      task_result.reset();
      if (OB_ISNULL(slice = child_slices_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("slice event is NULL", K(ret));
      } else if (OB_FAIL(get_task_location(slice->get_ob_slice_id(), task_result.get_task_location()))) {
        LOG_WARN("fail to get task location", K(ret));
      } else if (OB_FAIL(task_result.add_slice_event(*slice))) {
        LOG_WARN("fail to add slice event", K(ret));
      } else if (OB_FAIL(child_task_results.push_back(task_result))) {
        LOG_WARN("fail to push back task result", K(ret));
      } else {
        LOG_DEBUG("add head slice to task info failed", K(ret), K(task_result), K(task_info));
      }
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::task_add_empty_part(ObTaskInfo*& task_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_or_create_task_info(server_, task_info))) {
    LOG_WARN("get or create task info failed", K(ret), K_(server));
  }
  return ret;
}

int ObDistributedTaskSpliter::task_add_empty_slice(ObTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  bool by_partition = false;
  if (OB_FAIL(need_split_task_by_partition(by_partition))) {
    LOG_WARN("check generate task by partition failed", K(ret), K_(server));
  } else if (by_partition) {
    task_info.get_child_task_results().reset();
  }
  return ret;
}

int ObDistributedTaskSpliter::get_task_location(const ObSliceID& ob_slice_id, ObTaskLocation& task_location)
{
  int ret = OB_SUCCESS;
  uint64_t child_job_id = ob_slice_id.get_job_id();
  uint64_t child_task_id = ob_slice_id.get_task_id();
  ObJob* child_job = NULL;
  if (OB_ISNULL(job_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is NULL", K(ret));
  } else {
    task_location.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < job_->get_child_count(); i++) {
      ObTaskControl* tc = NULL;
      if (OB_FAIL(job_->get_child_job(i, child_job))) {
        LOG_WARN("fail to get child job", K(ret));
      } else if (OB_ISNULL(child_job)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child job is NULL", K(ret));
      } else if (child_job_id == child_job->get_job_id()) {
        if (OB_FAIL(child_job->get_task_control(*exec_ctx_, tc))) {
          LOG_WARN("fail get task ctrl", K(ret));
        } else if (OB_FAIL(tc->get_task_location(child_task_id, task_location))) {
          LOG_WARN("fail to get task location", K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

int ObDistributedTaskSpliter::calc_head_slice_count()
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  int64_t i = 0;
  const ObSliceEvent* head_slice = NULL;
  const ObSliceEvent* slice = NULL;
  if (OB_UNLIKELY(head_slice_idx_ < 0)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("head slice idx is out of range", K(ret), K(head_slice_idx_));
  } else if (head_slice_idx_ >= child_slices_.count()) {
    head_slice_count_ = 0;
  } else if (OB_ISNULL(head_slice = child_slices_.at(head_slice_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("head slice is NULL", K(ret));
  } else {
    for (i = head_slice_idx_ + 1; OB_SUCC(ret) && i < child_slices_.count(); i++) {
      if (OB_ISNULL(slice = child_slices_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("slice is NULL", K(ret));
      } else if (OB_FAIL(head_slice->get_shuffle_keys().compare(
                     slice->get_shuffle_keys(), repart_part_, repart_subpart_, cmp))) {
        LOG_WARN("fail to compare shuffle keys", K(ret));
      } else if (0 != cmp) {
        break;
      }
    }
    head_slice_count_ = i - head_slice_idx_;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
