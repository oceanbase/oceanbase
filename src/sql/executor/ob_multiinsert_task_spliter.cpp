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

#include "lib/hash/ob_iteratable_hashset.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/executor/ob_multiinsert_task_spliter.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_phy_operator_type.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/executor/ob_job_conf.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase {
namespace sql {

ObMultiInsertTaskSpliter::ObMultiInsertTaskSpliter()
    : phy_table_loc_(NULL), prepare_done_flag_(false), store_(), next_task_idx_(0)
{}

ObMultiInsertTaskSpliter::~ObMultiInsertTaskSpliter()
{
  for (int64_t i = 0; i < store_.count(); ++i) {
    ObTaskInfo* t = store_.at(i);
    if (OB_LIKELY(NULL != t)) {
      t->~ObTaskInfo();
    }
  }
}

int ObMultiInsertTaskSpliter::prepare()
{
  int ret = OB_SUCCESS;

  ObPhyOperator* root_op = NULL;
  prepare_done_flag_ = false;
  if (OB_ISNULL(plan_ctx_) || OB_ISNULL(exec_ctx_) || OB_ISNULL(allocator_) || OB_ISNULL(job_) ||
      OB_ISNULL(job_conf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("param not init", K_(plan_ctx), K_(exec_ctx), K_(allocator), K_(job), K_(job_conf));
  } else if (OB_UNLIKELY(NULL == (root_op = job_->get_root_op()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root op is NULL", K(ret));
  } else {
    ObSEArray<const ObTableModify*, 16> insert_ops;
    if (OB_FAIL(ObTaskSpliter::find_insert_ops(insert_ops, *root_op))) {
      LOG_WARN("fail to find insert ops", K(ret), "root_op_id", root_op->get_id());
    } else if (OB_UNLIKELY(1 != insert_ops.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("there must be one and only one insert op in distr mode", K(ret), K(common::lbt()));
    } else {
      // const ObPhyTableLocation *table_loc = NULL;
      const ObTableModify* insert_op = insert_ops.at(0);
      if (OB_ISNULL(insert_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert op can't be null", K(ret));
      } else {
        uint64_t table_location_key = insert_op->get_table_id();
        if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(
                *exec_ctx_, table_location_key, table_location_key, phy_table_loc_))) {
          LOG_WARN("fail to get phy table location", K(ret));
        } else if (OB_ISNULL(phy_table_loc_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get phy table location", K(ret), K(phy_table_loc_));
        } else {
          prepare_done_flag_ = true;
        }
      }
    }
  }
  return ret;
}

int ObMultiInsertTaskSpliter::get_next_task(ObTaskInfo*& task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    LOG_WARN("unexpected error. allocator is null", K(ret));
  } else if (OB_UNLIKELY(false == prepare_done_flag_)) {
    ret = prepare();
  }
  // after success prepare
  if (OB_SUCC(ret)) {
    void* ptr = NULL;
    ObTaskInfo::ObRangeLocation range_loc(*allocator_);
    if (OB_FAIL(get_next_range_location(range_loc))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail get next task", K(ret));
      }
    } else if (OB_UNLIKELY(NULL == (ptr = allocator_->alloc(sizeof(ObTaskInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail allocate task", K(ret));
    } else {
      ObTaskInfo* t = new (ptr) ObTaskInfo(*allocator_);
      if (OB_FAIL(store_.push_back(t))) {
        LOG_WARN("fail to push taskinfo into store", K(ret));
      } else if (OB_FAIL(t->set_range_location(range_loc))) {
        LOG_WARN("fail to set range_location", K(ret), K(range_loc));
      } else {
        ObTaskID ob_task_id;
        ObTaskLocation task_loc;
        ob_task_id.set_ob_job_id(job_->get_ob_job_id());
        ob_task_id.set_task_id(next_task_idx_);
        task_loc.set_ob_task_id(ob_task_id);
        task_loc.set_server(range_loc.server_);
        t->set_task_split_type(get_type());
        t->set_pull_slice_id(next_task_idx_);
        t->set_location_idx(next_task_idx_);
        t->set_task_location(task_loc);
        t->set_root_op(job_->get_root_op());
        t->set_state(OB_TASK_STATE_NOT_INIT);
        //        job_->set_scan_job();
        task = t;
        // move to next info
        next_task_idx_++;
      }
    }
  }
  return ret;
}

int ObMultiInsertTaskSpliter::get_next_range_location(ObTaskInfo::ObRangeLocation& range_loc)
{
  int ret = OB_SUCCESS;
  static const int64_t TABLE_COUNT = 1;
  range_loc.reset();
  const ObPartitionReplicaLocation* part_location = NULL;
  if (OB_ISNULL(phy_table_loc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy table loation is null", K(ret), K(phy_table_loc_));
  } else if (OB_UNLIKELY(next_task_idx_ >= phy_table_loc_->get_partition_cnt())) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(part_location = phy_table_loc_->get_part_replic_by_index(next_task_idx_))) {
    // A partition corresponds to a task
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. can not get part location", K(ret), K(*phy_table_loc_));
  } else {
    const ObPartitionReplicaLocation& part_loc = *part_location;
    const ObReplicaLocation& rep_loc = part_loc.get_replica_location();
    if (!rep_loc.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid replica loc", K(ret), K(rep_loc));
    } else {
      range_loc.server_ = rep_loc.server_;
      if (OB_FAIL(range_loc.part_locs_.init(TABLE_COUNT))) {
        LOG_WARN("init  part_locs_ failed", K(ret));
      } else {
        ObTaskInfo::ObPartLoc task_part_loc;
        if (OB_FAIL(part_loc.get_partition_key(task_part_loc.partition_key_))) {
          LOG_WARN("fail to get partition key", K(ret), K(part_loc));
        } else if (FALSE_IT(task_part_loc.renew_time_ = part_loc.get_renew_time())) {
        } else if (!task_part_loc.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid task partition location", K(ret), K(task_part_loc), K(part_loc));
        } else if (OB_FAIL(range_loc.part_locs_.push_back(task_part_loc))) {
          LOG_WARN("fail to push back partition key", K(ret), K(task_part_loc));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
