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

#include "sql/engine/dml/ob_multi_part_lock_op.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/dml/ob_table_modify.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
OB_SERIALIZE_MEMBER((ObMultiPartLockSpec, ObTableModifySpec));

int ObMultiPartLockOp::inner_open()
{
  int ret = OB_SUCCESS;
  const ObMultiPartLockSpec& spec = MY_SPEC;
  if (OB_FAIL(init_multi_dml_ctx(
          ctx_, spec.table_dml_infos_, spec.get_phy_plan(), NULL /*subplan_root*/, spec.se_subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  } else {
    got_row_ = false;
  }
  return ret;
}

int ObMultiPartLockOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_FAIL(shuffle_lock_row())) {
    LOG_WARN("failed to shuffle lock row", K(ret));
  }
  if (OB_ITER_END == ret && got_row_) {
    if (OB_FAIL(process_mini_task())) {
      if (ret != OB_TRY_LOCK_ROW_CONFLICT && ret != OB_TRANSACTION_SET_VIOLATION) {
        LOG_WARN("failed to process mini task", K(ret));
      }
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObMultiPartLockOp::shuffle_lock_row()
{
  int ret = OB_SUCCESS;
  const int64_t idx = 0;  // index of the primary key
  const ObMultiPartLockSpec& spec = MY_SPEC;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("params have null", K(ret), K(plan_ctx));
  }
  for (int64_t tid = 0; OB_SUCC(ret) && tid < spec.table_dml_infos_.count(); ++tid) {
    const ObGlobalIndexDMLInfo& pk_dml_info = spec.table_dml_infos_.at(tid).index_infos_.at(idx);
    const SeDMLSubPlan& lock_plan = pk_dml_info.se_subplans_.at(LOCK_OP);
    const ObExpr* calc_part_id_expr = pk_dml_info.calc_part_id_exprs_.at(0);
    const bool need_filter_null = spec.table_dml_infos_.at(tid).need_check_filter_null_;

    ObGlobalIndexDMLCtx& pk_dml_ctx = table_dml_ctxs_.at(tid).index_ctxs_.at(idx);

    int64_t part_idx = OB_INVALID_INDEX;
    bool is_null = false;
    if (need_filter_null &&
        OB_FAIL(check_rowkey_is_null(lock_plan.access_exprs_, spec.table_dml_infos_.at(tid).rowkey_cnt_, is_null))) {
      LOG_WARN("failed to check rowkey is null", K(ret));
    } else if (is_null) {
      // do nothing
    } else if (OB_FAIL(calc_part_id(calc_part_id_expr, pk_dml_ctx.partition_ids_, part_idx))) {
      LOG_WARN("failed to calc partition id", K(ret));
    } else if (OB_FAIL(multi_dml_plan_mgr_.add_part_row(tid, idx, part_idx, LOCK_OP, lock_plan.access_exprs_))) {
      LOG_WARN("get or create values op failed", K(ret));
    } else {
      got_row_ = true;
      plan_ctx->add_affected_rows(1LL);
    }
  }
  return ret;
}

int ObMultiPartLockOp::process_mini_task()
{
  int ret = OB_SUCCESS;
  const ObMultiPartLockSpec& spec = MY_SPEC;
  if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx_, spec.table_dml_infos_, table_dml_ctxs_))) {
    LOG_WARN("failed to extended dml context", K(ret));
  } else if (OB_FAIL(multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("failed to build multi partition dml task", K(ret));
  } else {
    ObIArray<ObTaskInfo*>& task_info_list = multi_dml_plan_mgr_.get_mini_task_infos();
    const ObMiniJob& subplan_job = multi_dml_plan_mgr_.get_subplan_job();
    bool table_need_first = multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObMultiDMLInfo::ObIsMultiDMLGuard guard(*ctx_.get_physical_plan_ctx());
    UNUSED(guard);
    if (OB_FAIL(mini_task_executor_.execute(ctx_, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
  }
  return ret;
}

int ObMultiPartLockOp::inner_close()
{
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int wait_ret = mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp());
  int close_ret = ObTableModifyOp::inner_close();
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

}  // namespace sql
}  // namespace oceanbase
