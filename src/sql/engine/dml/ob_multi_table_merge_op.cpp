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

#include "sql/engine/dml/ob_multi_table_merge_op.h"
#include "sql/engine/dml/ob_multi_dml_plan_mgr.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;

namespace sql {
OB_SERIALIZE_MEMBER((ObMultiTableMergeSpec, ObTableMergeSpec));

int ObMultiTableMergeOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (OB_ISNULL(child_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(child_));
  } else if (OB_FAIL(init_multi_dml_ctx(ctx_,
                 MY_SPEC.table_dml_infos_,
                 MY_SPEC.get_phy_plan(),
                 NULL /*subplan_root*/,
                 MY_SPEC.se_subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_FAIL(shuffle_merge_row(got_row))) {
    LOG_WARN("shuffle merge row failed", K(ret));
  } else if (!got_row) {
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx_, MY_SPEC.table_dml_infos_, table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi partition dml task failed", K(ret));
  } else {
    const ObMiniJob& subplan_job = multi_dml_plan_mgr_.get_subplan_job();
    ObIArray<ObTaskInfo*>& task_info_list = multi_dml_plan_mgr_.get_mini_task_infos();
    bool table_need_first = multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    OZ(mini_task_executor_.execute(ctx_, subplan_job, task_info_list, table_need_first, result));
  }
  return ret;
}

int ObMultiTableMergeOp::inner_close()
{
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int wait_ret = mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp());
  int close_ret = ObTableModifyOp::inner_close();
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiTableMergeOp::shuffle_merge_row(bool& got_row)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  while (OB_SUCC(ret) && OB_SUCC(child_->get_next_row())) {
    clear_evaluated_flag();
    bool is_match = false;
    if (OB_FAIL(check_is_match(is_match))) {
      LOG_WARN("failed to calc is match", K(ret));
    } else if (is_match) {
      if (MY_SPEC.has_update_clause_) {
        OZ(process_update(got_row));
      }
    } else {
      if (MY_SPEC.has_insert_clause_) {
        OZ(process_insert(got_row));
      }
    }
  }

  if (OB_ITER_END != ret) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("failed to deal with merge into", K(ret));
    }
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMultiTableMergeOp::process_update(bool& got_row)
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  bool need_delete = false;
  bool is_row_changed = false;
  bool is_conflict = false;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  OZ(calc_condition(MY_SPEC.update_conds_, need_update));
  if (need_update) {
    OZ(generate_origin_row(is_conflict));
    OZ(calc_delete_condition(MY_SPEC.delete_conds_, need_delete));
    OV(!is_conflict, need_delete ? OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS : OB_ERR_UPDATE_TWICE);
    OX(affected_rows_ += 1);
    OX(got_row = true);
    OZ(update_row(need_delete));
  }
  return ret;
}

int ObMultiTableMergeOp::process_insert(bool& got_row)
{
  int ret = OB_SUCCESS;
  bool is_true_cond = false;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(calc_condition(MY_SPEC.insert_conds_, is_true_cond))) {
    LOG_WARN("fail to calc condition", K(MY_SPEC.insert_conds_), K(ret));
  } else if (is_true_cond) {
    OZ(check_row_null(MY_SPEC.storage_row_output_, MY_SPEC.column_infos_));
    bool is_filtered = false;
    OZ(filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered));
    OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
    CK(1 == MY_SPEC.table_dml_infos_.count());
    CK(1 == table_dml_ctxs_.count());
    if (OB_SUCC(ret)) {
      const ObIArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = MY_SPEC.table_dml_infos_.at(0).index_infos_;
      ObIArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = table_dml_ctxs_.at(0).index_ctxs_;
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); i++) {
        const ObExpr* calc_part_expr = global_index_infos.at(i).calc_part_id_exprs_.at(0);
        const SeDMLSubPlanArray& se_subplans = global_index_infos.at(i).se_subplans_;
        int64_t part_idx = OB_INVALID_INDEX;
        ObIArray<int64_t>& part_ids = global_index_ctxs.at(i).partition_ids_;
        OZ(calc_part_id(calc_part_expr, part_ids, part_idx));
        OZ(multi_dml_plan_mgr_.add_part_row(0, i, part_idx, INSERT_OP, se_subplans.at(INSERT_OP).access_exprs_));
        LOG_DEBUG("multi table insert row",
            "row",
            ROWEXPR2STR(eval_ctx_, se_subplans.at(INSERT_OP).access_exprs_),
            K(i),
            K(part_idx),
            K(part_ids));
      }
    }
    OX(plan_ctx->add_affected_rows(1));
    OX(got_row = true);
  }
  return ret;
}

int ObMultiTableMergeOp::update_row(bool need_delete)
{
  int ret = OB_SUCCESS;
  CK(1 == MY_SPEC.table_dml_infos_.count());
  CK(1 == table_dml_ctxs_.count());
  if (OB_SUCC(ret)) {
    const ObTableDMLInfo& table_dml_info = MY_SPEC.table_dml_infos_.at(0);
    ObTableDMLCtx& table_dml_ctx = table_dml_ctxs_.at(0);
    const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_dml_infos = table_dml_info.index_infos_;
    ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_dml_ctxs = table_dml_ctx.index_ctxs_;
    OZ(check_row_null(MY_SPEC.new_row_, MY_SPEC.column_infos_));
    bool is_filtered = false;
    OZ(filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered));
    OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
    for (int64_t i = 0; OB_SUCC(ret) && i < global_index_dml_infos.count(); i++) {
      const ObExpr* old_calc_part_expr = NULL;
      const ObExpr* new_calc_part_expr = NULL;
      int64_t old_part_idx = OB_INVALID_INDEX;
      int64_t new_part_idx = OB_INVALID_INDEX;
      ObIArray<int64_t>& part_ids = global_index_dml_ctxs.at(i).partition_ids_;
      const SeDMLSubPlanArray& se_subplans = global_index_dml_infos.at(i).se_subplans_;
      CK(3 == global_index_dml_infos.at(i).calc_part_id_exprs_.count());
      CK(se_subplans.count() == DML_OP_CNT);
      if (OB_SUCC(ret)) {
        old_calc_part_expr = global_index_dml_infos.at(i).calc_part_id_exprs_.at(1);
        new_calc_part_expr = global_index_dml_infos.at(i).calc_part_id_exprs_.at(2);
        CK(OB_NOT_NULL(old_calc_part_expr));
        CK(OB_NOT_NULL(new_calc_part_expr));
        OZ(calc_part_id(old_calc_part_expr, part_ids, old_part_idx));
      }
      OZ(multi_dml_plan_mgr_.add_part_row(0, i, old_part_idx, DELETE_OP, se_subplans.at(DELETE_OP).access_exprs_));
      LOG_DEBUG("multi table delete row",
          "row",
          ROWEXPR2STR(eval_ctx_, se_subplans.at(DELETE_OP).access_exprs_),
          K(new_part_idx),
          K(old_part_idx),
          K(part_ids));
      if (!need_delete) {
        OZ(calc_part_id(new_calc_part_expr, part_ids, new_part_idx));
        if (OB_SUCC(ret) && new_part_idx != old_part_idx) {
          if (!MY_SPEC.table_dml_infos_.at(0).get_enable_row_movement()) {
            ret = OB_ERR_UPD_CAUSE_PART_CHANGE;
          } else if (OB_INVALID_INDEX == new_part_idx) {
            ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
            LOG_WARN("updated partition key is beyong highest legal partition key", K(ret));
          }
        }
        OZ(multi_dml_plan_mgr_.add_part_row(
            0, i, new_part_idx, UPDATE_INSERT_OP, se_subplans.at(UPDATE_INSERT_OP).access_exprs_));
        LOG_DEBUG("multi table insert row",
            "row",
            ROWEXPR2STR(eval_ctx_, se_subplans.at(UPDATE_INSERT_OP).access_exprs_),
            K(i),
            K(new_part_idx),
            K(old_part_idx),
            K(part_ids));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
