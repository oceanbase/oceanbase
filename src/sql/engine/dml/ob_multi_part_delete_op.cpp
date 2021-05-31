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
#include "sql/engine/dml/ob_multi_part_delete_op.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/dml/ob_table_modify.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
OB_SERIALIZE_MEMBER((ObMultiPartDeleteSpec, ObTableDeleteSpec));

int ObMultiPartDeleteOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (OB_FAIL(init_multi_dml_ctx(
          ctx_, MY_SPEC.table_dml_infos_, MY_SPEC.get_phy_plan(), NULL /*subplan_root*/, MY_SPEC.se_subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  }
  //  if (OB_SUCC(ret)) {
  //    const ObTableDMLInfo &first_dml_info = table_dml_infos_.at(0);
  //    const ObGlobalIndexDMLInfo &first_index_dml_info = first_dml_info.index_infos_.at(0);
  //    const ObTableModify *first_sub_delete = first_index_dml_info.dml_subplans_.at(DELETE_OP).subplan_root_;
  //    if (OB_NOT_NULL(first_sub_delete) && first_sub_delete->get_trigger_args().count() > 0) {
  //      OZ(delete_ctx->init_trigger_params(first_sub_delete->get_trigger_event(),
  //                                         first_sub_delete->get_all_timing_points(),
  //                                         first_sub_delete->get_trigger_columns().get_projector(),
  //                                         first_sub_delete->get_trigger_columns().get_count()));
  //    }
  //  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_FAIL(init_returning_store())) {
    LOG_WARN("init returning row store failed", K(ret));
  } else if (OB_FAIL(shuffle_delete_row(got_row))) {
    LOG_WARN("shuffle delete row failed", K(ret));
  } else if (!got_row) {
    // do nothing
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx_, MY_SPEC.table_dml_infos_, table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi partition dml task failed", K(ret));
  } else {
    ObIArray<ObTaskInfo*>& task_info_list = multi_dml_plan_mgr_.get_mini_task_infos();
    const ObMiniJob& subplan_job = multi_dml_plan_mgr_.get_subplan_job();
    bool table_need_first = multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObMultiDMLInfo::ObIsMultiDMLGuard guard(*ctx_.get_physical_plan_ctx());
    if (OB_FAIL(mini_task_executor_.execute(ctx_, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
  }
  return ret;
}

int ObMultiPartDeleteOp::inner_close()
{
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int wait_ret = mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp());
  int close_ret = ObTableModifyOp::inner_close();
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiPartDeleteOp::shuffle_delete_row(bool& got_row)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSqlCtx* sql_ctx = NULL;
  got_row = false;
  CK(OB_NOT_NULL(sql_ctx = ctx_.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  CK(OB_NOT_NULL(MY_SPEC.get_phy_plan()));
  while (OB_SUCC(ret) && OB_SUCC(inner_get_next_row())) {
    LOG_DEBUG("inner get next row", K(ObToStringExprRow(eval_ctx_, child_->get_spec().output_)));
    for (int64_t k = 0; OB_SUCC(ret) && k < MY_SPEC.table_dml_infos_.count(); ++k) {
      clear_evaluated_flag();
      const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = MY_SPEC.table_dml_infos_.at(k).index_infos_;
      ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = table_dml_ctxs_.at(k).index_ctxs_;
      //      ObTableModifySpec *sub_delete =
      //         global_index_ctxs.at(0).se_subplans_.at(DELETE_OP).subplan_root_;
      //      CK(OB_NOT_NULL(sub_delete));
      //      OZ(TriggerHandle::init_param_old_row(*sub_delete, *delete_ctx, *old_row), *old_row);
      //      OX(LOG_DEBUG("TRIGGER", K(*old_row), K(delete_ctx->tg_old_row_)));
      //      OZ(TriggerHandle::do_handle_before_row(*sub_delete, *delete_ctx, NULL), *old_row);
      CK(!global_index_infos.empty());
      CK(!global_index_ctxs.empty());
      if (MY_SPEC.is_returning_) {
        OZ(returning_datum_store_.add_row(MY_SPEC.output_, &eval_ctx_));
      }
      if (OB_SUCC(ret)) {
        const SeDMLSubPlanArray& primary_dml_subplans = global_index_ctxs.at(0).se_subplans_;
        if (MY_SPEC.table_dml_infos_.at(k).need_check_filter_null_) {
          bool is_null = false;
          if (OB_FAIL(check_rowkey_is_null(primary_dml_subplans.at(DELETE_OP).access_exprs_,
                  MY_SPEC.table_dml_infos_.at(k).rowkey_cnt_,
                  is_null))) {
            LOG_WARN("check rowkey is null failed", K(ret), K(MY_SPEC.table_dml_infos_), K(k));
          } else if (is_null) {
            continue;
          }
        }
        if (OB_SUCC(ret)) {
          bool is_distinct = false;
          if (OB_FAIL(check_rowkey_whether_distinct(primary_dml_subplans.at(DELETE_OP).access_exprs_,
                  MY_SPEC.table_dml_infos_.at(k).rowkey_cnt_,
                  MY_SPEC.table_dml_infos_.at(k).distinct_algo_,
                  table_dml_ctxs_.at(k).se_rowkey_dist_ctx_,
                  is_distinct))) {
            LOG_WARN("check rowkey whether distinct failed", K(ret));
          } else if (!is_distinct) {
            continue;
          }
        }
      }
      ObTableModifySpec* delete_spec = global_index_infos.at(0).se_subplans_.at(DELETE_OP).subplan_root_;
      CK(OB_NOT_NULL(delete_spec));
      OZ(ForeignKeyHandle::do_handle_old_row(*this, delete_spec->fk_args_, child_->get_spec().output_));
      //      OZ(TriggerHandle::do_handle_after_row(*sub_delete, *delete_ctx), *old_row);
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); ++i) {
        clear_evaluated_flag();
        const ObExpr* calc_part_id_expr = global_index_infos.at(i).calc_part_id_exprs_.at(0);
        ObIArray<int64_t>& part_ids = global_index_ctxs.at(i).partition_ids_;
        const SeDMLSubPlanArray& se_subplans = global_index_ctxs.at(i).se_subplans_;
        int64_t part_idx = OB_INVALID_INDEX;
        CK(1 == global_index_infos.at(i).calc_part_id_exprs_.count());
        CK(DML_OP_CNT == se_subplans.count());
        OZ(calc_part_id(calc_part_id_expr, part_ids, part_idx));
        if (OB_SUCC(ret)) {
          if (OB_FAIL(multi_dml_plan_mgr_.add_part_row(k, i, part_idx, DELETE_OP, se_subplans.at(0).access_exprs_))) {
            LOG_WARN("add row to dynamic mini task scheduler failed", K(ret));
          } else {
            LOG_DEBUG("shuffle delete row",
                K(k),
                K(part_idx),
                K(part_ids),
                K(i),
                K(ObToStringExprRow(eval_ctx_, se_subplans.at(0).access_exprs_)));
          }
        }
      }  // for  global_index_infos
    }    // for table_dml_infos
    got_row = true;
    plan_ctx->add_affected_rows(1);
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && MY_SPEC.is_returning_) {
    bool need_dump = false;
    OZ(returning_datum_store_.finish_add_row(need_dump));
    OZ(returning_datum_iter_.init(&returning_datum_store_, ObChunkDatumStore::BLOCK_SIZE));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get next row failed", K(ret));
  }
  return ret;
}

int ObMultiPartDeleteOp::inner_get_next_row()
{
  clear_evaluated_flag();
  return child_->get_next_row();
}

int ObMultiPartDeleteOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_returning_) {
    ret = OB_ITER_END;
  } else {
    ret = returning_datum_iter_.get_next_row(MY_SPEC.output_, eval_ctx_);
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
