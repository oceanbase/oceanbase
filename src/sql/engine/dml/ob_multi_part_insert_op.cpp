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
#include "sql/engine/dml/ob_multi_part_insert_op.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER((ObMultiPartInsertSpec, ObTableInsertSpec));

int ObMultiPartInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  /*  if (OB_SUCC(ret) && OB_NOT_NULL(insert_ctx)) {*/
  // const ObTableDMLInfo &first_dml_info = table_dml_infos_.at(0);
  // const ObGlobalIndexDMLInfo &first_index_dml_info = first_dml_info.index_infos_.at(0);
  // const ObTableModify *first_sub_insert = first_index_dml_info.dml_subplans_.at(INSERT_OP).subplan_root_;
  // if (OB_NOT_NULL(first_sub_insert) && first_sub_insert->get_trigger_args().count() > 0) {
  // OZ(insert_ctx->init_trigger_params(first_sub_insert->get_trigger_event(),
  // first_sub_insert->get_all_timing_points(),
  // first_sub_insert->get_trigger_columns().get_projector(),
  // first_sub_insert->get_trigger_columns().get_count()));
  //}
  /*}*/
  if (OB_FAIL(init_multi_dml_ctx(
          ctx_, MY_SPEC.table_dml_infos_, MY_SPEC.get_phy_plan(), NULL /*subplan_root*/, MY_SPEC.se_subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_FAIL(init_returning_store())) {
    LOG_WARN("init returning row store failed", K(ret));
  } else if (OB_FAIL(shuffle_insert_row(got_row))) {
    LOG_WARN("shuffle insert row failed", K(ret));
  } else if (!got_row) {
    // do nothing
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx_, MY_SPEC.table_dml_infos_, table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi partition dml task failed", K(ret));
  } else {
    const ObMiniJob& subplan_job = multi_dml_plan_mgr_.get_subplan_job();
    ObIArray<ObTaskInfo*>& task_info_list = multi_dml_plan_mgr_.get_mini_task_infos();
    bool table_need_first = multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObMultiDMLInfo::ObIsMultiDMLGuard guard(*ctx_.get_physical_plan_ctx());
    if (OB_FAIL(mini_task_executor_.execute(ctx_, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
  }
  return ret;
}

int ObMultiPartInsertOp::inner_close()
{
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int wait_ret = mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp());
  int close_ret = ObTableInsertOp::inner_close();
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiPartInsertOp::shuffle_insert_row(bool& got_row)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService* schema_service = NULL;
  ObTaskExecutorCtx* task_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSqlCtx* sql_ctx = NULL;
  int64_t affected_rows = 0;
  bool is_filtered = false;
  got_row = false;
  CK(OB_NOT_NULL(schema_service = task_ctx->schema_service_));
  CK(OB_NOT_NULL(MY_SPEC.get_phy_plan()));
  CK(OB_NOT_NULL(sql_ctx = ctx_.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  CK(MY_SPEC.table_dml_infos_.count() == 1);
  CK(table_dml_ctxs_.count() == 1);
  if (OB_SUCC(ret)) {
    // Insert can only be a different global index of the same table,
    // and does not support inserting multiple user tables in one statement
    const ObIArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = MY_SPEC.table_dml_infos_.at(0).index_infos_;
    ObIArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = table_dml_ctxs_.at(0).index_ctxs_;
    ObTableModifySpec* sub_insert = global_index_infos.at(0).se_subplans_.at(INSERT_OP).subplan_root_;
    CK(OB_NOT_NULL(sub_insert));
    const ObExprPtrIArray* output = NULL;
    while (OB_SUCC(ret) && OB_SUCC(prepare_next_storage_row(output))) {
      got_row = true;
      ++affected_rows;
      OZ(check_row_null(*output, sub_insert->column_infos_));
      if (MY_SPEC.is_returning_) {
        OZ(returning_datum_store_.add_row(MY_SPEC.output_, &eval_ctx_));
      }
      OZ(ForeignKeyHandle::do_handle_new_row(*this, sub_insert->fk_args_, *output));
      OZ(filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered));
      OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); ++i) {
        ObDatum* partition_id_datum = NULL;
        const ObExpr* calc_part_id_expr = global_index_infos.at(i).calc_part_id_exprs_.at(0);
        const SeDMLSubPlan& dml_subplan = global_index_infos.at(i).se_subplans_.at(0);
        ObIArray<int64_t>& part_ids = global_index_ctxs.at(i).partition_ids_;
        int64_t part_idx = OB_INVALID_INDEX;
        CK(OB_NOT_NULL(calc_part_id_expr));
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(ObSQLUtils::clear_evaluated_flag(global_index_infos.at(i).calc_exprs_, eval_ctx_))) {
          LOG_WARN("fail to clear evaluated flag", K(ret));
        } else if (OB_FAIL(calc_part_id_expr->eval(eval_ctx_, partition_id_datum))) {
          LOG_WARN("fail to calc part id", K(ret), K(calc_part_id_expr));
        } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == partition_id_datum->get_int()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("no partition matched", K(ret), K(child_->get_spec().output_));
        } else {
          if (OB_FAIL(add_var_to_array_no_dup(part_ids, partition_id_datum->get_int(), &part_idx))) {
            LOG_WARN("Failed to add var to array no dup", K(ret));
          }
        }
        if (OB_SUCC(ret) && 0 == i && global_index_infos.at(0).hint_part_ids_.count() > 0) {
          // for the data table, if partition hint ids are not empty,
          // should deal dml partition selection
          if (!has_exist_in_array(global_index_infos.at(0).hint_part_ids_, partition_id_datum->get_int())) {
            ret = OB_PARTITION_NOT_MATCH;
            LOG_DEBUG("Partition not match", K(ret), K(*partition_id_datum));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(multi_dml_plan_mgr_.add_part_row(0, i, part_idx, INSERT_OP, dml_subplan.access_exprs_))) {
            LOG_WARN("add row to dynamic mini task scheduler failed", K(ret));
          } else {
            LOG_DEBUG("shuffle insert row",
                K(part_idx),
                K(part_ids),
                K(ObToStringExprRow(eval_ctx_, dml_subplan.access_exprs_)));
          }
        }
      }  // for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); ++i)
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("get next row from expr values failed", K(ret));
  }
  if (OB_SUCC(ret) && MY_SPEC.is_returning_) {
    bool need_dump = false;
    OZ(returning_datum_store_.finish_add_row(need_dump));
    OZ(returning_datum_iter_.init(&returning_datum_store_, ObChunkDatumStore::BLOCK_SIZE));
  }
  if (OB_SUCC(ret)) {
    plan_ctx->add_row_matched_count(affected_rows);
    plan_ctx->add_affected_rows(affected_rows);
    // sync last user specified value after iter ends(compatible with MySQL)
    if (OB_FAIL(plan_ctx->sync_last_value_local())) {
      LOG_WARN("failed to sync last value", K(ret));
    }
    int sync_ret = OB_SUCCESS;
    if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
      LOG_WARN("failed to sync value globally", K(sync_ret));
    }
    if (OB_SUCC(ret)) {
      ret = sync_ret;
    }
  }
  return ret;
}

int ObMultiPartInsertOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.is_returning_) {
    ret = ObTableInsertOp::get_next_row();
  } else {
    ret = returning_datum_iter_.get_next_row(MY_SPEC.output_, eval_ctx_);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
