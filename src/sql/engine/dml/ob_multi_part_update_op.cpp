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
#include "sql/engine/dml/ob_multi_part_update_op.h"
#include "sql/engine/dml/ob_multi_dml_plan_mgr.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
OB_SERIALIZE_MEMBER((ObMultiPartUpdateSpec, ObTableUpdateSpec));

int ObMultiPartUpdateOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (OB_FAIL(init_multi_dml_ctx(
          ctx_, MY_SPEC.table_dml_infos_, MY_SPEC.get_phy_plan(), NULL /*subplan_root*/, MY_SPEC.se_subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_FAIL(init_returning_store())) {
    LOG_WARN("init returning row store failed", K(ret));
  } else if (OB_FAIL(shuffle_update_row(got_row))) {
    LOG_WARN("shuffle update row failed", K(ret));
  } else if (!got_row) {
    // Multi part update did not iterate out any data
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

int ObMultiPartUpdateOp::inner_close()
{
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int wait_ret = mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp());
  int close_ret = ObTableModifyOp::inner_close();
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

// int ObMultiPartUpdateOp::merge_implicit_cursor(const ObNewRow &full_row,
//                                             bool is_update,
//                                             bool client_found_rows,
//                                             ObPhysicalPlanCtx &plan_ctx) const
//{
//  int ret = OB_SUCCESS;
//  int64_t stmt_id = OB_INVALID_INDEX;
//  if (OB_UNLIKELY(stmt_id_idx_ < 0) || OB_UNLIKELY(stmt_id_idx_ >= full_row.count_)) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("stmt_id_idx is invalid", K(ret), K_(stmt_id_idx), K(full_row.count_));
//  } else if (OB_UNLIKELY(full_row.is_invalid())) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("full_row is invalid", K(ret), K(full_row));
//  } else if (OB_FAIL(full_row.cells_[stmt_id_idx_].get_int(stmt_id))) {
//    LOG_WARN("get stmt id from full row failed", K(ret), K(full_row));
//  } else {
//    ObImplicitCursorInfo implicit_cursor;
//    implicit_cursor.stmt_id_ = stmt_id;
//    implicit_cursor.found_rows_ = 1;
//    implicit_cursor.matched_rows_ = 1;
//    if (is_update) {
//      implicit_cursor.affected_rows_ = 1;
//      implicit_cursor.duplicated_rows_ = 1;
//    } else if (client_found_rows) {
//      implicit_cursor.affected_rows_ = 1;
//    }
//    if (OB_FAIL(plan_ctx.merge_implicit_cursor_info(implicit_cursor))) {
//      LOG_WARN("merge implicit cursor info to plan ctx failed", K(ret), K(implicit_cursor));
//    }
//    LOG_DEBUG("merge implicit cursor", K(ret), K(full_row), K(implicit_cursor));
//  }
//  return ret;
//}

// Update contains the global index generation plan, divided into several situations:
// If the updated row and the old row are in the same partition, an update operation is generated
// and the corresponding op is UPDATE_OP
// If the updated row and the old row are in different partitions, a delete operation on the old row
// should be generated, and the op is DELETE_OP
// Then generate an insert operation for the new row, the op is INSERT_OP
int ObMultiPartUpdateOp::shuffle_update_row(bool& got_row)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObSqlCtx* sql_ctx = NULL;
  got_row = false;
  CK(OB_NOT_NULL(sql_ctx = ctx_.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  CK(OB_NOT_NULL(MY_SPEC.get_phy_plan()));
  while (OB_SUCC(ret) && OB_SUCC(inner_get_next_row())) {
    for (int64_t k = 0; OB_SUCC(ret) && k < MY_SPEC.table_dml_infos_.count(); ++k) {
      const ObTableDMLInfo& table_dml_info = MY_SPEC.table_dml_infos_.at(k);
      ObTableDMLCtx& table_dml_ctx = table_dml_ctxs_.at(k);
      const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_dml_infos = table_dml_info.index_infos_;
      ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_dml_ctxs = table_dml_ctx.index_ctxs_;
      const ObTableUpdateSpec* sub_update =
          dynamic_cast<const ObTableUpdateSpec*>(global_index_dml_infos.at(0).se_subplans_.at(UPDATE_OP).subplan_root_);
      if (MY_SPEC.is_returning_) {
        OZ(returning_datum_store_.add_row(MY_SPEC.output_, &eval_ctx_));
      }
      CK(OB_NOT_NULL(sub_update));
      bool is_updated = false;
      bool is_filtered = false;
      if (OB_SUCC(ret) && table_dml_info.need_check_filter_null_) {
        bool is_null = false;
        if (OB_FAIL(
                check_rowkey_is_null(table_dml_info.assign_columns_.new_row_, table_dml_info.rowkey_cnt_, is_null))) {
          LOG_WARN("check rowkey is null failed", K(ret), K(MY_SPEC.table_dml_infos_), K(k));
        } else if (is_null) {
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        bool is_distinct = false;
        if (OB_FAIL(check_rowkey_whether_distinct(table_dml_info.assign_columns_.old_row_,
                table_dml_info.rowkey_cnt_,
                table_dml_info.distinct_algo_,
                table_dml_ctx.se_rowkey_dist_ctx_,
                is_distinct))) {
          LOG_WARN("check rowkey whether distinct failed", K(ret));
        } else if (!is_distinct) {
          continue;
        }
      }
      OZ(check_row_null(table_dml_info.assign_columns_.new_row_, sub_update->column_infos_));
      OZ(check_updated_value(*this,
          table_dml_info.assign_columns_.get_assign_columns(),
          table_dml_info.assign_columns_.old_row_,
          table_dml_info.assign_columns_.new_row_,
          is_updated));
      // if (OB_SUCC(ret) && OB_INVALID_INDEX != stmt_id_idx_) {
      //   OZ(merge_implicit_cursor(*full_row,
      //                            is_updated,
      //                            my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS,
      //                            *plan_ctx));
      // }
      if (OB_SUCC(ret) && is_updated) {
        OZ(ForeignKeyHandle::do_handle(*this, sub_update->fk_args_, sub_update->old_row_, sub_update->new_row_));
        OZ(filter_row_for_check_cst(sub_update->check_constraint_exprs_, is_filtered));
        OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
      }
      // OZ(TriggerHandle::do_handle_after_row(*sub_update, *update_ctx),
      //   old_row, new_row);
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_dml_infos.count(); ++i) {
        const ObExpr* old_calc_part_expr = NULL;
        const ObExpr* new_calc_part_expr = NULL;
        ObIArray<int64_t>& part_ids = global_index_dml_ctxs.at(i).partition_ids_;
        const SeDMLSubPlanArray& se_subplans = global_index_dml_infos.at(i).se_subplans_;
        int64_t old_part_idx = OB_INVALID_INDEX;
        int64_t new_part_idx = OB_INVALID_INDEX;
        got_row = true;
        CK(2 == global_index_dml_infos.at(i).calc_part_id_exprs_.count());
        CK(se_subplans.count() == DML_OP_CNT);
        if (OB_SUCC(ret)) {
          old_calc_part_expr = global_index_dml_infos.at(i).calc_part_id_exprs_.at(0);
          new_calc_part_expr = global_index_dml_infos.at(i).calc_part_id_exprs_.at(1);
          CK(OB_NOT_NULL(old_calc_part_expr));
          CK(OB_NOT_NULL(new_calc_part_expr));
          OZ(ObSQLUtils::clear_evaluated_flag(global_index_dml_infos.at(i).calc_exprs_, eval_ctx_));
          OZ(calc_part_id(old_calc_part_expr, part_ids, old_part_idx));
        }
        if (OB_SUCC(ret) && is_updated) {
          // if old row is not updated, new part index is not needed to be calculated
          OZ(calc_part_id(new_calc_part_expr, part_ids, new_part_idx));
        }
        if (OB_SUCC(ret)) {
          if (!is_updated || old_part_idx == new_part_idx) {
            // The updated row and the old row are in the same partition, and an update operation is directly generated
            if (OB_FAIL(multi_dml_plan_mgr_.add_part_row(
                    k, i, old_part_idx, UPDATE_OP, se_subplans.at(UPDATE_OP).access_exprs_))) {
              LOG_WARN("get or create values op failed", K(ret));
            } else {
              LOG_DEBUG("shuffle update row",
                  K(part_ids),
                  K(old_part_idx),
                  K(k),
                  K(i),
                  K(ObToStringExprRow(eval_ctx_, se_subplans.at(UPDATE_OP).access_exprs_)));
            }
          } else if (false == table_dml_info.get_enable_row_movement()) {
            ret = OB_ERR_UPD_CAUSE_PART_CHANGE;
          } else if (OB_INVALID_INDEX == new_part_idx) {
            ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
            LOG_WARN("updated partition key is beyond highest legal partition key", K(ret));
          } else {
            if (OB_FAIL(multi_dml_plan_mgr_.add_part_row(
                    k, i, old_part_idx, DELETE_OP, se_subplans.at(DELETE_OP).access_exprs_))) {
              LOG_WARN("get or create values op failed", K(ret));
            } else if (OB_FAIL(multi_dml_plan_mgr_.add_part_row(
                           k, i, new_part_idx, INSERT_OP, se_subplans.at(INSERT_OP).access_exprs_))) {
              LOG_WARN("get or create values op failed", K(ret));
            } else {
              LOG_DEBUG("shuffle update row across partition",
                  K(part_ids),
                  K(old_part_idx),
                  K(new_part_idx),
                  K(ObToStringExprRow(eval_ctx_, se_subplans.at(INSERT_OP).access_exprs_)),
                  K(ObToStringExprRow(eval_ctx_, se_subplans.at(DELETE_OP).access_exprs_)));
            }
          }
        }
      }  // for (int64_t i = 0; OB_SUCC(ret) && i < global_index_dml_infos.count(); ++i)
      // need to restore before get_next_row from child
      OZ(restore_and_reset_fk_res_info());
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get next row failed", K(ret));
  }
  if (OB_SUCC(ret) && MY_SPEC.is_returning_) {
    bool need_dump = false;
    OZ(returning_datum_store_.finish_add_row(need_dump));
    OZ(returning_datum_iter_.init(&returning_datum_store_, ObChunkDatumStore::BLOCK_SIZE));
  }
  if (OB_SUCC(ret)) {
    plan_ctx->add_row_matched_count(found_rows_);
    plan_ctx->add_row_duplicated_count(changed_rows_);
    plan_ctx->add_affected_rows(
        my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS ? found_rows_ : affected_rows_);
  }
  return ret;
}

int ObMultiPartUpdateOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(child_));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child op failed", K(ret));
      }
    } else {
      clear_evaluated_flag();
    }
  }
  return ret;
}

int ObMultiPartUpdateOp::get_next_row()
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
