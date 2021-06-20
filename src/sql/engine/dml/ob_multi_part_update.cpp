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
#include "sql/engine/dml/ob_multi_part_update.h"
#include "sql/engine/dml/ob_multi_dml_plan_mgr.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {
class ObMultiPartUpdate::ObMultiPartUpdateCtx : public ObTableModifyCtx, public ObMultiDMLCtx {
  friend class ObMultiPartUpdate;

public:
  explicit ObMultiPartUpdateCtx(ObExecContext& ctx)
      : ObTableModifyCtx(ctx), ObMultiDMLCtx(ctx.get_allocator()), found_rows_(0), changed_rows_(0), affected_rows_(0)
  {}
  ~ObMultiPartUpdateCtx()
  {}
  virtual void destroy() override
  {
    ObTableModifyCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
  void inc_found_rows()
  {
    ++found_rows_;
  }
  void inc_changed_rows()
  {
    ++changed_rows_;
  }
  void inc_affected_rows()
  {
    ++affected_rows_;
  }

private:
  int64_t found_rows_;
  int64_t changed_rows_;
  int64_t affected_rows_;
};

ObMultiPartUpdate::ObMultiPartUpdate(ObIAllocator& allocator) : ObTableModify(allocator), ObMultiDMLInfo(allocator)
{}

ObMultiPartUpdate::~ObMultiPartUpdate()
{}

int ObMultiPartUpdate::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObMultiPartUpdateCtx* update_ctx = NULL;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table update context from exec_ctx failed", K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(shuffle_update_row(ctx, got_row))) {
    LOG_WARN("shuffle update row failed", K(ret));
  } else if (!got_row) {
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx, table_dml_infos_, update_ctx->table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(update_ctx->multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi partition dml task failed", K(ret));
  } else {
    ObIArray<ObTaskInfo*>& task_info_list = update_ctx->multi_dml_plan_mgr_.get_mini_task_infos();
    const ObMiniJob& subplan_job = update_ctx->multi_dml_plan_mgr_.get_subplan_job();
    bool table_need_first = update_ctx->multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObIsMultiDMLGuard guard(*ctx.get_physical_plan_ctx());
    if (OB_FAIL(update_ctx->mini_task_executor_.execute(ctx, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
    UNUSED(result);
  }
  if (OB_SUCC(ret) && is_returning()) {
    update_ctx->returning_row_iterator_ = update_ctx->returning_row_store_.begin();
  }
  return ret;
}

int ObMultiPartUpdate::inner_close(ObExecContext& ctx) const
{
  int wait_ret = wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiPartUpdateCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableModify::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiPartUpdate::merge_implicit_cursor(
    const ObNewRow& full_row, bool is_update, bool client_found_rows, ObPhysicalPlanCtx& plan_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t stmt_id = OB_INVALID_INDEX;
  if (OB_UNLIKELY(stmt_id_idx_ < 0) || OB_UNLIKELY(stmt_id_idx_ >= full_row.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_id_idx is invalid", K(ret), K_(stmt_id_idx), K(full_row.count_));
  } else if (OB_UNLIKELY(full_row.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("full_row is invalid", K(ret), K(full_row));
  } else if (OB_FAIL(full_row.cells_[stmt_id_idx_].get_int(stmt_id))) {
    LOG_WARN("get stmt id from full row failed", K(ret), K(full_row));
  } else {
    ObImplicitCursorInfo implicit_cursor;
    implicit_cursor.stmt_id_ = stmt_id;
    implicit_cursor.found_rows_ = 1;
    implicit_cursor.matched_rows_ = 1;
    if (is_update) {
      implicit_cursor.affected_rows_ = 1;
      implicit_cursor.duplicated_rows_ = 1;
    } else if (client_found_rows) {
      implicit_cursor.affected_rows_ = 1;
    }
    if (OB_FAIL(plan_ctx.merge_implicit_cursor_info(implicit_cursor))) {
      LOG_WARN("merge implicit cursor info to plan ctx failed", K(ret), K(implicit_cursor));
    }
    LOG_DEBUG("merge implicit cursor", K(ret), K(full_row), K(implicit_cursor));
  }
  return ret;
}

int ObMultiPartUpdate::shuffle_update_row(ObExecContext& ctx, bool& got_row) const
{
  int ret = OB_SUCCESS;
  ObMultiPartUpdateCtx* update_ctx = NULL;
  const ObNewRow* full_row = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObSqlCtx* sql_ctx = NULL;
  got_row = false;
  CK(OB_NOT_NULL(plan_ctx = ctx.get_physical_plan_ctx()));
  CK(OB_NOT_NULL(my_session = ctx.get_my_session()));
  CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table insert context from exec_ctx failed", K(get_id()));
  } else if (OB_ISNULL(task_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task executor ctx is null");
  } else if (OB_ISNULL(get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan is null");
  }
  while (OB_SUCC(ret) && OB_SUCC(inner_get_next_row(ctx, full_row))) {
    for (int64_t k = 0; OB_SUCC(ret) && k < table_dml_infos_.count(); ++k) {
      const ObTableDMLInfo& table_dml_info = table_dml_infos_.at(k);
      ObTableDMLCtx& table_dml_ctx = update_ctx->table_dml_ctxs_.at(k);
      const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_dml_infos = table_dml_info.index_infos_;
      ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_dml_ctxs = table_dml_ctx.index_ctxs_;
      const ObTableModify* sub_update = global_index_dml_infos.at(0).dml_subplans_.at(UPDATE_OP).subplan_root_;
      bool is_updated = false;
      bool is_filtered = false;
      common::ObNewRow old_row;
      common::ObNewRow new_row;
      table_dml_info.assign_columns_.project_old_and_new_row(*full_row, old_row, new_row);
      if (is_returning()) {
        OZ(save_returning_row(
            update_ctx->expr_ctx_, *full_row, update_ctx->returning_row_, update_ctx->returning_row_store_));
      }
      CK(OB_NOT_NULL(sub_update));
      if (OB_SUCC(ret)) {
        bool is_null = false;
        if (table_dml_info.need_check_filter_null_) {
          if (OB_FAIL(check_rowkey_is_null(old_row, table_dml_info.rowkey_cnt_, is_null))) {
            LOG_WARN("check rowkey ixs null failed", K(ret), K(new_row), K(table_dml_info));
          } else if (is_null) {
            // filter out empty row
            continue;
          }
        } else {
#if !defined(NDEBUG)
          if (need_check_pk_is_null()) {
            if (OB_FAIL(check_rowkey_is_null(old_row, table_dml_info.rowkey_cnt_, is_null))) {
              LOG_WARN("check rowkey ixs null failed", K(ret), K(new_row), K(table_dml_info));
            } else if (is_null) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("delete row failed validity check", K(ret));
            }
          }
#endif
        }
      }
      if (OB_SUCC(ret)) {
        bool is_distinct = false;
        if (OB_FAIL(check_rowkey_whether_distinct(ctx,
                old_row,
                table_dml_info.rowkey_cnt_,
                table_dml_info.distinct_algo_,
                table_dml_ctx.rowkey_dist_ctx_,
                is_distinct))) {
          LOG_WARN("check rowkey whether distinct failed", K(ret));
        } else if (!is_distinct) {
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        OZ(check_row_null(ctx, new_row, sub_update->get_column_infos()), new_row);
      }
      OZ(check_updated_value(*update_ctx, table_dml_info.assign_columns_, old_row, new_row, is_updated));
      if (OB_SUCC(ret) && OB_INVALID_INDEX != stmt_id_idx_) {
        OZ(merge_implicit_cursor(
            *full_row, is_updated, my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS, *plan_ctx));
      }
      if (OB_SUCC(ret) && is_updated) {
        OZ(ForeignKeyHandle::do_handle(
            global_index_dml_infos.at(0).dml_subplans_.at(UPDATE_OP).subplan_root_, *update_ctx, old_row, new_row));
        OZ(ObPhyOperator::filter_row_for_check_cst(
            update_ctx->expr_ctx_, new_row, sub_update->check_constraint_exprs(), is_filtered));
        OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_dml_infos.count(); ++i) {
        const ObTableLocation* old_tbl_loc = NULL;
        const ObTableLocation* new_tbl_loc = NULL;
        ObIArray<int64_t>& part_ids = global_index_dml_ctxs.at(i).partition_ids_;
        const DMLSubPlanArray& dml_subplans = global_index_dml_infos.at(i).dml_subplans_;
        int64_t old_part_idx = OB_INVALID_INDEX;
        int64_t new_part_idx = OB_INVALID_INDEX;
        got_row = true;
        CK(global_index_dml_infos.at(i).table_locs_.count() == 2);
        CK(dml_subplans.count() == DML_OP_CNT);
        CK(OB_NOT_NULL(old_tbl_loc = global_index_dml_infos.at(i).table_locs_.at(0)));
        CK(OB_NOT_NULL(new_tbl_loc = global_index_dml_infos.at(i).table_locs_.at(1)));
        OZ(old_tbl_loc->calculate_partition_ids_by_row(ctx, sql_ctx->schema_guard_, *full_row, part_ids, old_part_idx));
        if (OB_SUCC(ret) && is_updated) {
          // if old row is not updated, new part index is not needed to be calculated
          OZ(new_tbl_loc->calculate_partition_ids_by_row(
              ctx, sql_ctx->schema_guard_, *full_row, part_ids, new_part_idx));
        }
        if (OB_SUCC(ret)) {
          if (!is_updated || old_part_idx == new_part_idx) {
            // old row and new row in same part, generate a update op
            ObNewRow cur_full_row = *full_row;
            cur_full_row.projector_ = dml_subplans.at(UPDATE_OP).value_projector_;
            cur_full_row.projector_size_ = dml_subplans.at(UPDATE_OP).value_projector_size_;
            if (OB_FAIL(update_ctx->multi_dml_plan_mgr_.add_part_row(k, i, old_part_idx, UPDATE_OP, cur_full_row))) {
              LOG_WARN("get or create values op failed", K(ret));
            } else {
              LOG_DEBUG("shuffle update row", K(part_ids), K(old_part_idx), K(dml_subplans), K(cur_full_row));
            }
          } else if (false == table_dml_info.get_enable_row_movement()) {
            ret = OB_ERR_UPD_CAUSE_PART_CHANGE;
          } else if (OB_INVALID_INDEX == new_part_idx) {
            ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
            LOG_WARN("updated partition key is beyond highest legal partition key", K(ret));
          } else {
            ObNewRow old_index_row = *full_row;
            ObNewRow new_index_row = *full_row;
            old_index_row.projector_ = dml_subplans.at(DELETE_OP).value_projector_;
            old_index_row.projector_size_ = dml_subplans.at(DELETE_OP).value_projector_size_;
            new_index_row.projector_ = dml_subplans.at(INSERT_OP).value_projector_;
            new_index_row.projector_size_ = dml_subplans.at(INSERT_OP).value_projector_size_;
            if (OB_FAIL(update_ctx->multi_dml_plan_mgr_.add_part_row(k, i, old_part_idx, DELETE_OP, old_index_row))) {
              LOG_WARN("get or create values op failed", K(ret));
            } else if (OB_FAIL(update_ctx->multi_dml_plan_mgr_.add_part_row(
                           k, i, new_part_idx, INSERT_OP, new_index_row))) {
              LOG_WARN("get or create values op failed", K(ret));
            } else {
              LOG_DEBUG("shuffle update row across partition",
                  K(part_ids),
                  K(old_part_idx),
                  K(new_part_idx),
                  K(dml_subplans),
                  K(old_index_row),
                  K(new_index_row));
            }
          }
        }
      }  // for (int64_t i = 0; OB_SUCC(ret) && i < global_index_dml_infos.count(); ++i)
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get next row failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    plan_ctx->add_row_matched_count(update_ctx->found_rows_);
    plan_ctx->add_row_duplicated_count(update_ctx->changed_rows_);
    plan_ctx->add_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS
                                    ? update_ctx->found_rows_
                                    : update_ctx->affected_rows_);
  }
  return ret;
}

int ObMultiPartUpdate::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(child_op_));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child op failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMultiPartUpdate::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMultiPartUpdateCtx* update_ctx = NULL;
  ObNewRow* return_row = NULL;
  if (!is_returning()) {
    ret = ObTableModify::get_next_row(ctx, row);
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(ret), K(is_returning()), K(update_ctx));
  } else if (OB_FAIL(update_ctx->returning_row_iterator_.get_next_row(return_row, NULL))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    row = return_row;
  }
  return ret;
}

int ObMultiPartUpdate::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObMultiPartUpdateCtx* update_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiPartUpdateCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else {
    update_ctx = static_cast<ObMultiPartUpdateCtx*>(op_ctx);
    if (OB_FAIL(update_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_))) {
      LOG_WARN("init multi dml ctx failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(update_ctx)) {
    /*
     * TODO:
     * we support one only table: table_dml_infos_.at(0), we need block more than one table here.
     */
    const ObTableDMLInfo& first_dml_info = table_dml_infos_.at(0);
    const ObGlobalIndexDMLInfo& first_index_dml_info = first_dml_info.index_infos_.at(0);
    const ObTableModify* first_sub_update = first_index_dml_info.dml_subplans_.at(UPDATE_OP).subplan_root_;
  }
  if (OB_SUCC(ret) && is_returning()) {
    OZ(update_ctx->alloc_row_cells(projector_size_, update_ctx->returning_row_));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
