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
#include "sql/engine/dml/ob_multi_table_insert_up_op.h"
#include "sql/engine/dml/ob_table_update_op.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER((ObMultiTableInsertUpSpec, ObTableInsertUpSpec));

int ObMultiTableInsertUpOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(init_multi_dml_ctx(
          ctx_, MY_SPEC.table_dml_infos_, MY_SPEC.get_phy_plan(), NULL /*subplan_root*/, MY_SPEC.se_subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  } else if (OB_FAIL(MY_SPEC.duplicate_key_checker_.init_checker_ctx(dupkey_checker_ctx_))) {
    LOG_WARN("init duplicate key checker context failed", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_FAIL(replace_row_store_.init(UINT64_MAX,
                 my_session->get_effective_tenant_id(),
                 ObCtxIds::DEFAULT_CTX_ID,
                 ObModIds::OB_SQL_CHUNK_ROW_STORE,
                 false /*enable_dump*/))) {
    LOG_WARN("fail to init datum store", K(ret));
  } else if (OB_FAIL(load_insert_up_row())) {
    LOG_WARN("load insert update row failed", K(ret));
  } else if (OB_FAIL(MY_SPEC.duplicate_key_checker_.build_duplicate_rowkey_map(ctx_, dupkey_checker_ctx_))) {
    LOG_WARN("build duplicate rowkey map failed", K(ret));
  } else if (OB_FAIL(shuffle_insert_up_row(got_row))) {
    LOG_WARN("shuffle replace row failed", K(ret));
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
    } else {
      plan_ctx->add_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS
                                      ? affected_rows_ + duplicate_rows_
                                      : affected_rows_);
      plan_ctx->set_row_matched_count(found_rows_);
      plan_ctx->add_row_duplicated_count(changed_rows_);
      OZ(plan_ctx->sync_last_value_local());
      int sync_ret = OB_SUCCESS;
      if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
        LOG_WARN("failed to sync value globally", K(sync_ret));
      }
      if (OB_SUCC(ret)) {
        ret = sync_ret;
      }
    }
  }
  return ret;
}

int ObMultiTableInsertUpOp::inner_close()
{
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int wait_ret = mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp());
  int close_ret = ObTableModifyOp::inner_close();
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiTableInsertUpOp::load_insert_up_row()
{
  int ret = OB_SUCCESS;
  do {
    // load replace row to buffer for duplicate key check
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child op failed", K(ret));
      }
    } else if (OB_FAIL(calc_insert_row())) {
      LOG_WARN("calc insert row failed", K(ret));
    } else if (OB_FAIL(replace_row_store_.add_row(MY_SPEC.insert_row_, &eval_ctx_))) {
      LOG_WARN("add replace row to row store failed", K(ret));
    } else {
      LOG_DEBUG("insert up row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.insert_row_));
    }
  } while (OB_SUCC(ret));
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMultiTableInsertUpOp::shuffle_insert_up_row(bool& got_row)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObSqlCtx* sql_ctx = NULL;
  got_row = false;
  CK(OB_NOT_NULL(MY_SPEC.get_phy_plan()));
  CK(OB_NOT_NULL(sql_ctx = ctx_.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  CK(MY_SPEC.output_.count() == MY_SPEC.table_column_exprs_.count());
  if (OB_SUCC(ret)) {
    // insert into on duplicate key update only randomly updates one duplicated row at a time
    ObSEArray<ObConstraintValue, 1> constraint_values;
    ObChunkDatumStore::Iterator insert_row_iter;
    OZ(replace_row_store_.begin(insert_row_iter));
    const ObChunkDatumStore::StoredRow* insert_row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(insert_row_iter.get_next_row(MY_SPEC.table_column_exprs_, eval_ctx_, &insert_row))) {
      got_row = true;
      constraint_values.reuse();
      LOG_DEBUG("insert iter next row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.table_column_exprs_));
      OZ(MY_SPEC.duplicate_key_checker_.check_duplicate_rowkey(
          dupkey_checker_ctx_, MY_SPEC.table_column_exprs_, constraint_values));
      if (OB_SUCC(ret)) {
        if (OB_LIKELY(constraint_values.empty())) {
          // execute insert action
          OZ(shuffle_insert_row(MY_SPEC.table_column_exprs_, *insert_row));
          LOG_DEBUG("shuffle_insert_row", "insert_row", ROWEXPR2STR(eval_ctx_, MY_SPEC.table_column_exprs_));
        } else {
          clear_evaluated_flag();
          OZ(insert_row->to_expr(MY_SPEC.insert_row_, eval_ctx_));
          // execute update action
          const ObChunkDatumStore::StoredRow* duplicated_row = constraint_values.at(0).current_datum_row_;
          OZ(shuffle_update_row(*duplicated_row));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get next row from expr values failed", K(ret));
  }
  if (dupkey_checker_ctx_.update_incremental_row()) {
    release_multi_part_shuffle_info();
    OZ(dupkey_checker_ctx_.shuffle_final_data(ctx_, MY_SPEC.table_column_exprs_, *this));
  }
  return ret;
}

int ObMultiTableInsertUpOp::shuffle_final_delete_row(ObExecContext& ctx, const ObExprPtrIArray& delete_row)
{
  int ret = OB_SUCCESS;
  // ObSchemaGetterGuard *schema_guard = NULL;
  // ObMultiTableInsertUpCtx *replace_ctx = NULL;
  // ObSqlCtx *sql_ctx = NULL;
  // ObTaskExecutorCtx &task_ctx = ctx.get_task_exec_ctx();
  // CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  // CK(OB_NOT_NULL(schema_guard = sql_ctx->schema_guard_));
  // CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id())));
  ObTableModifySpec* delete_spec =
      MY_SPEC.table_dml_infos_.at(0).index_infos_.at(0).se_subplans_.at(DELETE_OP).subplan_root_;
  CK(OB_NOT_NULL(delete_spec));
  OZ(ForeignKeyHandle::do_handle_old_row(*this, delete_spec->fk_args_, delete_row));
  OZ(MY_SPEC.shuffle_dml_row(ctx, *this, delete_row, DELETE_OP));
  return ret;
}

int ObMultiTableInsertUpOp::shuffle_final_insert_row(ObExecContext& ctx, const ObExprPtrIArray& insert_row)
{
  int ret = OB_SUCCESS;
  // ObSqlCtx *sql_ctx = NULL;
  // ObSchemaGetterGuard *schema_guard = NULL;
  // ObMultiTableInsertUpCtx *replace_ctx = NULL;
  // ObTaskExecutorCtx &task_ctx = ctx.get_task_exec_ctx();
  // ObMultiVersionSchemaService *schema_service = NULL;
  // CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  // CK(OB_NOT_NULL(schema_guard = sql_ctx->schema_guard_));
  // CK(OB_NOT_NULL(schema_service = task_ctx.schema_service_));
  // CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id())));
  ObTableModifySpec* insert_spec =
      MY_SPEC.table_dml_infos_.at(0).index_infos_.at(0).se_subplans_.at(INSERT_OP).subplan_root_;
  CK(OB_NOT_NULL(insert_spec));
  OZ(ForeignKeyHandle::do_handle_new_row(*this, insert_spec->fk_args_, insert_row));
  OZ(MY_SPEC.shuffle_dml_row(ctx, *this, insert_row, INSERT_OP));
  return ret;
}

int ObMultiTableInsertUpOp::shuffle_insert_row(
    const ObExprPtrIArray& insert_exprs, const ObChunkDatumStore::StoredRow& insert_row)
{
  int ret = OB_SUCCESS;
  ++affected_rows_;
  ++found_rows_;
  OZ(check_row_null(insert_exprs, MY_SPEC.column_infos_));
  // OZ(ForeignKeyHandle::do_handle_new_row(table_dml_infos_.at(0)
  //                                       .index_infos_.at(0)
  //                                       .dml_subplans_.at(INSERT_OP)
  //                                       .subplan_root_,
  //                                       insert_up_ctx, row));
  ObTableModifySpec* insert_spec =
      MY_SPEC.table_dml_infos_.at(0).index_infos_.at(0).se_subplans_.at(INSERT_OP).subplan_root_;
  CK(OB_NOT_NULL(insert_spec));
  OZ(ForeignKeyHandle::do_handle_new_row(*this, insert_spec->fk_args_, MY_SPEC.insert_row_));

  OZ(MY_SPEC.duplicate_key_checker_.insert_new_row(dupkey_checker_ctx_, insert_exprs, insert_row), insert_row);
  OZ(MY_SPEC.shuffle_dml_row(ctx_, *this, insert_exprs, INSERT_OP), insert_row);
  return ret;
}

int ObMultiTableInsertUpOp::shuffle_update_row(const ObChunkDatumStore::StoredRow& duplicated_row)
{
  int ret = OB_SUCCESS;
  bool is_row_changed = false;
  const ObTableDMLInfo* table_dml_info = NULL;
  const ObIArrayWrap<ObGlobalIndexDMLInfo>* global_index_infos = NULL;
  ObIArrayWrap<ObGlobalIndexDMLCtx>* global_index_ctxs = NULL;
  CK(MY_SPEC.table_dml_infos_.count() == 1);
  CK(table_dml_ctxs_.count() == 1);
  if (OB_SUCC(ret)) {
    table_dml_info = &(MY_SPEC.table_dml_infos_.at(0));
    global_index_infos = &(MY_SPEC.table_dml_infos_.at(0).index_infos_);
    global_index_ctxs = &(table_dml_ctxs_.at(0).index_ctxs_);
  }
  OZ(duplicated_row.to_expr(MY_SPEC.old_row_, eval_ctx_));
  LOG_DEBUG("shuffle_update_row",
      "old_row",
      ROWEXPR2STR(eval_ctx_, MY_SPEC.old_row_),
      "new_row",
      ROWEXPR2STR(eval_ctx_, MY_SPEC.new_row_));
  // send unchanged rows to sub plan for locking
  OZ(calc_update_rows(is_row_changed));
  if (OB_SUCC(ret)) {
    ++found_rows_;
  }
  int64_t flag = ObActionFlag::OP_MIN_OBJ;
  if (!is_row_changed) {
    flag = ObActionFlag::OP_LOCK_ROW;
    OZ(mark_lock_row_flag(flag));
  } else {
    OZ(mark_lock_row_flag(flag));
    ++changed_rows_;
    // The affected-rows value per row is 1 if the row is inserted as a new row,
    // 2 if an existing row is updated, and 0 if an existing row is set to its current values
    affected_rows_ += 2;
    // row is changed, so delete old row and insert new row in constraint infos

    ObChunkDatumStore::StoredRow* new_row = NULL;
    OZ(convert_exprs_to_stored_row(ctx_.get_allocator(), eval_ctx_, MY_SPEC.new_row_, new_row));
    CK(OB_NOT_NULL(new_row));
    OZ(MY_SPEC.duplicate_key_checker_.delete_old_row(dupkey_checker_ctx_, MY_SPEC.old_row_));
    OZ(MY_SPEC.duplicate_key_checker_.insert_new_row(dupkey_checker_ctx_, MY_SPEC.new_row_, *new_row));
  }
  if (is_row_changed) {
    const ObTableUpdateSpec* sub_update =
        dynamic_cast<const ObTableUpdateSpec*>(global_index_infos->at(0).se_subplans_.at(UPDATE_OP).subplan_root_);
    OZ(check_row_null(MY_SPEC.new_row_, MY_SPEC.column_infos_));
    CK(OB_NOT_NULL(sub_update));
    OZ(ForeignKeyHandle::do_handle(*this, sub_update->fk_args_, sub_update->old_row_, sub_update->new_row_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos->count(); ++i) {
    ObExpr* old_calc_part_id_expr = global_index_infos->at(i).calc_part_id_exprs_.at(0);
    ObExpr* new_calc_part_id_expr = global_index_infos->at(i).calc_part_id_exprs_.at(1);
    if (old_calc_part_id_expr != NULL && new_calc_part_id_expr != NULL) {
      const SeDMLSubPlanArray& se_subplans = global_index_infos->at(i).se_subplans_;
      ObIArray<int64_t>& part_ids = global_index_ctxs->at(i).partition_ids_;
      int64_t old_part_idx = OB_INVALID_INDEX;
      int64_t new_part_idx = OB_INVALID_INDEX;
      OZ(calc_part_id(old_calc_part_id_expr, part_ids, old_part_idx));
      if (OB_SUCC(ret) && 0 == i && global_index_infos->at(0).hint_part_ids_.count() > 0) {
        if (!has_exist_in_array(global_index_infos->at(0).hint_part_ids_, part_ids.at(old_part_idx))) {
          ret = OB_PARTITION_NOT_MATCH;
          LOG_WARN("Partition not match", K(ret), K(part_ids), K(old_part_idx));
        }
      }
      if (OB_SUCC(ret) && is_row_changed) {
        OZ(calc_part_id(new_calc_part_id_expr, part_ids, new_part_idx));
        if (OB_SUCC(ret) && 0 == i && global_index_infos->at(0).hint_part_ids_.count() > 0) {
          if (!has_exist_in_array(global_index_infos->at(0).hint_part_ids_, part_ids.at(new_part_idx))) {
            ret = OB_PARTITION_NOT_MATCH;
            LOG_WARN("Partition not match", K(ret), K(part_ids), K(old_part_idx));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (!is_row_changed || old_part_idx == new_part_idx) {
        OZ(multi_dml_plan_mgr_.add_part_row(0, i, old_part_idx, UPDATE_OP, se_subplans.at(UPDATE_OP).access_exprs_));
        LOG_DEBUG("shuffle update row", K(ret), K(part_ids), K(old_part_idx));
      } else {
        ObTableUpdateSpec* update_spec = static_cast<ObTableUpdateSpec*>(se_subplans.at(UPDATE_OP).subplan_root_);
        CK(OB_NOT_NULL(update_spec));
        OZ(multi_dml_plan_mgr_.add_part_row(0, i, old_part_idx, DELETE_OP, update_spec->old_row_));
        OZ(multi_dml_plan_mgr_.add_part_row(0, i, new_part_idx, INSERT_OP, update_spec->new_row_));
        LOG_DEBUG("shuffle update row across partition", K(ret), K(part_ids), K(old_part_idx), K(new_part_idx));
      }
    } else {
      LOG_DEBUG("calc part id expr is null", KP(old_calc_part_id_expr), KP(new_calc_part_id_expr));
    }
  }  // for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos->count(); ++i)
  return ret;
}

int ObMultiTableInsertUpOp::convert_exprs_to_stored_row(
    ObIAllocator& allocator, ObEvalCtx& eval_ctx, const ObExprPtrIArray& exprs, ObChunkDatumStore::StoredRow*& new_row)
{
  int ret = OB_SUCCESS;
  new_row = NULL;
  char* buf = NULL;
  int64_t row_size = 0;
  if (OB_FAIL(ObChunkDatumStore::row_copy_size(exprs, eval_ctx, row_size))) {
    LOG_WARN("failed to calc copy size", K(ret));
  } else {
    const int64_t STORE_ROW_HEADER_SIZE = sizeof(ObChunkDatumStore::StoredRow);
    int64_t buffer_len = STORE_ROW_HEADER_SIZE + row_size;
    if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buf failed", K(ret));
    } else if (OB_ISNULL(new_row = new (buf) ObChunkDatumStore::StoredRow())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to new row", K(ret));
    } else {
      int64_t pos = STORE_ROW_HEADER_SIZE;
      if (OB_FAIL(new_row->copy_datums(
              exprs, eval_ctx, buf + pos, buffer_len - STORE_ROW_HEADER_SIZE, row_size, 0 /*extra_size*/))) {
        LOG_WARN("failed to deep copy row", K(ret), K(buffer_len));
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
