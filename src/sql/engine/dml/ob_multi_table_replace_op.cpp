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
#include "lib/allocator/ob_allocator.h"
#include "sql/engine/dml/ob_multi_table_replace_op.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
using namespace storage;
namespace sql {
OB_SERIALIZE_MEMBER((ObMultiTableReplaceSpec, ObTableReplaceSpec));

int ObMultiTableReplaceOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  if (OB_FAIL(init_multi_dml_ctx(
          ctx_, MY_SPEC.table_dml_infos_, MY_SPEC.get_phy_plan(), NULL /*subplan_root*/, MY_SPEC.se_subplan_root_))) {
    LOG_WARN("init multi dml ctx failed", K(ret));
  } else if (OB_FAIL(MY_SPEC.duplicate_key_checker_.init_checker_ctx(dupkey_checker_ctx_))) {
    LOG_WARN("init duplicate key checker context failed", K(ret));
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_FAIL(replace_row_store_.init(UINT64_MAX,
                 my_session->get_effective_tenant_id(),
                 ObCtxIds::DEFAULT_CTX_ID,
                 ObModIds::OB_SQL_CHUNK_ROW_STORE,
                 false /*enable_dump*/))) {
    LOG_WARN("fail to init datum store", K(ret));
  } else if (OB_FAIL(load_replace_row(replace_row_store_))) {
    LOG_WARN("load replace row failed", K(ret));
  } else if (OB_FAIL(MY_SPEC.duplicate_key_checker_.build_duplicate_rowkey_map(ctx_, dupkey_checker_ctx_))) {
    LOG_WARN("build duplicate rowkey map failed", K(ret));
  } else if (OB_FAIL(shuffle_replace_row(got_row))) {
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
    }
  }
  return ret;
}

int ObMultiTableReplaceOp::inner_close()
{
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int wait_ret = mini_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp());
  int close_ret = ObTableModifyOp::inner_close();
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiTableReplaceOp::load_replace_row(ObChunkDatumStore& datum_store)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(inner_get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child op failed", K(ret));
      }
    } else if (OB_FAIL(datum_store.add_row(MY_SPEC.output_, &eval_ctx_))) {
      LOG_WARN("add replace row to row store failed", K(ret));
    } else {
      LOG_DEBUG("multi table replace output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
      ++record_;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMultiTableReplaceOp::shuffle_replace_row(bool& got_row)
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
    ObSEArray<ObConstraintValue, 2> constraint_values;
    ObChunkDatumStore::Iterator replace_row_iter;
    OZ(replace_row_store_.begin(replace_row_iter));
    const ObChunkDatumStore::StoredRow* replace_row = NULL;
    while (
        OB_SUCC(ret) && OB_SUCC(replace_row_iter.get_next_row(MY_SPEC.table_column_exprs_, eval_ctx_, &replace_row))) {
      got_row = true;
      ++affected_rows_;
      constraint_values.reuse();
      // OZ(replace_row->to_expr(MY_SPEC.table_column_exprs_, eval_ctx_));
      LOG_DEBUG("insert row",
          "row",
          ROWEXPR2STR(eval_ctx_, MY_SPEC.table_column_exprs_),
          "output",
          ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
      OZ(MY_SPEC.duplicate_key_checker_.check_duplicate_rowkey(
          dupkey_checker_ctx_, MY_SPEC.table_column_exprs_, constraint_values));
      for (int64_t i = 0; OB_SUCC(ret) && i < constraint_values.count(); ++i) {
        // delete duplicated row
        const ObChunkDatumStore::StoredRow* delete_row = constraint_values.at(i).current_datum_row_;
        bool same_row = false;
        CK(OB_NOT_NULL(delete_row));
        OZ(delete_row->to_expr(MY_SPEC.table_column_exprs_, eval_ctx_));
        ObTableModifySpec* delete_spec =
            MY_SPEC.table_dml_infos_.at(0).index_infos_.at(0).se_subplans_.at(DELETE_OP).subplan_root_;
        CK(OB_NOT_NULL(delete_spec));
        OZ(ForeignKeyHandle::do_handle_old_row(*this, delete_spec->fk_args_, MY_SPEC.table_column_exprs_));
        LOG_DEBUG("delete row",
            "row",
            ROWEXPR2STR(eval_ctx_, MY_SPEC.table_column_exprs_),
            "output",
            ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
        OZ(MY_SPEC.duplicate_key_checker_.delete_old_row(dupkey_checker_ctx_, MY_SPEC.table_column_exprs_));
        // OZ(delete_row->to_expr(MY_SPEC.output_, eval_ctx_));
        OZ(MY_SPEC.shuffle_dml_row(ctx_, *this, MY_SPEC.table_column_exprs_, DELETE_OP), *delete_row);
        OZ(replace_row->to_expr(MY_SPEC.output_, eval_ctx_));
        if (OB_SUCC(ret) && OB_LIKELY(MY_SPEC.only_one_unique_key_)) {
          OZ(check_values(same_row));
        }
        if (OB_SUCC(ret) && !same_row) {
          ++delete_count_;
          ++affected_rows_;
        }
      }
      OZ(replace_row->to_expr(MY_SPEC.table_column_exprs_, eval_ctx_));
      OZ(check_row_null(MY_SPEC.table_column_exprs_, MY_SPEC.column_infos_));
      ObTableModifySpec* insert_spec =
          MY_SPEC.table_dml_infos_.at(0).index_infos_.at(0).se_subplans_.at(INSERT_OP).subplan_root_;
      CK(OB_NOT_NULL(insert_spec));
      OZ(ForeignKeyHandle::do_handle_new_row(*this, insert_spec->fk_args_, MY_SPEC.table_column_exprs_));
      // add new row to constraint info map
      OZ(MY_SPEC.shuffle_dml_row(ctx_, *this, MY_SPEC.table_column_exprs_, INSERT_OP), *replace_row);
      // OZ(replace_row->to_expr(MY_SPEC.table_column_exprs_, eval_ctx_));
      OZ(MY_SPEC.duplicate_key_checker_.insert_new_row(dupkey_checker_ctx_, MY_SPEC.table_column_exprs_, *replace_row),
          *replace_row);
      LOG_DEBUG("insert row",
          "row",
          ROWEXPR2STR(eval_ctx_, MY_SPEC.table_column_exprs_),
          "output",
          ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
    }  // while row store end
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
  }
  if (OB_SUCC(ret)) {
    plan_ctx->set_affected_rows(affected_rows_);
    plan_ctx->set_row_matched_count(record_);
    plan_ctx->set_row_duplicated_count(delete_count_);
    if (OB_FAIL(plan_ctx->sync_last_value_local())) {
      // sync last user specified value after iter ends(compatible with MySQL)
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

int ObMultiTableReplaceOp::shuffle_final_delete_row(ObExecContext& ctx, const ObExprPtrIArray& delete_row)
{
  int ret = OB_SUCCESS;
  // ObSchemaGetterGuard schema_guard;
  // ObTaskExecutorCtx &task_ctx = ctx.get_task_exec_ctx();
  // ObMultiVersionSchemaService *schema_service = NULL;
  // CK(OB_NOT_NULL(schema_service = task_ctx.schema_service_));
  ObTableModifySpec* delete_spec =
      MY_SPEC.table_dml_infos_.at(0).index_infos_.at(0).se_subplans_.at(DELETE_OP).subplan_root_;
  CK(OB_NOT_NULL(delete_spec));
  OZ(ForeignKeyHandle::do_handle_old_row(*this, delete_spec->fk_args_, delete_row));
  // OZ(schema_service->get_tenant_schema_guard(
  //        ctx.get_my_session()->get_effective_tenant_id(), schema_guard));
  OZ(MY_SPEC.shuffle_dml_row(ctx, *this, delete_row, DELETE_OP));
  return ret;
}

int ObMultiTableReplaceOp::shuffle_final_insert_row(ObExecContext& ctx, const ObExprPtrIArray& insert_row)
{
  int ret = OB_SUCCESS;
  // ObSchemaGetterGuard schema_guard;
  // ObMultiTableReplaceCtx *replace_ctx = NULL;
  // ObTaskExecutorCtx &task_ctx = ctx.get_task_exec_ctx();
  // ObMultiVersionSchemaService *schema_service = NULL;
  // CK(OB_NOT_NULL(schema_service = task_ctx.schema_service_));
  // CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id())));
  ObTableModifySpec* insert_spec =
      MY_SPEC.table_dml_infos_.at(0).index_infos_.at(0).se_subplans_.at(INSERT_OP).subplan_root_;
  CK(OB_NOT_NULL(insert_spec));
  OZ(ForeignKeyHandle::do_handle_new_row(*this, insert_spec->fk_args_, insert_row));
  // OZ(schema_service->get_tenant_schema_guard(
  //        ctx.get_my_session()->get_effective_tenant_id(), schema_guard));
  OZ(MY_SPEC.shuffle_dml_row(ctx, *this, insert_row, INSERT_OP));
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
