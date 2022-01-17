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
#include "sql/engine/dml/ob_multi_table_replace.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
using namespace storage;
namespace sql {
class ObMultiTableReplace::ObMultiTableReplaceCtx : public ObTableReplace::ObTableReplaceCtx, public ObMultiDMLCtx {
  friend class ObMultiTableReplace;

public:
  explicit ObMultiTableReplaceCtx(ObExecContext& ctx)
      : ObTableReplaceCtx(ctx),
        ObMultiDMLCtx(ctx.get_allocator()),
        replace_row_store_(ctx.get_allocator(), ObModIds::OB_SQL_ROW_STORE, OB_SERVER_TENANT_ID, false),
        dupkey_checker_ctx_(ctx.get_allocator(), &expr_ctx_, mini_task_executor_, &replace_row_store_)
  {}
  ~ObMultiTableReplaceCtx()
  {}
  virtual void destroy() override
  {
    ObTableReplace::ObTableReplaceCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
    dupkey_checker_ctx_.destroy();
    replace_row_store_.reset();
  }

private:
  common::ObRowStore replace_row_store_;
  ObDupKeyCheckerCtx dupkey_checker_ctx_;
};

ObMultiTableReplace::ObMultiTableReplace(ObIAllocator& alloc) : ObTableReplace(alloc), ObMultiDMLInfo(alloc)
{}

ObMultiTableReplace::~ObMultiTableReplace()
{}

int ObMultiTableReplace::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObMultiTableReplaceCtx* replace_ctx = NULL;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table replace context from exec_ctx failed", K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(load_replace_row(ctx, replace_ctx->replace_row_store_))) {
    LOG_WARN("load replace row failed", K(ret));
  } else if (OB_FAIL(duplicate_key_checker_.build_duplicate_rowkey_map(ctx, replace_ctx->dupkey_checker_ctx_))) {
    LOG_WARN("build duplicate rowkey map failed", K(ret));
  } else if (OB_FAIL(shuffle_replace_row(ctx, got_row))) {
    LOG_WARN("shuffle replace row failed", K(ret));
  } else if (!got_row) {
    // do nothing
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx, table_dml_infos_, replace_ctx->table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(replace_ctx->multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi table dml task failed", K(ret));
  } else {
    const ObMiniJob& subplan_job = replace_ctx->multi_dml_plan_mgr_.get_subplan_job();
    ObIArray<ObTaskInfo*>& task_info_list = replace_ctx->multi_dml_plan_mgr_.get_mini_task_infos();
    bool table_need_first = replace_ctx->multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObIsMultiDMLGuard guard(*ctx.get_physical_plan_ctx());
    if (OB_FAIL(replace_ctx->mini_task_executor_.execute(ctx, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
    UNUSED(result);
  }
  return ret;
}

int ObMultiTableReplace::inner_close(ObExecContext& ctx) const
{
  int wait_ret =
      wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableReplace::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiTableReplace::load_replace_row(ObExecContext& ctx, ObRowStore& row_store) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* replace_row = NULL;
  ObMultiTableReplaceCtx* replace_ctx = NULL;
  CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id())));
  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_row(ctx, replace_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child op failed", K(ret));
      }
    } else if (OB_FAIL(row_store.add_row(*replace_row))) {
      LOG_WARN("add replace row to row store failed", K(ret), KPC(replace_row));
    } else {
      ++replace_ctx->record_;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObMultiTableReplace::shuffle_replace_row(ObExecContext& ctx, bool& got_row) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObMultiTableReplaceCtx* replace_ctx = NULL;
  ObNewRow* replace_row = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObSqlCtx* sql_ctx = NULL;
  got_row = false;
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)));
  CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id())));
  CK(OB_NOT_NULL(task_ctx = ctx.get_task_executor_ctx()));
  CK(OB_NOT_NULL(get_phy_plan()));
  CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  if (OB_SUCC(ret)) {
    ObSEArray<ObConstraintValue, 2> constraint_values;
    ObRowStore::Iterator replace_row_iter = replace_ctx->replace_row_store_.begin();
    while (OB_SUCC(ret) && OB_SUCC(replace_row_iter.get_next_row(replace_row, NULL))) {
      got_row = true;
      ++replace_ctx->affected_rows_;
      constraint_values.reuse();
      OZ(duplicate_key_checker_.check_duplicate_rowkey(
          replace_ctx->dupkey_checker_ctx_, *replace_row, constraint_values));
      for (int64_t i = 0; OB_SUCC(ret) && i < constraint_values.count(); ++i) {
        // delete duplicated row
        const ObNewRow* delete_row = constraint_values.at(i).current_row_;
        bool same_row = false;
        OZ(ForeignKeyHandle::do_handle_old_row(
            table_dml_infos_.at(0).index_infos_.at(0).dml_subplans_.at(DELETE_OP).subplan_root_,
            *replace_ctx,
            *delete_row));
        OZ(duplicate_key_checker_.delete_old_row(replace_ctx->dupkey_checker_ctx_, *delete_row));
        OZ(shuffle_dml_row(ctx, *sql_ctx->schema_guard_, *replace_ctx, *delete_row, DELETE_OP), *delete_row);
        if (OB_SUCC(ret) && OB_LIKELY(only_one_unique_key_)) {
          OZ(check_values(*replace_row, *delete_row, same_row));
        }
        if (OB_SUCC(ret) && !same_row) {
          ++replace_ctx->delete_count_;
          ++replace_ctx->affected_rows_;
        }
      }
      OZ(check_row_null(ctx, *replace_row, column_infos_), *replace_row);
      OZ(ForeignKeyHandle::do_handle_new_row(
          table_dml_infos_.at(0).index_infos_.at(0).dml_subplans_.at(INSERT_OP).subplan_root_,
          *replace_ctx,
          *replace_row));
      // add new row to constraint info map
      OZ(shuffle_dml_row(ctx, *sql_ctx->schema_guard_, *replace_ctx, *replace_row, INSERT_OP), *replace_row);
      OZ(duplicate_key_checker_.insert_new_row(replace_ctx->dupkey_checker_ctx_, *replace_row), *replace_row);
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("get next row from expr values failed", K(ret));
    }
    if (replace_ctx->dupkey_checker_ctx_.update_incremental_row()) {
      replace_ctx->release_multi_part_shuffle_info();
      OZ(duplicate_key_checker_.shuffle_final_data(ctx, replace_ctx->dupkey_checker_ctx_, *this));
    }
  }
  if (OB_SUCC(ret)) {
    plan_ctx->set_affected_rows(replace_ctx->affected_rows_);
    plan_ctx->set_row_matched_count(replace_ctx->record_);
    plan_ctx->set_row_duplicated_count(replace_ctx->delete_count_);
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

int ObMultiTableReplace::shuffle_final_delete_row(ObExecContext& ctx, const ObNewRow& delete_row) const
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObMultiTableReplaceCtx* replace_ctx = NULL;
  ObTaskExecutorCtx& task_ctx = ctx.get_task_exec_ctx();
  ObMultiVersionSchemaService* schema_service = NULL;
  CK(OB_NOT_NULL(schema_service = task_ctx.schema_service_));
  CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id())));
  OZ(ForeignKeyHandle::do_handle_old_row(
      table_dml_infos_.at(0).index_infos_.at(0).dml_subplans_.at(DELETE_OP).subplan_root_, *replace_ctx, delete_row));
  OZ(schema_service->get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), schema_guard));
  OZ(shuffle_dml_row(ctx, schema_guard, *replace_ctx, delete_row, DELETE_OP));
  return ret;
}

int ObMultiTableReplace::shuffle_final_insert_row(ObExecContext& ctx, const ObNewRow& insert_row) const
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObMultiTableReplaceCtx* replace_ctx = NULL;
  ObTaskExecutorCtx& task_ctx = ctx.get_task_exec_ctx();
  ObMultiVersionSchemaService* schema_service = NULL;
  CK(OB_NOT_NULL(schema_service = task_ctx.schema_service_));
  CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id())));
  OZ(ForeignKeyHandle::do_handle_new_row(
      table_dml_infos_.at(0).index_infos_.at(0).dml_subplans_.at(INSERT_OP).subplan_root_, *replace_ctx, insert_row));
  OZ(schema_service->get_tenant_schema_guard(ctx.get_my_session()->get_effective_tenant_id(), schema_guard));
  OZ(shuffle_dml_row(ctx, schema_guard, *replace_ctx, insert_row, INSERT_OP));
  return ret;
}

int ObMultiTableReplace::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObMultiTableReplaceCtx* replace_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiTableReplaceCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  } else {
    replace_ctx = static_cast<ObMultiTableReplaceCtx*>(op_ctx);
    if (OB_FAIL(replace_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_))) {
      LOG_WARN("init multi dml ctx failed", K(ret));
    } else if (OB_FAIL(duplicate_key_checker_.init_checker_ctx(replace_ctx->dupkey_checker_ctx_))) {
      LOG_WARN("init duplicate key checker context failed", K(ret));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
