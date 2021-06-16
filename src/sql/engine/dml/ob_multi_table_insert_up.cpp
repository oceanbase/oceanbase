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
#include "sql/engine/dml/ob_table_update.h"
#include "sql/engine/dml/ob_multi_table_insert_up.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#define GET_VAL_BY_VERSION(a, b) (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200 ? a : b)
namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
namespace sql {
class ObMultiTableInsertUp::ObMultiTableInsertUpCtx : public ObTableInsertUp::ObTableInsertUpCtx, public ObMultiDMLCtx {
  friend class ObMultiTableInsertUp;

public:
  explicit ObMultiTableInsertUpCtx(ObExecContext& ctx)
      : ObTableInsertUpCtx(ctx),
        ObMultiDMLCtx(ctx.get_allocator()),
        replace_row_store_(ctx.get_allocator(), ObModIds::OB_SQL_ROW_STORE, OB_SERVER_TENANT_ID, false),
        dupkey_checker_ctx_(ctx.get_allocator(), &expr_ctx_, mini_task_executor_, &replace_row_store_),
        changed_rows_(0),
        affected_rows_(0),
        duplicate_rows_(0)
  {}
  ~ObMultiTableInsertUpCtx()
  {}
  virtual void destroy() override
  {
    ObTableInsertUp::ObTableInsertUpCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
    dupkey_checker_ctx_.destroy();
    replace_row_store_.reset();
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
  common::ObRowStore replace_row_store_;
  ObDupKeyCheckerCtx dupkey_checker_ctx_;
  int64_t changed_rows_;
  int64_t affected_rows_;
  int64_t duplicate_rows_;
};

ObMultiTableInsertUp::ObMultiTableInsertUp(ObIAllocator& alloc) : ObTableInsertUp(alloc), ObMultiDMLInfo(alloc)
{}

ObMultiTableInsertUp::~ObMultiTableInsertUp()
{}

int ObMultiTableInsertUp::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObMultiTableInsertUpCtx* insert_up_ctx = NULL;
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(ctx)));
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(insert_up_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table replace context from exec_ctx failed", K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(load_insert_up_row(ctx, insert_up_ctx->replace_row_store_))) {
    LOG_WARN("load insert update row failed", K(ret));
  } else if (OB_FAIL(duplicate_key_checker_.build_duplicate_rowkey_map(ctx, insert_up_ctx->dupkey_checker_ctx_))) {
    LOG_WARN("build duplicate rowkey map failed", K(ret));
  } else if (OB_FAIL(shuffle_insert_up_row(ctx, got_row))) {
    LOG_WARN("shuffle replace row failed", K(ret));
  } else if (!got_row) {
    // do nothing
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx, table_dml_infos_, insert_up_ctx->table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(insert_up_ctx->multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi table dml task failed", K(ret));
  } else {
    const ObMiniJob& subplan_job = insert_up_ctx->multi_dml_plan_mgr_.get_subplan_job();
    ObIArray<ObTaskInfo*>& task_info_list = insert_up_ctx->multi_dml_plan_mgr_.get_mini_task_infos();
    bool table_need_first = insert_up_ctx->multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObIsMultiDMLGuard guard(*ctx.get_physical_plan_ctx());
    if (OB_FAIL(
            insert_up_ctx->mini_task_executor_.execute(ctx, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    } else {
      plan_ctx->add_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS
                                      ? insert_up_ctx->affected_rows_ + insert_up_ctx->duplicate_rows_
                                      : insert_up_ctx->affected_rows_);
      plan_ctx->set_row_matched_count(insert_up_ctx->found_rows_);
      plan_ctx->add_row_duplicated_count(insert_up_ctx->changed_rows_);
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

int ObMultiTableInsertUp::inner_close(ObExecContext& ctx) const
{
  int wait_ret =
      wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableInsertUp::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiTableInsertUp::load_insert_up_row(ObExecContext& ctx, ObRowStore& row_store) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* insert_up_row = NULL;
  ObMultiTableInsertUpCtx* insert_up_ctx = NULL;
  if (OB_ISNULL(insert_up_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table insert update context from exec_ctx failed", K(get_id()));
  } else {
    do {
      if (OB_FAIL(child_op_->get_next_row(ctx, insert_up_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from child op failed", K(ret));
        }
      } else if (OB_FAIL(calc_insert_row(ctx, insert_up_ctx->expr_ctx_, insert_up_row))) {
        LOG_WARN("calc insert row failed", K(ret));
      } else if (OB_FAIL(row_store.add_row(GET_VAL_BY_VERSION(insert_up_ctx->get_cur_row(), *insert_up_row), false))) {
        LOG_WARN("add replace row to row store failed", K(ret), K(insert_up_ctx->get_cur_row()), KPC(insert_up_row));
      }
    } while (OB_SUCC(ret));
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMultiTableInsertUp::shuffle_insert_up_row(ObExecContext& ctx, bool& got_row) const
{
  int ret = OB_SUCCESS;
  ObNewRow* insert_up_row = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObMultiTableInsertUpCtx* insert_up_ctx = NULL;
  ObSqlCtx* sql_ctx = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  got_row = false;
  if (OB_ISNULL(insert_up_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table insert context from exec_ctx failed", K(get_id()));
  } else if (OB_ISNULL(task_ctx = ctx.get_task_executor_ctx()) || OB_ISNULL(sql_ctx = ctx.get_sql_ctx()) ||
             OB_ISNULL(schema_guard = sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task executor ctx is null");
  } else {
    // insert into on duplicate key update only randomly updates one duplicated row at a time
    ObSEArray<ObConstraintValue, 1> constraint_values;
    ObRowStore::Iterator replace_row_iter = insert_up_ctx->replace_row_store_.begin();
    while (OB_SUCC(ret) && OB_SUCC(replace_row_iter.get_next_row(insert_up_row, NULL))) {
      got_row = true;
      constraint_values.reuse();
      ObNewRow* projected_row = NULL;
      if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
        insert_up_ctx->get_cur_row().cells_ = insert_up_row->cells_;
        insert_up_ctx->get_cur_row().count_ = insert_up_row->count_;
        const ObNewRow* cur_row = &insert_up_ctx->get_cur_row();
        OZ(ob_create_row(ctx.get_allocator(), column_ids_.count(), projected_row));
        OZ(ob_write_row_by_projector(ctx.get_allocator(), *cur_row, *projected_row));
      }
      OZ(duplicate_key_checker_.check_duplicate_rowkey(
          insert_up_ctx->dupkey_checker_ctx_, GET_VAL_BY_VERSION(*projected_row, *insert_up_row), constraint_values));
      if (OB_SUCC(ret)) {
        if (OB_LIKELY(constraint_values.empty())) {
          // execute insert action
          OZ(shuffle_insert_row(
              ctx, *schema_guard, *insert_up_ctx, GET_VAL_BY_VERSION(*projected_row, *insert_up_row)));
        } else {
          // execute update action
          const ObNewRow* duplicated_row = constraint_values.at(0).current_row_;
          OZ(shuffle_update_row(ctx,
              *schema_guard,
              *insert_up_ctx,
              GET_VAL_BY_VERSION(insert_up_ctx->get_cur_row(), *insert_up_row),
              *duplicated_row));
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
  if (insert_up_ctx->dupkey_checker_ctx_.update_incremental_row()) {
    insert_up_ctx->release_multi_part_shuffle_info();
    OZ(duplicate_key_checker_.shuffle_final_data(ctx, insert_up_ctx->dupkey_checker_ctx_, *this));
  }
  return ret;
}

int ObMultiTableInsertUp::shuffle_final_delete_row(ObExecContext& ctx, const ObNewRow& delete_row) const
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = NULL;
  ObMultiTableInsertUpCtx* replace_ctx = NULL;
  ObSqlCtx* sql_ctx = NULL;
  ObTaskExecutorCtx& task_ctx = ctx.get_task_exec_ctx();
  CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  CK(OB_NOT_NULL(schema_guard = sql_ctx->schema_guard_));
  CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id())));
  OZ(ForeignKeyHandle::do_handle_old_row(
      table_dml_infos_.at(0).index_infos_.at(0).dml_subplans_.at(DELETE_OP).subplan_root_, *replace_ctx, delete_row));
  OZ(shuffle_dml_row(ctx, *schema_guard, *replace_ctx, delete_row, DELETE_OP));
  return ret;
}

int ObMultiTableInsertUp::shuffle_final_insert_row(ObExecContext& ctx, const ObNewRow& insert_row) const
{
  int ret = OB_SUCCESS;
  ObSqlCtx* sql_ctx = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  ObMultiTableInsertUpCtx* replace_ctx = NULL;
  ObTaskExecutorCtx& task_ctx = ctx.get_task_exec_ctx();
  ObMultiVersionSchemaService* schema_service = NULL;
  CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  CK(OB_NOT_NULL(schema_guard = sql_ctx->schema_guard_));
  CK(OB_NOT_NULL(schema_service = task_ctx.schema_service_));
  CK(OB_NOT_NULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id())));
  OZ(ForeignKeyHandle::do_handle_new_row(
      table_dml_infos_.at(0).index_infos_.at(0).dml_subplans_.at(INSERT_OP).subplan_root_, *replace_ctx, insert_row));
  OZ(shuffle_dml_row(ctx, *schema_guard, *replace_ctx, insert_row, INSERT_OP));
  return ret;
}

int ObMultiTableInsertUp::shuffle_insert_row(
    ObExecContext& ctx, ObPartMgr& part_mgr, ObMultiTableInsertUpCtx& insert_up_ctx, const ObNewRow& row) const
{
  int ret = OB_SUCCESS;
  ++insert_up_ctx.affected_rows_;
  ++insert_up_ctx.found_rows_;
  OZ(check_row_null(ctx, row, column_infos_), row);
  OZ(ForeignKeyHandle::do_handle_new_row(
      table_dml_infos_.at(0).index_infos_.at(0).dml_subplans_.at(INSERT_OP).subplan_root_, insert_up_ctx, row));
  OZ(duplicate_key_checker_.insert_new_row(insert_up_ctx.dupkey_checker_ctx_, row));
  OZ(shuffle_dml_row(ctx, part_mgr, insert_up_ctx, row, INSERT_OP));
  return ret;
}

int ObMultiTableInsertUp::shuffle_update_row(ObExecContext& ctx, ObPartMgr& part_mgr,
    ObMultiTableInsertUpCtx& insert_up_ctx, const ObNewRow& insert_row, const ObNewRow& duplicate_row) const
{
  int ret = OB_SUCCESS;
  bool is_row_changed = false;
  const ObTableDMLInfo* table_dml_info = NULL;
  const ObIArrayWrap<ObGlobalIndexDMLInfo>* global_index_infos = NULL;
  ObIArrayWrap<ObGlobalIndexDMLCtx>* global_index_ctxs = NULL;
  ObNewRow tmp_old_row;
  ObNewRow tmp_new_row;
  ObNewRow* old_row = NULL;
  ObNewRow* new_row = NULL;
  CK(table_dml_infos_.count() == 1);
  CK(insert_up_ctx.table_dml_ctxs_.count() == 1);
  if (OB_SUCC(ret)) {
    table_dml_info = &(table_dml_infos_.at(0));
    global_index_infos = &(table_dml_infos_.at(0).index_infos_);
    global_index_ctxs = &(insert_up_ctx.table_dml_ctxs_.at(0).index_ctxs_);
  }
  // Execute locally, if the row has not changed, the data of this row will not be iterated to the storage layer,
  // but the data of this row will be locked in the SQL layer
  // But in multi table dml, even if the new row and the old row have not changed,
  // because the data may not be local, RPC is required
  // So even if the data has not changed, it is necessary to iterate the data to the sub-plan,
  // and the sub-plan determines whether to iterate to the storage layer and lock it
  OZ(calc_update_rows(ctx, insert_row, duplicate_row, is_row_changed));
  if (OB_SUCC(ret)) {
    insert_up_ctx.inc_found_rows();
    table_dml_info->assign_columns_.project_old_and_new_row(insert_up_ctx.get_update_row(), tmp_old_row, tmp_new_row);
  }
  if (!is_row_changed) {
    ++insert_up_ctx.duplicate_rows_;
    OZ(ObTableModify::mark_lock_row_flag(table_dml_info->assign_columns_.assign_columns_, tmp_new_row));
  } else {
    insert_up_ctx.inc_changed_rows();
    // The affected-rows value per row is 1 if the row is inserted as a new row,
    // 2 if an existing row is updated, and 0 if an existing row is set to its current values
    insert_up_ctx.affected_rows_ += 2;
    // row is changed, so delete old row and insert new row in constraint infos
    OZ(ob_create_row(ctx.get_allocator(), tmp_old_row.get_count(), old_row));
    OZ(ob_create_row(ctx.get_allocator(), tmp_new_row.get_count(), new_row));
    OZ(ob_write_row_by_projector(ctx.get_allocator(), tmp_old_row, *old_row));
    OZ(ob_write_row_by_projector(ctx.get_allocator(), tmp_new_row, *new_row));
    OZ(duplicate_key_checker_.delete_old_row(insert_up_ctx.dupkey_checker_ctx_, *old_row));
    OZ(duplicate_key_checker_.insert_new_row(insert_up_ctx.dupkey_checker_ctx_, *new_row));
  }
  if (is_row_changed) {
    CK(OB_NOT_NULL(old_row));
    CK(OB_NOT_NULL(new_row));
    OZ(check_row_null(ctx, *new_row, column_infos_), *new_row);
    OZ(ForeignKeyHandle::do_handle(
        global_index_infos->at(0).dml_subplans_.at(UPDATE_OP).subplan_root_, insert_up_ctx, *old_row, *new_row));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos->count(); ++i) {
    const ObTableLocation* old_table_location = global_index_infos->at(i).table_locs_.at(1);
    const ObTableLocation* new_table_location = global_index_infos->at(i).table_locs_.at(2);
    if (old_table_location != NULL && new_table_location != NULL) {
      const DMLSubPlanArray& dml_subplans = global_index_infos->at(i).dml_subplans_;
      ObIArray<int64_t>& part_ids = global_index_ctxs->at(i).partition_ids_;
      int64_t old_part_idx = OB_INVALID_INDEX;
      int64_t new_part_idx = OB_INVALID_INDEX;
      OZ(old_table_location->calculate_partition_ids_by_row(
          ctx, &part_mgr, insert_up_ctx.get_update_row(), part_ids, old_part_idx));
      if (OB_SUCC(ret) && 0 == i) {
        OZ(old_table_location->deal_dml_partition_selection(part_ids.at(old_part_idx)), part_ids, old_part_idx);
      }
      if (OB_SUCC(ret) && is_row_changed) {
        OZ(new_table_location->calculate_partition_ids_by_row(
            ctx, &part_mgr, insert_up_ctx.get_update_row(), part_ids, new_part_idx));
        if (OB_SUCC(ret) && 0 == i) {
          OZ(new_table_location->deal_dml_partition_selection(part_ids.at(new_part_idx)), part_ids, new_part_idx);
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (!is_row_changed || old_part_idx == new_part_idx) {
        ObNewRow cur_row;
        cur_row.cells_ = insert_up_ctx.get_update_row().cells_;
        cur_row.count_ = insert_up_ctx.get_update_row().count_;
        cur_row.projector_ = dml_subplans.at(UPDATE_OP).value_projector_;
        cur_row.projector_size_ = dml_subplans.at(UPDATE_OP).value_projector_size_;
        OZ(insert_up_ctx.multi_dml_plan_mgr_.add_part_row(0, i, old_part_idx, UPDATE_OP, cur_row));
        LOG_DEBUG("shuffle update row", K(ret), K(part_ids), K(old_part_idx), K(dml_subplans), K(cur_row));
      } else {
        ObNewRow old_index_row = *old_row;
        old_index_row.projector_ = dml_subplans.at(DELETE_OP).value_projector_;
        old_index_row.projector_size_ = dml_subplans.at(DELETE_OP).value_projector_size_;
        ObNewRow new_index_row = *new_row;
        new_index_row.projector_ = dml_subplans.at(INSERT_OP).value_projector_;
        new_index_row.projector_size_ = dml_subplans.at(INSERT_OP).value_projector_size_;
        OZ(insert_up_ctx.multi_dml_plan_mgr_.add_part_row(0, i, old_part_idx, DELETE_OP, old_index_row));
        OZ(insert_up_ctx.multi_dml_plan_mgr_.add_part_row(0, i, new_part_idx, INSERT_OP, new_index_row));
        LOG_DEBUG("shuffle update row across partition",
            K(ret),
            K(part_ids),
            K(old_part_idx),
            K(new_part_idx),
            K(dml_subplans),
            K(old_index_row),
            K(new_index_row));
      }
    }
  }  // for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos->count(); ++i)
  return ret;
}

int ObMultiTableInsertUp::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObMultiTableInsertUpCtx* insert_up_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiTableInsertUpCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  } else {
    insert_up_ctx = static_cast<ObMultiTableInsertUpCtx*>(op_ctx);
    int64_t update_row_column_count = scan_column_ids_.count() + assignment_infos_.count();
    if (OB_FAIL(insert_up_ctx->init(
            ctx, old_projector_size_, primary_key_ids_.count(), update_row_column_count, column_ids_.count()))) {
      LOG_WARN("fail to init update ctx", K(ret));
    } else if (OB_FAIL(insert_up_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_))) {
      LOG_WARN("init multi dml ctx failed", K(ret));
    } else if (OB_FAIL(duplicate_key_checker_.init_checker_ctx(insert_up_ctx->dupkey_checker_ctx_))) {
      LOG_WARN("init duplicate key checker context failed", K(ret));
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
