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
#include "sql/engine/dml/ob_multi_part_insert.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
namespace sql {
class ObMultiPartInsert::ObMultiPartInsertCtx : public ObTableInsert::ObTableInsertCtx, public ObMultiDMLCtx {
  friend class ObMultiPartInsert;

public:
  explicit ObMultiPartInsertCtx(ObExecContext& ctx) : ObTableInsertCtx(ctx), ObMultiDMLCtx(ctx.get_allocator())
  {}
  ~ObMultiPartInsertCtx()
  {}
  virtual void destroy() override
  {
    ObTableInsert::ObTableInsertCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
};

ObMultiPartInsert::ObMultiPartInsert(ObIAllocator& alloc)
    : ObTableInsert(alloc),
      ObMultiDMLInfo(alloc),
      insert_row_exprs_(),
      insert_projector_(NULL),
      insert_projector_size_(0)

{}

ObMultiPartInsert::~ObMultiPartInsert()
{}

void ObMultiPartInsert::reset()
{
  insert_row_exprs_.reset();
  insert_projector_ = NULL;
  insert_projector_size_ = 0;
  ObTableInsert::reset();
}

void ObMultiPartInsert::reuse()
{
  insert_row_exprs_.reset();
  insert_projector_ = NULL;
  insert_projector_size_ = 0;
  ObTableInsert::reuse();
}

int ObMultiPartInsert::set_insert_row_exprs()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_exprs_.move(insert_row_exprs_))) {
    LOG_WARN("failed to set insert row exprs", K(ret));
  } else {
    insert_projector_ = projector_;
    insert_projector_size_ = projector_size_;
    projector_ = NULL;
    projector_size_ = 0;
  }
  return ret;
}

int ObMultiPartInsert::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObMultiPartInsertCtx* insert_ctx = NULL;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartInsertCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table insert context from exec_ctx failed", K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(shuffle_insert_row(ctx, got_row))) {
    LOG_WARN("shuffle insert row failed", K(ret));
  } else if (!got_row) {
    // do nothing
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx, table_dml_infos_, insert_ctx->table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(insert_ctx->multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi partition dml task failed", K(ret));
  } else {
    const ObMiniJob& subplan_job = insert_ctx->multi_dml_plan_mgr_.get_subplan_job();
    ObIArray<ObTaskInfo*>& task_info_list = insert_ctx->multi_dml_plan_mgr_.get_mini_task_infos();
    bool table_need_first = insert_ctx->multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    ObIsMultiDMLGuard guard(*ctx.get_physical_plan_ctx());
    if (OB_FAIL(insert_ctx->mini_task_executor_.execute(ctx, subplan_job, task_info_list, table_need_first, result))) {
      LOG_WARN("execute multi table dml task failed", K(ret));
    }
    UNUSED(result);
  }
  if (OB_SUCC(ret) && is_returning()) {
    insert_ctx->returning_row_iterator_ = insert_ctx->returning_row_store_.begin();
  }
  return ret;
}

int ObMultiPartInsert::inner_close(ObExecContext& ctx) const
{
  int wait_ret = wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiPartInsertCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableInsert::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiPartInsert::shuffle_insert_row(ObExecContext& ctx, bool& got_row) const
{
  int ret = OB_SUCCESS;
  ObMultiPartInsertCtx* insert_ctx = NULL;
  const ObNewRow* insert_row = NULL;
  ObMultiVersionSchemaService* schema_service = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSqlCtx* sql_ctx = NULL;
  int64_t affected_rows = 0;
  bool is_filtered = false;
  got_row = false;
  CK(OB_NOT_NULL(plan_ctx = ctx.get_physical_plan_ctx()));
  CK(OB_NOT_NULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartInsertCtx, ctx, get_id())));
  CK(OB_NOT_NULL(task_ctx = ctx.get_task_executor_ctx()));
  CK(OB_NOT_NULL(schema_service = task_ctx->schema_service_));
  CK(OB_NOT_NULL(get_phy_plan()));
  CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));
  CK(table_dml_infos_.count() == 1);
  CK(insert_ctx->table_dml_ctxs_.count() == 1);
  if (OB_SUCC(ret)) {
    const ObIArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = table_dml_infos_.at(0).index_infos_;
    ObIArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = insert_ctx->table_dml_ctxs_.at(0).index_ctxs_;
    ObTableModify* sub_insert = global_index_infos.at(0).dml_subplans_.at(INSERT_OP).subplan_root_;
    CK(OB_NOT_NULL(sub_insert));
    while (OB_SUCC(ret) && OB_SUCC(ObTableInsert::get_next_row(ctx, insert_row))) {
      got_row = true;
      ++affected_rows;
      CK(OB_NOT_NULL(insert_row));
      OZ(calculate_virtual_column(insert_ctx->expr_ctx_, *const_cast<ObNewRow*>(insert_row), affected_rows));
      OZ(validate_virtual_column(insert_ctx->expr_ctx_, *const_cast<ObNewRow*>(insert_row), affected_rows));
      if (OB_SUCC(ret) && is_returning()) {
        OZ(save_returning_row(
            insert_ctx->expr_ctx_, *insert_row, insert_ctx->returning_row_, insert_ctx->returning_row_store_));
      }
      OZ(check_row_null(ctx, *insert_row, sub_insert->get_column_infos()), *insert_row);
      OZ(ForeignKeyHandle::do_handle_new_row(sub_insert, *insert_ctx, *insert_row));
      OZ(ObPhyOperator::filter_row_for_check_cst(
          insert_ctx->expr_ctx_, *insert_row, check_constraint_exprs_, is_filtered));
      OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); ++i) {
        const ObTableLocation* cur_table_location = global_index_infos.at(i).table_locs_.at(0);
        const DMLSubPlan& dml_subplan = global_index_infos.at(i).dml_subplans_.at(0);
        ObIArray<int64_t>& part_ids = global_index_ctxs.at(i).partition_ids_;
        int64_t part_idx = OB_INVALID_INDEX;
        CK(OB_NOT_NULL(cur_table_location));
        OC((cur_table_location->calculate_partition_ids_by_row)(
            ctx, sql_ctx->schema_guard_, *insert_row, part_ids, part_idx));
        // check again after trigger
        OV(part_idx >= 0, OB_NO_PARTITION_FOR_GIVEN_VALUE, *insert_row);
        if (OB_SUCC(ret) && 0 == i) {
          // for the data table, if partition hint ids are not empty, should deal dml partition selection
          OZ(cur_table_location->deal_dml_partition_selection(part_ids.at(part_idx)), part_ids, part_idx);
        }
        if (OB_SUCC(ret)) {
          ObNewRow cur_row;
          cur_row.cells_ = insert_row->cells_;
          cur_row.count_ = insert_row->count_;
          cur_row.projector_ = dml_subplan.value_projector_;
          cur_row.projector_size_ = dml_subplan.value_projector_size_;
          if (OB_FAIL(insert_ctx->multi_dml_plan_mgr_.add_part_row(0, i, part_idx, INSERT_OP, cur_row))) {
            LOG_WARN("add row to dynamic mini task scheduler failed", K(ret));
          } else {
            LOG_DEBUG("shuffle insert row", K(part_idx), K(part_ids), K(*insert_row), K(dml_subplan));
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

int ObMultiPartInsert::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMultiPartInsertCtx* insert_ctx = NULL;
  ObNewRow* return_row = NULL;
  if (!is_returning()) {
    ret = ObTableInsert::get_next_row(ctx, row);
  } else if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObMultiPartInsertCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(ret), K(is_returning()), K(insert_ctx));
  } else if (OB_FAIL(insert_ctx->returning_row_iterator_.get_next_row(return_row, NULL))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    row = return_row;
  }
  return ret;
}

int ObMultiPartInsert::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObMultiPartInsertCtx* insert_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiPartInsertCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute() || is_returning()))) {
    LOG_WARN("init current row failed", K(ret));
  } else {
    insert_ctx = static_cast<ObMultiPartInsertCtx*>(op_ctx);
    if (OB_FAIL(insert_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_))) {
      LOG_WARN("init multi dml ctx failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(insert_ctx)) {
    const ObTableDMLInfo& first_dml_info = table_dml_infos_.at(0);
    const ObGlobalIndexDMLInfo& first_index_dml_info = first_dml_info.index_infos_.at(0);
    const ObTableModify* first_sub_insert = first_index_dml_info.dml_subplans_.at(INSERT_OP).subplan_root_;
  }
  if (OB_SUCC(ret)) {
    insert_ctx->new_row_exprs_ = &calc_exprs_;
    insert_ctx->new_row_projector_ = projector_;
    insert_ctx->new_row_projector_size_ = projector_size_;
  }
  if (OB_SUCC(ret) && is_returning()) {
    insert_ctx->new_row_exprs_ = &insert_row_exprs_;
    insert_ctx->new_row_projector_ = insert_projector_;
    insert_ctx->new_row_projector_size_ = insert_projector_size_;
    OZ(insert_ctx->alloc_row_cells(projector_size_, insert_ctx->returning_row_));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
