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

#include "sql/engine/dml/ob_multi_table_merge.h"
#include "sql/engine/dml/ob_multi_dml_plan_mgr.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;

namespace sql {

class ObMultiTableMerge::ObMultiTableMergeCtx : public ObTableMergeCtx, public ObMultiDMLCtx {
  friend class ObMultiTableMerge;

public:
  explicit ObMultiTableMergeCtx(ObExecContext& ctx) : ObTableMergeCtx(ctx), ObMultiDMLCtx(ctx.get_allocator())
  {}
  ~ObMultiTableMergeCtx()
  {}
  virtual void destroy() override
  {
    ObTableMergeCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
};

ObMultiTableMerge::ObMultiTableMerge(ObIAllocator& alloc) : ObTableMerge(alloc), ObMultiDMLInfo(alloc)
{}

ObMultiTableMerge::~ObMultiTableMerge()
{}

int ObMultiTableMerge::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = nullptr;
  ObSQLSessionInfo* my_session = nullptr;
  ObMultiTableMergeCtx* merge_ctx = nullptr;
  int64_t update_row_column_count = scan_column_ids_.count() + assignment_infos_.count();
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableMergeCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get multi table update context from exec_ctx failed", K(ret), K(get_id()));
  } else if (OB_FAIL(merge_ctx->init(ctx,
                 has_insert_clause_,
                 has_update_clause_,
                 column_ids_.count(),
                 rowkey_desc_.count(),
                 update_row_column_count))) {
    LOG_WARN("fail to init merge ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*merge_ctx, true))) {
    LOG_WARN("fail to init cur row", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(shuffle_merge_row(ctx, got_row))) {
    LOG_WARN("shuffle merge row failed", K(ret));
  } else if (!got_row) {
  } else if (OB_FAIL(ObTableModify::extend_dml_stmt(ctx, table_dml_infos_, merge_ctx->table_dml_ctxs_))) {
    LOG_WARN("extend dml stmt failed", K(ret));
  } else if (OB_FAIL(merge_ctx->multi_dml_plan_mgr_.build_multi_part_dml_task())) {
    LOG_WARN("build multi partition dml task failed", K(ret));
  } else {
    ObIArray<ObTaskInfo*>& task_info_list = merge_ctx->multi_dml_plan_mgr_.get_mini_task_infos();
    const ObMiniJob& subplan_job = merge_ctx->multi_dml_plan_mgr_.get_subplan_job();
    bool table_need_first = merge_ctx->multi_dml_plan_mgr_.table_need_first();
    ObMiniTaskResult result;
    OZ(merge_ctx->mini_task_executor_.execute(ctx, subplan_job, task_info_list, table_need_first, result));
  }
  return ret;
}

int ObMultiTableMerge::inner_close(ObExecContext& ctx) const
{
  int wait_ret = wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiTableMergeCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableModify::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }

  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiTableMerge::shuffle_merge_row(ObExecContext& ctx, bool& got_row) const
{
  int ret = OB_SUCCESS;
  ObMultiTableMergeCtx* merge_ctx = nullptr;
  const ObNewRow* child_row = nullptr;
  ObTaskExecutorCtx* task_ctx = nullptr;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  ObSQLSessionInfo* my_session = nullptr;
  ObSqlCtx* sql_ctx = NULL;
  CK(OB_NOT_NULL(plan_ctx = ctx.get_physical_plan_ctx()));
  CK(OB_NOT_NULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableMergeCtx, ctx, get_id())));
  CK(OB_NOT_NULL(task_ctx = ctx.get_task_executor_ctx()));
  CK(OB_NOT_NULL(my_session = ctx.get_my_session()));
  CK(OB_NOT_NULL(plan_ctx = ctx.get_physical_plan_ctx()));
  CK(OB_NOT_NULL(sql_ctx = ctx.get_sql_ctx()));
  CK(OB_NOT_NULL(sql_ctx->schema_guard_));

  ObExprCtx& expr_ctx = merge_ctx->expr_ctx_;
  while (OB_SUCC(ret) && OB_SUCC(inner_get_next_row(ctx, child_row))) {
    bool is_match = false;
    if (OB_FAIL(copy_cur_row_by_projector(*merge_ctx, child_row))) {
      LOG_WARN("failed to copy cur row by projector", K(ret));
    } else if (OB_FAIL(check_is_match(expr_ctx, *child_row, is_match))) {
      LOG_WARN("failed to check is match", K(ret));
    } else if (is_match) {
      if (has_update_clause_) {
        OZ(process_update(*sql_ctx->schema_guard_, ctx, expr_ctx, child_row, got_row));
      }
    } else {
      if (has_insert_clause_) {
        OZ(process_insert(*sql_ctx->schema_guard_, ctx, expr_ctx, child_row, got_row));
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

int ObMultiTableMerge::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
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

int ObMultiTableMerge::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = nullptr;
  ObMultiTableMergeCtx* merge_ctx = nullptr;
  OZ(CREATE_PHY_OPERATOR_CTX(ObMultiTableMergeCtx, ctx, get_id(), get_type(), op_ctx));
  OV(OB_NOT_NULL(op_ctx));
  OX(merge_ctx = static_cast<ObMultiTableMergeCtx*>(op_ctx));
  OZ(merge_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_));
  return ret;
}

int ObMultiTableMerge::process_update(ObSchemaGetterGuard& schema_guard, ObExecContext& ctx, ObExprCtx& expr_ctx,
    const ObNewRow* child_row, bool& got_row) const
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  bool need_delete = false;
  bool is_conflict = false;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  OV(OB_NOT_NULL(child_row));
  OV(OB_NOT_NULL(plan_ctx = ctx.get_physical_plan_ctx()));
  OZ(calc_condition(expr_ctx, *child_row, update_conds_, need_update));
  if (need_update) {
    OZ(generate_origin_row(ctx, child_row, is_conflict));
    OZ(calc_update_row(ctx, child_row));
    OZ(calc_delete_condition(ctx, *child_row, need_delete));
    OV(!is_conflict, need_delete ? OB_ERR_SPECIFIED_ROW_NO_LONGER_EXISTS : OB_ERR_UPDATE_TWICE);
    OX(plan_ctx->add_affected_rows(1));
    OX(got_row = true);
    OZ(update_row(schema_guard, ctx, need_delete));
  }
  return ret;
}

int ObMultiTableMerge::process_insert(ObSchemaGetterGuard& schema_guard, ObExecContext& ctx, ObExprCtx& expr_ctx,
    const ObNewRow* child_row, bool& got_row) const
{
  int ret = OB_SUCCESS;
  bool is_true_cond = false;
  ObMultiTableMergeCtx* merge_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  OV(OB_NOT_NULL(child_row));
  OV(OB_NOT_NULL(plan_ctx = ctx.get_physical_plan_ctx()));
  OZ(calc_condition(expr_ctx, *child_row, insert_conds_, is_true_cond));
  if (is_true_cond) {
    OV(OB_NOT_NULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableMergeCtx, ctx, get_id())));
    OZ(calc_insert_row(ctx, expr_ctx, child_row));
    OZ(check_row_null(ctx, merge_ctx->insert_row_, column_infos_), merge_ctx->insert_row_);
    bool is_filtered = false;
    OZ(ObPhyOperator::filter_row_for_check_cst(
        merge_ctx->expr_ctx_, merge_ctx->insert_row_, check_constraint_exprs_, is_filtered));
    OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
    OZ(ForeignKeyHandle::do_handle_new_row(*merge_ctx, fk_args_, merge_ctx->insert_row_), merge_ctx->insert_row_);
    for (int64_t k = 0; OB_SUCC(ret) && k < table_dml_infos_.count(); k++) {
      const ObIArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = table_dml_infos_.at(k).index_infos_;
      ObIArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = merge_ctx->table_dml_ctxs_.at(k).index_ctxs_;
      for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); i++) {
        const ObTableLocation* cur_table_location = global_index_infos.at(i).table_locs_.at(0);
        const DMLSubPlan& dml_subplan = global_index_infos.at(i).dml_subplans_.at(INSERT_OP);
        ObIArray<int64_t>& part_ids = global_index_ctxs.at(i).partition_ids_;
        int64_t part_idx = OB_INVALID_INDEX;
        ObNewRow cur_row;
        OV(OB_NOT_NULL(cur_table_location));
        OZ(cur_table_location->calculate_partition_ids_by_row(
            ctx, &schema_guard, merge_ctx->insert_row_, part_ids, part_idx));
        OV(part_idx >= 0, OB_NO_PARTITION_FOR_GIVEN_VALUE, merge_ctx->insert_row_);
        OX(cur_row.cells_ = merge_ctx->insert_row_.cells_);
        OX(cur_row.count_ = merge_ctx->insert_row_.count_);
        OX(cur_row.projector_ = dml_subplan.value_projector_);
        OX(cur_row.projector_size_ = dml_subplan.value_projector_size_);
        OZ(merge_ctx->multi_dml_plan_mgr_.add_part_row(k, i, part_idx, INSERT_OP, cur_row));
      }
    }
    OX(plan_ctx->add_affected_rows(1));
    OX(got_row = true);
  }
  return ret;
}

int ObMultiTableMerge::update_row(ObSchemaGetterGuard& schema_guard, ObExecContext& ctx, bool need_delete) const
{
  int ret = OB_SUCCESS;
  ObMultiTableMergeCtx* merge_ctx = nullptr;
  OV(OB_NOT_NULL(merge_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableMergeCtx, ctx, get_id())));
  OV(OB_LIKELY(table_dml_infos_.count() == 1));
  if (OB_SUCC(ret)) {
    const ObArrayWrap<ObGlobalIndexDMLInfo>& global_index_dml_infos = table_dml_infos_.at(0).index_infos_;
    ObArrayWrap<ObGlobalIndexDMLCtx>& global_index_dml_ctxs = merge_ctx->table_dml_ctxs_.at(0).index_ctxs_;
    OZ(check_row_null(ctx, merge_ctx->new_row_, column_infos_), merge_ctx->new_row_);
    bool is_filtered = false;
    OZ(ObPhyOperator::filter_row_for_check_cst(
        merge_ctx->expr_ctx_, merge_ctx->new_row_, check_constraint_exprs_, is_filtered));
    OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
    OZ(ForeignKeyHandle::do_handle(*merge_ctx, fk_args_, merge_ctx->old_row_, merge_ctx->new_row_),
        merge_ctx->old_row_,
        merge_ctx->new_row_);
    for (int64_t i = 0; OB_SUCC(ret) && i < global_index_dml_infos.count(); i++) {
      const ObTableLocation* tbl_loc = nullptr;
      ObIArray<int64_t>& part_ids = global_index_dml_ctxs.at(i).partition_ids_;
      const DMLSubPlanArray& dml_subplans = global_index_dml_infos.at(i).dml_subplans_;
      int64_t part_idx = OB_INVALID_INDEX;
      int64_t new_part_idx = OB_INVALID_INDEX;
      ObNewRow cur_row;
      OV(global_index_dml_infos.at(i).table_locs_.count() == 1);
      OV(dml_subplans.count() == DML_OP_CNT);
      OV(OB_NOT_NULL(tbl_loc = global_index_dml_infos.at(i).table_locs_.at(0)));
      OZ(tbl_loc->calculate_partition_ids_by_row(ctx, &schema_guard, merge_ctx->old_row_, part_ids, part_idx));
      OX(cur_row.cells_ = merge_ctx->old_row_.cells_);
      OX(cur_row.count_ = merge_ctx->old_row_.count_);
      OX(cur_row.projector_ = dml_subplans.at(DELETE_OP).value_projector_);
      OX(cur_row.projector_size_ = dml_subplans.at(DELETE_OP).value_projector_size_);
      OZ(merge_ctx->multi_dml_plan_mgr_.add_part_row(0, i, part_idx, DELETE_OP, cur_row));
      if (need_delete) {
        OZ(ForeignKeyHandle::do_handle_old_row(*merge_ctx, fk_args_, merge_ctx->new_row_), merge_ctx->new_row_);
      } else {
        OZ(tbl_loc->calculate_partition_ids_by_row(ctx, &schema_guard, merge_ctx->new_row_, part_ids, new_part_idx));
        if (OB_SUCC(ret) && new_part_idx != part_idx) {
          if (!table_dml_infos_.at(0).get_enable_row_movement()) {
            ret = OB_ERR_UPD_CAUSE_PART_CHANGE;
          } else if (OB_INVALID_INDEX == new_part_idx) {
            ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
            LOG_WARN("updated partition key is beyong highest legal partition key", K(ret));
          }
        }
        OX(cur_row.cells_ = merge_ctx->new_row_.cells_);
        OX(cur_row.count_ = merge_ctx->new_row_.count_);
        OX(cur_row.projector_ = dml_subplans.at(INSERT_OP).value_projector_);
        OX(cur_row.projector_size_ = dml_subplans.at(INSERT_OP).value_projector_size_);
        OZ(merge_ctx->multi_dml_plan_mgr_.add_part_row(0, i, new_part_idx, INSERT_OP, cur_row));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
