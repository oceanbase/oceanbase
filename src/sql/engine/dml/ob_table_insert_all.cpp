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
#include "sql/engine/dml/ob_table_insert_all.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
namespace sql {
class ObMultiTableInsert::ObMultiTableInsertCtx : public ObTableInsert::ObTableInsertCtx, public ObMultiDMLCtx {
  friend class ObMultiTableInsert;

public:
  explicit ObMultiTableInsertCtx(ObExecContext& ctx) : ObTableInsertCtx(ctx), ObMultiDMLCtx(ctx.get_allocator())
  {}
  ~ObMultiTableInsertCtx()
  {}
  virtual void destroy() override
  {
    ObTableInsert::ObTableInsertCtx::destroy();
    ObMultiDMLCtx::destroy_ctx();
  }
};

ObMultiTableInsert::ObMultiTableInsert(ObIAllocator& alloc)
    : ObTableInsert(alloc), ObMultiDMLInfo(alloc), is_multi_insert_first_(false), multi_table_insert_infos_(alloc)
{}

ObMultiTableInsert::~ObMultiTableInsert()
{
  reset();
}

void ObMultiTableInsert::reset()
{
  is_multi_insert_first_ = false;
  for (int64_t i = 0; i < multi_table_insert_infos_.count(); ++i) {
    if (multi_table_insert_infos_.at(i) != NULL) {
      multi_table_insert_infos_.at(i)->~InsertTableInfo();
      multi_table_insert_infos_.at(i) = NULL;
    }
  }
  multi_table_insert_infos_.reset();
  ObTableInsert::reset();
}

int ObMultiTableInsert::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObMultiTableInsertCtx* insert_ctx = NULL;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertCtx, ctx, get_id()))) {
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
  }
  return ret;
}

int ObMultiTableInsert::inner_close(ObExecContext& ctx) const
{
  int wait_ret = wait_all_task(GET_PHY_OPERATOR_CTX(ObMultiTableInsertCtx, ctx, get_id()), ctx.get_physical_plan_ctx());
  int close_ret = ObTableInsert::inner_close(ctx);
  if (OB_SUCCESS != wait_ret || OB_SUCCESS != close_ret) {
    LOG_WARN("inner close failed", K(wait_ret), K(close_ret));
  }
  return (OB_SUCCESS == close_ret) ? wait_ret : close_ret;
}

int ObMultiTableInsert::shuffle_insert_row(ObExecContext& ctx, bool& got_row) const
{
  int ret = OB_SUCCESS;
  ObMultiTableInsertCtx* insert_ctx = NULL;
  const ObNewRow* input_row = NULL;
  ObMultiVersionSchemaService* schema_service = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSqlCtx* sql_ctx = NULL;
  ObExprCtx* expr_ctx = NULL;
  int64_t affected_rows = 0;
  got_row = false;
  if (OB_ISNULL(child_op_) || OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx()) ||
      OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObMultiTableInsertCtx, ctx, get_id())) ||
      OB_ISNULL(task_ctx = ctx.get_task_executor_ctx()) || OB_ISNULL(schema_service = task_ctx->schema_service_) ||
      OB_ISNULL(get_phy_plan()) || OB_ISNULL(sql_ctx = ctx.get_sql_ctx()) || OB_ISNULL(sql_ctx->schema_guard_) ||
      OB_ISNULL(expr_ctx = &insert_ctx->expr_ctx_) ||
      OB_UNLIKELY(table_dml_infos_.count() != insert_ctx->table_dml_ctxs_.count()) ||
      OB_UNLIKELY(multi_table_insert_infos_.count() != table_dml_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument",
        K(plan_ctx),
        K(insert_ctx),
        K(task_ctx),
        K(schema_service),
        K(get_phy_plan()),
        K(sql_ctx),
        K(expr_ctx),
        K(table_dml_infos_.count()),
        K(child_op_),
        K(multi_table_insert_infos_.count()),
        K(ret));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(child_op_->get_next_row(ctx, input_row))) {
      if (OB_ISNULL(input_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(input_row));
      } else if (OB_FAIL(process_row(ctx, *insert_ctx, input_row))) {
        LOG_WARN("failed to process row", K(ret));
      } else {
        bool continue_insert = true;
        bool is_match_condition = true;
        bool have_insert_row = false;
        int64_t pre_when_conds_idx = -1;
        for (int64_t index = 0; OB_SUCC(ret) && continue_insert && index < table_dml_infos_.count(); ++index) {
          const ObIArrayWrap<ObGlobalIndexDMLInfo>& global_index_infos = table_dml_infos_.at(index).index_infos_;
          ObIArrayWrap<ObGlobalIndexDMLCtx>& global_index_ctxs = insert_ctx->table_dml_ctxs_.at(index).index_ctxs_;
          const DMLSubPlan& insert_dml_sub = global_index_infos.at(0).dml_subplans_.at(INSERT_OP);
          ObTableModify* sub_insert = insert_dml_sub.subplan_root_;
          InsertTableInfo* table_info = multi_table_insert_infos_.at(index);
          bool is_filtered = false;
          ObNewRow insert_row;
          if (OB_ISNULL(sub_insert) || OB_ISNULL(table_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(sub_insert), K(table_info));
          } else if (OB_FAIL(prepare_insert_row(input_row, insert_dml_sub, insert_row))) {
            LOG_WARN("failed to reset insert row", K(ret));
          } else if (pre_when_conds_idx != table_info->when_conds_idx_ && OB_FAIL(check_match_conditions(*expr_ctx,
                                                                              insert_row,
                                                                              have_insert_row,
                                                                              table_info,
                                                                              pre_when_conds_idx,
                                                                              continue_insert,
                                                                              is_match_condition))) {
            LOG_WARN("failed to check match conditions", K(ret));
          } else if (!is_match_condition) {
            /*do nothing*/
          } else if (OB_FAIL(calculate_virtual_column(
                         table_info->virtual_column_exprs_, insert_ctx->expr_ctx_, insert_row))) {
            LOG_WARN("failed to calculate virtual column", K(ret));
          } else if (OB_FAIL(validate_virtual_column(insert_ctx->expr_ctx_, insert_row, affected_rows))) {
            LOG_WARN("failed to validate virtual column", K(ret));
          } else if (OB_FAIL(check_row_null(ctx, insert_row, sub_insert->get_column_infos()))) {
            LOG_WARN("failed to check row null", K(ret), K(insert_row));
          } else if (OB_FAIL(ObPhyOperator::filter_row_for_check_cst(
                         insert_ctx->expr_ctx_, insert_row, table_info->check_constraint_exprs_, is_filtered))) {
            LOG_WARN("failed to filter row for multi table check cst", K(ret));
          } else if (is_filtered) {
            ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
            LOG_WARN("check constraint violated", K(ret));
          } else if (OB_FAIL(ForeignKeyHandle::do_handle_new_row(sub_insert, *insert_ctx, insert_row))) {
            LOG_WARN("failed to do handle new row", K(ret));
          } else {
            ++affected_rows;
            got_row = true;
            have_insert_row = true;
            for (int64_t i = 0; OB_SUCC(ret) && i < global_index_infos.count(); ++i) {
              const ObTableLocation* cur_table_loc = global_index_infos.at(i).table_locs_.at(0);
              const DMLSubPlan& dml_subplan = global_index_infos.at(i).dml_subplans_.at(0);
              ObIArray<int64_t>& part_ids = global_index_ctxs.at(i).partition_ids_;
              int64_t part_idx = OB_INVALID_INDEX;
              if (OB_ISNULL(cur_table_loc)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret), K(cur_table_loc));
              } else if (OB_FAIL(cur_table_loc->calculate_partition_ids_by_row(
                             ctx, sql_ctx->schema_guard_, insert_row, part_ids, part_idx))) {
                LOG_WARN("failed to calculate partition ids by row", K(ret));
              } else if (part_idx < 0) {
                ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
                LOG_WARN("table has no partition for value", K(part_idx), K(insert_row), K(ret));
              } else if (0 == i && OB_FAIL(cur_table_loc->deal_dml_partition_selection(part_ids.at(part_idx)))) {
                LOG_WARN("failed to deal dml partition selection", K(ret));
              } else {
                ObNewRow cur_row;
                cur_row.cells_ = insert_row.cells_;
                cur_row.count_ = insert_row.count_;
                cur_row.projector_ = dml_subplan.value_projector_;
                cur_row.projector_size_ = dml_subplan.value_projector_size_;
                if (OB_FAIL(insert_ctx->multi_dml_plan_mgr_.add_part_row(index, i, part_idx, INSERT_OP, cur_row))) {
                  LOG_WARN("add row to dynamic mini task scheduler failed", K(ret));
                } else {
                  LOG_TRACE("shuffle insert row", K(part_idx), K(part_ids), K(insert_row), K(dml_subplan));
                }
              }
            }
          }
        }
      }
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

int ObMultiTableInsert::process_row(
    ObExecContext& ctx, ObMultiTableInsertCtx& insert_ctx, const ObNewRow*& insert_row) const
{
  int ret = OB_SUCCESS;
  ObExprCtx& expr_ctx = insert_ctx.expr_ctx_;
  if (OB_ISNULL(insert_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_row));
  } else if (OB_FAIL(ObTableInsert::copy_insert_row(
                 insert_ctx, insert_row, insert_ctx.get_cur_row(), need_copy_row_for_compute()))) {
    LOG_WARN("fail to copy cur row failed", K(ret));
  } else if (OB_FAIL(do_column_convert(expr_ctx, *insert_ctx.new_row_exprs_, *const_cast<ObNewRow*>(insert_row)))) {
    LOG_WARN("calculate or validate row failed", K(ret));
  } else if (OB_ISNULL(insert_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_row), K(ret));
  } else {
    ctx.get_physical_plan_ctx()->record_last_insert_id_cur_stmt();
    SQL_ENG_LOG(DEBUG, "insert get next row", K(*insert_row), K(column_ids_));
  }
  return ret;
}

int ObMultiTableInsert::prepare_insert_row(
    const ObNewRow* input_row, const DMLSubPlan& insert_dml_sub, ObNewRow& new_row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(input_row));
  } else {
    new_row.cells_ = input_row->cells_;
    new_row.count_ = input_row->count_;
    new_row.projector_ = insert_dml_sub.value_projector_;
    new_row.projector_size_ = insert_dml_sub.value_projector_size_;
  }
  return ret;
}

int ObMultiTableInsert::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  return ObTableInsert::get_next_row(ctx, row);
}

int ObMultiTableInsert::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObMultiTableInsertCtx* insert_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMultiTableInsertCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  } else {
    insert_ctx = static_cast<ObMultiTableInsertCtx*>(op_ctx);
    if (OB_FAIL(insert_ctx->init_multi_dml_ctx(ctx, table_dml_infos_, get_phy_plan(), subplan_root_, NULL))) {
      LOG_WARN("init multi dml ctx failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    insert_ctx->new_row_exprs_ = &calc_exprs_;
    insert_ctx->new_row_projector_ = projector_;
    insert_ctx->new_row_projector_size_ = projector_size_;
  }
  return ret;
}

int ObMultiTableInsert::check_match_conditions(ObExprCtx& expr_ctx, const ObNewRow& row, bool have_insert_row,
    const InsertTableInfo* table_info, int64_t& pre_when_conds_idx, bool& continue_insert, bool& is_match) const
{
  int ret = OB_SUCCESS;
  bool is_filtered = false;
  is_match = false;
  ObSEArray<ObISqlExpression*, 8> conds_expr;
  if (OB_ISNULL(table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected NULL", K(ret), K(table_info));
  } else if (have_insert_row && (is_multi_insert_first() || table_info->when_conds_idx_ == -1)) {
    continue_insert = false;
    is_match = false;
  } else if (table_info->when_conds_idx_ != -1 && OB_UNLIKELY(table_info->match_conds_exprs_.get_size() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(table_info->match_conds_exprs_.get_size()));
  } else {
    pre_when_conds_idx = table_info->when_conds_idx_;
    if (table_info->when_conds_idx_ == -1) {
      is_match = true;
    } else if (OB_FAIL(filter_row(expr_ctx, row, table_info->match_conds_exprs_, is_filtered))) {
      LOG_WARN("failed to filter row", K(ret));
    } else if (is_filtered) {
      is_match = false;
    } else {
      is_match = true;
    }
  }
  return ret;
}

int ObMultiTableInsert::calculate_virtual_column(
    const ObDList<ObSqlExpression>& calc_exprs, common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPhyOperator::calculate_row_inner(expr_ctx, calc_row, calc_exprs))) {
    LOG_WARN("failed to calculate row inner", K(ret));
  }
  return ret;
}

int ObMultiTableInsert::add_multi_table_insert_infos(InsertTableInfo*& insert_info)
{
  return multi_table_insert_infos_.push_back(insert_info);
}

int ObMultiTableInsert::filter_row(
    ObExprCtx& expr_ctx, const ObNewRow& row, const ObDList<ObSqlExpression>& filters, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  if (!filters.is_empty()) {
    DLIST_FOREACH(node, filters)
    {
      if (OB_UNLIKELY(NULL == node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null");
      } else if (OB_FAIL(filter_row_inner(expr_ctx, row, node, is_filtered))) {
        LOG_WARN("fail to filter row", K(ret));
      } else if (is_filtered) {
        break;
      }
    }
  }
  return ret;
}

int ObMultiTableInsert::filter_row_inner(
    ObExprCtx& expr_ctx, const ObNewRow& row, const ObISqlExpression* expr, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  ObObj result;
  bool is_true = false;
  if (OB_ISNULL(expr) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(expr), K(expr_ctx.calc_buf_), K(ret));
  } else if (OB_FAIL(expr->calc(expr_ctx, row, result))) {
    LOG_WARN("failed to calc expression", K(ret), K(*expr));
  } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
    LOG_WARN("failed to call is true", K(ret));
  } else if (!is_true) {
    is_filtered = true;
  }
  return ret;
}

int InsertTableInfo::create_insert_table_info(ObIAllocator& alloc, InsertTableInfo*& insert_table_info)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  insert_table_info = NULL;
  if (OB_ISNULL(ptr = (alloc.alloc(sizeof(InsertTableInfo))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory", K(ret), K(ptr));
  } else {
    insert_table_info = new (ptr) InsertTableInfo();
  }
  return ret;
}

int ObMultiTableInsert::deep_copy_rows(ObMultiTableInsertCtx*& insert_ctx, const ObNewRow& row, ObNewRow& new_row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(insert_ctx) || OB_ISNULL(insert_ctx->expr_ctx_.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(insert_ctx));
  } else {
    const int64_t buf_len = sizeof(ObNewRow) + row.get_deep_copy_size();
    int64_t pos = sizeof(ObNewRow);
    char* buf = NULL;
    ObNewRow* tmp_new_row = NULL;
    if (OB_ISNULL(buf = static_cast<char*>(insert_ctx->expr_ctx_.calc_buf_->alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc new row failed", K(ret), K(buf_len));
    } else if (OB_ISNULL(tmp_new_row = new (buf) ObNewRow())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new_row is null", K(ret), K(buf_len));
    } else if (OB_FAIL(tmp_new_row->deep_copy(row, buf, buf_len, pos))) {
      LOG_WARN("deep copy row failed", K(ret), K(buf_len), K(pos));
    } else {
      new_row.cells_ = tmp_new_row->cells_;
      new_row.count_ = tmp_new_row->count_;
      new_row.projector_ = tmp_new_row->projector_;
      new_row.projector_size_ = tmp_new_row->projector_size_;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
