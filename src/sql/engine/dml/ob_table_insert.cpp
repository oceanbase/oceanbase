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
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"
#include "share/schema/ob_table_dml_param.h"
namespace oceanbase {
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {
ObTableInsert::ObTableInsert(ObIAllocator& alloc) : ObTableModify(alloc)
{}

ObTableInsert::~ObTableInsert()
{}

int ObTableInsert::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  // Currently, get_partition_cnt would return the total partition num, which is the OK as long
  // as auto-partition splitting is not happening.
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObTableInsertCtx* insert_ctx = NULL;
  NG_TRACE(insert_open);
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get insert operator context failed", K(ret), K_(id));
  } else if (OB_FAIL(
                 ObTableModify::init_dml_param(ctx, index_tid_, *this, false, &table_param_, insert_ctx->dml_param_))) {
    LOG_WARN("failed to init dml param", K(ret));
  } else {
    insert_ctx->dml_param_.is_ignore_ = is_ignore_;
  }
  if (OB_SUCC(ret) && gi_above_) {
    if (OB_FAIL(get_gi_task(ctx))) {
      LOG_WARN("get granule iterator task failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_table_insert(ctx))) {
      LOG_WARN("do table insert failed", K(ret));
    }
  }
  return ret;
}

int ObTableInsert::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableInsertInput* insert_input = NULL;
  ObTableInsertCtx* insert_ctx = NULL;
  if (!gi_above_ || from_multi_table_dml()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table insert rescan not supported", K(ret));
  } else if (OB_ISNULL(insert_input = GET_PHY_OP_INPUT(ObTableInsertInput, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table insert input is null", K(ret), K(get_id()));
  } else if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table insert context is null", K(ret), K(get_id()));
  } else {
    insert_ctx->part_infos_.reset();
    if (nullptr != insert_ctx->rowkey_dist_ctx_) {
      insert_ctx->rowkey_dist_ctx_->clear();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(ObTableModify::rescan(ctx))) {
        LOG_WARN("rescan child operator failed", K(ret));
      } else if (OB_FAIL(do_table_insert(ctx))) {
        LOG_WARN("do table delete failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableInsert::do_table_insert(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableInsertCtx* insert_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t affected_rows = 0;
  NG_TRACE(insert_start);
  if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get insert operator context failed", K(ret), K(get_id()));
  } else if (insert_ctx->iter_end_) {
    // can not get new GI task
    LOG_DEBUG("can't get gi task, iter end", K(insert_ctx->iter_end_), K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_FAIL(get_part_location(ctx, insert_ctx->part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (is_returning()) {
    // do nothing
  } else if (OB_FAIL(insert_rows(ctx, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("insert row to partition storage failed", K(ret));
    }
    // if auto-increment value used when duplicate, reset last_insert_id_cur_stmt
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      plan_ctx->set_last_insert_id_cur_stmt(0);
    }
  } else if (!from_multi_table_dml()) {
    plan_ctx->add_row_matched_count(affected_rows);
    plan_ctx->add_affected_rows(affected_rows);
    // sync last user specified value after iter ends(compatible with MySQL)
    if (OB_FAIL(plan_ctx->sync_last_value_local())) {
      LOG_WARN("failed to sync last value", K(ret));
    }
  }

  if (!insert_ctx->iter_end_ && !from_multi_table_dml() && !is_returning()) {
    int sync_ret = OB_SUCCESS;
    if (OB_SUCCESS != (sync_ret = plan_ctx->sync_last_value_global())) {
      LOG_WARN("failed to sync value globally", K(sync_ret));
    }
    NG_TRACE(sync_auto_value);
    if (OB_SUCC(ret)) {
      ret = sync_ret;
    }
  }
  NG_TRACE(insert_end);
  return ret;
}

OB_INLINE int ObTableInsert::insert_rows(ObExecContext& ctx, int64_t& affected_rows) const
{
  int ret = OB_SUCCESS;
  ObTableInsertCtx* insert_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  ObPartitionService* partition_service = NULL;
  ObDMLRowIterator dml_row_iter(ctx, *this);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session is null");
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_UNLIKELY(insert_ctx->part_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part infos is empty", K(insert_ctx->part_infos_.empty()));
  } else if (OB_FAIL(dml_row_iter.init())) {
    LOG_WARN("init dml row iterator", K(ret));
  } else if (OB_LIKELY(insert_ctx->part_infos_.count() == 1)) {
    NG_TRACE(insert_start);
    const ObPartitionKey& pkey = insert_ctx->part_infos_.at(0).partition_key_;
    if (!from_multi_table_dml()) {
      if (OB_FAIL(set_autoinc_param_pkey(ctx, pkey))) {
        LOG_WARN("set autoinc param pkey failed", K(pkey), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_service->insert_rows(
              my_session->get_trans_desc(), insert_ctx->dml_param_, pkey, column_ids_, &dml_row_iter, affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("insert row to partition storage failed", K(ret));
        }
      }
    }
  } else {
    // multi part insert
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_ctx->part_infos_.count(); ++i) {
      const ObPartitionKey& pkey = insert_ctx->part_infos_.at(i).partition_key_;
      insert_ctx->part_row_cnt_ = insert_ctx->part_infos_.at(i).part_row_cnt_;
      ObNewRow* row = NULL;
      int64_t affected_row = 0;
      while (OB_SUCC(ret) && OB_SUCC(dml_row_iter.get_next_row(row))) {
        if (OB_FAIL(partition_service->insert_row(
                my_session->get_trans_desc(), insert_ctx->dml_param_, pkey, column_ids_, *row))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ObString drk_buffer = get_duplicated_rowkey_buffer(primary_key_ids_, *row, my_session->get_timezone_info());
            ObString duplicted_name = ObString::make_string("PRIMARY");
            LOG_USER_ERROR(
                OB_ERR_PRIMARY_KEY_DUPLICATE, drk_buffer.ptr(), duplicted_name.length(), duplicted_name.ptr());
          } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("insert row to partition storage failed", K(ret));
          }
        } else {
          affected_rows += affected_row;
          if (insert_ctx->part_row_cnt_ <= 0) {
            break;
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("process insert row failed", K(ret));
    }
  }
  return ret;
}

int ObTableInsert::inner_close(ObExecContext& ctx) const
{
  NG_TRACE(insert_close);
  return ObTableModify::inner_close(ctx);
}

int64_t ObTableInsert::to_string_kv(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TID, table_id_, K_(index_tid), N_CID, column_ids_);
  return pos;
}

int ObTableInsert::add_filter(ObSqlExpression* expr)
{
  UNUSED(expr);
  return OB_NOT_IMPLEMENT;
}

int ObTableInsert::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableInsertCtx* insert_ctx = NULL;
  if (OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op_ is null");
  } else if (OB_ISNULL(insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert ctx is null", K(ret));
  } else if (insert_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(get_id()), K(insert_ctx->iter_end_));
    ret = OB_ITER_END;
  } else {
    if (from_multi_table_dml()) {
      --insert_ctx->part_row_cnt_;
    }
    if (OB_SUCC(ret)) {
      ret = child_op_->get_next_row(ctx, row);
    }
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  return ret;
}

OB_INLINE int ObTableInsert::is_valid(ObTableInsertCtx*& insert_ctx, ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  insert_ctx = GET_PHY_OPERATOR_CTX(ObTableInsertCtx, ctx, get_id());

#if !defined(NDEBUG)
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inner status", K(child_op_));
  } else if (OB_ISNULL(insert_ctx)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(insert_ctx->new_row_exprs_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("new row expr is null", K(ret));
  }
#endif
  if (OB_SUCC(ret)) {
    if (PHY_EXPR_VALUES != child_op_->get_type() || child_op_->get_rows() > 10) {
      if (OB_FAIL(try_check_status(ctx))) {
        LOG_WARN("check status failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    insert_ctx->expr_ctx_.calc_buf_->reset();
  }
  return ret;
}

int ObTableInsert::get_next_row(ObExecContext& ctx, const ObNewRow*& insert_row) const
{
  int ret = OB_SUCCESS;
  ObTableInsertCtx* insert_ctx = NULL;
  if (OB_FAIL(is_valid(insert_ctx, ctx))) {
    LOG_WARN("the operator is not valid", K(ret));
  } else if (OB_FAIL(inner_get_next_row(ctx, insert_row))) {
    if (OB_ITER_END == ret) {
      NG_TRACE(insert_iter_end);
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_ISNULL(insert_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid insert row", K(ret), K(insert_row), K(insert_ctx->expr_ctx_.calc_buf_));
  } else if (OB_FAIL(copy_insert_row(
                 *insert_ctx, insert_row, insert_ctx->get_cur_row(), need_copy_row_for_compute() || is_returning()))) {
    LOG_WARN("fail to copy cur row failed", K(ret));
  } else if (OB_FAIL(process_row(ctx, insert_ctx, insert_row))) {
    LOG_WARN("fail to process the row", K(ret));
  } else {
    insert_ctx->curr_row_num_++;
  }
  return ret;
}

int ObTableInsert::get_next_rows(ObExecContext& ctx, const ObNewRow*& insert_row, int64_t& row_count) const
{
  int ret = OB_SUCCESS;
  row_count = 0;
  ObTableInsertCtx* insert_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  if (OB_LIKELY(1 == child_op_->get_rows() && plan_ctx->get_bind_array_count() <= 0)) {
    // if insert values is only one row,use get_next_row(),
    // directly take the memory of the row iterated from values to avoid the memory copy of the row
    row_count = 1;
    ret = get_next_row(ctx, insert_row);
  } else {
    // init rows if first bulk of rows, otherwise reuse the rows and reset allocator for deep copy strings
    if (OB_FAIL(is_valid(insert_ctx, ctx))) {
      LOG_WARN("check is valid failed", K(ret));
    } else if (insert_ctx->first_bulk_) {
      insert_ctx->first_bulk_ = false;
      if (!child_op_->is_exact_rows()) {
        insert_ctx->estimate_rows_ = 1;
      } else {
        insert_ctx->estimate_rows_ = child_op_->get_rows();
        if (plan_ctx->get_bind_array_count() > 0) {
          insert_ctx->estimate_rows_ *= plan_ctx->get_bind_array_count();
        }
      }
      insert_ctx->expr_ctx_.calc_buf_ = &insert_ctx->get_calc_buf();
      if (OB_FAIL(init_cur_rows(insert_ctx->estimate_rows_, *insert_ctx, true))) {
        LOG_WARN("fail to init cur rows", K(insert_ctx->estimate_rows_), K(ret));
      }
    } else {
      insert_ctx->rewind();
      static_cast<ObArenaAllocator*>(insert_ctx->expr_ctx_.calc_buf_)->reset_remain_one_page();
    }

    while (OB_SUCCESS == ret && row_count < insert_ctx->estimate_rows_ &&
           row_count < ObPhyOperator::ObPhyOperatorCtx::BULK_COUNT) {
      if (OB_FAIL(child_op_->get_next_row(ctx, insert_row))) {
        if (OB_ITER_END == ret) {
          NG_TRACE(insert_iter_end);
          if (plan_ctx->get_bind_array_count() > 0) {
            if (OB_FAIL(child_op_->switch_iterator(ctx))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("switch child iterator failed", K(ret));
              }
            }
          }
        } else {
          LOG_WARN("fail to get next row", K(row_count), K(ret));
        }
      } else if (OB_FAIL(copy_insert_rows(*insert_ctx, insert_row))) {
        LOG_WARN("failed to copy insert rows", K(ret));
      } else if (OB_FAIL(process_row(ctx, insert_ctx, insert_row))) {
        LOG_WARN("fail to process the row", K(insert_row), K(ret));
      } else if (OB_FAIL(deep_copy_row(insert_ctx, insert_row))) {
        LOG_WARN("fail to deep copy the row", K(insert_row), K(ret));
      } else {
        row_count++;
        insert_ctx->curr_row_num_++;
      }
    }

    insert_row = &insert_ctx->get_first_row();

    if (OB_ITER_END == ret && row_count > 0) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTableInsert::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableInsertCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableInsertCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create table insert operator context failed", K(ret), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator context is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    op_ctx->new_row_exprs_ = &calc_exprs_;
    op_ctx->new_row_projector_ = projector_;
    op_ctx->new_row_projector_size_ = projector_size_;
  }
  return ret;
}

int ObTableInsert::do_column_convert(
    ObExprCtx& expr_ctx, const ObDList<ObSqlExpression>& new_row_exprs, ObNewRow& insert_row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(calculate_row_inner(expr_ctx, insert_row, new_row_exprs))) {
    LOG_WARN("failed to calculate new row", K(ret));
  } else if (OB_FAIL(validate_normal_column(expr_ctx, expr_ctx.column_conv_ctx_, insert_row))) {
    LOG_WARN("failed to validate normal column", K(ret));
  }
  OZ(calculate_virtual_column(expr_ctx, insert_row, 0));
  OZ(validate_virtual_column(expr_ctx, insert_row, 0));
  return ret;
}

OB_INLINE int ObTableInsert::process_row(
    ObExecContext& ctx, ObTableInsertCtx* insert_ctx, const ObNewRow*& insert_row) const
{
  int ret = OB_SUCCESS;
  ObExprCtx& expr_ctx = insert_ctx->expr_ctx_;
  bool is_filtered = false;
  if (OB_FAIL(do_column_convert(expr_ctx, *insert_ctx->new_row_exprs_, *const_cast<ObNewRow*>(insert_row)))) {
    int64_t err_col_idx = (expr_ctx.err_col_idx_) % column_ids_.count();
    log_user_error_inner(ret, err_col_idx, insert_ctx->curr_row_num_ + 1, ctx);
    LOG_WARN("calculate or validate row failed", K(ret), K(expr_ctx.err_col_idx_), K(err_col_idx));
  } else if (OB_ISNULL(insert_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(insert_row), K(ret));
  } else {
    ctx.get_physical_plan_ctx()->record_last_insert_id_cur_stmt();
    SQL_ENG_LOG(DEBUG, "insert get next row", K(*insert_row), K(column_ids_));
  }
  if (OB_SUCC(ret) && !from_multi_table_dml() && (PHY_INSERT == get_type() || PHY_INSERT_RETURNING == get_type())) {
    int64_t row_num = insert_ctx->curr_row_num_ + 1;
    OZ(calculate_virtual_column(expr_ctx, *const_cast<ObNewRow*>(insert_row), row_num));
    OZ(validate_virtual_column(expr_ctx, *const_cast<ObNewRow*>(insert_row), row_num));
    OZ(check_row_null(ctx, *insert_row, column_infos_), *insert_row);
    OZ(ForeignKeyHandle::do_handle_new_row(*insert_ctx, fk_args_, *insert_row));
    OZ(ObPhyOperator::filter_row_for_check_cst(
        insert_ctx->expr_ctx_, *insert_row, check_constraint_exprs_, is_filtered));
    if (is_filtered) {
      ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
      LOG_WARN("row is filtered by check filters, running is stopped");
    }
  }
  return ret;
}

int ObTableInsert::deep_copy_row(ObTableInsertCtx* insert_ctx, const ObNewRow*& insert_row) const
{
  int ret = OB_SUCCESS;
  if (insert_ctx->estimate_rows_ != 1) {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_row->get_count(); i++) {
      int64_t copy_size = insert_row->get_cell(i).get_deep_copy_size();
      if (copy_size != 0) {
        pos = 0;
        char* memory = NULL;
        common::ObObj* temp_obj = const_cast<common::ObObj*>(&insert_row->get_cell(i));

        if (OB_ISNULL(memory = static_cast<char*>(insert_ctx->expr_ctx_.calc_buf_->alloc(copy_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory for ObObj failed", "size", copy_size);
        } else if (OB_FAIL(temp_obj->deep_copy(*temp_obj, memory, copy_size, pos))) {
          LOG_WARN("deep copy for ObObj failed", K(memory), K(copy_size), K(pos));
        }
      }
    }
  }
  return ret;
}

int ObTableInsert::copy_insert_row(
    ObTableInsertCtx& insert_ctx, const ObNewRow*& input_row, ObNewRow& buf_row, bool need_copy) const
{
  int ret = OB_SUCCESS;
  buf_row.count_ = column_count_;
  buf_row.projector_ = insert_ctx.new_row_projector_;
  buf_row.projector_size_ = insert_ctx.new_row_projector_size_;
  if (OB_ISNULL(input_row)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(need_copy)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < input_row->get_count(); ++i) {
      int64_t real_idx = input_row->projector_size_ > 0 ? input_row->projector_[i] : i;
      if (OB_UNLIKELY(real_idx >= input_row->count_ || real_idx >= buf_row.count_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid row count", K(real_idx), K(input_row->count_), K(buf_row.count_));
      } else {
        buf_row.cells_[real_idx] = input_row->cells_[real_idx];
      }
    }
  } else if (OB_UNLIKELY(input_row->count_ != buf_row.count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is not equal", K(ret));
  } else {
    buf_row.cells_ = input_row->cells_;
  }
  if (OB_SUCC(ret)) {
    input_row = &buf_row;
  }
  return ret;
}

int ObTableInsert::copy_insert_rows(ObTableInsertCtx& insert_ctx, const ObNewRow*& input_row) const
{
  int ret = OB_SUCCESS;
  if (insert_ctx.index_ >= insert_ctx.row_count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index is not valid", K(ret), K(insert_ctx.index_));
  } else if (OB_FAIL(copy_insert_row(insert_ctx, input_row, insert_ctx.get_cur_rows()[insert_ctx.index_], true))) {
    LOG_WARN("failed to copy row", K(ret));
  } else {
    ++insert_ctx.index_;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
