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

#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_autoincrement_service.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"
#include "share/schema/ob_table_dml_param.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {

OB_SERIALIZE_MEMBER((ObTableInsertOpInput, ObTableModifyOpInput));

OB_SERIALIZE_MEMBER((ObTableInsertSpec, ObTableModifySpec));

int ObTableInsertOp::switch_iterator(ObExecContext& ctx)
{
  UNUSED(ctx);
  return common::OB_ITER_END;
}

OB_INLINE int ObTableInsertOp::insert_rows(
    storage::ObDMLBaseParam& dml_param, const ObIArray<DMLPartInfo>& part_infos, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(ctx_);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx_);
  ObPartitionService* partition_service = NULL;
  ObSeInsertRowIterator dml_row_iter(ctx_, *this);
  if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_UNLIKELY(part_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part infos is empty", K(part_infos.empty()));
  } else if (OB_FAIL(dml_row_iter.init())) {
    LOG_WARN("init dml row iterator", K(ret));
  } else if (OB_LIKELY(part_infos.count() == 1)) {
    NG_TRACE(insert_start);
    const ObPartitionKey& pkey = part_infos.at(0).partition_key_;
    if (!MY_SPEC.from_multi_table_dml()) {
      // auto increment is handled in multi table dml operator
      if (OB_FAIL(set_autoinc_param_pkey(pkey))) {
        LOG_WARN("set autoinc param pkey failed", K(pkey), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_service->insert_rows(
              my_session->get_trans_desc(), dml_param, pkey, MY_SPEC.column_ids_, &dml_row_iter, affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("insert row to partition storage failed", K(ret));
        }
      }
    }
  } else {
    // multi part insert
    for (int64_t i = 0; OB_SUCC(ret) && i < part_infos.count(); ++i) {
      const ObPartitionKey& pkey = part_infos.at(i).partition_key_;
      part_row_cnt_ = part_infos.at(i).part_row_cnt_;
      ObNewRow* row = NULL;
      int64_t affected_row = 0;
      while (OB_SUCC(ret) && OB_SUCC(dml_row_iter.get_next_row(row))) {
        if (OB_FAIL(partition_service->insert_row(
                my_session->get_trans_desc(), dml_param, pkey, MY_SPEC.column_ids_, *row))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            LOG_WARN("pk conflict", K(ret));
          } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("insert row to partition storage failed", K(ret));
          }
        } else {
          affected_rows += affected_row;
          if (part_row_cnt_ <= 0) {
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

int ObTableInsertOp::inner_open()
{
  int ret = OB_SUCCESS;
  NG_TRACE(insert_open);
  OZ(ObTableModifyOp::inner_open());
  OZ(ObTableModify::init_dml_param_se(ctx_, MY_SPEC.index_tid_, false, &MY_SPEC.table_param_, dml_param_));
  dml_param_.is_ignore_ = MY_SPEC.is_ignore_;
  if (OB_SUCC(ret) && MY_SPEC.gi_above_) {
    OZ(get_gi_task());
  }
  if (OB_SUCC(ret)) {
    OZ(do_table_insert());
  }
  return ret;
}

int ObTableInsertOp::rescan()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.gi_above_ || MY_SPEC.from_multi_table_dml()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table insert rescan not supported", K(ret));
  } else {
    part_infos_.reset();
    is_end_ = false;
    if (nullptr != rowkey_dist_ctx_) {
      rowkey_dist_ctx_->clear();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_gi_task())) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(ObTableModifyOp::rescan())) {
        LOG_WARN("rescan child operator failed", K(ret));
      } else if (OB_FAIL(do_table_insert())) {
        LOG_WARN("do table delete failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableInsertOp::do_table_insert()
{
  int ret = OB_SUCCESS;
  NG_TRACE(insert_start);
  int64_t affected_rows = 0;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (iter_end_) {
    // no GI task
    LOG_DEBUG("can't get gi task, iter end", K(iter_end_), K(MY_SPEC.id_));
  } else if (OB_FAIL(get_part_location(part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (MY_SPEC.is_returning_) {
    // returning do insert in get_next_row, do nothing here.
  } else if (OB_FAIL(insert_rows(dml_param_, part_infos_, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("insert row to partition storage failed", K(ret));
    }
    // if auto-increment value used when duplicate, reset last_insert_id_cur_stmt
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      plan_ctx->set_last_insert_id_cur_stmt(0);
    }
  } else if (!MY_SPEC.from_multi_table_dml()) {
    plan_ctx->add_row_matched_count(affected_rows);
    plan_ctx->add_affected_rows(affected_rows);
    // sync last user specified value after iter ends(compatible with MySQL)
    if (OB_FAIL(plan_ctx->sync_last_value_local())) {
      LOG_WARN("failed to sync last value", K(ret));
    }
  }
  if (!iter_end_ && !MY_SPEC.from_multi_table_dml() && !MY_SPEC.is_returning_) {
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

int ObTableInsertOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(MY_SPEC.id_), K(iter_end_));
    ret = OB_ITER_END;
  } else if (is_end_) {
    if (MY_SPEC.from_multi_table_dml()) {
      --part_row_cnt_;
    }
    ret = OB_ITER_END;
  } else {
    if (MY_SPEC.from_multi_table_dml()) {
      --part_row_cnt_;
    }
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      } else {
        is_end_ = true;
      }
    } else {
      clear_evaluated_flag();
      LOG_TRACE("insert output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.storage_row_output_));
    }
  }

  return ret;
}

OB_INLINE int ObTableInsertOp::process_row()
{
  int ret = OB_SUCCESS;
  // check insert value
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.storage_row_output_.count(); i++) {
    ObExpr* expr = MY_SPEC.storage_row_output_.at(i);
    ObDatum* datum = NULL;
    if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
      // compatible with old code
      log_user_error_inner(ret, i, curr_row_num_ + 1, MY_SPEC.column_infos_);
    }
  }

  if (OB_SUCC(ret)) {
    ctx_.get_physical_plan_ctx()->record_last_insert_id_cur_stmt();
  }

  if (OB_SUCC(ret) && !MY_SPEC.from_multi_table_dml() &&
      (PHY_INSERT == MY_SPEC.type_ || PHY_INSERT_RETURNING == MY_SPEC.type_)) {
    OZ(check_row_null(MY_SPEC.storage_row_output_, MY_SPEC.column_infos_));
    OZ(ForeignKeyHandle::do_handle_new_row(*this, MY_SPEC.fk_args_, MY_SPEC.storage_row_output_));
    bool is_filtered = false;
    OZ(filter_row_for_check_cst(MY_SPEC.check_constraint_exprs_, is_filtered));
    OV(!is_filtered, OB_ERR_CHECK_CONSTRAINT_VIOLATED);
  }
  return ret;
}

int ObTableInsertOp::prepare_next_storage_row(const ObExprPtrIArray*& output)
{
  int ret = OB_SUCCESS;
  output = &MY_SPEC.storage_row_output_;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check status failed", K(ret));
  } else if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END == ret) {
      NG_TRACE(insert_iter_end);
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  } else if (OB_FAIL(process_row())) {
    LOG_WARN("fail to process row", K(ret));
  } else {
    curr_row_num_++;
    LOG_DEBUG("insert output row",
        "output row",
        ROWEXPR2STR(eval_ctx_, MY_SPEC.storage_row_output_),
        "storage row output",
        ROWEXPR2STR(eval_ctx_, MY_SPEC.storage_row_output_));
  }

  return ret;
}

int ObTableInsertOp::inner_close()
{
  NG_TRACE(insert_close);
  return ObTableModifyOp::inner_close();
}

int ObSeInsertRowIterator::setup_row_copy_mem()
{
  int ret = OB_SUCCESS;
  if (NULL != row_copy_mem_) {
    row_copy_mem_->get_arena_allocator().reset_remain_one_page();
  } else {
    lib::ContextParam param;
    param
        .set_mem_attr(
            ctx_.get_my_session()->get_effective_tenant_id(), ObModIds::OB_SQL_INSERT, ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(row_copy_mem_, param))) {
      LOG_WARN("create memory entity failed", K(ret));
    }
  }
  return ret;
}

int ObSeInsertRowIterator::get_next_rows(common::ObNewRow*& rows, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  row_count = 0;
  ObTableInsertOp& insert_op = static_cast<ObTableInsertOp&>(op_);
  if ((1 == insert_op.get_child()->get_spec().rows_ && plan_ctx->get_bind_array_count() <= 0)) {
    ret = get_next_row(rows);
    row_count = (OB_SUCCESS == ret) ? 1 : 0;
  } else if (OB_FAIL(setup_row_copy_mem())) {
    LOG_WARN("setup row copy memory", K(ret));
  } else {
    ObOperator* child_op = insert_op.get_child();
    if (first_bulk_) {
      first_bulk_ = false;
      if (PHY_EXPR_VALUES != child_op->get_spec().type_) {
        estimate_rows_ = 1;
      } else {
        estimate_rows_ = child_op->get_spec().rows_;
        if (plan_ctx->get_bind_array_count() > 0) {
          estimate_rows_ *= plan_ctx->get_bind_array_count();
        }
      }
      int64_t init_row_cnt = 0;
      if (OB_FAIL(
              create_cur_rows(estimate_rows_, insert_op.get_spec().storage_row_output_.count(), rows, init_row_cnt))) {
        LOG_WARN("fail to init cur rows", K(estimate_rows_), K(ret));
      } else {
        batch_rows_ = rows;
        row_count_ = init_row_cnt;
      }
    } else {
      index_ = 0;
    }
    ObNewRow* cur_row = NULL;
    if (OB_FAIL(insert_op.try_check_status())) {
      LOG_WARN("check status failed", K(ret));
    }
    while (OB_SUCC(ret) && row_count < estimate_rows_ && row_count < BULK_COUNT) {
      if (OB_FAIL(get_next_row(cur_row))) {
        if (OB_ITER_END == ret) {
          // do nothing
        } else {
          LOG_WARN("fail to get next row", K(row_count), K(ret));
        }
      } else if (OB_ISNULL(cur_row)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("cur row is null", K(ret));
      } else if (OB_FAIL(copy_cur_rows(*cur_row))) {
        LOG_WARN("fail to copy cur rows by projector", K(*cur_row), K(ret));
      } else {
        row_count++;
      }
    }
    rows = batch_rows_;
    if (OB_ITER_END == ret && row_count > 0) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

void ObSeInsertRowIterator::reset()
{
  DMLRowIterator::reset();
  batch_rows_ = NULL;
  first_bulk_ = true;
  estimate_rows_ = 0;
  row_count_ = 0;
  index_ = 0;
  destroy_row_copy_mem();
}

int ObSeInsertRowIterator::create_cur_rows(int64_t total_cnt, int64_t col_cnt, common::ObNewRow*& row, int64_t& row_cnt)
{
  int ret = OB_SUCCESS;
  row_cnt = total_cnt > BULK_COUNT ? BULK_COUNT : total_cnt;
  void* ptr = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(row_cnt * sizeof(ObNewRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(ERROR, "alloc memory for row failed", "size", row_cnt * sizeof(ObNewRow));
  } else {
    row = new (ptr) ObNewRow[row_cnt];
    if (OB_SUCCESS == (ret = alloc_rows_cells(col_cnt, row_cnt, row))) {
      for (int64_t i = 0; i < row_cnt; i++) {
        row[i].projector_ = NULL;
        row[i].projector_size_ = 0;
      }
    }
  }

  return ret;
}

int ObSeInsertRowIterator::alloc_rows_cells(const int64_t col_cnt, const int64_t row_cnt, common::ObNewRow* rows)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (row_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(col_cnt));
  } else if (col_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(col_cnt));
  } else if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(row_cnt * col_cnt * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for row failed", "size", row_cnt * col_cnt * sizeof(ObObj));
  } else {
    for (int64_t i = 0; i < row_cnt; i++) {
      long unsigned offset = i * col_cnt * sizeof(ObObj);
      rows[i].cells_ = new (static_cast<char*>(ptr) + offset) common::ObObj[col_cnt];
      rows[i].count_ = col_cnt;
    }
  }
  return ret;
}

int ObSeInsertRowIterator::copy_cur_rows(const ObNewRow& src_row)
{
  int ret = OB_SUCCESS;
  if (index_ >= row_count_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("index is not valid", K(index_), K(ret));
  } else if (OB_ISNULL(row_copy_mem_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row copy memory allocator is NULL", K(ret));
  } else {
    ObNewRow& dst_row = batch_rows_[index_];
    if (dst_row.get_count() != src_row.get_count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("different row column", K(dst_row.get_count()), K(src_row.get_count()));
    } else {
      for (int64_t i = 0; i < src_row.get_count(); i++) {
        if (OB_FAIL(ob_write_obj(row_copy_mem_->get_arena_allocator(), src_row.cells_[i], dst_row.cells_[i]))) {
          LOG_WARN("fail to copy obj", K(ret));
        }
      }
      index_++;
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
