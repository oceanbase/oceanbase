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
#include "ob_table_replace.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_autoincrement_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_query_iterator_factory.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;

namespace sql {
class ObTaskExecutorCtx;

OB_SERIALIZE_MEMBER((ObTableReplace, ObTableModify), only_one_unique_key_, primary_key_ids_, res_obj_types_);

void ObTableReplace::reset()
{
  only_one_unique_key_ = false;
  res_obj_types_.reset();
  ObTableModify::reset();
}

void ObTableReplace::reuse()
{
  only_one_unique_key_ = false;
  res_obj_types_.reuse();
  ObTableModify::reuse();
}

int ObTableReplace::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  NG_TRACE(replace_open);
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObTableReplaceCtx* replace_ctx = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  ObExprCtx expr_ctx;
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_FAIL(replace_ctx->init(column_ids_.count()))) {
    LOG_WARN("fail to init replace ctx", K(ret));
  } else if (OB_FAIL(init_cur_row(*replace_ctx, true))) {
    LOG_WARN("fail to init cur row", K(ret));
  } else if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid physical plan", K(my_phy_plan_));
  } else if (OB_FAIL(my_phy_plan_->get_base_table_version(index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    // init dml param
    replace_ctx->dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    replace_ctx->dml_param_.schema_version_ = schema_version;
    replace_ctx->dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    replace_ctx->dml_param_.sql_mode_ = my_session->get_sql_mode();
    replace_ctx->dml_param_.tz_info_ = TZ_INFO(my_session);
    replace_ctx->dml_param_.table_param_ = &table_param_;
    replace_ctx->dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    // check gi above
    if (gi_above_) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule iterator task failed", K(ret));
      }
    }
    // do table replace
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_table_replace(ctx))) {
        LOG_WARN("do table delete failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableReplace::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableReplaceCtx* replace_ctx = NULL;
  if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get replace operator context failed", K(ret), K(get_id()));
  } else {
    replace_ctx->part_infos_.reset();
    if (OB_FAIL(ObTableModify::rescan(ctx))) {
      LOG_WARN("rescan child operator failed", K(ret));
    } else if (gi_above_) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule task failed", K(ret));
      } else if (OB_FAIL(do_table_replace(ctx))) {
        LOG_WARN("do table replace failed", K(ret));
      }
    }
  }

  return ret;
}

int ObTableReplace::do_table_replace(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* insert_row = NULL;
  ObNewRowIterator* duplicated_rows = NULL;
  ObTableReplaceCtx* replace_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObPartitionKey* pkey;
  ObExprCtx expr_ctx;
  if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get replace operator context failed", K(ret), K(get_id()));
  } else if (replace_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(replace_ctx->iter_end_), K(get_id()));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_FAIL(get_part_location(ctx, replace_ctx->part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (replace_ctx->part_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of part info is not right", K(ret), K(replace_ctx->part_infos_.count()));
  } else if (OB_ISNULL(pkey = &replace_ctx->part_infos_.at(0).partition_key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition key is null", K(ret), K(pkey));
  } else if (OB_FAIL(set_autoinc_param_pkey(ctx, *pkey))) {
    LOG_WARN("set autoinc param pkey failed", K(pkey), K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expression context failed", K(ret));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(try_check_status(ctx)) && OB_SUCC(get_next_row(ctx, insert_row))) {
      if (OB_FAIL(try_insert(ctx, expr_ctx, insert_row, *pkey, replace_ctx->dml_param_, duplicated_rows))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          if (OB_FAIL(do_replace(ctx, *pkey, duplicated_rows, replace_ctx->dml_param_))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              LOG_WARN("fail to do replace row", K(ret));
            }
          }
        } else {
          LOG_WARN("failed to insert row", K(ret), K(*insert_row));
        }
      }
      NG_TRACE_TIMES(2, revert_insert_iter);
      if (OB_UNLIKELY(duplicated_rows != NULL)) {
        ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
        duplicated_rows = NULL;
      }
      NG_TRACE_TIMES(2, revert_insert_iter_end);
      if (OB_SUCC(ret)) {
        ctx.get_physical_plan_ctx()->record_last_insert_id_cur_stmt();
      }
    }  // end while
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to replace", K(ret));
    } else {
      ret = OB_SUCCESS;
      //: for replace, OB_CLIENT_FOUND_ROWS has no effect to replace_ctx->affected_rows_
      plan_ctx->set_affected_rows(replace_ctx->affected_rows_);
      if (OB_FAIL(plan_ctx->set_row_matched_count(replace_ctx->record_))) {
        LOG_WARN("fail to set row matched count", K(ret), K(replace_ctx->record_));
      } else if (OB_FAIL(plan_ctx->set_row_duplicated_count(replace_ctx->delete_count_))) {
        LOG_WARN("fail to set row duplicate count", K(replace_ctx->delete_count_));
      } else if (OB_FAIL(plan_ctx->sync_last_value_local())) {
        // sync last user specified value after iter ends(compatible with MySQL)
        LOG_WARN("failed to sync last value", K(ret));
      }
    }

    NG_TRACE(sync_auto_value);
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

int ObTableReplace::inner_close(ObExecContext& ctx) const
{
  NG_TRACE(replace_end);
  return ObTableModify::inner_close(ctx);
}

int64_t ObTableReplace::to_string_kv(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TID, table_id_, N_CID, column_ids_, N_PRIMARY_CID, primary_key_ids_, N_HAS_INDEX, only_one_unique_key_);
  return pos;
}

int ObTableReplace::add_filter(ObSqlExpression* expr)
{
  UNUSED(expr);
  return OB_NOT_SUPPORTED;
}

int ObTableReplace::add_column_res_type(const ObObjType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(res_obj_types_.push_back(type))) {
    LOG_WARN("failed to push res obj type", K(ret));
  }
  return ret;
}

int ObTableReplace::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableReplaceCtx* replace_ctx = NULL;
  if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else {
    replace_ctx->expr_ctx_.calc_buf_->reset();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_get_next_row(ctx, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("inner get next row from child op failed", K(ret));
      }
    } else if (OB_FAIL(calculate_row(replace_ctx->expr_ctx_, *const_cast<ObNewRow*>(row)))) {
      int64_t err_col_idx = (replace_ctx->expr_ctx_.err_col_idx_) % column_ids_.count();
      log_user_error_inner(ret, err_col_idx, replace_ctx->record_ + 1, ctx);
    }
  }
  return ret;
}

int ObTableReplace::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create table replace operator context failed", K(ret), K_(id), K_(type));
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObTableReplace::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableReplaceCtx* replace_ctx = NULL;
  if (OB_ISNULL(child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (replace_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(get_id()), K(replace_ctx->iter_end_));
    ret = OB_ITER_END;
  } else if (OB_FAIL(child_op_->get_next_row(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from child op failed", K(ret));
    }
  } else if (!res_obj_types_.empty() && OB_FAIL(do_type_cast(ctx, row))) {
    LOG_WARN("do type cast failed", K(ret));
  } else if (OB_FAIL(copy_cur_row(*replace_ctx, row))) {
    LOG_WARN("copy current row failed", K(ret));
  }
  return ret;
}

int ObTableReplace::shallow_copy_row(
    ObExecContext& ctx, const common::ObNewRow*& src_row, common::ObNewRow& dst_row) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src_row is null", K(ret));
  } else if (OB_ISNULL(dst_row.cells_)) {
    int64_t cell_size = sizeof(ObObj) * src_row->count_;
    char* cell_buf = static_cast<char*>(ctx.get_allocator().alloc(cell_size));
    if (OB_ISNULL(cell_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory");
    } else {
      dst_row.cells_ = new (cell_buf) ObObj[src_row->count_];
      dst_row.count_ = src_row->count_;
    }
  }
  if (OB_SUCC(ret)) {
    if (src_row->count_ != dst_row.count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the cell size of src_row is not equal to dst_row", K(ret), K(src_row->count_), K(dst_row.count_));
    } else {
      dst_row.projector_size_ = src_row->projector_size_;
      dst_row.projector_ = src_row->projector_;
      for (int64_t i = 0; i < src_row->count_; ++i) {
        dst_row.cells_[i] = src_row->cells_[i];
      }
    }
  }
  return ret;
}

int ObTableReplace::do_type_cast(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;

  ObTableReplaceCtx* replace_ctx = NULL;
  if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else {
    int64_t type_size = res_obj_types_.count();
    int64_t cell_size = row->get_count();
    if (type_size != cell_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj type size is not equal to the number of obj", K(type_size), K(cell_size));
    } else if (OB_FAIL(shallow_copy_row(ctx, row, replace_ctx->cast_row_))) {
      LOG_WARN("fail to shallow copy row");
    } else {
      ObObj tmp_obj;
      const ObObj* res_obj = NULL;
      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < row->get_count(); cell_idx++) {
        ObObj& obj = replace_ctx->cast_row_.get_cell(cell_idx);
        ObObjType expect_type = res_obj_types_.at(cell_idx);
        if (obj.is_null()) {
          // do nothing
        } else if (OB_FAIL(ObObjCaster::to_type(
                       expect_type, replace_ctx->expr_ctx_.column_conv_ctx_, obj, tmp_obj, res_obj)) ||
                   OB_ISNULL(res_obj)) {
          ret = COVER_SUCC(OB_ERR_UNEXPECTED);
          LOG_WARN("failed to cast object", K(ret), K(obj), "type", expect_type);
        } else if (res_obj == &tmp_obj) {
          obj = *res_obj;
        }
      }
      if (OB_SUCC(ret)) {
        row = &replace_ctx->cast_row_;
      } else {
        // if conversion fail, in order to prevent the error message from changing,
        // need to reset ret to OB_SUCCESS. will be converted again in the column conv.
        // so it will not affect the final result
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObTableReplace::try_insert(ObExecContext& ctx, ObExprCtx& expr_ctx, const ObNewRow* insert_row,
    const ObPartitionKey& part_key, const storage::ObDMLBaseParam& dml_param, ObNewRowIterator*& duplicated_rows) const
{
  int ret = OB_SUCCESS;
  ObTableReplaceCtx* replace_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t cur_affected = 0;
  if (OB_ISNULL(insert_row) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(insert_row), K(expr_ctx.calc_buf_));
  } else if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(check_row_null(ctx, *insert_row, column_infos_))) {
    LOG_WARN("fail to check_row_null", K(ret), K(*insert_row));
  } else if (OB_FAIL(ForeignKeyHandle::do_handle_new_row(*replace_ctx, fk_args_, *insert_row))) {
    LOG_WARN("fail to handle foreign key", K(ret), K(*insert_row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids_.count(); i++) {
      replace_ctx->insert_row_.cells_[i] = insert_row->get_cell(i);
    }
    SQL_ENG_LOG(DEBUG, "calc row for insert:", K(ret), K(replace_ctx->insert_row_));
    replace_ctx->record_++;
    NG_TRACE_TIMES(2, replace_start_insert);
    if (OB_SUCC(partition_service->insert_row(my_session->get_trans_desc(),
            dml_param,
            part_key,
            column_ids_,
            primary_key_ids_,
            replace_ctx->insert_row_,
            INSERT_RETURN_ALL_DUP,
            cur_affected,
            duplicated_rows))) {
      replace_ctx->affected_rows_ += cur_affected;
    }
    NG_TRACE_TIMES(2, replace_end_insert);
    SQL_ENG_LOG(DEBUG, "try to insert:", K(ret), K(column_ids_), K(primary_key_ids_), K(only_one_unique_key_));
  }
  return ret;
}

int ObTableReplace::do_replace(ObExecContext& ctx, const ObPartitionKey& part_key,
    const common::ObNewRowIterator* duplicated_rows, const storage::ObDMLBaseParam& dml_param) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObPartitionService* partition_service = NULL;
  ObSingleRowIteratorWrapper row_iter;
  common::ObNewRowIterator* result = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObTableReplaceCtx* replace_ctx = NULL;
  storage::ObTableScanParam scan_param;
  if (OB_ISNULL(duplicated_rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(duplicated_rows));
  } else if (OB_ISNULL(replace_ctx = GET_PHY_OPERATOR_CTX(ObTableReplaceCtx, ctx, id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(scan_row(ctx, part_key, partition_service, scan_param, duplicated_rows, &result))) {
    LOG_WARN("fail to scan row value", K(ret));
  } else {
    int64_t cur_affected = 0;
    if (OB_SUCC(ret)) {
      // 2. delete all duplicated rows
      ObNewRow* dup_row = NULL;
      while (OB_SUCC(ret) && OB_SUCC(result->get_next_row(dup_row))) {
        SQL_ENG_LOG(DEBUG, "get conflict row", K(ret), K(column_ids_), K(primary_key_ids_), K(*dup_row));
        row_iter.reset();
        row_iter.set_row(dup_row);
        NG_TRACE_TIMES(2, replace_start_delete);
        if (OB_ISNULL(dup_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dup row is NULL", K(ret));
        } else if (OB_FAIL(ForeignKeyHandle::do_handle_old_row(*replace_ctx, fk_args_, *dup_row))) {
          LOG_WARN("fail to handle foreign key", K(ret), K(*dup_row));
        } else if (OB_FAIL(partition_service->delete_rows(
                       my_session->get_trans_desc(), dml_param, part_key, column_ids_, &row_iter, cur_affected))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to delete row", K(ret), K(*dup_row));
          }
        } else {
          NG_TRACE_TIMES(2, replace_end_delete);
          SQL_ENG_LOG(DEBUG, "delete rows success", K(*dup_row), K(replace_ctx->insert_row_), K(only_one_unique_key_));
          bool is_equal = false;
          if (true == only_one_unique_key_) {
            if (OB_FAIL(check_values(*dup_row, replace_ctx->insert_row_, is_equal))) {
              SQL_ENG_LOG(WARN, "fail to check values", K(ret));
            } else if (is_equal) {
              // nothing to do
              // to add more condition when we support foreignal key and trigger
            }
          }
          if (!is_equal) {
            replace_ctx->delete_count_++;
            replace_ctx->affected_rows_ += cur_affected;
          }
        }
        SQL_ENG_LOG(DEBUG, "delete row finish", K(ret), K(*dup_row));
      }  // end while
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next dup_row", K(ret));
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      SQL_ENG_LOG(DEBUG, "delete row success", K(replace_ctx->delete_count_));
      // 3. insert again
      row_iter.reset();
      row_iter.set_row(const_cast<ObNewRow*>(&replace_ctx->insert_row_));
      NG_TRACE_TIMES(2, replace_start_insert);
      ret = partition_service->insert_rows(
          my_session->get_trans_desc(), dml_param, part_key, column_ids_, &row_iter, cur_affected);
      if (OB_FAIL(ret)) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to insert", K(ret), K(replace_ctx->insert_row_));
        }
      } else {
        SQL_ENG_LOG(DEBUG, "insert row success", K(replace_ctx->insert_row_));
        replace_ctx->affected_rows_ += cur_affected;
      }
      NG_TRACE_TIMES(2, replace_end_insert);
      SQL_ENG_LOG(DEBUG, "insert again:", K(ret), K(replace_ctx->insert_row_));
    }
    int revert_scan_return = OB_SUCCESS;
    if (result != NULL) {
      NG_TRACE_TIMES(2, revert_scan_iter);
      if (NULL != result && OB_SUCCESS != (revert_scan_return = partition_service->revert_scan_iter(result))) {
        LOG_WARN("fail to to revert scan iter");
        if (OB_SUCC(ret)) {
          ret = revert_scan_return;
        }
      } else {
        result = NULL;
      }
      NG_TRACE_TIMES(2, revert_scan_iter_end);
    }
  }
  return ret;
}

int ObTableReplace::scan_row(ObExecContext& ctx, const ObPartitionKey& part_key, ObPartitionService* partition_service,
    storage::ObTableScanParam& scan_param, const common::ObNewRowIterator* duplicated_rows,
    common::ObNewRowIterator** result) const
{
  int ret = OB_SUCCESS;
  ObNewRow* dup_row = NULL;
  ObExprCtx expr_ctx;
  ObSQLSessionInfo* my_session = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  NG_TRACE_TIMES(2, replace_start_scan_row);
  if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical plan context failed");
  } else if (OB_FAIL(wrap_expr_ctx(ctx, expr_ctx))) {
    LOG_WARN("wrap expression context failed", K(ret));
  } else if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid physical plan", K(my_phy_plan_));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(const_cast<common::ObNewRowIterator*>(duplicated_rows)->get_next_row(dup_row))) {
      SQL_ENG_LOG(DEBUG, "get conflict row", K(*dup_row));
      ObRowkey tmp_key(dup_row->cells_, dup_row->count_);
      ObRowkey key;
      if (OB_FAIL(tmp_key.deep_copy(key, *expr_ctx.calc_buf_))) {
        LOG_WARN("fail to deep copy rowkey", K(ret));
      } else {
        common::ObNewRange range;
        if (OB_FAIL(range.build_range(index_tid_, key))) {
          LOG_WARN("fail to build key range", K(ret), K_(table_id), K(key));
        } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
          LOG_WARN("fail to push back key range", K(ret), K(range));
        }
      }
    }
    if (ret != OB_ITER_END) {
      LOG_WARN("get next row not return ob_iter_end", K(ret));
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_SUCCESS;
      scan_param.timeout_ = plan_ctx->get_ps_timeout_timestamp();
      ObQueryFlag query_flag(ObQueryFlag::Forward,  // scan_order
          false,                                    // daily_merge
          false,                                    // optimize
          false,                                    // sys scan
          false,                                    // full_row
          false,                                    // index_back
          false,                                    // query_stat
          ObQueryFlag::MysqlMode,                   // sql_mode
          true                                      // read_latest
      );
      scan_param.scan_flag_.flag_ = query_flag.flag_;
      scan_param.reserved_cell_count_ = column_ids_.count();
      scan_param.for_update_ = false;
      scan_param.column_ids_.reset();
      if (OB_FAIL(scan_param.column_ids_.assign(column_ids_))) {
        LOG_WARN("fail to assign column id", K(ret));
      } else if (OB_FAIL(wrap_expr_ctx(ctx, scan_param.expr_ctx_))) {
        LOG_WARN("wrap table replace expr ctx failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        scan_param.virtual_column_exprs_.reset();
        DLIST_FOREACH(node, virtual_column_exprs_)
        {
          const ObColumnExpression* expr = static_cast<const ObColumnExpression*>(node);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("node or node expr is NULL", K(ret));
          } else if (OB_FAIL(scan_param.virtual_column_exprs_.push_back(expr))) {
            LOG_WARN("fail to push back virtual column expr", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        scan_param.limit_param_.limit_ = -1;
        scan_param.limit_param_.offset_ = 0;
        scan_param.trans_desc_ = &my_session->get_trans_desc();
        scan_param.index_id_ = index_tid_;
        scan_param.sql_mode_ = my_session->get_sql_mode();
        scan_param.pkey_ = part_key;
        NG_TRACE_TIMES(2, replace_start_table_scan);
        if (OB_FAIL(my_phy_plan_->get_base_table_version(index_tid_, scan_param.schema_version_))) {
          LOG_WARN("fail to get table schema version", K(ret), K(index_tid_));
        } else if (OB_FAIL(partition_service->table_scan(scan_param, *result))) {
          LOG_WARN("fail to table scan", K(ret));
        }
        NG_TRACE_TIMES(2, replace_end_table_scan);
      }
    }
  }
  return ret;
}

int ObTableReplace::check_values(const ObNewRow& old_row, const ObNewRow& new_row, bool& is_equal) const
{
  int ret = OB_SUCCESS;
  is_equal = true;
  CK(old_row.get_count() >= column_ids_.count());
  CK(new_row.get_count() >= column_ids_.count());
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < column_ids_.count(); i++) {
    if (share::schema::ObColumnSchemaV2::is_hidden_pk_column_id(column_ids_[i])) {
      // skip hiden pk
    } else if (old_row.get_cell(i) != new_row.get_cell(i)) {
      is_equal = false;
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
