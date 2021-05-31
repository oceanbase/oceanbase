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
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/engine/dml/ob_table_update.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"
namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
namespace sql {

ObTableUpdate::ObTableUpdate(ObIAllocator& alloc)
    : ObTableModify(alloc),
      updated_column_ids_(alloc),
      updated_column_infos_(alloc),
      is_global_index_(false),
      new_spk_exprs_()
{}

ObTableUpdate::~ObTableUpdate()
{}

void ObTableUpdate::reset()
{
  updated_column_infos_.reset();
  updated_column_ids_.reset();
  is_global_index_ = false;
  ObTableModify::reset();
}

void ObTableUpdate::reuse()
{
  updated_column_infos_.reuse();
  updated_column_ids_.reuse();
  is_global_index_ = false;
  ObTableModify::reuse();
}

int ObTableUpdate::set_updated_column_info(
    int64_t array_index, uint64_t column_id, uint64_t project_index, bool auto_filled_timestamp)
{
  int ret = OB_SUCCESS;
  ColumnContent column;
  column.projector_index_ = project_index;
  column.auto_filled_timestamp_ = auto_filled_timestamp;
  CK(array_index >= 0 && array_index < updated_column_ids_.count());
  CK(array_index >= 0 && array_index < updated_column_infos_.count());
  if (OB_SUCC(ret)) {
    updated_column_ids_.at(array_index) = column_id;
    updated_column_infos_.at(array_index) = column;
  }
  return ret;
}

int ObTableUpdate::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateCtx* update_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  int64_t schema_version = 0;
  int64_t binlog_row_image = ObBinlogRowImage::FULL;
  NG_TRACE(update_open);
  if (OB_FAIL(ObTableModify::inner_open(ctx))) {
    LOG_WARN("open child operator failed", K(ret));
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed");
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid physical plan", K(ret), K(my_phy_plan_));
  } else if (OB_FAIL(my_phy_plan_->get_base_table_version(index_tid_, schema_version))) {
    LOG_WARN("failed to get base table version", K(ret));
  } else if (OB_FAIL(my_session->get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    update_ctx->dml_param_.timeout_ = plan_ctx->get_ps_timeout_timestamp();
    update_ctx->dml_param_.schema_version_ = schema_version;
    update_ctx->dml_param_.is_total_quantity_log_ = (ObBinlogRowImage::FULL == binlog_row_image);
    update_ctx->dml_param_.tz_info_ = TZ_INFO(my_session);
    update_ctx->dml_param_.sql_mode_ = my_session->get_sql_mode();
    update_ctx->dml_param_.prelock_ = my_session->get_prelock();
    update_ctx->dml_param_.table_param_ = &table_param_;
    update_ctx->dml_param_.tenant_schema_version_ = plan_ctx->get_tenant_schema_version();
    update_ctx->dml_param_.is_ignore_ = is_ignore_;
    if (gi_above_) {
      if (OB_FAIL(get_gi_task(ctx))) {
        LOG_WARN("get granule iterator task failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_table_update(ctx))) {
        LOG_WARN("do table update failed", K(ret));
      }
    }
  }
  NG_TRACE(update_end);
  return ret;
}

int ObTableUpdate::do_table_update(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTableUpdateCtx* update_ctx = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K(ret), K_(id));
  } else if (update_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(update_ctx->iter_end_), K(get_id()));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(get_part_location(ctx, update_ctx->part_infos_))) {
    LOG_WARN("get part location failed", K(ret));
  } else if (is_returning()) {
    // do nothing, handle in ObTableUpdateReturning
  } else if (OB_ISNULL(plan_ctx = ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical plan context failed", K(ret));
  } else if (OB_FAIL(update_rows(ctx, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("update rows to partition storage failed", K(ret), K(column_ids_), K(updated_column_ids_));
    }
  } else if (!from_multi_table_dml()) {
    // dml meta info will be counted in multiple table dml operator, not here
    plan_ctx->add_row_matched_count(update_ctx->get_found_rows());
    plan_ctx->add_row_duplicated_count(update_ctx->get_changed_rows());
    plan_ctx->add_affected_rows(my_session->get_capability().cap_flags_.OB_CLIENT_FOUND_ROWS
                                    ? update_ctx->get_found_rows()
                                    : update_ctx->get_affected_rows());
  }
  SQL_ENG_LOG(DEBUG, "update rows end", K(ret), K(affected_rows), K(column_ids_), K(updated_column_ids_));
  return ret;
}

int ObTableUpdate::inner_close(ObExecContext& ctx) const
{
  return ObTableModify::inner_close(ctx);
}

int ObTableUpdate::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateInput* update_input = NULL;
  ObTableUpdateCtx* update_ctx = NULL;
  if (!gi_above_ || from_multi_table_dml()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table update rescan not supported", K(ret));
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table update context is null", K(ret), K(get_id()));
  } else if (OB_FAIL(ObTableModify::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else {
    update_ctx->found_rows_ = 0;
    update_ctx->changed_rows_ = 0;
    update_ctx->affected_rows_ = 0;
    update_ctx->has_got_old_row_ = false;
    update_ctx->part_infos_.reset();
    update_ctx->part_key_.reset();
    if (nullptr != update_ctx->rowkey_dist_ctx_) {
      update_ctx->rowkey_dist_ctx_->clear();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_gi_task(ctx))) {
      LOG_WARN("get granule task failed", K(ret));
    } else if (OB_FAIL(do_table_update(ctx))) {
      LOG_WARN("do table update failed", K(ret));
    }
  }
  return ret;
}

int ObTableUpdate::get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateCtx* update_ctx = NULL;
  ObPartitionArray part_keys;
  // update operator must has project operation
  if (OB_FAIL(try_check_status(ctx))) {
    LOG_WARN("check status failed", K(ret));
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else {
    update_ctx->expr_ctx_.calc_buf_->reuse();
    update_ctx->expr_ctx_.row_ctx_.reset();
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!update_ctx->has_got_old_row_) {
    NG_TRACE_TIMES(2, update_start_next_row);
    const ObNewRow* full_row = NULL;
    bool is_updated = false;
    // fiter out unchanged row
    while (OB_SUCC(ret) && !is_updated && OB_SUCC(inner_get_next_row(ctx, full_row))) {
      CK(OB_NOT_NULL(full_row));
      if (OB_SUCC(ret)) {
        project_old_and_new_row(*update_ctx, *full_row);

        // check_rowkey_is_null and check_rowkey_whether_distinct only works for
        // mysql mode, mainly for the following cases
        // update t2 left outer join t1 on t1.c1 = t2.c1 set t1.c2 = t2.c2;
        if (OB_SUCC(ret) && !from_multi_table_dml()) {
          bool is_null = false;
          if (need_filter_null_row_) {
            if (OB_FAIL(check_rowkey_is_null(update_ctx->old_row_, primary_key_ids_.count(), is_null))) {
              LOG_WARN("check rowkey is null failed", K(ret), K(update_ctx->get_cur_row()), K(primary_key_ids_));
            } else if (is_null) {
              continue;
            }
          } else {
#if !defined(NDEBUG)
            if (need_check_pk_is_null()) {
              if (OB_FAIL(check_rowkey_is_null(update_ctx->old_row_, primary_key_ids_.count(), is_null))) {
                LOG_WARN("failed to check rowkey is null", K(ret));
              } else if (OB_UNLIKELY(is_null)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("update row failed validity check", K(ret));
              }
            }
#endif
          }
        }
        if (OB_SUCC(ret) && !from_multi_table_dml()) {
          bool is_distinct = false;
          if (OB_FAIL(check_rowkey_whether_distinct(ctx,
                  update_ctx->old_row_,
                  primary_key_ids_.count(),
                  distinct_algo_,
                  update_ctx->rowkey_dist_ctx_,
                  is_distinct))) {
            LOG_WARN("check rowkey whether distinct failed", K(ret));
          } else if (!is_distinct) {
            continue;
          }
        }
        if (OB_SUCC(ret) && !from_multi_table_dml()) {
          ObNewRow& old_row = update_ctx->old_row_;
          ObNewRow& new_row = update_ctx->new_row_;
          OZ(check_row_null(ctx, new_row, column_infos_), new_row);
        }
      }
      if (OB_SUCC(ret)) {
        // check update row whether changed
        if (OB_LIKELY(!from_multi_table_dml() && !is_returning())) {
          // if update operator from multi table dml,
          // the row value will be check in multiple table dml operator
          // dml meta info will also be counted in multiple table dml operator
          OZ(check_updated_value(*update_ctx, *this, update_ctx->old_row_, update_ctx->new_row_, is_updated));
        } else if (OB_LIKELY(check_row_whether_changed(update_ctx->new_row_))) {
          is_updated = true;
        }
      }
      if (OB_SUCC(ret)) {
        ObNewRow& old_row = update_ctx->old_row_;
        ObNewRow& new_row = update_ctx->new_row_;
        if (is_updated) {
          if (!from_multi_table_dml()) {
            bool is_filtered = false;
            OZ(ForeignKeyHandle::do_handle(*update_ctx, fk_args_, old_row, new_row), old_row, new_row);
            OZ(ObPhyOperator::filter_row_for_check_cst(
                update_ctx->expr_ctx_, new_row, check_constraint_exprs_, is_filtered));
            if (is_filtered && OB_SUCC(ret)) {
              ret = OB_ERR_CHECK_CONSTRAINT_VIOLATED;
              LOG_WARN("row is filtered by check filters, running is stopped", K(ret));
            }
          }
        } else {
          if (OB_FAIL(build_lock_row(*update_ctx, update_ctx->old_row_))) {
            LOG_WARN("build lock row failed", K(ret), K(update_ctx->old_row_));
          } else if (OB_FAIL(lock_row(ctx, update_ctx->lock_row_, update_ctx->dml_param_, update_ctx->part_key_))) {
            if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
              LOG_WARN("fail to lock row", K(ret), K(update_ctx->lock_row_), K(update_ctx->part_key_));
            }
          } else {
            LOG_DEBUG("lock row", K(ret), K(update_ctx->lock_row_), K(update_ctx->part_key_));
          }
        }
      }
    }  // while
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("get next row from child operator failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      // old row, keep old projector
      row = &(update_ctx->old_row_);
      LOG_DEBUG("get old row", K(update_ctx->old_row_));
      update_ctx->has_got_old_row_ = true;
    }
  } else {
    // new row
    const ObNewRow& new_row = update_ctx->new_row_;
    DLIST_FOREACH(cur_expr, new_spk_exprs_)
    {
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current new spke expr is null", K(new_spk_exprs_));
      } else {
        const ObColumnExpression* new_spk_expr = static_cast<const ObColumnExpression*>(cur_expr);
        int64_t result_idx = new_spk_expr->get_result_index();
        if (OB_UNLIKELY(result_idx < 0) || OB_UNLIKELY(result_idx >= new_row.count_)) {
          LOG_WARN("result index is invalid", K(ret), K(result_idx), K(new_row.count_));
        } else if (OB_FAIL(new_spk_expr->calc(update_ctx->expr_ctx_, new_row, new_row.cells_[result_idx]))) {
          LOG_WARN("calc new spk expr failed", K(ret), KPC(new_spk_expr), K(result_idx), K(new_row));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("get new row", K(update_ctx->new_row_));
      row = &(update_ctx->new_row_);
      update_ctx->has_got_old_row_ = false;
    }
  }
  NG_TRACE_TIMES(2, update_end_next_row);
  return ret;
}

int ObTableUpdate::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateCtx* update_ctx = NULL;
  if (OB_FAIL(ObTableModify::switch_iterator(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch single child operator iterator failed", K(ret));
    }
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctx is null", K(ret));
  } else {
    update_ctx->has_got_old_row_ = false;
  }
  return ret;
}

bool ObTableUpdate::check_row_whether_changed(const ObNewRow& new_row) const
{
  bool bret = false;
  if (updated_column_infos_.count() > 0 && new_row.is_valid()) {
    int64_t projector_index = updated_column_infos_.at(0).projector_index_;
    if (projector_index >= 0 && projector_index < new_row.get_count()) {
      const ObObj& updated_value = new_row.get_cell(projector_index);
      bret = !(updated_value.is_ext() && ObActionFlag::OP_LOCK_ROW == updated_value.get_ext());
    }
  }
  return bret;
}

int ObTableUpdate::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateCtx* update_ctx = NULL;
  if (OB_ISNULL(child_op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("child_op_ is null");
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (update_ctx->iter_end_) {
    LOG_DEBUG("can't get gi task, iter end", K(get_id()), K(update_ctx->iter_end_));
    ret = OB_ITER_END;
  } else {
    if (from_multi_table_dml()) {
      if (update_ctx->part_row_cnt_ <= 0) {
        if (update_ctx->cur_part_idx_ < update_ctx->part_infos_.count()) {
          update_ctx->part_row_cnt_ = update_ctx->part_infos_.at(update_ctx->cur_part_idx_).part_row_cnt_;
          update_ctx->part_key_ = update_ctx->part_infos_.at(update_ctx->cur_part_idx_).partition_key_;
          ++update_ctx->cur_part_idx_;
        }
      }
      if (OB_SUCC(ret)) {
        --update_ctx->part_row_cnt_;
      }
    }
    if (OB_SUCC(ret)) {
      ret = child_op_->get_next_row(ctx, row);
    }
  }

  if (OB_ITER_END == ret) {
    NG_TRACE(update_iter_end);
  }
  return ret;
}

void ObTableUpdate::project_old_and_new_row(ObTableUpdateCtx& ctx, const ObNewRow& full_row) const
{
  ctx.full_row_ = full_row;
  ObNewRow& new_row = ctx.new_row_;
  ObNewRow& old_row = ctx.old_row_;
  new_row.cells_ = full_row.cells_;
  new_row.count_ = full_row.count_;
  new_row.projector_ = ctx.new_row_projector_;
  new_row.projector_size_ = ctx.new_row_projector_size_;
  old_row = full_row;
  // old row and new row have the same projector size
  // but full row projector size is old row projector size + updated column count
  // so old row copy from full row and must reset projector size
  old_row.projector_size_ = ctx.new_row_projector_size_;
}

int ObTableUpdate::build_lock_row(ObTableUpdateCtx& update_ctx, const ObNewRow& old_row) const
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(update_ctx.lock_row_.cells_));
  CK(update_ctx.lock_row_.count_ <= old_row.get_count());
  for (int64_t i = 0; OB_SUCC(ret) && i < update_ctx.lock_row_.count_; ++i) {
    update_ctx.lock_row_.cells_[i] = old_row.get_cell(i);
  }
  return ret;
}

int64_t ObTableUpdate::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_TID,
      table_id_,
      K_(index_tid),
      N_CID,
      column_ids_,
      N_UPDATED_CID,
      updated_column_ids_,
      K_(updated_column_infos),
      K_(is_global_index));
  return pos;
}

int ObTableUpdate::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateCtx* op_ctx = NULL;
  OZ(CREATE_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id(), get_type(), op_ctx));
  CK(OB_NOT_NULL(op_ctx));
  OZ(op_ctx->alloc_row_cells(projector_size_, op_ctx->lock_row_));
  if (OB_SUCC(ret)) {
    op_ctx->new_row_projector_ = projector_;
    op_ctx->new_row_projector_size_ = projector_size_;
  }
  return ret;
}

inline int ObTableUpdate::update_rows(ObExecContext& ctx, int64_t& affected_rows) const
{
  int ret = OB_SUCCESS;
  ObTableUpdateCtx* update_ctx = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  ObPartitionService* partition_service = NULL;
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session is null");
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(update_ctx = GET_PHY_OPERATOR_CTX(ObTableUpdateCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_UNLIKELY(update_ctx->part_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part infos is empty", K(ret));
  } else if (OB_LIKELY(update_ctx->part_infos_.count() == 1)) {
    update_ctx->part_key_ = update_ctx->part_infos_.at(0).partition_key_;
    ObDMLRowIterator dml_row_iter(ctx, *this);
    if (OB_FAIL(dml_row_iter.init())) {
      LOG_WARN("init dml row iterator", K(ret));
    } else if (OB_FAIL(partition_service->update_rows(my_session->get_trans_desc(),
                   update_ctx->dml_param_,
                   update_ctx->part_key_,
                   column_ids_,
                   updated_column_ids_,
                   &dml_row_iter,
                   affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("insert row to partition storage failed", K(ret));
      }
    }
  } else {
    const ObNewRow* old_row = NULL;
    const ObNewRow* new_row = NULL;
    if (OB_FAIL(update_ctx->create_cur_rows(2, update_ctx->new_row_projector_size_, NULL, 0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("create current rows failed", K(ret), K_(projector_size));
    }
    while (OB_SUCC(ret) && OB_SUCC(get_next_row(ctx, old_row)) && OB_SUCC(get_next_row(ctx, new_row))) {
      if (OB_FAIL(copy_cur_row_by_projector(update_ctx->cur_rows_[0], old_row))) {
        LOG_WARN("copy old row failed", K(ret));
      } else if (OB_FAIL(copy_cur_row_by_projector(update_ctx->cur_rows_[1], new_row))) {
        LOG_WARN("copy new row failed", K(ret));
      } else if (OB_FAIL(partition_service->update_row(my_session->get_trans_desc(),
                     update_ctx->dml_param_,
                     update_ctx->part_key_,
                     column_ids_,
                     updated_column_ids_,
                     *old_row,
                     *new_row))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("update row to partition storage failed", K(ret));
        }
      } else {
        LOG_DEBUG("update multi part", K_(update_ctx->part_key), K(*old_row), K(*new_row));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("process update row failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableUpdate)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObTableUpdate, ObTableModify));
  LST_DO_CODE(OB_UNIS_ENCODE, updated_column_ids_, updated_column_infos_, is_global_index_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_dlist(new_spk_exprs_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize calc_exprs_", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableUpdate)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObTableUpdate, ObTableModify));
  LST_DO_CODE(OB_UNIS_DECODE, updated_column_ids_, updated_column_infos_, is_global_index_);
  OB_UNIS_DECODE_EXPR_DLIST(ObColumnExpression, new_spk_exprs_, my_phy_plan_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableUpdate)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObTableUpdate, ObTableModify));
  LST_DO_CODE(OB_UNIS_ADD_LEN, updated_column_ids_, updated_column_infos_, is_global_index_);
  len += get_dlist_serialize_size(new_spk_exprs_);
  return len;
}
}  // namespace sql
}  // namespace oceanbase
