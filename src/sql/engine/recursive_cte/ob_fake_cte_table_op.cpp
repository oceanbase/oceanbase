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
#include "sql/engine/recursive_cte/ob_fake_cte_table_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObFakeCTETableSpec, ObOpSpec),
                      column_involved_offset_,
                      column_involved_exprs_,
                      is_bulk_search_,
                      identify_seq_expr_,
                      is_union_distinct_);

int ObFakeCTETableOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("Fail to check physical plan status", K(ret));
  } else if (is_mysql_mode()) {
    if (OB_FAIL(get_next_batch_from_intermedia_table(1, true)) && ret != OB_ITER_END) {
      LOG_WARN("Fail to get next batch data from chunk store", K(ret));
    }
  } else {
    if (empty_) {
      ret = OB_ITER_END;
    } else if (!MY_SPEC.is_bulk_search_ && OB_FAIL(get_next_single_row())) {
      LOG_WARN("Fail to get next pump row", K(ret));
    } else if (MY_SPEC.is_bulk_search_ && OB_FAIL(get_next_bulk_row())) {
      LOG_WARN("Fail to get next bulk row", K(ret));
    }
  }
  return ret;
}

int ObFakeCTETableOp::get_next_single_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pump_row_) && OB_UNLIKELY(MY_SPEC.column_involved_exprs_.count() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Pump row is null", K(ret));
  } else if (OB_NOT_NULL(pump_row_) &&
             OB_FAIL(pump_row_->to_expr(MY_SPEC.column_involved_exprs_, eval_ctx_))) {
    LOG_WARN("Stored row to expr failed", K(ret));
  } else {
    empty_ = true;
  }
  return ret;
}

int ObFakeCTETableOp::get_next_bulk_row()
{
  int ret = OB_SUCCESS;
  int64_t expr_cnt = MY_SPEC.column_involved_exprs_.count();
  if (OB_UNLIKELY(bulk_rows_.empty() && expr_cnt != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Bulk rows is empty", K(ret));
  } else if (OB_UNLIKELY(bulk_rows_.empty())) {
    empty_ = true;
  } else if (OB_UNLIKELY(read_bluk_cnt_ >= bulk_rows_.count()
             || OB_ISNULL(bulk_rows_.at(read_bluk_cnt_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Current bulk row is null or out of range", K(read_bluk_cnt_), K(ret));
  } else if (OB_FAIL(to_expr(MY_SPEC.column_involved_exprs_, MY_SPEC.column_involved_offset_,
                             bulk_rows_.at(read_bluk_cnt_), eval_ctx_))) {
    LOG_WARN("Stored row to expr not in recursive bulk failed", K(ret));
  } else if (is_oracle_mode()) {
    MY_SPEC.identify_seq_expr_->locate_datum_for_write(eval_ctx_).set_uint(read_bluk_cnt_);
  }
  if (OB_SUCC(ret)) {
    read_bluk_cnt_++;
    if (OB_UNLIKELY(read_bluk_cnt_ == bulk_rows_.count())) {
      empty_ = true;
      bulk_rows_.reset();
    }
  }
  return ret;
}

int ObFakeCTETableOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("Fail to check physical plan status", K(ret));
  } else if (is_mysql_mode()) {
    if (OB_FAIL(get_next_batch_from_intermedia_table(max_row_cnt, false))) {
      LOG_WARN("Fail to get next batch data from chunk store", K(ret));
    }
  } else {
    if (empty_) {
      brs_.end_ = true;
      brs_.size_ = 0;
    } else if (!MY_SPEC.is_bulk_search_ && OB_FAIL(get_next_single_batch(max_row_cnt))) {
      LOG_WARN("Fail to get next single batch", K(ret));
    } else if (MY_SPEC.is_bulk_search_ && OB_FAIL(get_next_bulk_batch(max_row_cnt))) {
      LOG_WARN("Fail to get next bulk batch", K(ret));
    }
  }
  return ret;
}

int ObFakeCTETableOp::get_next_single_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_single_row())) {
    if (ret == OB_ITER_END) {
      brs_.end_ = true;
      brs_.size_ = 0;
      ret = OB_SUCCESS;
    } else {
      LOG_INFO("Fail to get result from cte table", K(ret));
    }
  } else {
    brs_.size_ = 1;
  }
  return ret;
}

int ObFakeCTETableOp::get_next_bulk_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = 0;
  int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t expr_cnt = MY_SPEC.column_involved_exprs_.count();
  if (OB_UNLIKELY(bulk_rows_.empty() && expr_cnt != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Bulk rows is empty", K(ret));
  } else if (OB_UNLIKELY(bulk_rows_.empty())) {
    empty_ = true;
  } else if (OB_UNLIKELY(read_bluk_cnt_ >= bulk_rows_.count())
             || OB_ISNULL(bulk_rows_.at(read_bluk_cnt_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Current bulk row is null or out of range", K(read_bluk_cnt_), K(ret));
  } else if (FALSE_IT(read_rows = std::min(batch_size, bulk_rows_.count() - read_bluk_cnt_))) {
  } else if (OB_FAIL(attach_rows(MY_SPEC.column_involved_exprs_, MY_SPEC.column_involved_offset_,
                                 bulk_rows_, read_bluk_cnt_, eval_ctx_, read_rows))) {
    LOG_WARN("Failed to attach rows", K(ret));
  } else if (is_oracle_mode()) {
    ObDatum *datums = MY_SPEC.identify_seq_expr_->locate_datums_for_update(eval_ctx_, read_rows);
    for (int64_t i = 0; i < read_rows; ++i) {
      datums[i].set_uint(read_bluk_cnt_ + i);
    }
  }
  if (OB_SUCC(ret)) {
    brs_.size_ = read_rows;
    brs_.end_ = (0 == read_rows);
    read_bluk_cnt_ += read_rows;
    if (OB_UNLIKELY(read_bluk_cnt_ == bulk_rows_.count())) {
      empty_ = true;
      bulk_rows_.reset();
    }
  }
  return ret;
}

int ObFakeCTETableOp::get_next_batch_from_intermedia_table(const int64_t max_row_cnt, bool is_called_by_get_next_row_interface)
{
  int ret = OB_SUCCESS;

  //MY_SPEC.max_batch_size_ is 0 when turn off vectorization 1.0
  int64_t batch_size = is_called_by_get_next_row_interface ? 1 : std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t read_rows = 0;
  const ObRADatumStore::StoredRow *srows = nullptr;

  reader_age_.inc();
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  for (int64_t i = 0; i < batch_size && OB_SUCC(ret); i++) {
    if (next_read_row_id_ > round_limit_) {
      ret = OB_INDEX_OUT_OF_RANGE;
    } else if (OB_FAIL(intermedia_data_reader_.get_row(next_read_row_id_, srows))) {
      // In general next_read_row_id_ <= round_limit_ < intermedia_data_reader_.get_row_cnt()
      // So ret should never equals to OB_INDEX_OUT_OF_RANGE here, we expect that we can definitly
      // get data
      LOG_WARN("RCTE Fake CTE Table get data from ObRADatumStore failed", K(ret), K(batch_size),
               K(read_rows), K(next_read_row_id_), K(round_limit_));
    } else {
      guard.set_batch_idx(i);

      if (OB_FAIL(to_expr(MY_SPEC.column_involved_exprs_, MY_SPEC.column_involved_offset_,
                          const_cast<ObRADatumStore::StoredRow *>(srows), eval_ctx_))) {
        LOG_WARN("Fail to pass data from store_row to exprs ", K(ret));
      } else {
        read_rows++;
        next_read_row_id_++;
      }
    }
  }

  if (OB_SUCC(ret) || ret == OB_INDEX_OUT_OF_RANGE) {
    for (int64_t i = 0; i < MY_SPEC.column_involved_exprs_.count(); i++) {
      ObExpr *e = MY_SPEC.column_involved_exprs_.at(i);
      e->set_evaluated_projected(eval_ctx_);
      ObEvalInfo &info = e->get_eval_info(eval_ctx_);
      info.notnull_ = false;
      info.point_to_frame_ = false;
    } 
    brs_.size_ = read_rows;
    brs_.end_ = (ret == OB_INDEX_OUT_OF_RANGE);
    read_bluk_cnt_ += read_rows;
    if (is_called_by_get_next_row_interface && ret == OB_INDEX_OUT_OF_RANGE) {
      ret = OB_ITER_END;
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_UNLIKELY(next_read_row_id_ == intermedia_data_reader_.get_row_cnt())) { empty_ = true; }
  }
  return ret;
}

void ObFakeCTETableOp::reuse()
{
  int ret = OB_SUCCESS;
  empty_ = true;
  read_bluk_cnt_ = 0;
  cur_identify_seq_ = 0;
  bulk_rows_.reset();
  next_read_row_id_ = 0;
  round_limit_ = 0;
  intermedia_table_.reuse();
  intermedia_data_reader_.reuse();
  if (OB_NOT_NULL(pump_row_)) {
    allocator_.free(const_cast<ObChunkDatumStore::StoredRow *>(pump_row_));
    pump_row_ = nullptr;
  }
}

void ObFakeCTETableOp::destroy()
{
  intermedia_table_.reset();
  intermedia_data_reader_.reset();
  destroy_mem_context();

  ObOperator::destroy();
}

int ObFakeCTETableOp::add_single_row(ObChunkDatumStore::StoredRow *row)
{
  int ret = OB_SUCCESS;
  /**
   * R union 返回的数据是按照fake cte table的所有基准列来返回的
   * fake cte table的基准列不一定是全部被使用了的，所以要把StoredRow被使用的cell拷贝到new StoredRow中
   */
  const ObChunkDatumStore::StoredRow *new_row = nullptr;
  ObChunkDatumStore::StoredRow *old_row = nullptr;
  if (OB_UNLIKELY(0 == MY_SPEC.column_involved_offset_.count())) {
    empty_ = false;
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fake cte table add nullptr row", KPC(row));
  } else if (OB_FAIL(deep_copy_row(row, new_row, MY_SPEC.column_involved_offset_,
                                    ObSearchMethodOp::ROW_EXTRA_SIZE, allocator_))) {
    LOG_WARN("Fail to deep copy stored row", K(ret));
  } else {
    old_row = const_cast<ObChunkDatumStore::StoredRow *>(pump_row_);
    pump_row_ = new_row;
    empty_ = false;
    if (nullptr != old_row) {
      allocator_.free(old_row);
      old_row = nullptr;
    }
  }
  return ret;
}

int ObFakeCTETableOp::add_single_row_to_intermedia_table(const common::ObIArray<ObExpr *> &exprs,
                                                         ObEvalCtx *ctx)
{
  int ret = OB_SUCCESS;
  ObRADatumStore::StoredRow **sr = nullptr;
  if (OB_FAIL(process_dump())) {
    LOG_WARN("Failed to process dumping logic of CTE Table", K(ret));
  } else if (OB_FAIL(intermedia_table_.add_row(exprs, ctx, sr))) {
    LOG_WARN("Fail to append data into CTE intermedia table", K(ret));
  }
  return ret;
}

int ObFakeCTETableOp::inner_rescan()
{
  int ret = ObOperator::inner_rescan();
  if (!MY_SPEC.is_bulk_search_ && pump_row_ != nullptr) {
    empty_ = false;
  } else if (MY_SPEC.is_bulk_search_ && !bulk_rows_.empty()) {
    empty_ = false;
  }
  return ret;
}

int ObFakeCTETableOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.column_involved_exprs_.count() != MY_SPEC.column_involved_offset_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fake cte table spec", K(ret));
  } else if (OB_ISNULL(MY_SPEC.identify_seq_expr_)) {
    if (is_oracle_mode() && MY_SPEC.is_bulk_search_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null identify seq expr", K(ret));
    }
  } else if (OB_NOT_NULL(MY_SPEC.identify_seq_expr_)) {
    if (!is_oracle_mode() || !MY_SPEC.is_bulk_search_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected not null identify seq expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    if (is_oracle_mode()) {
      ObMemAttr attr(tenant_id, ObModIds::OB_SQL_CTE_ROW, ObCtxIds::WORK_AREA);
      allocator_.set_attr(attr);
    } else {
      // mysql mode
      if (OB_FAIL(init_mem_context())) {
        LOG_WARN("failed to init mem_context", K(ret));
      } else if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(), tenant_id,
                                                 128, // 需填入
                                                 MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else if (OB_FAIL(
                   intermedia_table_.init(0, MTL_ID(), ObCtxIds::WORK_AREA, "FakeCteTable"))) {
        LOG_WARN("Fail to init intermedia table of CTE operator", K(ret));
      } else {
        intermedia_data_reader_.set_iteration_age(&reader_age_);
        intermedia_table_.set_allocator(mem_context_->get_malloc_allocator());
        intermedia_table_.set_mem_stat(&sql_mem_processor_);
        intermedia_table_.set_io_observer(&io_event_observer_);
        intermedia_table_.set_dir_id(sql_mem_processor_.get_dir_id());
      }
    }
  }
  return ret;
}

int ObFakeCTETableOp::inner_close()
{
  int ret = OB_SUCCESS;
  reuse();
  sql_mem_processor_.unregister_profile();
  return ret;
}

int ObFakeCTETableOp::copy_datums(ObChunkDatumStore::StoredRow *row, common::ObDatum *datums,
  int64_t cnt, const common::ObIArray<int64_t> &chosen_datums, char *buf, const int64_t size,
  const int64_t row_size, const uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == row || row->payload_ != buf
                  || size < 0 || nullptr == datums || chosen_datums.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(size), K(datums));
  } else {
    row->cnt_ = static_cast<uint32_t>(chosen_datums.count());
    int64_t pos = sizeof(ObDatum) * row->cnt_ + row_extend_size;
    row->row_size_ = static_cast<int32_t>(row_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < row->cnt_; ++i) {
      int64_t idx = chosen_datums.at(i);
      if (OB_UNLIKELY(idx >= cnt)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(row->payload_), KP(buf),
                  K(size), K(datums), K(idx), K(cnt));
      } else {
        ObDatum *datum = new (&row->cells()[i])ObDatum();
        if (OB_FAIL(datum->deep_copy(datums[idx], buf, size, pos))) {
          LOG_WARN("failed to copy datum", K(ret), K(i), K(pos),
            K(size), K(row_size), K(datums[idx]), K(datums[idx].len_));
        }
      }
    }
  }
  return ret;
}

int ObFakeCTETableOp::deep_copy_row(const ObChunkDatumStore::StoredRow *src_row,
                                    const ObChunkDatumStore::StoredRow *&dst_row,
                                    const ObIArray<int64_t> &chosen_index,
                                    int64_t extra_size,
                                    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row) || OB_ISNULL(src_row->cells())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src row is null", K(ret), K(src_row));
  } else if (chosen_index.empty()) {
    dst_row = nullptr;
  } else {
    char *buf = nullptr;
    int64_t row_size = sizeof(ObDatum) * chosen_index.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < chosen_index.count(); i++) {
      int64_t idx = chosen_index.at(i);
      if (OB_UNLIKELY(idx >= src_row->cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("chosen index greater than src_row count", K(ret),
                  K(chosen_index), K(src_row->cnt_));
      } else {
        row_size += src_row->cells()[idx].len_;
      }
    }
    if (OB_SUCC(ret)) {
      int64_t buffer_len = 0;
      int64_t head_size = sizeof(ObChunkDatumStore::StoredRow);
      int64_t pos = head_size;
      ObChunkDatumStore::StoredRow *new_row = nullptr;
      buffer_len = row_size + head_size + extra_size;
      if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(buffer_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed", K(ret));
      } else if (OB_ISNULL(new_row = new(buf)ObChunkDatumStore::StoredRow())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to new row", K(ret));
      } else if (OB_FAIL(copy_datums(new_row, const_cast<ObDatum *>(src_row->cells()),
                                    src_row->cnt_, chosen_index, buf + pos,
                                    buffer_len - head_size, row_size, extra_size))) {
        LOG_WARN("failed to deep copy row", K(ret), K(buffer_len), K(row_size));
      } else {
        dst_row = new_row;
      }
    }
  }
  return ret;
}

int ObFakeCTETableOp::to_expr(
  const common::ObIArray<ObExpr*> &exprs,
  const common::ObIArray<int64_t> &chosen_index,
  ObChunkDatumStore::StoredRow *row, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr row", K(ret), K(row));
  } else {
    for (int64_t i = 0;  OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *expr = exprs.at(i);
      if (expr->is_const_expr()) {
        continue;
      } else if (chosen_index.at(i) >= row->cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx out of range", K(ret), K(chosen_index), K(row->cnt_), K(chosen_index.at(i)));
      } else {
        const ObDatum &src = row->cells()[chosen_index.at(i)];
        if (OB_LIKELY(expr->is_variable_res_buf())) {
          ObDatum &dst = expr->locate_expr_datum(ctx);
          dst = src;
        } else {
          ObDatum &dst = expr->locate_datum_for_write(ctx);
          dst.pack_ = src.pack_;
          MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
        }
        expr->set_evaluated_projected(ctx);
      }
    } 
  }
  return ret;
}

int ObFakeCTETableOp::to_expr(
  const common::ObIArray<ObExpr*> &exprs,
  const common::ObIArray<int64_t> &chosen_index,
  ObRADatumStore::StoredRow *row, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr row", K(ret), K(row));
  } else {
    for (int64_t i = 0;  OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *expr = exprs.at(i);
      if (expr->is_const_expr()) {
        continue;
      } else if (chosen_index.at(i) >= row->cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx out of range", K(ret), K(chosen_index), K(row->cnt_), K(chosen_index.at(i)));
      } else {
        const ObDatum &src = row->cells()[chosen_index.at(i)];
        // Since we batch output to upper operator
        // We don't know when would these data be consumed by upper operator
        // So we have to deep copy them
        if (OB_LIKELY(expr->is_variable_res_buf())) {
          if (OB_FAIL(expr->deep_copy_datum(ctx, src))) {
            LOG_WARN("expr datum deep copy failed", K(ret));
          }
        } else {
          ObDatum &dst = expr->locate_datum_for_write(ctx);
          dst.pack_ = src.pack_;
          MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
        }
      }
    } 
  }
  return ret;
}

int ObFakeCTETableOp::attach_rows(
    const common::ObIArray<ObExpr*> &exprs,
    const common::ObIArray<int64_t > &chosen_index,
    const common::ObArray<ObChunkDatumStore::StoredRow *> &srows,
    const int64_t rows_offset, ObEvalCtx &ctx, const int64_t read_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < exprs.count(); col_idx++) {
    ObExpr *e = exprs.at(col_idx);
    int64_t idx = chosen_index.at(col_idx);
    if (e->is_const_expr()) {
      continue;
    } else if (OB_LIKELY(e->is_variable_res_buf())) {
      ObDatum *datums = e->locate_batch_datums(ctx);
      if (!e->is_batch_result()) {
        if (OB_UNLIKELY(idx >= srows.at(0)->cnt_)) { 
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx out of range", K(ret), K(idx), K(chosen_index), K(srows.at(0)->cnt_));
        } else {
          datums[0] = srows.at(0)->cells()[idx];
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < read_rows; i++) {
          if  (OB_UNLIKELY(idx >= srows.at(rows_offset+i)->cnt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("idx out of range", K(ret), K(idx), K(read_rows),
                K(i), K(chosen_index), K(srows.at(rows_offset+i)->cnt_));
          } else {
            datums[i] = srows.at(rows_offset+i)->cells()[idx];
          }
        }
      }
    } else {
      if (!e->is_batch_result()) {
        ObDatum *datums = e->locate_datums_for_update(ctx, 1);
        if (OB_UNLIKELY(idx >= srows.at(0)->cnt_)) { 
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx out of range", K(ret), K(idx), K(chosen_index), K(srows.at(0)->cnt_));
        } else {
          const ObDatum &src = srows.at(0)->cells()[idx];
          ObDatum &dst = datums[0];
          dst.pack_ = src.pack_;
          MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
        }
      } else {
        ObDatum *datums = e->locate_datums_for_update(ctx, read_rows);
        for (int64_t i = 0; OB_SUCC(ret) && i < read_rows; i++) {
          if  (OB_UNLIKELY(idx >= srows.at(rows_offset+i)->cnt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("idx out of range", K(ret), K(idx), K(read_rows),
                K(i), K(chosen_index), K(srows.at(rows_offset+i)->cnt_));
          } else {
            const ObDatum &src = srows.at(rows_offset+i)->cells()[idx];
            ObDatum &dst = datums[i];
            dst.pack_ = src.pack_;
            MEMCPY(const_cast<char *>(dst.ptr_), src.ptr_, src.len_);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      e->set_evaluated_projected(ctx);
      ObEvalInfo &info = e->get_eval_info(ctx);
      info.notnull_ = false;
      info.point_to_frame_ = false;
    }
  }
  return ret;
}

int ObFakeCTETableOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    ObSQLSessionInfo *session = ctx_.get_my_session();
    uint64_t tenant_id = session->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, ObModIds::OB_SQL_CTE_ROW, ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    }
  }
  return ret;
}

void ObFakeCTETableOp::destroy_mem_context()
{
  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
}

int ObFakeCTETableOp::process_dump()
{
  int ret = common::OB_SUCCESS;

  bool updated = false;
  bool should_dump = false;
  const static int64_t UPDATE_MEM_SIZE_PERIODIC_CNT = 1024;
  ObLamdaSubstituteRCTEMaxAvailableMemChecker max_available_mem_checker(
    intermedia_table_.get_row_cnt());
  ObLamdaSubstituteRCTEExtendMaxMemChecker extend_max_mem_checker(
    sql_mem_processor_.get_data_size());
  if (!GCONF.is_sql_operator_dump_enabled()) {
    // do nothing, disable dump
  } else if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
               &mem_context_->get_malloc_allocator(), max_available_mem_checker, updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if ((updated || need_dump())
             && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                  &mem_context_->get_malloc_allocator(), extend_max_mem_checker, should_dump,
                  sql_mem_processor_.get_data_size()))) {
    LOG_WARN("fail to extend max memory size", K(ret), K(updated));
  } else if (should_dump) {
    // for hash deduplicate and later read request reasons
    // keep as much data in memory as possiable
    if (OB_FAIL(intermedia_table_.dump(false, intermedia_table_.get_mem_hold() >> 2))) {
      LOG_WARN("Failed to dump CTE table data", K(ret), K(intermedia_table_.get_mem_hold()));
    }
    LOG_TRACE("trace CTE table dump", K(sql_mem_processor_.get_data_size()),
              K(intermedia_table_.get_inmemory_rows()), K(sql_mem_processor_.get_mem_bound()));
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
