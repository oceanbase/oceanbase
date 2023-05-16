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

#include "ob_px_row_store.h"
#include "common/object/ob_object.h"
#include "common/cell/ob_cell_writer.h"
#include "common/cell/ob_cell_reader.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_tenant_mem_manager.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

/*
 * 本文件说明：
 * 为了提高效率，在反序列化流程上 ObPxNewRow 做了一些比较 trick 的事情
 *
 * 一般流程：
 *  obj -> netbuf -> transport -> netbuf -> obj -> use it
 *      sender           |            receiver
 *
 * ObPxNewRow 流程：
 *  obj -> netbuf -> transport -> netbuf -> copy netbuf to local buf -> obj -> use it
 *      sender           |            receiver
 *
 * 为什么要 copy netbuf to local buf 呢？因为 process(DtlMsg) 结束后 DtlMsg 还需要保持住，
 * 不能随着 process() 结束、netbuf 释放而释放
 */

void ObPxNewRow::set_eof_row()
{
  row_cell_count_ = EOF_ROW_FLAG;
  des_row_buf_size_ = 0;
  des_row_buf_ = NULL;
}

OB_DEF_SERIALIZE(ObPxNewRow)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(row_cell_count_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to serialize row_cell_count", K_(row_cell_count), K(ret));
  } else if (OB_LIKELY(NULL != row_)) {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < row_->get_count(); ++idx) {
      const ObObj &cell = row_->get_cell(idx);
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, cell))) {
        LOG_WARN("fail append cell to buf", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxNewRow)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(row_cell_count_);
  if (OB_LIKELY(NULL != row_ && row_cell_count_ > 0)) {
    for (int64_t idx = 0; idx < row_->get_count(); ++idx) {
      const ObObj &cell = row_->get_cell(idx);
      len += serialization::encoded_length(cell);
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObPxNewRow)
{
  int ret = OB_SUCCESS;
  // reset value
  des_row_buf_ = NULL;
  des_row_buf_size_ = 0;
  OB_UNIS_DECODE(row_cell_count_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to deserialize row_cell_count", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_LIKELY(row_cell_count_ > 0)) {
    if (OB_UNLIKELY(pos >= data_len)) {
      ret = OB_SERIALIZE_ERROR;
      LOG_WARN("invalid serialization data", K(pos), K(data_len), K_(row_cell_count), K(ret));
    } else {
      // 延迟到 get_row 阶段读取 row 的 cells
      des_row_buf_ = (char*)buf + pos;
      des_row_buf_size_ = data_len - pos;
      pos += des_row_buf_size_;
    }
  }
  return ret;
}

// 用于将 row 从 DTL 内存中拷贝到 get_next_row 上下文中对外吐出
// 如果不拷贝，则 DTL process 调用结束后， row 的内存就会被释放,
// get_next_row 中获得的将是非法内存引用
int ObPxNewRow::deep_copy(ObIAllocator &alloc, const ObPxNewRow &other)
{
  int ret = OB_SUCCESS;
  row_cell_count_ = other.row_cell_count_;
  des_row_buf_size_ = other.des_row_buf_size_;
  if (des_row_buf_size_ > 0) {
    des_row_buf_ = static_cast<char *>(alloc.alloc(des_row_buf_size_));
    if (OB_ISNULL(des_row_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      MEMCPY(des_row_buf_, other.des_row_buf_, des_row_buf_size_);
    }
  } else {
    des_row_buf_ = NULL;
    if (row_cell_count_ == EOF_ROW_FLAG) {
      ret = OB_ITER_END; // 用特殊值标记最后一行
    }
  }
  return ret;
}

// 将远端传来的 row 反序列出来，构造成 ObNewRow 结构
int ObPxNewRow::get_row_from_serialization(ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (row_cell_count_ == EOF_ROW_FLAG) {
    ret = OB_ITER_END; // 用特殊值标记最后一行
  } else if (OB_ISNULL(des_row_buf_) ||
             OB_ISNULL(row.cells_) ||
             row_cell_count_ > row.count_) {
    // 注意：当接收端需要添加表达式到 row 里时，row_cell_count_ 可能小于 row.count_
    //       例如， select 1+1, row from distr_table;
    ret = OB_NOT_INIT;
    LOG_WARN("row not init",
             KP_(des_row_buf),
             K_(row_cell_count),
             "row_cell_count", row.count_,
             "row_prj_count", row.get_count(),
             KP(row.cells_));
  } else {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cell_count_; ++i) {
      if (OB_FAIL(serialization::decode(des_row_buf_, des_row_buf_size_, pos, row.cells_[i]))) {
        LOG_WARN("fail deserialize cell", K(i), K_(row_cell_count), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(pos != des_row_buf_size_)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("deserialize row fail", K_(row_cell_count), K(pos), K_(des_row_buf_size), K(ret));
      }
    }
  }
  return ret;
}

int ObReceiveRowReader::add_buffer(dtl::ObDtlLinkedBuffer &buf, bool &transferred)
{
  int ret = OB_SUCCESS;
  transferred = false;
  if (!buf.is_data_msg()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not data message", K(ret));
  } else if (buf.msg_type() < 0) {
    // for interm result iterator.
    dtl::ObDtlMsgType msg_type = static_cast<dtl::ObDtlMsgType>(-buf.msg_type());
    if (dtl::PX_DATUM_ROW == msg_type) {
      if (NULL != datum_iter_ && datum_iter_->is_valid() && datum_iter_->has_next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rows must be all iterated before new iterate added", K(ret));
      } else {
        datum_iter_ = reinterpret_cast<ObChunkDatumStore::Iterator *>(buf.buf());
      }
    } else {
      if (NULL != row_iter_ && row_iter_->is_valid() && row_iter_->has_next()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rows must be all iterated before new iterate added", K(ret));
      } else {
        row_iter_ = reinterpret_cast<ObChunkRowStore::Iterator *>(buf.buf());
      }
    }
  } else {
    // add buffer to receive list.
    int64_t rows = 0;
    if (dtl::PX_DATUM_ROW == buf.msg_type()) {
      auto block = reinterpret_cast<ObChunkDatumStore::Block *>(buf.buf());
      rows = block->rows_;
      if (rows > 0 && OB_FAIL(block->swizzling(NULL))) {
        LOG_WARN("block swizzling failed", K(ret));
      }
    } else {
      auto block = reinterpret_cast<ObChunkRowStore::Block *>(buf.buf());
      rows = block->rows_;
      if (rows > 0 && OB_FAIL(block->swizzling(NULL))) {
        LOG_WARN("block swizzling failed", K(ret));
      }
    }
    if (OB_SUCC(ret)){
      if (rows > 0) {
        transferred = true;
        LOG_DEBUG("add rows to reader", K(rows), KP(this));
        recv_list_rows_ += rows;
        // add buffer to receive list
        buf.next_ = NULL;
        if (NULL == recv_head_) {
          recv_head_ = &buf;
          recv_tail_ = &buf;

          cur_iter_pos_ = 0;
          cur_iter_rows_ = 0;
        } else {
          recv_tail_->next_ = &buf;
          recv_tail_ = &buf;
        }
      } else {
        // no need to add buffer with no rows, keep %transferred false, return OB_ITER_END
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

void ObReceiveRowReader::free(dtl::ObDtlLinkedBuffer *buf)
{
  // free buffer to DFC memory manager, see: ObDtlBasicChannel::free_buf()
  if (NULL != buf) {
    LOG_DEBUG("free dtl linked buffer", KP(buf), K(buf->tenant_id()));
    int ret = OB_SUCCESS;
    auto mgr = DTL.get_dfc_server().get_tenant_mem_manager(buf->tenant_id());
    CK(NULL != mgr);
    OZ(mgr->free(buf));
  }
}

inline void ObReceiveRowReader::free_buffer_list(dtl::ObDtlLinkedBuffer *buf)
{
  while (NULL != buf) {
    dtl::ObDtlLinkedBuffer *next = reinterpret_cast<dtl::ObDtlLinkedBuffer *>(buf->next_);
    free(buf);
    buf = next;
  }
}

void ObReceiveRowReader::move_to_iterated(const int64_t rows)
{
  auto cur = recv_head_;
  if (recv_tail_ == recv_head_) {
    recv_tail_ = NULL;
    recv_head_ = NULL;
  } else {
    recv_head_ = reinterpret_cast<dtl::ObDtlLinkedBuffer *>(recv_head_->next_);
  }

  cur->next_ = iterated_buffers_;
  iterated_buffers_ = cur;

  recv_list_rows_ -= rows;
  cur_iter_rows_ = 0;
  cur_iter_pos_ = 0;
}

template <typename BLOCK, typename ROW>
const ROW *ObReceiveRowReader::next_store_row()
{
  const ROW *srow = NULL;
  if (NULL != recv_head_) {
    BLOCK *b = reinterpret_cast<BLOCK *>(recv_head_->buf());
    if (cur_iter_rows_ == b->rows_) {
      move_to_iterated(b->rows_);
      if (NULL != recv_head_) {
        b = reinterpret_cast<BLOCK *>(recv_head_->buf());
      } else {
        b = NULL;
      }
    }
    if (NULL != b) {
      int ret = b->get_store_row(cur_iter_pos_, srow);
      if (OB_FAIL(ret)) {
        LOG_WARN("fetch store row failed", K(ret));
      } else {
        cur_iter_rows_ += 1;
      }
    }
  }
  return srow;
}

int ObReceiveRowReader::get_next_row(common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (NULL != row_iter_) {
    ret = row_iter_->get_next_row(row);
  } else {
    free_iterated_buffers();
    const ObChunkRowStore::StoredRow *srow
        = next_store_row<ObChunkRowStore::Block, ObChunkRowStore::StoredRow>();
    if (NULL == srow) {
      ret = OB_ITER_END;
    } else {
      ret = ObChunkRowStore::RowIterator::store_row2new_row(row, *srow);
    }
  }
  return ret;
}

int ObReceiveRowReader::to_expr(const ObChunkDatumStore::StoredRow *srow,
                                const ObIArray<ObExpr*> &dynamic_const_exprs,
                                const ObIArray<ObExpr*> &exprs,
                                ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(srow) || (srow->cnt_ != exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (uint32_t i = 0; i < srow->cnt_; ++i) {
      if (exprs.at(i)->is_static_const_) {
        continue;
      } else {
        exprs.at(i)->locate_expr_datum(eval_ctx) = srow->cells()[i];
        exprs.at(i)->set_evaluated_projected(eval_ctx);
      }
    }
    // deep copy dynamic const expr datum
    if (dynamic_const_exprs.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < dynamic_const_exprs.count(); i++) {
        ObExpr *expr = dynamic_const_exprs.at(i);
        if (0 == expr->res_buf_off_) {
          // for compat 4.0, do nothing
        } else if (OB_FAIL(expr->deep_copy_self_datum(eval_ctx))) {
          LOG_WARN("fail to deep copy datum", K(ret), K(eval_ctx), K(*expr));
        }
      }
    }
  }
  return ret;
}

int ObReceiveRowReader::get_next_row(const ObIArray<ObExpr*> &exprs,
                                     const ObIArray<ObExpr*> &dynamic_const_exprs,
                                     ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (NULL != datum_iter_) {
    const ObChunkDatumStore::StoredRow *srow = NULL;
    if (OB_FAIL(datum_iter_->get_next_row(srow))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next stored row failed", K(ret));
      }
    } else {
      ret = to_expr(srow, dynamic_const_exprs, exprs, eval_ctx);
    }
  } else {
    free_iterated_buffers();
    const ObChunkDatumStore::StoredRow *srow
        = next_store_row<ObChunkDatumStore::Block, ObChunkDatumStore::StoredRow>();
    if (NULL == srow) {
      ret = OB_ITER_END;
    } else {
      ret = to_expr(srow, dynamic_const_exprs, exprs, eval_ctx);
    }
  }

  return ret;
}

int ObReceiveRowReader::attach_rows(const common::ObIArray<ObExpr*> &exprs,
                                    const ObIArray<ObExpr*> &dynamic_const_exprs,
                                    ObEvalCtx &eval_ctx,
                                    const ObChunkDatumStore::StoredRow **srows,
                                    const int64_t read_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(srows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t col_idx = 0; col_idx < exprs.count(); col_idx++) {
      if (exprs.at(col_idx)->is_static_const_) {
        continue;
      } else {
        ObExpr *e = exprs.at(col_idx);
        ObDatum *datums = e->locate_batch_datums(eval_ctx);
        if (!e->is_batch_result()) {
          datums[0] = srows[0]->cells()[col_idx];
        } else {
          for (int64_t i = 0; i < read_rows; i++) {
            datums[i] = srows[i]->cells()[col_idx];
          }
        }
        e->set_evaluated_projected(eval_ctx);
        ObEvalInfo &info = e->get_eval_info(eval_ctx);
        info.notnull_ = false;
        info.point_to_frame_ = false;
      }
    }
    // deep copy dynamic const expr datum
    if (OB_SUCC(ret) && dynamic_const_exprs.count() > 0 && read_rows > 0) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
      batch_info_guard.set_batch_size(read_rows);
      batch_info_guard.set_batch_idx(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < dynamic_const_exprs.count(); i++) {
        ObExpr *expr = dynamic_const_exprs.at(i);
        OB_ASSERT(!expr->is_batch_result());
        if (0 == expr->res_buf_off_) {
          // for compat 4.0, do nothing
        } else if (OB_FAIL(expr->deep_copy_self_datum(eval_ctx))) {
          LOG_WARN("fail to deep copy datum", K(ret), K(eval_ctx), K(*expr));
        }
      }
    }
  }

  return ret;
}

int ObReceiveRowReader::get_next_batch(const ObIArray<ObExpr*> &exprs,
                                       const ObIArray<ObExpr*> &dynamic_const_exprs,
                                       ObEvalCtx &eval_ctx,
                                       const int64_t max_rows,
                                       int64_t &read_rows,
                                       const ObChunkDatumStore::StoredRow **srows)
{
  int ret = OB_SUCCESS;
  typedef ObChunkDatumStore Store;
  if (NULL == srows) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL store rows", K(ret));
  } else if (NULL != datum_iter_) {
    if (max_rows > eval_ctx.max_batch_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(max_rows), K(eval_ctx.max_batch_size_));
    } else if (OB_FAIL(datum_iter_->get_next_batch(srows, max_rows, read_rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next batch failed", K(ret), K(max_rows));
      } else {
        read_rows = 0;
      }
    } else {
      OZ(attach_rows(exprs, dynamic_const_exprs, eval_ctx, srows, read_rows));
    }
  } else {
    free_iterated_buffers();
    read_rows = 0;
    const Store::StoredRow *srow = NULL;
    while (read_rows < max_rows
           && NULL != (srow = next_store_row<Store::Block, Store::StoredRow>())) {
      srows[read_rows++] = srow;
    }
    if (0 == read_rows) {
      ret = OB_ITER_END;
    } else {
      LOG_DEBUG("read rows", K(read_rows), KP(this));
      OZ(attach_rows(exprs, dynamic_const_exprs, eval_ctx, srows, read_rows));
    }
  }
  return ret;
}

void ObReceiveRowReader::reset()
{
  free_buffer_list(recv_head_);
  recv_head_ = NULL;
  recv_tail_ = NULL;

  free_buffer_list(iterated_buffers_);
  iterated_buffers_ = NULL;

  cur_iter_pos_ = 0;
  cur_iter_rows_ = 0;
  recv_list_rows_ = 0;

  datum_iter_ = NULL;
  row_iter_ = NULL;
}


