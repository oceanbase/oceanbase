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
#include "src/sql/engine/basic/chunk_store/ob_store_row_convertor.h"
#include "src/sql/engine/basic/ob_temp_block_store.h"
namespace oceanbase
{
namespace sql
{

int ObStoreRowConvertor::init(ObTempBlockStore *store, const RowMeta *row_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store) || OB_ISNULL(row_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), KP(store), KP(row_meta));
  } else {
    store_ = store;
    row_meta_ = row_meta;
  }

  return ret;
}

int ObStoreRowConvertor::stored_row_to_compact_row(const ObChunkDatumStore::StoredRow *srow, const ObCompactRow *&crow)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  if (OB_ISNULL(srow)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), KP(srow));
  } else if (OB_FAIL(calc_conv_row_size(srow, row_size)) || row_size <= 0) {
    LOG_WARN("fail to calc row size", K(row_size));
  } else if (OB_FAIL(alloc_compact_row_buf(row_size))) {
    LOG_WARN("fail to alloc conver buffer", K(ret), K(row_size), KP(crow_buf_));
  } else {
    ObCompactRow *tmp_crow = new (crow_buf_)ObCompactRow();
    int64_t row_size = row_meta_->get_row_fixed_size();
    if (OB_ISNULL(row_meta_) || OB_ISNULL(srow)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", KP(row_meta_), KP(srow));
    } else if (OB_UNLIKELY(row_size > crow_size_)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", K(row_size), K(crow_size_));
    } else {
      tmp_crow->init(*row_meta_);
      for (int64_t i = 0; i < srow->cnt_ && OB_SUCC(ret); ++i) {
        const ObDatum &datum = srow->cells()[i];
        if (row_size > crow_size_) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf not enough", K(row_size), K(crow_size_));
        } else {
          if (datum.is_null()) {
            tmp_crow->set_null(*row_meta_, i);
          } else {
            tmp_crow->set_cell_payload(*row_meta_, i, datum.ptr_, datum.len_);
          }
          if (row_meta_->fixed_expr_reordered() && row_meta_->project_idx(i) < row_meta_->fixed_cnt_) {
          } else {
            row_size += datum.len_;
          }
        }
      }
      if (OB_SUCC(ret)) {
        tmp_crow->set_row_size(row_size);
        crow = tmp_crow;
      }
    }
  }
  return ret;
}

int ObStoreRowConvertor::compact_row_to_stored_row(const ObCompactRow *crow, const ObChunkDatumStore::StoredRow *&srow)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  if (OB_ISNULL(crow)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ptr", K(ret), KP(crow));
  } else if (OB_FAIL(calc_conv_row_size(crow, row_size)) || row_size <= 0) {
    LOG_WARN("fail to calc row size", K(row_size));
  } else if (OB_FAIL(alloc_stored_row_buf(row_size))) {
    LOG_WARN("fail to alloc conver buffer", K(ret), K(row_size), KP(srow_buf_));
  } else {
    ObChunkDatumStore::StoredRow *tmp_srow = new (srow_buf_)ObChunkDatumStore::StoredRow();
    int64_t row_size = 0;
    if (OB_ISNULL(row_meta_) || OB_ISNULL(tmp_srow)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", KP(row_meta_), KP(crow));
    } else {
      int64_t cur_data_pos = sizeof(ObDatum) * row_meta_->col_cnt_;
      for (int64_t i = 0; i < row_meta_->col_cnt_; i++) {
        ObDatum cur_datum = crow->get_datum(*row_meta_, i);
        ObDatum &target_datum = tmp_srow->cells()[i];
        if (cur_datum.is_null()) {
          target_datum.set_null();
          target_datum.ptr_ = nullptr;
        } else {
          target_datum.pack_ = cur_datum.pack_;
          target_datum.ptr_ = tmp_srow->payload_ + cur_data_pos;
          MEMCPY((void *)target_datum.ptr_, cur_datum.ptr_, cur_datum.len_);
          cur_data_pos += cur_datum.len_;
        }
      }
      if (OB_SUCC(ret)) {
        tmp_srow->cnt_ = row_meta_->col_cnt_;
        tmp_srow->row_size_ = cur_data_pos + sizeof(ObChunkDatumStore::StoredRow);
        srow = tmp_srow;
      }
    }
  }
  return ret;
}

int ObStoreRowConvertor::get_compact_row_from_expr(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, const ObCompactRow *&crow)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  if (OB_FAIL(calc_conv_row_size(exprs, ctx, row_size)) || row_size <= 0) {
    LOG_WARN("fail to calc row size", K(row_size));
  } else if (OB_FAIL(alloc_compact_row_buf(row_size))) {
    LOG_WARN("fail to alloc conver buffer", K(ret), K(row_size), KP(crow_buf_));
  } else {
    ObCompactRow *tmp_crow = new (crow_buf_)ObCompactRow();
    int64_t row_size = row_meta_->get_row_fixed_size();
    if (OB_ISNULL(row_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null pointer", KP(row_meta_));
    } else if (OB_UNLIKELY(row_size > crow_size_)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      tmp_crow->init(*row_meta_);
      for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
        ObExpr *expr = exprs.at(i);
        ObDatum *datum = nullptr;
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get daumt", K(ret));
        } else if (OB_FAIL(expr->eval(ctx, datum))) {
          SQL_ENG_LOG(WARN, "failed to eval expr datum", KPC(expr), K(ret));
        } else if (OB_ISNULL(datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get daumt", K(ret));
        } else if (row_size > crow_size_) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf not enough", K(row_size), K(crow_size_));
        } else {
          if (datum->is_null()) {
            tmp_crow->set_null(*row_meta_, i);
          } else {
            tmp_crow->set_cell_payload(*row_meta_, i, datum->ptr_, datum->len_);
          }
          if (row_meta_->fixed_expr_reordered() && row_meta_->project_idx(i) < row_meta_->fixed_cnt_) {
          } else {
            row_size += datum->len_;
          }
        }
      }
      if (OB_SUCC(ret)) {
        tmp_crow->set_row_size(row_size);
        crow = tmp_crow;
      }
    }
  }
  return ret;
}

int ObStoreRowConvertor::calc_conv_row_size(const ObChunkDatumStore::StoredRow *srow, int64_t &row_size)
{
  int ret = OB_SUCCESS;
  row_size = 0;
  const int64_t fixed_row_size = row_meta_->get_row_fixed_size();
  row_size += fixed_row_size;
  const bool reordered = row_meta_->fixed_expr_reordered();
  for (int64_t col_idx = 0; col_idx < srow->cnt_; col_idx++) {
    if (reordered && row_meta_->project_idx(col_idx) < row_meta_->fixed_cnt_) {
      continue;
    }
    ObDatum datum = srow->cells()[col_idx];
    if (!datum.is_null()) {
      row_size += datum.len_;
    }
  }
  return ret;
}

int ObStoreRowConvertor::calc_conv_row_size(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, int64_t &row_size)
{
  int ret = OB_SUCCESS;
  row_size = 0;
  if (OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", KP(row_meta_));
  } else {
    const int64_t fixed_row_size = row_meta_->get_row_fixed_size();
    row_size += fixed_row_size;
    int64_t batch_idx = ctx.get_batch_idx();
    const bool reordered = row_meta_->fixed_expr_reordered();
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < exprs.count(); col_idx++) {
      if (reordered && row_meta_->project_idx(col_idx) < row_meta_->fixed_cnt_) {
        continue;
      }
      ObExpr *expr = exprs.at(col_idx);
      ObDatum *datum = nullptr;
      if (OB_ISNULL(expr)) {
      } else if (OB_FAIL(expr->eval(ctx, datum))) {
        SQL_ENG_LOG(WARN, "failed to eval expr datum", KPC(expr), K(ret));
      } else if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "the datum is null", K(ret), KP(datum));
      } else if (!datum->is_null()) {
        row_size += datum->len_;
      }
    }
  }
  return ret;
}

int ObStoreRowConvertor::calc_conv_row_size(const ObCompactRow *crow, int64_t &row_size)
{
  int ret = OB_SUCCESS;
  const int64_t HEAD_SIZE = 8;
  row_size = 0;
  if (OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret), KP(row_meta_));
  } else {
    row_size = HEAD_SIZE + sizeof(ObDatum) * row_meta_->col_cnt_ + crow->get_row_size() - row_meta_->fix_data_off_;
  }
  return ret;
}

}
}