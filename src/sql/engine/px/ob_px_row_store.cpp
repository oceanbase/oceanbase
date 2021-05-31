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

using namespace oceanbase::common;
using namespace oceanbase::sql;

/*
 * Description:
 *
 * General process:
 *  obj -> netbuf -> transport -> netbuf -> obj -> use it
 *      sender           |            receiver
 *
 * ObPxNewRow process:
 *  obj -> netbuf -> transport -> netbuf -> copy netbuf to local buf -> obj -> use it
 *      sender           |            receiver
 *
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
      const ObObj& cell = row_->get_cell(idx);
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
      const ObObj& cell = row_->get_cell(idx);
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
      des_row_buf_ = (char*)buf + pos;
      des_row_buf_size_ = data_len - pos;
      pos += des_row_buf_size_;
    }
  }
  return ret;
}

// used to copy row from DTL memory to get_next_row context
// if do not copy, the memory of row will be released after the DTL process call ends
// get_next_row will handle the wild pointer
int ObPxNewRow::deep_copy(ObIAllocator& alloc, const ObPxNewRow& other)
{
  int ret = OB_SUCCESS;
  row_cell_count_ = other.row_cell_count_;
  des_row_buf_size_ = other.des_row_buf_size_;
  if (des_row_buf_size_ > 0) {
    des_row_buf_ = static_cast<char*>(alloc.alloc(des_row_buf_size_));
    if (OB_ISNULL(des_row_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      MEMCPY(des_row_buf_, other.des_row_buf_, des_row_buf_size_);
    }
  } else {
    des_row_buf_ = NULL;
    if (row_cell_count_ == EOF_ROW_FLAG) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObPxNewRow::get_row(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (has_iter()) {
    if (OB_FAIL(iter_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get row from iterator", K(ret));
      }
    }
  } else {
    if (OB_FAIL(get_row_from_serialization(row))) {
      LOG_WARN("failed to get row from serialization", K(ret));
    }
  }
  return ret;
}

int ObPxNewRow::get_next_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (has_iter()) {
    if (OB_FAIL(iter_->get_next_row(exprs, ctx))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get row from iterator", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: get datum without iterator", K(ret));
  }
  return ret;
}

int ObPxNewRow::get_row_from_serialization(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (row_cell_count_ == EOF_ROW_FLAG) {
    ret = OB_ITER_END;  // mark last row using special value EOF_ROW_FLAG
  } else if (OB_ISNULL(des_row_buf_) || OB_ISNULL(row.cells_) || row_cell_count_ > row.count_) {
    // notice:when the receiving end needs to add expressions to row, row_cell_count_ may be less than row.count_
    //       eg. select 1+1, row from distr_table;
    ret = OB_NOT_INIT;
    LOG_WARN("row not init",
        KP_(des_row_buf),
        K_(row_cell_count),
        "row_cell_count",
        row.count_,
        "row_prj_count",
        row.get_count(),
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

//--------ObPxNewRowIterator
ObPxNewRowIterator::ObPxNewRowIterator()
    : is_eof_(false), is_iter_end_(false), rows_(0), row_store_(), row_store_it_(), is_inited_(false)
{}

ObPxNewRowIterator::~ObPxNewRowIterator()
{
  reset();
}

void ObPxNewRowIterator::set_iterator_end()
{
  if (is_eof_) {
    is_iter_end_ = true;
    row_store_.remove_added_blocks();
  }
}
void ObPxNewRowIterator::set_end()
{
  is_iter_end_ = true;
  is_eof_ = true;
}

int ObPxNewRowIterator::load_buffer(const dtl::ObDtlLinkedBuffer& buffer)
{
  int ret = OB_SUCCESS;
  reset();
  row_store_.init(0);
  ObChunkRowStore::Block* block = reinterpret_cast<ObChunkRowStore::Block*>(const_cast<char*>(buffer.buf()));
  row_store_.add_block(block, true);
  // LOG_TRACE("block item info", K(ret), K(row_store_.get_row_cnt()), K(block->rows()),
  //   K(buffer.is_eof()), K(buffer.size()));
  if (OB_FAIL(row_store_.begin(row_store_it_))) {
    LOG_WARN("begin iterator failed", K(ret));
  } else {
    rows_ = block->rows();
    is_eof_ = buffer.is_eof();
    is_inited_ = true;
  }
  return ret;
}

int ObPxNewRowIterator::get_next_row(common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (is_iter_end_) {
    if (!is_eof_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is not eof", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  } else if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is not init", K(ret));
  } else if (!has_next()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(row_store_it_.get_next_row(row))) {
  }
  if (OB_FAIL(ret)) {
    if (OB_ITER_END == ret) {
      if (!is_eof_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect eof", K(ret));
      }
      LOG_TRACE("iterator end from px row store", K(ret), K(is_eof_));
      reset();
    } else {
      LOG_WARN("trace get row from row store", K(ret), K(is_eof_));
    }
  }
  return ret;
}

void ObPxNewRowIterator::reset()
{
  row_store_.remove_added_blocks();
  row_store_it_.reset();
  row_store_.reset();
  is_eof_ = false;
  is_iter_end_ = false;
  is_inited_ = false;
}
//------- end ObPxNewRowIterator-----------

//-------- start ObPxDatumRowIterator --------
ObPxDatumRowIterator::ObPxDatumRowIterator()
    : is_eof_(false), is_iter_end_(false), rows_(0), datum_store_(), datum_store_it_(), is_inited_(false)
{}

ObPxDatumRowIterator::~ObPxDatumRowIterator()
{
  reset();
}

void ObPxDatumRowIterator::set_iterator_end()
{
  if (is_eof_) {
    is_iter_end_ = true;
    datum_store_.remove_added_blocks();
  }
}

void ObPxDatumRowIterator::set_end()
{
  is_iter_end_ = true;
  is_eof_ = true;
}

int ObPxDatumRowIterator::load_buffer(const dtl::ObDtlLinkedBuffer& buffer)
{
  int ret = OB_SUCCESS;
  reset();
  datum_store_.init(0);
  ObChunkDatumStore::Block* block = reinterpret_cast<ObChunkDatumStore::Block*>(const_cast<char*>(buffer.buf()));
  datum_store_.add_block(block, true);
  if (OB_FAIL(datum_store_.begin(datum_store_it_))) {
    LOG_WARN("begin iterator failed", K(ret));
  } else {
    rows_ = block->rows();
    is_eof_ = buffer.is_eof();
    is_inited_ = true;
  }
  return ret;
}

int ObPxDatumRowIterator::get_next_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx)
{
  int ret = OB_SUCCESS;
  if (is_iter_end_) {
    if (!is_eof_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is not eof", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  } else if (!is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row store is not init", K(ret));
  } else if (!has_next()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(datum_store_it_.get_next_row(exprs, eval_ctx))) {
  }
  if (OB_FAIL(ret)) {
    if (OB_ITER_END == ret) {
      if (!is_eof_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect eof", K(ret));
      }
      LOG_TRACE("iterator end from px row store", K(ret), K(is_eof_));
      reset();
    } else {
      LOG_WARN("trace get row from row store", K(ret), K(is_eof_));
    }
  }
  return ret;
}

void ObPxDatumRowIterator::reset()
{
  datum_store_.remove_added_blocks();
  datum_store_it_.reset();
  datum_store_.reset();
  is_eof_ = false;
  is_iter_end_ = false;
  is_inited_ = false;
}
//-------- end ObPxDatumRowIterator --------
