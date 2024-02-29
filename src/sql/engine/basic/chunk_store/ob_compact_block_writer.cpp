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

#include "sql/engine/basic/chunk_store/ob_compact_block_writer.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bitmap.h"
#include "sql/engine/ob_bit_vector.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
using namespace common;

namespace sql
{


int ObCompactBlockWriter::CurRowInfo::init(const ChunkRowMeta *row_meta, const uint8_t offset_width, char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_meta) || OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret), KP(row_meta), KP(buf));
  } else if (offset_width != BASE_OFFSET_SIZE && offset_width != EXTENDED_OFFSET_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("offset_width is invalid", K(ret), K(offset_width));
  } else {
    buf_ = buf;
    cur_var_offset_pos_ = 0;
    var_column_cnt_ = row_meta->col_cnt_ - row_meta->fixed_cnt_;
    bitmap_size_ = sql::ObBitVector::memory_size(row_meta->col_cnt_);
    bit_vec_ = sql::to_bit_vector(buf_ + HEAD_SIZE);
    bit_vec_->reset(row_meta->col_cnt_);
    data_offset_ = HEAD_SIZE + bitmap_size_ + (var_column_cnt_ + 1) * offset_width; // the start of fixed data buffer
    var_offset_ = data_offset_ + row_meta->var_data_off_;
  }
  return ret;
}

void ObCompactBlockWriter::CurRowInfo::reset()
{
  buf_ = nullptr;
  bit_vec_ = nullptr;
  var_column_cnt_ = 0;
  cur_var_offset_pos_ = 0;
  bitmap_size_ = 0;
  data_offset_ = 0;
  var_offset_ = 0;
}

int ObCompactBlockWriter::add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ensure_init())) {
    LOG_WARN("fail to ensure init", K(ret));
  } else if (OB_FAIL(ensure_write(exprs, ctx))) {
    LOG_WARN("fail to ensure write", K(ret));
  } else {
    if ((cur_row_offset_width_ == BASE_OFFSET_SIZE) &&
        OB_FAIL(inner_add_row<uint16_t>(exprs, ctx))) {
      LOG_WARN("fail to add row", K(ret));
    } else if ((cur_row_offset_width_ == EXTENDED_OFFSET_SIZE) &&
                OB_FAIL(inner_add_row<uint32_t>(exprs, ctx))) {
      LOG_WARN("fail to add row", K(ret));
    }
  }
  return ret;
}

int ObCompactBlockWriter::add_row(const ObChunkDatumStore::StoredRow &src_sr, ObChunkDatumStore::StoredRow **dst_sr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ensure_init())) {
    LOG_WARN("fail to ensure init", K(ret));
  } else if (OB_FAIL(ensure_write(src_sr))) {
    LOG_WARN("fail to ensure write", K(ret));
  } else{
    if ((cur_row_offset_width_ == BASE_OFFSET_SIZE) &&
        OB_FAIL(inner_build_from_stored_row<uint16_t>(src_sr))) {
      LOG_WARN("fail to build from stored row", K(ret));
    } else if ((cur_row_offset_width_ == EXTENDED_OFFSET_SIZE) &&
                OB_FAIL(inner_build_from_stored_row<uint32_t>(src_sr))) {
      LOG_WARN("fail to build from stored row", K(ret));
    }
  }

  return ret;
}


int ObCompactBlockWriter::add_row(const blocksstable::ObStorageDatum *storage_datums,
                                  const ObStorageColumnGroupSchema &cg_schema,
                                  const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ensure_init())) {
    LOG_WARN("fail to ensure init", K(ret));
  } else if (OB_FAIL(ensure_write(storage_datums, cg_schema, extra_size))) {
    LOG_WARN("fail to ensure write", K(ret));
  } else {
    if ((cur_row_offset_width_ == BASE_OFFSET_SIZE) &&
         OB_FAIL(inner_add_row<uint16_t>(storage_datums, cg_schema, extra_size, stored_row))) {
      LOG_WARN("add row to block failed", K(ret), K(storage_datums), K(cg_schema), K(extra_size));
    } else if (cur_row_offset_width_ == EXTENDED_OFFSET_SIZE &&
         OB_FAIL(inner_add_row<uint32_t>(storage_datums, cg_schema, extra_size, stored_row))) {
      LOG_WARN("add row to block failed", K(ret), K(storage_datums), K(cg_schema), K(extra_size));
    }
  }
  return ret;
}

template <typename T>
int ObCompactBlockWriter::inner_add_row(const blocksstable::ObStorageDatum *storage_datums,
                                        const ObStorageColumnGroupSchema &cg_schema,
                                        const int64_t extra_size, ObChunkDatumStore::StoredRow **stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row meta is null", K(ret), KP(row_meta_));
  } else if (OB_FAIL(row_info_.init(row_meta_, sizeof(T), get_cur_buf()))) {
    LOG_WARN("fail to init row info", K(ret));
  } else if (OB_ISNULL(row_info_.buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is null", K(ret));
  } else {
    T *var_offset_array = reinterpret_cast<T*>(row_info_.buf_ + HEAD_SIZE + row_info_.bitmap_size_);
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_schema.column_cnt_; i++) {
      int64_t column_idx = cg_schema.column_idxs_ ? cg_schema.column_idxs_[i] : i;
      if (OB_FAIL(inner_process_datum<T>(storage_datums[column_idx], i, *row_meta_, row_info_))) {
        LOG_WARN("fail to process datum", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // head
      uint32_t row_size = row_info_.var_offset_;
      MEMCPY(row_info_.buf_, &row_size, sizeof(row_size)); // row_size_
      MEMCPY(row_info_.buf_ + sizeof(row_size), &cur_row_offset_width_, sizeof(cur_row_offset_width_)); // the offset width
      // last offset;
      T tmp_offset = row_info_.var_offset_ - row_info_.data_offset_;
      MEMCPY(var_offset_array + row_info_.cur_var_offset_pos_, &tmp_offset, sizeof(T));
      // payload
      if (OB_FAIL(advance(row_size))) {
        LOG_WARN("fail to advance buf", K(ret));
      }
    }
  }

  return ret;
}

template <typename T>
int ObCompactBlockWriter::inner_build_from_stored_row(const ObChunkDatumStore::StoredRow &sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row meta is null", K(ret), KP(row_meta_));
  } else if (OB_FAIL(row_info_.init(row_meta_, sizeof(T), get_cur_buf()))) {
    LOG_WARN("fail to init row info", K(ret));
  } else if (OB_ISNULL(row_info_.buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is null", K(ret));
  } else {
    T *var_offset_array = reinterpret_cast<T*>(row_info_.buf_ + HEAD_SIZE + row_info_.bitmap_size_);
    for (int64_t i = 0 ; OB_SUCC(ret) && i < sr.cnt_; i++) {
      if (OB_FAIL(inner_process_datum<T>(sr.cells()[i], i, *row_meta_, row_info_))) {
        LOG_WARN("fail to process datum", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      // head
      uint32_t row_size = row_info_.var_offset_;
      MEMCPY(row_info_.buf_, &row_size, sizeof(row_size)); // row_size_
      MEMCPY(row_info_.buf_ + sizeof(row_size), &cur_row_offset_width_, sizeof(cur_row_offset_width_)); // the offset width
      // last offset
      T tmp_offset = row_info_.var_offset_ - row_info_.data_offset_;
      MEMCPY(var_offset_array + row_info_.cur_var_offset_pos_, &tmp_offset, sizeof(T));
      // payload
      if (OB_FAIL(advance(row_size))) {
        LOG_WARN("fail to advance buf", K(ret));
      }
    }
  }

  return ret;
}

// before call this function -- we need to ensure the size if enough.
template <typename T>
int ObCompactBlockWriter::inner_add_row(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row meta is null", K(ret), KP(row_meta_));
  } else if (OB_FAIL(row_info_.init(row_meta_, sizeof(T), get_cur_buf()))) {
    LOG_WARN("fail to init row info", K(ret));
  } else if (OB_ISNULL(row_info_.buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is null", K(ret));
  } else {
    T *var_offset_array = reinterpret_cast<T*>(row_info_.buf_ + HEAD_SIZE + row_info_.bitmap_size_);
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *expr = exprs.at(i);
      ObDatum *in_datum = NULL;
      if (OB_UNLIKELY(NULL == expr)) {
        // Set datum to NULL for NULL expr
        row_info_.bit_vec_->set(i);
        if (row_meta_->column_length_[i] == 0)  {
          T tmp_offset = row_info_.var_offset_ - row_info_.data_offset_;
          MEMCPY(var_offset_array + row_info_.cur_var_offset_pos_, &tmp_offset, sizeof(T));
          row_info_.cur_var_offset_pos_ ++;
        }
      } else if (OB_FAIL(expr->eval(ctx, in_datum))) {
        LOG_WARN("expression evaluate failed", K(ret));
      } else if (OB_NOT_NULL(in_datum)) {
        if (OB_FAIL(inner_process_datum<T>(*in_datum, i, *row_meta_, row_info_))) {
          LOG_WARN("fail to process datum", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // head
      uint32_t row_size = row_info_.var_offset_;
      MEMCPY(row_info_.buf_, &row_size, sizeof(row_size)); // row_size_
      MEMCPY(row_info_.buf_ + sizeof(row_size), &cur_row_offset_width_, sizeof(cur_row_offset_width_)); // the offset width
      // last offset
      T tmp_offset = row_info_.var_offset_ - row_info_.data_offset_;
      MEMCPY(var_offset_array + row_info_.cur_var_offset_pos_, &tmp_offset, sizeof(T));

      // payload
      if (OB_FAIL(advance(row_size))) {
        LOG_WARN("fail to advance buf", K(ret));
      }
    }
  }

  return ret;
}

template <typename T>
int ObCompactBlockWriter::inner_process_datum(const ObDatum &src_datum, const int64_t cur_pos,
                                              const ChunkRowMeta &row_meta, CurRowInfo &row_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_info.buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else {
    T *var_offset_array = reinterpret_cast<T*>(row_info.buf_ + HEAD_SIZE + row_info.bitmap_size_);
    if (OB_ISNULL(row_info.bit_vec_) || cur_pos < 0 || cur_pos >= row_meta.col_cnt_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get unexpected null bitmap", K(ret));
    } else if (src_datum.is_null()) {
      row_info.bit_vec_->set(cur_pos);
      if (row_meta.column_length_[cur_pos] == 0)  {
        T tmp_offset = row_info.var_offset_ - row_info.data_offset_;
        MEMCPY(var_offset_array + row_info.cur_var_offset_pos_, &tmp_offset, sizeof(T));
        row_info.cur_var_offset_pos_ ++;
      }
    } else if (row_meta.column_length_[cur_pos] == 0) { // the column is variable size;
      if (row_info.cur_var_offset_pos_ >= row_info.var_column_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the offset is out of range", K(ret), K(row_info));
      } else {
        T tmp_offset = row_info.var_offset_ - row_info.data_offset_;
        MEMCPY(var_offset_array + row_info.cur_var_offset_pos_, &tmp_offset, sizeof(T));
        MEMCPY(row_info.buf_ + row_info.var_offset_, src_datum.ptr_, src_datum.len_);
        row_info.var_offset_ += src_datum.len_;
        row_info.cur_var_offset_pos_ ++;
      }
    } else { // the column is fixed size;
      if (src_datum.len_ != row_meta.column_length_[cur_pos]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fixe data length do not match", K(ret), K(src_datum.len_),
                 K(row_meta.column_length_[cur_pos]));
      } else {
        MEMCPY(row_info.buf_ + row_info.data_offset_ + row_meta.column_offset_[cur_pos], src_datum.ptr_,
             row_meta.column_length_[cur_pos]);
      }
    }
  }
  return ret;
}

int ObCompactBlockWriter::get_row_stored_size(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx, uint64_t &size)
{
  int ret = OB_SUCCESS;
  const ChunkRowMeta &row_meta = *get_meta();
  int64_t bit_map_size = sql::ObBitVector::memory_size(row_meta.col_cnt_);
  int64_t data_size = 0;
  int64_t offset_size = 0;
  int8_t tmp_offset_width = 0; // byte size of each offset in a row;
  size = 0;

  ObExpr *expr = nullptr;
  common::ObDatum *datum = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (row_meta.column_length_[i] != 0) {
      data_size += row_meta.column_length_[i];
    } else {
      expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
      } else if (OB_FAIL(expr->eval(ctx, datum))) {
        SQL_ENG_LOG(WARN, "failed to eval expr datum", KPC(expr), K(ret));
      } else if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "the datum is null", K(ret), KP(datum));
      } else if (!datum->is_null()) {
        data_size += datum->len_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    cur_row_offset_width_ = get_offset_width(data_size);
    offset_size = (row_meta.col_cnt_ - row_meta.fixed_cnt_ + 1) * cur_row_offset_width_;
    size = data_size + offset_size + bit_map_size + HEAD_SIZE;

    // offset_width_ is used in add_row
    cur_row_size_ = size;
  }

  return ret;
}

int ObCompactBlockWriter::get_row_stored_size(const ObChunkDatumStore::StoredRow &sr, uint64_t &size)
{
  int ret = OB_SUCCESS;
  const ChunkRowMeta &row_meta = *get_meta();
  int64_t bit_map_size = sql::ObBitVector::memory_size(row_meta.col_cnt_);
  int64_t data_size = 0;
  int64_t offset_size = 0;
  size = 0;

  ObExpr *expr = nullptr;
  common::ObDatum *datum = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < sr.cnt_; ++i) {
    if (row_meta.column_length_[i] != 0) {
      data_size +=  row_meta.column_length_[i];
    } else if (!sr.cells()[i].is_null()) {
      data_size += sr.cells()[i].len_;
    }
  }

  if (OB_SUCC(ret)) {
    cur_row_offset_width_ = get_offset_width(data_size);
    offset_size = (row_meta.col_cnt_ - row_meta.fixed_cnt_ + 1) * cur_row_offset_width_;
    size = data_size + offset_size + bit_map_size + HEAD_SIZE;

    // offset_width_ is used in add_row
    cur_row_size_ = size;
  }
  return ret;
}


int ObCompactBlockWriter::get_row_stored_size(const blocksstable::ObStorageDatum *storage_datums,
                                              const ObStorageColumnGroupSchema &cg_schema,
                                              const int64_t extra_size, uint64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t head_size = sizeof(ObChunkDatumStore::StoredRow);
  const ChunkRowMeta &row_meta = *get_meta();
  int64_t bit_map_size = sql::ObBitVector::memory_size(row_meta.col_cnt_);
  int64_t datum_size = sizeof(ObDatum) * cg_schema.column_cnt_;
  int64_t data_size = 0;
  int64_t offset_size = 0;
  for (int64_t i = 0; i < cg_schema.column_cnt_; ++i) {
    if (row_meta.column_length_[i] != 0) {
      data_size += row_meta.column_length_[i];
    } else {
      int64_t column_idx = cg_schema.column_idxs_ ? cg_schema.column_idxs_[i] : i;
      if (!storage_datums[column_idx].is_null()) {
        data_size += storage_datums[column_idx].len_;
      }
    }
  }
  cur_row_offset_width_ = get_offset_width(data_size);
  offset_size = (row_meta.col_cnt_ - row_meta.fixed_cnt_ + 1) * cur_row_offset_width_;
  size = HEAD_SIZE + bit_map_size + offset_size + extra_size + data_size;
  return ret;
}


int ObCompactBlockWriter::close()
{
  int ret = OB_SUCCESS;
  inited_ = false;
  return ret;
}

int ObCompactBlockWriter::ensure_write(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  uint64_t row_size = 0;
  if (OB_FAIL(get_row_stored_size(exprs, ctx, row_size))) {
    LOG_WARN("fail to get row_size", K(exprs), K(ret));
  } else if (OB_FAIL(ensure_write(row_size))) {
    LOG_WARN("fail ensure write", K(ret));
  }
  return ret;
}

int ObCompactBlockWriter::ensure_write(const blocksstable::ObStorageDatum *storage_datums,
                                       const ObStorageColumnGroupSchema &cg_schema,
                                       const int64_t extra_size)
{
  int ret = OB_SUCCESS;
  uint64_t row_size = 0;
  if (OB_FAIL(get_row_stored_size(storage_datums, cg_schema, extra_size, row_size))) {
    LOG_WARN("fail to get row_size", K(cg_schema), K(extra_size), K(row_size), K(ret));
  } else if (OB_FAIL(ensure_write(row_size))) {
    LOG_WARN("fail to call inner ensure write", K(ret));
  }

  return ret;
}

int ObCompactBlockWriter::ensure_write(const ObChunkDatumStore::StoredRow &sr)
{
  int ret = OB_SUCCESS;
  uint64_t row_size = 0;
  if (OB_FAIL(get_row_stored_size(sr, row_size))) {
    LOG_WARN("fail to get row_size", K(sr), K(ret));
  } else if (OB_FAIL(ensure_write(row_size))) {
    LOG_WARN("fail ensure write", K(ret));
  }
  return ret;
}

int ObCompactBlockWriter::ensure_write(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (is_overflow(size)) {
    int64_t new_blk_size = size < DEFAULT_BUF_SIZE ? DEFAULT_BUF_SIZE : size;
    ObTempBlockStore::Block *tmp_blk = nullptr;
    if (OB_FAIL(store_->new_block(new_blk_size, tmp_blk, true))) {
      LOG_WARN("fail to alloc block", K(ret));
    } else if (OB_ISNULL(tmp_blk)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc block", K(ret));
    } else {
      cur_blk_ = tmp_blk;
    }
  } else {
    // do nothing, directly add row to buffer.
  }

  return ret;
}
int ObCompactBlockWriter::get_last_stored_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  const char *compact_row = get_last_row();
  int64_t size = 0;
  ObChunkDatumStore::StoredRow *tmp_sr = nullptr;
  sr = nullptr;
  // convert from compact format to storedrow;
  if (OB_ISNULL(compact_row) || OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret));
  } else if (OB_FAIL(inner_get_stored_row_size(compact_row, size))) {
    LOG_WARN("fail to calc size", K(ret));
  } else if (size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret));
  } else if (OB_ISNULL(tmp_sr = reinterpret_cast<ObChunkDatumStore::StoredRow*>(store_->alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocal memory", K(ret));
  } else {
    const int8_t offset_width = *reinterpret_cast<const int8_t*>(compact_row + sizeof(int32_t));
    if (offset_width == BASE_OFFSET_SIZE) {
      if (OB_FAIL(convert_to_stored_row<uint16_t>(compact_row, tmp_sr))) {
        LOG_WARN("fail to convert", K(ret), K(size));
      }
    } else if (offset_width == EXTENDED_OFFSET_SIZE) {
      if (OB_FAIL(convert_to_stored_row<uint32_t>(compact_row, tmp_sr))) {
        LOG_WARN("fail to convert", K(ret), K(size));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected offset width", K(offset_width), K(ret));
    }
    if (OB_FAIL(ret)) {
      store_->free(tmp_sr, size);
    }
  }
  if (OB_SUCC(ret)) {
    sr = tmp_sr;
    last_stored_row_ = tmp_sr;
    last_sr_size_ = size;
  }
  return ret;
}

int ObCompactBlockWriter::inner_get_stored_row_size(const char *compact_row, int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_ISNULL(row_meta_) || OB_ISNULL(compact_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret));
  } else if (OB_FAIL(ObCompactBlockReader::calc_stored_row_size(compact_row, row_meta_, size))){
    LOG_WARN("fail to get stored row size", K(ret));
  }

  return ret;
}

template <typename T>
int ObCompactBlockWriter::convert_to_stored_row(const char *compact_row, ObChunkDatumStore::StoredRow *sr)
{
  //TODO DAISI: this function is similar to ObCompactBlockReader::get_stored_row
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_meta_) || OB_ISNULL((compact_row))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret), KP(row_meta_), KP(compact_row));
  } else {
    const int64_t offset_width = sizeof(T);
    const int32_t row_size = *reinterpret_cast<const int32_t*>(compact_row);
    int64_t bitmap_size = sql::ObBitVector::memory_size(row_meta_->col_cnt_);
    const sql::ObBitVector *bit_vec = sql::to_bit_vector(compact_row + HEAD_SIZE);
    int64_t cur_var_offset_pos = 0;
    const int64_t var_column_cnt = row_meta_->col_cnt_ - row_meta_->fixed_cnt_;
    const int64_t data_offset = HEAD_SIZE + bitmap_size + (var_column_cnt + 1) * offset_width; // the start of fixed data buffer
    int64_t cur_data_ptr = sizeof(ObDatum) * row_meta_->col_cnt_ + sizeof(ObChunkDatumStore::StoredRow);
    const T *offset_array = reinterpret_cast<const T *>(compact_row + HEAD_SIZE + bitmap_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < row_meta_->col_cnt_; i++) {
      ObDatum *cur_datum = reinterpret_cast<ObDatum *>(sr->payload_ + i * sizeof(ObDatum));
      uint32_t len = 0;
      char *tmp_ptr = reinterpret_cast<char *>(sr) + cur_data_ptr;
      if (bit_vec->at(i)) {
        cur_datum->set_null();
        if (row_meta_->column_length_[i] == 0) {
          cur_var_offset_pos ++;
        }
      } else if (row_meta_->column_length_[i] == 0) {
        const T offset = offset_array[cur_var_offset_pos];
        if (cur_var_offset_pos < var_column_cnt) {
          len = offset_array[cur_var_offset_pos +  1] - offset;
        } else {
          ret = OB_INDEX_OUT_OF_RANGE;
          LOG_WARN("the var column idx in out of range", K(ret));
        }
        if (OB_SUCC(ret)) {
          // set datum->len_, use the pack_ to conver the NULL_FLAG
          cur_datum->pack_ = len;
          // set data
          MEMCPY(tmp_ptr, compact_row + data_offset + offset, len);
          // set datum->ptr_
          cur_datum->ptr_ = tmp_ptr;
          cur_var_offset_pos ++;
        }
      } else {
        const int32_t offset = row_meta_->column_offset_[i];
        len = row_meta_->column_length_[i];
        cur_datum->pack_ = len;
        MEMCPY(tmp_ptr, compact_row + data_offset + offset, len);
        cur_datum->ptr_ = tmp_ptr;
      }
      cur_data_ptr += len;
    }
    if (OB_SUCC(ret)) {
      sr->cnt_ = row_meta_->col_cnt_;
      sr->row_size_ =  cur_data_ptr;
    }
  }

  return ret;
}

}
}
