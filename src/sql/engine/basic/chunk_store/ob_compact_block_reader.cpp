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

#include "sql/engine/basic/chunk_store/ob_compact_block_reader.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bitmap.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObCompactBlockReader::CurRowInfo::init(const ChunkRowMeta *row_meta, const uint8_t offset_width, const char *buf)
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
    row_size_ = reinterpret_cast<const uint32_t*>(buf);
    // the n-th var_column in offset array.
    cur_var_offset_pos_ = 0;
    cur_data_ptr_ = sizeof(ObDatum) * row_meta->col_cnt_ + sizeof(ObChunkDatumStore::StoredRow);
    var_column_cnt_ = row_meta->col_cnt_ - row_meta->fixed_cnt_;
    int64_t var_offset_size = offset_width * (var_column_cnt_ + 1);
    bitmap_size_ = sql::ObBitVector::memory_size(row_meta->col_cnt_);
    bit_vec_ = sql::to_bit_vector(buf + HEAD_SIZE);
    data_offset_ = HEAD_SIZE + bitmap_size_ + var_offset_size; // the start of fixed data buffer
    fix_offset_ = data_offset_;
    var_col_end_offset_ = *row_size_ - data_offset_;
  }
  return ret;
}

int ObCompactBlockReader::get_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  sr = nullptr;
  if (OB_ISNULL(cur_blk_) || OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur block or row_meta is null", K(ret), KP(cur_blk_), KP(row_meta_));
  } else if (!blk_has_next_row()) {
    ret = OB_ITER_END;
  } else if (cur_pos_in_blk_ > cur_blk_->raw_size_ - sizeof(ObTempBlockStore::Block)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(cur_pos_in_blk_), KP(cur_blk_), K(cur_row_in_blk_), K(cur_blk_->cnt_), K(row_meta_->column_offset_));
  } else {
    int64_t size = 0;
    sr = nullptr;
    ObChunkDatumStore::StoredRow *tmp_sr = nullptr;
    if (OB_FAIL(get_stored_row_size(size))) {
      LOG_WARN("fail to get stored row size", K(ret));
    } else if (OB_FAIL(alloc_stored_row(tmp_sr, size))) {
      LOG_WARN("fail to alloc space for stored row", K(ret));
    } else if (OB_ISNULL(tmp_sr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sr is null", K(ret));
    } else if (cur_row_offset_width_ == BASE_OFFSET_SIZE && OB_FAIL(get_stored_row<uint16_t>(tmp_sr))){
      LOG_WARN("fail to get stored row", K(ret));
    } else if (cur_row_offset_width_ == EXTENDED_OFFSET_SIZE && OB_FAIL(get_stored_row<uint32_t>(tmp_sr)))  {
      LOG_WARN("fail to get stored row", K(ret));
    } else {
      sr = tmp_sr;
    }
  }
  return ret;
}



template <typename T>
int ObCompactBlockReader::get_stored_row(ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row meta shouldn't be null", K(ret), KP(row_meta_));
  } else if (OB_FAIL(row_info_.init(row_meta_, cur_row_offset_width_, &cur_blk_->payload_[cur_pos_in_blk_]))) {
    LOG_WARN("fail to init row info", K(ret));
  } else if (OB_ISNULL(row_info_.buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is null", K(ret));
  } else {
    const T *offset_array = reinterpret_cast<const T *>(row_info_.buf_ + HEAD_SIZE + row_info_.bitmap_size_);
    for (int64_t i = 0; OB_SUCC(ret) && i < row_meta_->col_cnt_; i++) {
      ObDatum *cur_datum = reinterpret_cast<ObDatum *>(sr->payload_ + i * sizeof(ObDatum));
      uint32_t len = 0;
      char *tmp_ptr = reinterpret_cast<char *>(sr) + row_info_.cur_data_ptr_;
      if (row_info_.bit_vec_->at(i)) {
        cur_datum->set_null();
        if (row_meta_->column_length_[i] == 0) {
          row_info_.cur_var_offset_pos_ ++;
        }
      } else if (row_meta_->column_length_[i] == 0) {
        T offset = offset_array[row_info_.cur_var_offset_pos_];
        if (row_info_.cur_var_offset_pos_ < row_info_.var_column_cnt_) {
          len = offset_array[row_info_.cur_var_offset_pos_ +  1] - offset;
        } else {
          ret = OB_INDEX_OUT_OF_RANGE;
          LOG_WARN("the var column idx in out of range", K(ret));
        }
        if (OB_SUCC(ret)) {
          // set datum->len_, use the pack_ to conver the NULL_FLAG
          cur_datum->pack_ = len;
          // set data
          MEMCPY(tmp_ptr, row_info_.buf_ + row_info_.fix_offset_ + offset, len);
          // set datum->ptr_
          cur_datum->ptr_ = tmp_ptr;
          row_info_.cur_var_offset_pos_ ++;
        }
      } else {
        int32_t offset = row_meta_->column_offset_[i];
        len = row_meta_->column_length_[i];
        cur_datum->pack_ = len;
        MEMCPY(tmp_ptr, row_info_.buf_ + row_info_.fix_offset_ + offset, len);
        cur_datum->ptr_ = tmp_ptr;
      }
      row_info_.cur_data_ptr_ += len;
    }
    if (OB_SUCC(ret)) {
      sr->cnt_ = row_meta_->col_cnt_;
      sr->row_size_ =  row_info_.cur_data_ptr_;
      cur_pos_in_blk_ += *row_info_.row_size_;
      cur_row_in_blk_++;
    }
  }
  return ret;
}

int ObCompactBlockReader::alloc_stored_row(ObChunkDatumStore::StoredRow *&sr, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (size <= sr_size_ ) {
  } else {
    if (OB_NOT_NULL(sr_buffer_) && OB_NOT_NULL(store_)) {
      store_->free(sr_buffer_, sr_size_);
      sr_buffer_ = nullptr;
    }
    sr_buffer_ = static_cast<char*>(store_->alloc(size));
    if (OB_ISNULL(sr_buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      sr_size_ = size;
    }
  }
  if (OB_SUCC(ret)) {
    MEMSET(sr_buffer_, 0, sr_size_);
    sr = reinterpret_cast<ObChunkDatumStore::StoredRow*>(sr_buffer_);
  }
  return ret;
}

int ObCompactBlockReader::get_stored_row_size(int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_ISNULL(cur_blk_) || OB_ISNULL(row_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur block or row_meta is null", K(ret), KP(cur_blk_), KP(row_meta_));
  } else if (OB_FAIL(ObCompactBlockReader::calc_stored_row_size(&cur_blk_->payload_[cur_pos_in_blk_], row_meta_, size))) {
    LOG_WARN("fail to get stored row size", K(ret));
  } else {
    cur_row_offset_width_ = *reinterpret_cast<const int8_t*>(cur_blk_->payload_ + cur_pos_in_blk_ + sizeof(uint32_t));
  }
  return ret;
}

int ObCompactBlockReader::calc_stored_row_size(const char *compact_row, const ChunkRowMeta *row_meta, int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_ISNULL(compact_row) || OB_ISNULL(row_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur block or row_meta is null", K(ret), KP(compact_row), KP(row_meta));
  } else {
    int64_t bit_map_size = sql::ObBitVector::memory_size(row_meta->col_cnt_);
    uint32_t row_size = *reinterpret_cast<const uint32_t*>(compact_row);
    int64_t offset_width = *reinterpret_cast<const int8_t*>(compact_row + sizeof(uint32_t));
    int64_t var_column_cnt = row_meta->col_cnt_ - row_meta->fixed_cnt_;
    if (offset_width != BASE_OFFSET_SIZE && offset_width != EXTENDED_OFFSET_SIZE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the offset width is unexpected", K(ret));
    }
    if (OB_SUCC(ret)) {
      size = row_size - HEAD_SIZE - bit_map_size - offset_width * (var_column_cnt + 1)\
            + row_meta->col_cnt_ * sizeof(ObDatum) + sizeof(ObChunkDatumStore::StoredRow);

    }
  }
  return ret;
}

}
}
