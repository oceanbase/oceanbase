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

#include "sql/engine/basic/chunk_store/ob_chunk_block.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "src/storage/ddl/ob_direct_load_struct.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ChunkRowMeta::init(const ObExprPtrIArray &exprs, const int32_t extra_size)
{
  int ret = OB_SUCCESS;
  if (extra_size < 0 || exprs.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is INVALID", K(ret), K(exprs), K(extra_size));
  } else {
    col_cnt_ = exprs.count();
    extra_size_ = extra_size;
    fixed_cnt_ = 0;
    fixed_offsets_ = NULL;
    projector_ = NULL;
    var_data_off_ = 0;

    if (OB_FAIL(column_length_.prepare_allocate(exprs.count()))) {
      LOG_WARN("fail to prepare allocate column_length", K(ret), K(exprs.count()));
    } else if (OB_FAIL(column_offset_.prepare_allocate(exprs.count()))) {
      LOG_WARN("fail to prepare allocate column_offset", K(ret), K(exprs.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
        ObExpr *e = exprs.at(i);
        if (OB_ISNULL(e)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null pointer", K(ret));
        } else if (is_fixed_length(e->datum_meta_.type_)) {
          int16_t len = get_type_fixed_length(e->datum_meta_.type_);
          column_length_.at(i) = len;
          column_offset_.at(i) = var_data_off_;
          var_data_off_ += len;
          fixed_cnt_++;
        } else {
          column_length_.at(i) = 0;
          column_offset_.at(i) = 0;
        }
      }
    }
    if (OB_SUCC(ret)) {
      nulls_off_ = 0;
      var_offsets_off_ = nulls_off_ + ObBitVector::memory_size(col_cnt_);
      extra_off_ = var_offsets_off_ + get_var_col_cnt() * sizeof(int32_t);
      fix_data_off_ = extra_off_ + extra_size_;
    }
    LOG_INFO("successfully init row meta", K(fixed_cnt_), K(col_cnt_), K(var_data_off_), K(column_offset_), K(fix_data_off_));
  }

  return ret;
}

int ChunkRowMeta::init(const ObIArray<storage::ObColumnSchemaItem> &col_array,  const int32_t extra_size)
{
  int ret = OB_SUCCESS;
  if (extra_size < 0 || col_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is INVALID", K(ret), K(col_array), K(extra_size));
  } else {
    col_cnt_ = col_array.count();
    extra_size_ = extra_size;
    fixed_cnt_ = 0;
    fixed_offsets_ = NULL;
    projector_ = NULL;
    var_data_off_ = 0;

    if (OB_FAIL(column_length_.prepare_allocate(col_array.count()))) {
      LOG_WARN("fail to prepare allocate column_length", K(ret), K(col_array.count()));
    } else if (OB_FAIL(column_offset_.prepare_allocate(col_array.count()))) {
      LOG_WARN("fail to prepare allocate column_offset", K(ret), K(col_array.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
        if (!col_array.at(i).is_valid_) {
          // the multiversion column and snapshot column;
          column_length_.at(i) = 8;
          column_offset_.at(i) = var_data_off_;
          var_data_off_ += 8;
          fixed_cnt_++;
        } else if (is_fixed_length(col_array.at(i).col_type_.get_type())) {
          int16_t len = get_type_fixed_length(col_array.at(i).col_type_.get_type());
          column_length_.at(i) = len;
          column_offset_.at(i) = var_data_off_;
          var_data_off_ += len;
          fixed_cnt_++;
        } else {
          column_length_.at(i) = 0;
          column_offset_.at(i) = 0;
        }
      }
    }
    if (OB_SUCC(ret)) {
      nulls_off_ = 0;
      var_offsets_off_ = nulls_off_ + ObBitVector::memory_size(col_cnt_);
      extra_off_ = var_offsets_off_ + get_var_col_cnt() * sizeof(int32_t);
      fix_data_off_ = extra_off_ + extra_size_;
    }
    LOG_INFO("successfully init row meta", K(fixed_cnt_), K(col_cnt_), K(var_data_off_), K(column_offset_), K(fix_data_off_));
  }

  return ret;
}

OB_DEF_SERIALIZE(ChunkRowMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_,
              column_length_,
              column_offset_);
  return ret;
}


OB_DEF_DESERIALIZE(ChunkRowMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_,
              column_length_,
              column_offset_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ChunkRowMeta)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_,
              column_length_,
              column_offset_);
  return len;
}


int WriterBufferHandler::write_data(char *data, const int64_t data_size)
{
  int ret = OB_SUCCESS;
  if (data_size > buf_size_ - cur_pos_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data size shouldn't be large than reamain size", K(ret));
  } else {
    MEMCPY(buf_, data, data_size);
    cur_pos_ += data_size;
  }

  return ret;
}

int WriterBufferHandler::init(const int64_t buf_size)
{
  return resize(buf_size);
}

int WriterBufferHandler::resize(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the allocator sholdn't be null", K(ret));
  } else {
    free_buffer();
    if (OB_SUCC(ret)) {
      buf_ = static_cast<char*>(store_->alloc(size));
      if (OB_ISNULL(buf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(size));
      } else {
        buf_size_ = size;
        cur_pos_ = 0;
        row_cnt_ = 0;
      }
    }
  }
  return ret;
}

void WriterBufferHandler::free_buffer()
{
  if (OB_NOT_NULL(buf_) && OB_NOT_NULL(store_)) {
    store_->free(buf_, buf_size_);
    buf_ = nullptr;
  }
}

}
}