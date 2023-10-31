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

#define USING_LOG_PREFIX STORAGE

#include "ob_icolumn_cs_encoder.h"
#include "ob_cs_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

ObIColumnCSEncoder::ObIColumnCSEncoder()
    : is_inited_(false),
      column_type_(),
      store_class_(ObExtendSC),
      ctx_(NULL),
      column_index_(0),
      row_count_(0), column_header_(),
      is_force_raw_(false),
      stream_offsets_(),
      int_stream_count_(0)

{
}

int ObIColumnCSEncoder::init(const ObColumnCSEncodingCtx &ctx,
                             const int64_t column_index,
                             const int64_t row_count)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!ctx.encoding_ctx_->is_valid() || column_index < 0
      || column_index >= ctx.encoding_ctx_->column_cnt_ || 0 == row_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(column_index), K(row_count));
  } else {
    const ObObjMeta& col_type = ctx.encoding_ctx_->col_descs_->at(column_index).col_type_;
    store_class_ = get_store_class_map()[ob_obj_type_class(col_type.get_type())];
    column_type_ = col_type;
    ctx_ = &ctx;
    column_index_ = column_index;
    row_count_ = row_count;
    column_header_.obj_type_ = column_type_.get_type();
    is_force_raw_ = ctx.force_raw_encoding_;
    is_inited_ = true;
  }
  return ret;
}

void ObIColumnCSEncoder::reuse()
{
  column_type_.reset();
  store_class_ = ObExtendSC;
  ctx_ = NULL;
  column_index_ = 0;
  row_count_ = 0;
  column_header_.reuse();
  is_force_raw_ = false;
  stream_offsets_.reuse();
  int_stream_count_ = 0;
  is_inited_ = false;
}

int ObIColumnCSEncoder::store_null_bitamp(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (!column_header_.has_null_bitmap()) {
    // has no bitmap, do nothing
  } else {
    char *bitmap = buf_writer.current();
    int64_t bitmap_size = ObCSEncodingUtil::get_bitmap_byte_size(row_count_);

    if (OB_FAIL(buf_writer.advance(bitmap_size))) {
      LOG_WARN("buffer advance failed", K(ret), K(bitmap_size), K(row_count_));
    } else {
      MEMSET(bitmap, 0, bitmap_size);
      const ObColDatums &datums = *ctx_->col_datums_;
      for (int64_t row_id = 0; row_id < row_count_; ++row_id)  {
        if (datums.at(row_id).is_null()) {
          bitmap[row_id / 8] |= (1 << (7 - row_id % 8));
        }
      }
    }
  }

  return ret;
}

int ObIColumnCSEncoder::get_stream_offsets(ObIArray<uint32_t> &offsets) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stream_offsets_.count(); i++) {
      if (OB_FAIL(offsets.push_back(stream_offsets_.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
