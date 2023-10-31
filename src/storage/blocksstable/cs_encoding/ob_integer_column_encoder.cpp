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

#include "ob_integer_column_encoder.h"
#include "ob_cs_encoding_util.h"
#include "ob_column_datum_iter.h"
#include "lib/codec/ob_codecs.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

ObIntegerColumnEncoder::ObIntegerColumnEncoder()
  : type_store_size_(-1),
    enc_ctx_(),
    integer_stream_encoder_(),
    integer_range_(0),
    precision_width_size_(-1)
{
}

ObIntegerColumnEncoder::~ObIntegerColumnEncoder() {}

int ObIntegerColumnEncoder::init(
  const ObColumnCSEncodingCtx &ctx, const int64_t column_index, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnCSEncoder::init(ctx, column_index, row_count))) {
    LOG_WARN("init base column encoder failed", K(ret), K(ctx), K(column_index), K(row_count));
  } else {
    column_header_.type_ = type_;
    column_header_.set_is_fixed_length();
    if (ObDecimalIntSC == store_class_) {
      type_store_size_ = sizeof(int64_t);
      precision_width_size_ = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(column_type_.get_stored_precision());
    } else {
      type_store_size_ = get_type_size_map()[column_type_.get_type()];
      precision_width_size_ = -1;
    }

    if ((ObIntSC != store_class_ && ObUIntSC != store_class_ && ObDecimalIntSC != store_class_) || type_store_size_ < 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported store class", K(ret), K_(store_class), K_(column_type), K_(type_store_size), K_(column_index));
    } else if (OB_FAIL(do_init_())) {
      LOG_WARN("fail to do init", K(ret));
    } else {
      LOG_DEBUG("init integer column encoder", K(ret), K_(store_class),
          K_(column_type), K_(type_store_size), K_(column_index), K_(precision_width_size));
    }
  }
  return ret;
}

void ObIntegerColumnEncoder::reuse()
{
  ObIColumnCSEncoder::reuse();
  type_store_size_ = -1;
  precision_width_size_ = -1;
  integer_stream_encoder_.reuse();
  integer_range_ = 0;
  enc_ctx_.reset();
}

int ObIntegerColumnEncoder::do_init_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row_count_ == ctx_->null_cnt_) { // all datums is null
    if (OB_FAIL(enc_ctx_.build_signed_stream_meta(
        0, 0, true/*is_replace_null*/, 0, precision_width_size_, is_force_raw_, integer_range_))) {
      LOG_WARN("fail to build_signed_stream_meta", K(ret));
    }
  } else if (ObIntSC == store_class_ || ObDecimalIntSC == store_class_) {
    if (OB_FAIL(build_signed_stream_meta_())) {
      LOG_WARN("fail to build signed encoder ctx", K(ret));
    }
  } else if (ObUIntSC == store_class_) {
    if (OB_FAIL(build_unsigned_encoder_ctx_())) {
      LOG_WARN("fail to build signed raw encoder ctx", K(ret));
    }
  } else {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("unexpected store class", K(ret), K_(store_class));
  }

  if (OB_SUCC(ret)) {
    if (ctx_->null_cnt_ > 0) {
      if (!enc_ctx_.meta_.is_use_null_replace_value()) {
        column_header_.set_has_null_bitmap();
      }
    }
    int_stream_count_ = 1;
    if (OB_FAIL(enc_ctx_.build_stream_encoder_info(
        ctx_->null_cnt_ > 0/*has_null*/,
        false/*not monotonic*/,
        &ctx_->encoding_ctx_->cs_encoding_opt_,
        ctx_->encoding_ctx_->previous_cs_encoding_.get_column_encoding(column_index_),
        0/*stream_idx*/, ctx_->encoding_ctx_->compressor_type_, ctx_->allocator_))) {
      LOG_WARN("fail to build_stream_encoder_info", K(ret));
    }
  }

  return ret;
}

int ObIntegerColumnEncoder::store_column(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // first stream offset include the column meta
    if (OB_FAIL(store_column_meta_(buf_writer))) {
      LOG_WARN("fail to store column optional meta", K(ret));
    } else {
      ObColumnDatumIter iter(*ctx_->col_datums_);
      if (OB_FAIL(integer_stream_encoder_.encode(enc_ctx_, iter, buf_writer))) {
        LOG_WARN("fail to encode stream", K(ret), K(enc_ctx_));
      } else if (OB_FAIL(stream_offsets_.push_back(buf_writer.length()))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        int_stream_encoding_types_[0] = enc_ctx_.meta_.get_encoding_type();
      }
    }
  }

  return ret;
}

int ObIntegerColumnEncoder::store_column_meta_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_null_bitamp(buf_writer))) {
    LOG_WARN("fail to store null bitmap", K(ret));
  }

  return ret;
}

int ObIntegerColumnEncoder::build_signed_stream_meta_()
{
  int ret = OB_SUCCESS;
  const int64_t int_min = static_cast<int64_t>(ctx_->integer_min_);
  const int64_t int_max = static_cast<int64_t>(ctx_->integer_max_);

  if (int_min > int_max) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", K(ret), KPC_(ctx));
  } else {
    int64_t new_int_max = int_max;
    int64_t new_int_min = int_min;

    uint64_t mask = INTEGER_MASK_TABLE[type_store_size_];
    uint64_t reverse_mask = ~mask;
    int64_t type_store_min = reverse_mask == 0 ? INT64_MIN : (reverse_mask | reverse_mask >> 1);
    int64_t type_store_max = mask >> 1;
    bool is_replace_null = false;
    int64_t null_replaced_value = 0;

    if (ctx_->null_cnt_ > 0) { // has null
      if (0 == int_min) {
        // In case int_min minus 1 turns negative, the largest not existed value is preferred
        if (int_max != type_store_max) {
          new_int_max = int_max + 1;
          ob_assert(new_int_max <= type_store_max);
          is_replace_null = true;
          null_replaced_value = new_int_max;
        } else {
          new_int_min = -1;
          is_replace_null = true;
          null_replaced_value = new_int_min;
        }
      } else if (int_min == type_store_min) {
        if (int_max != type_store_max) {
          // TODO(may be need other value between min and max)
          new_int_max = int_max + 1;
          ob_assert(new_int_max <= type_store_max);
          is_replace_null = true;
          null_replaced_value = new_int_max;
        } else {
          // over the value scope of 1/2/4/8 bytes, will use bitmap
          is_replace_null = false;
        }
      } else {
        new_int_min = int_min - 1;
        ob_assert(new_int_min >= type_store_min);
        is_replace_null = true;
        null_replaced_value = new_int_min;
      }
    }

    if (OB_FAIL(enc_ctx_.build_signed_stream_meta(new_int_min, new_int_max, is_replace_null,
        null_replaced_value, precision_width_size_, is_force_raw_, integer_range_))) {
      LOG_WARN("fail to build_signed_stream_meta", K(ret));
    }
  }

  return ret;
}

int ObIntegerColumnEncoder::build_unsigned_encoder_ctx_()
{
  int ret = OB_SUCCESS;
  const uint64_t uint_min = ctx_->integer_min_;
  const uint64_t uint_max = ctx_->integer_max_;

  if (uint_min > uint_max) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", K(ret), KPC_(ctx));
  } else {
    uint64_t new_uint_max = uint_max;
    uint64_t new_uint_min = uint_min;

    uint64_t mask = INTEGER_MASK_TABLE[type_store_size_];
    uint64_t type_store_max = mask;
    bool is_replace_null = false;
    int64_t null_replaced_value = 0;

    if (ctx_->null_cnt_ > 0) {
      if (uint_min == 0) {
        if (uint_max != type_store_max) {
          new_uint_max = uint_max + 1;
          ob_assert(new_uint_max <= type_store_max);
          is_replace_null = true;
          null_replaced_value = new_uint_max;
        } else {
          // over the value scope of 1/2/4/8 bytes, will use bitmap
          is_replace_null = false;
        }
      } else {
        new_uint_min = uint_min - 1;
        is_replace_null = true;
        null_replaced_value = new_uint_min;
      }
    }

    if (OB_FAIL(enc_ctx_.build_unsigned_stream_meta(
        new_uint_min, new_uint_max, is_replace_null, null_replaced_value,
        is_force_raw_, integer_range_))) {
      LOG_WARN("fail to build_unsigned_stream_meta", K(ret));
    }
  }

  return ret;
}

int64_t ObIntegerColumnEncoder::estimate_store_size() const
{
  int64_t size = INT64_MAX;
  if (!is_inited_) {
  } else if (is_force_raw_) {
  } else {
    size = ObCSEncodingUtil::get_bit_size(integer_range_) * row_count_ / CHAR_BIT;
    // has null datum and can't replace null value, need use bitmap
    if (column_header_.has_null_bitmap()) {
      size += ObCSEncodingUtil::get_bitmap_byte_size(row_count_);
    }
  }

  return size;
}

int ObIntegerColumnEncoder::get_identifier_and_stream_types(
    ObColumnEncodingIdentifier &identifier, const ObIntegerStream::EncodingType *&types) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    identifier.set(type_, int_stream_count_, 0);
    types = int_stream_encoding_types_;
  }
  return ret;
}

int ObIntegerColumnEncoder::get_maximal_encoding_store_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    size = sizeof(ObIntegerStreamMeta) +
        common::ObCodec::get_moderate_encoding_size(enc_ctx_.meta_.get_uint_width_size() * row_count_);
    if (column_header_.has_null_bitmap()) {
      size += ObCSEncodingUtil::get_bitmap_byte_size(row_count_);
    }
    size = std::min(size, ObCSEncodingUtil::MAX_COLUMN_ENCODING_STORE_SIZE);
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
