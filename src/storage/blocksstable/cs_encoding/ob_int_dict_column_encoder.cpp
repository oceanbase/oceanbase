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

#include "ob_int_dict_column_encoder.h"
#include "ob_cs_encoding_util.h"
#include "ob_string_stream_encoder.h"
#include "ob_integer_stream_encoder.h"
#include "ob_column_datum_iter.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"
#include "lib/codec/ob_codecs.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

int ObIntDictColumnEncoder::init(
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
    dict_encoding_meta_.distinct_val_cnt_ = ctx.ht_->size();
    dict_encoding_meta_.ref_row_cnt_ = row_count_;
    if (ctx_->null_cnt_ > 0) {
      dict_encoding_meta_.set_has_null();
    }
    column_header_.set_is_fixed_length();
    precision_width_size_ = -1;
    if (ObDecimalIntSC == store_class_) {
      precision_width_size_ =
          wide::ObDecimalIntConstValue::get_int_bytes_by_precision(column_type_.get_stored_precision());
    }
    if (OB_FAIL(build_integer_dict_encoder_ctx_())) {
      LOG_WARN("fail to build integer dict encoder ctx", K(ret));
    } else if (OB_FAIL(build_ref_encoder_ctx_())) {
      LOG_WARN("fail to build ref encoder ctx", K(ret));
    }
  }
  return ret;
}

int ObIntDictColumnEncoder::build_integer_dict_encoder_ctx_()
{
  int ret = OB_SUCCESS;
  const bool is_replace_null = false;
  const int64_t null_replaced_value = 0;
  const ObObjTypeClass tc = ob_obj_type_class(column_type_.get_type());
  if (ctx_->need_sort_) {
    // Only ObIntTC/ObUnitTC/ObDecimalIntSC have the same sorting rules as integer
    if (ObIntTC == tc || ObUIntTC == tc || ObDecimalIntSC == store_class_) {
      is_monotonic_inc_integer_dict_ = true;
    }
  }
  if (row_count_ == ctx_->null_cnt_) { // empty dict
    if (dict_encoding_meta_.distinct_val_cnt_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dict count", K(ret), KPC_(ctx), K_(dict_encoding_meta));
    }
  } else {
    if (ObIntSC == store_class_ || ObDecimalIntSC == store_class_) {
      const int64_t int_min = static_cast<int64_t>(ctx_->integer_min_);
      const int64_t int_max = static_cast<int64_t>(ctx_->integer_max_);
      if (OB_FAIL(integer_dict_enc_ctx_.build_signed_stream_meta(int_min, int_max, is_replace_null,
          null_replaced_value, precision_width_size_, is_force_raw_, dict_integer_range_))) {
        LOG_WARN("fail to build_signed_stream_meta", K(ret));
      }
    } else if (ObUIntSC == store_class_) {
      const uint64_t uint_min = static_cast<uint64_t>(ctx_->integer_min_);
      const uint64_t uint_max = static_cast<uint64_t>(ctx_->integer_max_);
      if (OB_FAIL(integer_dict_enc_ctx_.build_unsigned_stream_meta(
          uint_min, uint_max, is_replace_null, null_replaced_value,
          is_force_raw_, dict_integer_range_))) {
        LOG_WARN("fail to build_unsigned_stream_meta", K(ret));
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("unexpected store class", K(ret), K_(store_class));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(integer_dict_enc_ctx_.build_stream_encoder_info(
        false/*has_null*/,
        is_monotonic_inc_integer_dict_,
        &ctx_->encoding_ctx_->cs_encoding_opt_,
        ctx_->encoding_ctx_->previous_cs_encoding_.get_column_encoding(column_index_),
        0/*stream_idx*/, ctx_->encoding_ctx_->compressor_type_, ctx_->allocator_))) {
      LOG_WARN("fail to build_stream_encoder_info", K(ret));
    } else {
      ++int_stream_count_;
    }
  }

  return ret;
}

int ObIntDictColumnEncoder::store_column(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(sort_dict_())) {
    LOG_WARN("fail to sort dict", K(ret));
  } else if (OB_FAIL(store_dict_encoding_meta_(buf_writer))) {
    LOG_WARN("fail to store dict encoding meta", K(ret), K_(dict_encoding_meta));
  } else if (OB_FAIL(store_dict_(buf_writer))) {
    LOG_WARN("fail to store dict", K(ret), K_(dict_encoding_meta));
  } else if (OB_FAIL(store_dict_ref_(buf_writer))) {
    LOG_WARN("fail to store dict ref", K(ret), K_(dict_encoding_meta));
  }

  return ret;
}

int ObIntDictColumnEncoder::sort_dict_()
{
  int ret = OB_SUCCESS;

  if (!ctx_->need_sort_ || dict_encoding_meta_.distinct_val_cnt_ == 0) {
    // do nothing
  } else if (OB_FAIL(do_sort_dict_())) {
    LOG_WARN("fail to do_sort_dict", K(ret));
  } else {
    const ObObjTypeClass tc = ob_obj_type_class(column_type_.get_type());
    uint64_t min = 0;
    uint64_t max = 0;
    if (ObIntTC == tc || ObUIntTC == tc) {
      min = ctx_->ht_->begin()->header_->datum_->get_uint64();
      max = (ctx_->ht_->end() - 1)->header_->datum_->get_uint64();
    } else if (ObDecimalIntSC == store_class_) {
      if (precision_width_size_ == sizeof(uint32_t)) {
        min = ctx_->ht_->begin()->header_->datum_->get_decimal_int32();
        max = (ctx_->ht_->end() - 1)->header_->datum_->get_decimal_int32();
      } else {
        min = ctx_->ht_->begin()->header_->datum_->get_decimal_int64();
        max = (ctx_->ht_->end() - 1)->header_->datum_->get_decimal_int64();
      }
    }
    if (ObIntTC == tc || ObUIntTC == tc || ObDecimalIntSC == store_class_) {
      if (min != ctx_->integer_min_ || max != ctx_->integer_max_ ||
          !is_monotonic_inc_integer_dict_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("integer dict min or max error", K(ret), K(tc), K(store_class_),
            KPC_(ctx), K(min), K(max), K_(is_monotonic_inc_integer_dict));
      }
    }
  }
  return ret;
}

int ObIntDictColumnEncoder::store_dict_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (dict_encoding_meta_.distinct_val_cnt_ == 0) {
    // has no dict value, do nothing
  } else {
    ObIntegerStreamEncoder integer_encoder;
    ObDictDatumIter datum_iter(*ctx_->ht_);
    if (OB_FAIL(integer_encoder.encode(integer_dict_enc_ctx_, datum_iter, buf_writer))) {
      LOG_WARN("fail to store dict integer stream", K(ret), K(integer_dict_enc_ctx_));
    } else if (OB_FAIL(stream_offsets_.push_back(buf_writer.length()))) {
      LOG_WARN("fail to push back dict integer stream offset", K(ret));
    } else {
      int_stream_encoding_types_[int_stream_idx_] = integer_dict_enc_ctx_.meta_.get_encoding_type();
      int_stream_idx_++;
    }
  }
  return ret;
}

void ObIntDictColumnEncoder::reuse()
{
  ObDictColumnEncoder::reuse();
  integer_dict_enc_ctx_.reset();
  dict_integer_range_ = 0;
  is_monotonic_inc_integer_dict_ = false;
  precision_width_size_ = -1;
}

int ObIntDictColumnEncoder::get_string_data_len(uint32_t &len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    len = 0;
  }
  return ret;
}

int ObIntDictColumnEncoder::get_maximal_encoding_store_size(int64_t &size) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    size = sizeof(ObDictEncodingMeta);
    const int64_t distinct_cnt = dict_encoding_meta_.distinct_val_cnt_;
    if (0 == distinct_cnt) {
      // has no stream
    } else {
      // dict value stream
      size += sizeof(ObIntegerStreamMeta);
      const int64_t dict_orig_size = integer_dict_enc_ctx_.meta_.get_uint_width_size() * distinct_cnt;
      size += common::ObCodec::get_moderate_encoding_size(dict_orig_size);

      // ref store size
      size += sizeof(ObIntegerStreamMeta);
      const int64_t ref_orig_size = ref_enc_ctx_.meta_.get_uint_width_size() * dict_encoding_meta_.ref_row_cnt_;
      size += common::ObCodec::get_moderate_encoding_size(ref_orig_size);

      size = std::min(size, ObCSEncodingUtil::MAX_COLUMN_ENCODING_STORE_SIZE);
    }
  }

  return ret;
}

int64_t ObIntDictColumnEncoder::estimate_store_size() const
{
  // sizeof(ObDictEncodingMeta) + dict_size + ref_size
  int64_t size = INT64_MAX;
  if (!is_inited_) {
  } else if (is_force_raw_) {
  } else {
    const int64_t distinct = dict_encoding_meta_.distinct_val_cnt_;
    size = sizeof(ObDictEncodingMeta);
    if (distinct == 0) {
      // has no dict
    } else {
      size += ObCSEncodingUtil::get_bit_size(dict_integer_range_) * distinct / CHAR_BIT;
      size += ObCSEncodingUtil::get_bit_size(ref_stream_max_value_) * dict_encoding_meta_.ref_row_cnt_ / CHAR_BIT ; // ref store size
    }
  }
  return size;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
