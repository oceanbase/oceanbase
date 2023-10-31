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

#include "ob_str_dict_column_encoder.h"
#include "ob_cs_encoding_util.h"
#include "ob_string_stream_encoder.h"
#include "ob_column_datum_iter.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"
#include "lib/codec/ob_codecs.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

int ObStrDictColumnEncoder::init(
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
    if (OB_FAIL(build_string_dict_encoder_ctx_())) {
      LOG_WARN("fail to build string dict encoder ctx", K(ret));
    } else if (OB_FAIL(build_ref_encoder_ctx_())) {
      LOG_WARN("fail to build ref encoder ctx", K(ret));
    }
  }
  return ret;
}

void ObStrDictColumnEncoder::reuse()
{
  ObDictColumnEncoder::reuse();
  string_dict_enc_ctx_.reset();
}

int ObStrDictColumnEncoder::build_string_dict_encoder_ctx_()
{
  int ret = OB_SUCCESS;
  int32_t int_stream_idx = -1;

  if (row_count_ == ctx_->null_cnt_) { // empty dict
    if (dict_encoding_meta_.distinct_val_cnt_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dict count", K(ret), KPC_(ctx), K_(dict_encoding_meta));
    }
  } else {
    if (ctx_->fix_data_size_ >= 0) {
      column_header_.set_is_fixed_length();
    } else { // variable string
      ++int_stream_count_;
      int_stream_idx = 0;
    }
    if (OB_FAIL(string_dict_enc_ctx_.build_string_stream_meta(
        ctx_->fix_data_size_, false/*use_zero_len_as_null*/, ctx_->dict_var_data_size_))) {
      LOG_WARN("fail to build_string_stream_meta", K(ret));
    } else if (OB_FAIL(string_dict_enc_ctx_.build_string_stream_encoder_info(
        ctx_->encoding_ctx_->compressor_type_, is_force_raw_,
        &ctx_->encoding_ctx_->cs_encoding_opt_,
        ctx_->encoding_ctx_->previous_cs_encoding_.get_column_encoding(column_index_),
        int_stream_idx, ctx_->allocator_))) {
      LOG_WARN("fail to build_string_stream_encoder_info", K(ret), KPC_(ctx));
    }
  }

  return ret;
}

int ObStrDictColumnEncoder::store_column(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(sort_dict_())) {
    LOG_WARN("fail to do_sort_dict", K(ret));
  } else if (OB_FAIL(store_dict_encoding_meta_(buf_writer))) {
    LOG_WARN("fail to store dict encoding meta", K(ret), K_(dict_encoding_meta));
  } else if (OB_FAIL(store_dict_(buf_writer))) {
    LOG_WARN("fail to store dict", K(ret), K_(dict_encoding_meta));
  } else if (OB_FAIL(store_dict_ref_(buf_writer))) {
    LOG_WARN("fail to store dict ref", K(ret), K_(dict_encoding_meta));
  }

  return ret;
}

int ObStrDictColumnEncoder::sort_dict_()
{
  int ret = OB_SUCCESS;

  if (!ctx_->need_sort_ || dict_encoding_meta_.distinct_val_cnt_ == 0) {
    // do nothing
  } else if (OB_FAIL(do_sort_dict_())) {
    LOG_WARN("fail to do_sort_dict", K(ret));
  }
  return ret;
}

int ObStrDictColumnEncoder::store_dict_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (dict_encoding_meta_.distinct_val_cnt_ == 0) {
    // has no dict value, do nothing
  } else {
    ObStringStreamEncoder string_encoder;
    ObDictDatumIter datum_iter(*ctx_->ht_);
    ObSEArray<uint32_t, 2> offsets;
    if (OB_FAIL(string_encoder.encode(
        string_dict_enc_ctx_, datum_iter, buf_writer, ctx_->all_string_buf_writer_, offsets))) {
      LOG_WARN("fail to store dict string stream", K(ret), K(string_dict_enc_ctx_));
    } else if (OB_UNLIKELY(offsets.count() != 1 && offsets.count() != 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stream offsets count unexpected", K(ret), K(offsets));
    } else if (OB_UNLIKELY(buf_writer.length() != offsets.at(offsets.count() - 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last offset must equal to buf pos", K(ret), K(offsets), K(buf_writer.length()));
    } else if (!string_dict_enc_ctx_.meta_.is_fixed_len_string()) {
      int_stream_encoding_types_[int_stream_idx_] = string_encoder.get_offset_encoder_ctx().meta_.get_encoding_type();
      int_stream_idx_++;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < offsets.count(); i++) {
      if (OB_FAIL(stream_offsets_.push_back(offsets.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }

  return ret;
}

int64_t ObStrDictColumnEncoder::estimate_store_size() const
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
      if (string_dict_enc_ctx_.meta_.is_fixed_len_string()) {
        size += string_dict_enc_ctx_.meta_.get_fixed_string_len() * distinct;
      } else {
        // dict_data_size +  dict_string_offset_array_size
        size += ctx_->dict_var_data_size_;
        uint64_t avg_string_length = ctx_->dict_var_data_size_ / distinct;
        size += ObCSEncodingUtil::get_bit_size(avg_string_length) * distinct / CHAR_BIT ;
      }

      size += ObCSEncodingUtil::get_bit_size(ref_stream_max_value_) * dict_encoding_meta_.ref_row_cnt_ / CHAR_BIT ; // ref store size
    }
  }

  return size;
}

int ObStrDictColumnEncoder::get_maximal_encoding_store_size(int64_t &size) const
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
      int64_t offset_arry_orig_size = 0;
      if (!string_dict_enc_ctx_.meta_.is_fixed_len_string()) {
        offset_arry_orig_size = get_byte_packed_int_size(ctx_->dict_var_data_size_) * distinct_cnt;
        size += sizeof(ObIntegerStreamMeta);
        size += common::ObCodec::get_moderate_encoding_size(offset_arry_orig_size);
      }

      const int64_t ref_orig_size = ref_enc_ctx_.meta_.get_uint_width_size() * dict_encoding_meta_.ref_row_cnt_;
      size += sizeof(ObIntegerStreamMeta);
      size += common::ObCodec::get_moderate_encoding_size(ref_orig_size);

      size = std::min(size, ObCSEncodingUtil::MAX_COLUMN_ENCODING_STORE_SIZE);
    }
  }
  return ret;
}

int ObStrDictColumnEncoder::get_string_data_len(uint32_t &len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    len = string_dict_enc_ctx_.meta_.uncompressed_len_;
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
