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

#include "ob_string_column_encoder.h"
#include "storage/blocksstable/encoding/ob_encoding_hash_util.h"
#include "ob_column_datum_iter.h"
#include "ob_cs_encoding_util.h"
#include "lib/codec/ob_codecs.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

ObStringColumnEncoder::ObStringColumnEncoder()
  : enc_ctx_(),
    string_stream_encoder_()
{
}

ObStringColumnEncoder::~ObStringColumnEncoder() {}

int ObStringColumnEncoder::init(
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
    if (OB_FAIL(do_init_())) {
      LOG_WARN("fail to pre_handle", K(ret));
    }
  }

  return ret;
}

void ObStringColumnEncoder::reuse()
{
  ObIColumnCSEncoder::reuse();
  enc_ctx_.reset();
  string_stream_encoder_.reuse();
}

int ObStringColumnEncoder::do_init_()
{
  int ret = OB_SUCCESS;
  int64_t fixed_len = -1;
  bool is_use_zero_len_as_null = false;
  uint32_t uncompress_len = 0;
  int32_t int_stream_idx = -1;
  // use zero len as null if datum has no zero len and is not fixed length.
  // if all datums are null, the ctx_->fix_data_size_ equal -1,
  // so it also use zero len as null.
  if (ctx_->null_cnt_ > 0) {
    if (ctx_->has_zero_length_datum_) {
      column_header_.set_has_null_bitmap();
      if (ctx_->fix_data_size_ >= 0) {
        column_header_.set_is_fixed_length();
        fixed_len = ctx_->fix_data_size_;
      } else {
        // not USE_ZERO_LEN_AS_NULL and not IS_FIXED_LENGTH_STRING
      }
    } else if (ctx_->fix_data_size_ >= 0) { // fixed length string
      int64_t fix_padding_size = ctx_->fix_data_size_ * ctx_->null_cnt_;
      int64_t bitmap_size = ObCSEncodingUtil::get_bitmap_byte_size(row_count_);
      int64_t offset_size_arr_size = 0;
      if (is_force_raw_) {
        offset_size_arr_size = row_count_ * get_byte_packed_int_size(ctx_->var_data_size_);
      } else {
        // This is not an exact calculation, just estimate.
        // offset arry use inc deta and add a bit to each length bit for some metadata cost.
        int64_t length_bit_size = ObCSEncodingUtil::get_bit_size(ctx_->fix_data_size_);
        offset_size_arr_size = (length_bit_size + 1) * row_count_ / CHAR_BIT;
      }
      // use fix length has less storage cost
      if (fix_padding_size + bitmap_size < offset_size_arr_size) {
        column_header_.set_is_fixed_length();
        fixed_len = ctx_->fix_data_size_;
        column_header_.set_has_null_bitmap();
      } else {
        is_use_zero_len_as_null = true;
      }
      LOG_DEBUG("do init fixed length string", K(ret),
          K(fix_padding_size), K(bitmap_size), K(offset_size_arr_size), KPC_(ctx));
    } else { // variable length string
      is_use_zero_len_as_null = true;
    }
  } else { // has no null
    if (ctx_->fix_data_size_ >= 0) {
      column_header_.set_is_fixed_length();
      fixed_len = ctx_->fix_data_size_;
    } else {
      // not USE_ZERO_LEN_AS_NULL and not IS_FIXED_LENGTH_STRING
    }
  }

  if (fixed_len < 0) { // var len string, need offset int stream
    int_stream_count_ = 1;
    int_stream_idx = 0;
    uncompress_len = ctx_->var_data_size_;
  } else {
    uncompress_len = fixed_len * row_count_;
  }
  if (OB_FAIL(enc_ctx_.build_string_stream_meta(
      fixed_len, is_use_zero_len_as_null, uncompress_len))) {
    LOG_WARN("fail to build_string_stream_meta", K(ret));
  } else if (OB_FAIL(enc_ctx_.build_string_stream_encoder_info(
      ctx_->encoding_ctx_->compressor_type_,
      is_force_raw_, &ctx_->encoding_ctx_->cs_encoding_opt_,
      ctx_->encoding_ctx_->previous_cs_encoding_.get_column_encoding(column_index_),
      int_stream_idx, ctx_->allocator_))) {
    LOG_WARN("fail to build_string_stream_encoder_info", K(ret));
  }

  return ret;
}

int ObStringColumnEncoder::store_column(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  ObSEArray<uint32_t, 2> offsets;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // first stream offset include the column optional meta
    if (OB_FAIL(store_column_meta_(buf_writer))) {
      LOG_WARN("fail to store column optional meta", K(ret));
    } else {
      ObColumnDatumIter iter(*ctx_->col_datums_);
      if (OB_FAIL(string_stream_encoder_.encode(
          enc_ctx_, iter, buf_writer, ctx_->all_string_buf_writer_, offsets))) {
        LOG_WARN("fail to store stream", K(ret), K_(enc_ctx));
      } else if (OB_UNLIKELY(offsets.count() != 1 && offsets.count() != 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stream offset count unexpected", K(ret), K(offsets));
      } else if (OB_UNLIKELY(buf_writer.length() != offsets.at(offsets.count() - 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last offset must equal to buf pos", K(ret), K(offsets), K(buf_writer.length()));
      } else if (!enc_ctx_.meta_.is_fixed_len_string()) {
        int_stream_encoding_types_[0] = string_stream_encoder_.get_offset_encoder_ctx().meta_.get_encoding_type();
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < offsets.count(); i++) {
      if (OB_FAIL(stream_offsets_.push_back(offsets.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }

  return ret;
}

int ObStringColumnEncoder::store_column_meta_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_null_bitamp(buf_writer))) {
    LOG_WARN("fail to store null bitmap", K(ret));
  }
  return ret;
}

int64_t ObStringColumnEncoder::estimate_store_size() const
{
  int64_t size = INT64_MAX;
  if (!is_inited_) {
  } else if (is_force_raw_) {
  } else {
    if (enc_ctx_.meta_.is_fixed_len_string()) {
      size = enc_ctx_.meta_.get_fixed_string_len() * row_count_;
    } else { // variable length string
      int64_t avg_length = ctx_->var_data_size_ / row_count_;
      int64_t length_byte_size = ObCSEncodingUtil::get_bit_size(avg_length) * row_count_ / CHAR_BIT;
      size = ctx_->var_data_size_ + length_byte_size;
    }
    if (column_header_.has_null_bitmap()) {
      size += ObCSEncodingUtil::get_bitmap_byte_size(row_count_);
    }
  }

  return size;
}

int ObStringColumnEncoder::get_identifier_and_stream_types(
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

int ObStringColumnEncoder::get_maximal_encoding_store_size(int64_t &size) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    size = sizeof(ObStringStreamMeta);
    if (!enc_ctx_.meta_.is_fixed_len_string()) {
      int64_t offset_arry_orig_size = get_byte_packed_int_size(ctx_->var_data_size_) * row_count_;
      size += sizeof(ObIntegerStreamMeta);
      size += common::ObCodec::get_moderate_encoding_size(offset_arry_orig_size);
    }
    if (column_header_.has_null_bitmap()) {
      size += ObCSEncodingUtil::get_bitmap_byte_size(row_count_);
    }
    size = std::min(size, ObCSEncodingUtil::MAX_COLUMN_ENCODING_STORE_SIZE);
  }
  return ret;
}

int ObStringColumnEncoder::get_string_data_len(uint32_t &len) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    len = enc_ctx_.meta_.uncompressed_len_;
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
