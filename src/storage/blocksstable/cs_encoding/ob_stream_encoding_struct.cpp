
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

#include "ob_stream_encoding_struct.h"
#include "ob_column_encoding_struct.h"


namespace oceanbase
{
namespace blocksstable
{

//============================ ObIntegerStreamEncoderCtx ==============================//

DEFINE_SERIALIZE(ObIntegerStreamMeta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to encode version", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, attr_))) {
    LOG_WARN("fail to encode attr", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, type_))) {
    LOG_WARN("fail to encode type", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, width_))) {
    LOG_WARN("fail to encode width", K(ret));
  } else if (is_use_base() && OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, base_value_))) {
    LOG_WARN("fail to encode base_value_", K(ret));
  } else if (is_use_null_replace_value() && OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, null_replaced_value_))) {
    LOG_WARN("fail to encode null_replaced_value_", K(ret));
  } else if (is_decimal_int() && OB_FAIL(serialization::encode_i8(buf, buf_len, pos, decimal_precision_width_))) {
    LOG_WARN("fail to encode decimal_precision_width_", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObIntegerStreamMeta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t*)&version_))) {
    LOG_WARN("fail to decode version", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t*)&attr_))) {
    LOG_WARN("fail to decode attr", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t*)&type_))) {
    LOG_WARN("fail to encode type", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t*)&width_))) {
    LOG_WARN("fail to encode width", K(ret));
  } else if (is_use_base() && OB_FAIL(serialization::decode_vi64(buf, data_len, pos, (int64_t*)&base_value_))) {
    LOG_WARN("fail to decode base_value_", K(ret));
  } else if (is_use_null_replace_value() && OB_FAIL(serialization::decode_vi64(buf, data_len, pos, (int64_t*)&null_replaced_value_))) {
    LOG_WARN("fail to decode null_replaced_value_", K(ret));
  } else if (is_decimal_int() && OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t*)&decimal_precision_width_))) {
    LOG_WARN("fail to decode decimal_precision_width_", K(ret));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObIntegerStreamMeta)
{
  int64_t len = 0;
  len += serialization::encoded_length_i8(version_);
  len += serialization::encoded_length_i8(attr_);
  len += serialization::encoded_length_i8(type_);
  len += serialization::encoded_length_i8(width_);
  if (is_use_base()) {
    len += serialization::encoded_length_vi64(base_value_);
  }
  if (is_use_null_replace_value()) {
    len += serialization::encoded_length_vi64(null_replaced_value_);
  }
  if (is_decimal_int()) {
    len += serialization::encoded_length_i8(decimal_precision_width_);
  }
  return len;
}

int ObIntegerStreamEncoderCtx::build_signed_stream_meta(
    const int64_t min, const int64_t max, const bool is_replace_null,
    const int64_t replace_value, const int64_t precision_width_size,
    const bool force_raw, uint64_t &range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(min > max)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min), K(max));
  } else if (precision_width_size > 0 && OB_FAIL(meta_.set_precision_width_size(precision_width_size))) {
    LOG_WARN("fail to set_precision_width_size", K(ret), K(precision_width_size));
  } else {
    if (force_raw) {
      meta_.set_raw_encoding();
    }
    if (is_replace_null) {
      meta_.set_null_replaced_value(replace_value);
    }
    if (precision_width_size > 0) {
      meta_.set_precision_width_size(precision_width_size);
    }

    if (min < 0) {
      range = max - min;
      meta_.set_base_value((uint64_t)min);
      if (OB_FAIL(meta_.set_uint_width_size(get_byte_packed_int_size(range)))) {
        LOG_WARN("fail to set_uint_width_size", K(ret));
      }
    } else {  // min >= 0 then max must be > 0
      range = max;
      int64_t int_max_byte_size = get_byte_packed_int_size(max);
      // not use base when there is no negative value
      if (OB_FAIL(meta_.set_uint_width_size(int_max_byte_size))) {
        LOG_WARN("fail to set_uint_width_size", K(ret));
      }
    }
  }
  return ret;
}

int ObIntegerStreamEncoderCtx::build_unsigned_stream_meta(const uint64_t min,
    const uint64_t max, const bool is_replace_null, const uint64_t replace_value,
    const bool force_raw, uint64_t &range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(min > max)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min), K(max));
  } else {
    range = max;
    if (force_raw) {
      meta_.set_raw_encoding();
    }
    if (is_replace_null) {
      meta_.set_null_replaced_value(replace_value);
    }
    int64_t uint_max_byte_size = get_byte_packed_int_size(max);
    // not use base when there is no negative value
    if (OB_FAIL(meta_.set_uint_width_size(uint_max_byte_size))) {
      LOG_WARN("fail to set_uint_width_size", K(ret));
    }
  }

  return ret;
}
int ObIntegerStreamEncoderCtx::build_offset_array_stream_meta(const uint64_t end_offset,
                                                              bool force_raw)
{
  int ret = OB_SUCCESS;
  if (force_raw) {
    meta_.set_raw_encoding();
  }
  info_.is_monotonic_inc_ = true;
  int64_t width_size = get_byte_packed_int_size(end_offset);
  if (OB_FAIL(meta_.set_uint_width_size(width_size))) {
    LOG_WARN("fail to set_uint_width_size", KR(ret), K(width_size));
  }

  return ret;
}
int ObIntegerStreamEncoderCtx::build_stream_encoder_info(
                                 const bool has_null,
                                 bool is_monotonic_inc,
                                 const ObCSEncodingOpt *encoding_opt,
                                 const ObPreviousColumnEncoding *previous_encoding,
                                 const int32_t stream_idx,
                                 const ObCompressorType compressor_type,
                                 ObIAllocator *allocator_)
{
  int ret = OB_SUCCESS;

  info_.has_null_datum_ = has_null;
  info_.is_monotonic_inc_ = is_monotonic_inc;
  info_.encoding_opt_ = encoding_opt;
  info_.previous_encoding_ = previous_encoding;
  info_.stream_idx_ = stream_idx;
  info_.compressor_type_ = compressor_type;
  info_.allocator_ = allocator_;
  return ret;
}

int ObIntegerStreamEncoderCtx::try_use_previous_encoding(bool &use_previous)
{
  int ret = OB_SUCCESS;
  use_previous = false;
  const ObPreviousColumnEncoding *previous = info_.previous_encoding_;
  if (nullptr == previous) {
    // previous_encoding is not set
  } else if (previous->is_valid_ && !previous->need_redetect_) {
    if ((info_.stream_idx_ < 0)
        || (info_.stream_idx_ >= ObCSColumnHeader::MAX_INT_STREAM_COUNT_OF_COLUMN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("illegal stream_idx", K(ret), K_(info));
    } else {
      ObIntegerStream::EncodingType type = previous->stream_encoding_types_[info_.stream_idx_];
      use_previous = true;
      meta_.set_encoding_type(type);
    }
  }

  return ret;
}

//============================ ObStringStreamEncoderCtx ==============================//

DEFINE_SERIALIZE(ObStringStreamMeta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to encode version", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, attr_))) {
    LOG_WARN("fail to encode attr", K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, uncompressed_len_))) {
    LOG_WARN("fail to encode uncompressed_len_", K(ret));
  } else if (is_fixed_len_string() && OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, fixed_str_len_))) {
    LOG_WARN("fail to encode fixed_str_len_", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObStringStreamMeta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t*)&version_))) {
    LOG_WARN("fail to decode version", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, (int8_t*)&attr_))) {
    LOG_WARN("fail to decode attr", K(ret));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, (int32_t*)&uncompressed_len_))) {
    LOG_WARN("fail to decode uncompressed_len_", K(ret));
  } else if (is_fixed_len_string() && OB_FAIL(serialization::decode_vi32(buf, data_len, pos, (int32_t*)&fixed_str_len_))) {
    LOG_WARN("fail to decode fixed_str_len_", K(ret));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObStringStreamMeta)
{
  int64_t len = 0;
  len += serialization::encoded_length_i8(version_);
  len += serialization::encoded_length_i8(attr_);
  len += serialization::encoded_length_vi32(uncompressed_len_);
  if (is_fixed_len_string()) {
    len += serialization::encoded_length_vi32(fixed_str_len_);
  }
  return len;
}


int ObStringStreamEncoderCtx::build_string_stream_meta(const int64_t fixed_len,
                                                       const bool use_zero_length_as_null,
                                                       const uint32_t uncompress_len)
{
  int ret = OB_SUCCESS;
  if (fixed_len >= 0) {
    meta_.set_is_fixed_len_string();
    meta_.fixed_str_len_ = fixed_len;
  } else if (use_zero_length_as_null) {
    meta_.set_use_zero_len_as_null();
  }
  meta_.uncompressed_len_ = uncompress_len;
  return ret;
}

int ObStringStreamEncoderCtx::build_string_stream_encoder_info(
        const common::ObCompressorType compressor_type,
        const bool raw_encoding_str_offset,
        const ObCSEncodingOpt *encoding_opt,
        const ObPreviousColumnEncoding *previous_encoding,
        const int32_t int_stream_idx,
        ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  info_.compressor_type_ = compressor_type;
  info_.raw_encoding_str_offset_ = raw_encoding_str_offset;
  info_.encoding_opt_ = encoding_opt;
  info_.previous_encoding_ = previous_encoding;
  info_.int_stream_idx_ = int_stream_idx;
  info_.allocator_ = allocator;
  return ret;
}

}
}
