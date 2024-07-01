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

#include "ob_icolumn_decoder.h"
#include "ob_encoding_bitset.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObIColumnDecoder::get_is_null_bitmap_from_fixed_column(
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  // 定长列从column meta中直接读 bit packing 的 is_null_bitmap
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pd_filter_info.count_!= result_bitmap.size())
          || OB_ISNULL(col_data)
          || OB_UNLIKELY(!col_ctx.is_fix_length()
              && !col_ctx.is_bit_packing())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for getting isnull bitmap from fixed column",
        K(ret), K(pd_filter_info), K(result_bitmap.size()));
  } else {
    if (col_ctx.has_extend_value()) {
      int64_t row_count = pd_filter_info.count_;
      int64_t bm_block_count = (row_count % ObIColumnDecoder::BITS_PER_BLOCK == 0)
                                ? row_count / ObIColumnDecoder::BITS_PER_BLOCK
                                : row_count / ObIColumnDecoder::BITS_PER_BLOCK + 1;
      int64_t bm_block_bits = ObIColumnDecoder::BITS_PER_BLOCK;
      uint64_t read_buf[bm_block_count];
      MEMSET(read_buf, 0, sizeof(uint64_t) * bm_block_count);
      for (int64_t i = 0; OB_SUCC(ret) && i < bm_block_count - 1; ++i) {
        if (OB_FAIL(ObBitStream::get(col_data, pd_filter_info.start_ + i * bm_block_bits, bm_block_bits, read_buf[i]))) {
          LOG_WARN("Get extended value from column meta failed", K(ret), K(col_ctx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObBitStream::get(
                    col_data,
                    pd_filter_info.start_ + (bm_block_count - 1) * bm_block_bits,
                    bm_block_bits,
                    read_buf[bm_block_count - 1]))) {
        LOG_WARN("Get extended value from column meta failed", K(ret), K(col_ctx));
      } else {
        if (OB_FAIL(result_bitmap.load_blocks_from_array(read_buf, row_count))) {
          LOG_WARN("Failed to load result_bitmap from fix_size column null bitmap", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIColumnDecoder::get_is_null_bitmap_from_var_column(
    const ObColumnDecoderCtx &col_ctx,
    const ObIRowIndex* row_index,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  // 变长列需要遍历对应row区并更新result bitmap
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size())
          || OB_ISNULL(row_index)
          || OB_UNLIKELY(col_ctx.is_fix_length()
              || col_ctx.is_bit_packing())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for getting isnull bitmap from var column",
        K(ret), K(pd_filter_info), K(result_bitmap.size()));
  } else {
    if (col_ctx.has_extend_value()) {
      uint64_t value;
      const char* row_data = nullptr;
      int64_t row_len = 0;
      int64_t row_id = 0;
      for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
        row_id = offset + pd_filter_info.start_;
        if (OB_FAIL(locate_row_data(col_ctx, row_index, row_id, row_data, row_len))) {
          LOG_WARN("Failed to get row_data offset from row index", K(ret));
        } else if (OB_FAIL(ObBitStream::get(
                              reinterpret_cast<const unsigned char*>(row_data),
                              col_ctx.col_header_->extend_value_index_,
                              col_ctx.micro_block_header_->extend_value_bit_,
                              value))) {
          LOG_WARN("Get extended value from row data failed", K(ret), K(col_ctx));
        } else if (value & static_cast<uint64_t>(1)) {
          if (OB_FAIL(result_bitmap.set(offset))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(offset));
          }
        }
      }
    }
  }
  return ret;
}

int ObIColumnDecoder::set_null_datums_from_fixed_column(
    const ObColumnDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    const unsigned char *col_data,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(ObBitStream::get(col_data, row_id * ctx.micro_block_header_->extend_value_bit_,
        ctx.micro_block_header_->extend_value_bit_, val))) {
      LOG_WARN("Get extend value failed", K(ret), K(ctx));
    } else {
      datums[i].null_ = STORED_NOT_EXT != val;
      datums[i].len_ = 0;
    }
  }
  return ret;
}

int ObIColumnDecoder::set_null_datums_from_var_column(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  const char *row_data = nullptr;
  int64_t row_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(locate_row_data(ctx, row_index, row_id, row_data, row_len))) {
      LOG_WARN("Failed to get row_data offset from row index", K(ret), K(row_id));
    } else if (OB_FAIL(ObBitStream::get(
        reinterpret_cast<const unsigned char *>(row_data),
        ctx.col_header_->extend_value_index_,
        ctx.micro_block_header_->extend_value_bit_,
        val))) {
      LOG_WARN("Failed to get extend value from row data", K(ret), K(ctx));
    } else {
      datums[i].null_ = STORED_NOT_EXT != val;
      datums[i].len_ = 0;
    }
  }
  return ret;
}

int ObIColumnDecoder::set_null_vector_from_fixed_column(
    const ObColumnDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    const int64_t vec_offset,
    const unsigned char *col_data,
    ObIVector &vector) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != ctx.micro_block_header_->extend_value_bit_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("multi extend value bits, not a null bitmap");
  } else {
    const sql::ObBitVector *null_bitset = sql::to_bit_vector(col_data);
    for (int64_t i = 0; i < row_cap; ++i) {
      const int64_t row_id = row_ids[i];
      const int64_t curr_vec_offset = vec_offset + i;
      if (null_bitset->contain(row_id)) {
        vector.set_null(curr_vec_offset);
      }
    }
  }
  return ret;
}

int ObIColumnDecoder::batch_locate_var_len_row(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    ObVectorDecodeCtx &vector_ctx,
    bool &has_null) const
{
  int ret = OB_SUCCESS;
  has_null = false;
  if (OB_UNLIKELY(ctx.has_extend_value() && 1 != ctx.micro_block_header_->extend_value_bit_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("multi extend value bits, not a null bitmap", K(ret),
        K(ctx.micro_block_header_->extend_value_bit_), K(ctx));
  } else if (FALSE_IT(vector_ctx.reset_tmp_arr())) {
  } else if (OB_FAIL(row_index->batch_get(vector_ctx.row_ids_, vector_ctx.row_cap_,
      ctx.has_extend_value(), vector_ctx.ptr_arr_, vector_ctx.len_arr_))) {
    LOG_WARN("failed to batch get var row index", K(ret));
  } else if (ctx.has_extend_value()) {
    const int64_t null_idx = ctx.col_header_->extend_value_index_;
    for (int64_t i = 0; OB_SUCC(ret) && i < vector_ctx.row_cap_; ++i) {
      const sql::ObBitVector *null_bitset = sql::to_bit_vector(vector_ctx.ptr_arr_[i]);
      if (null_bitset->contain(null_idx)) {
        vector_ctx.ptr_arr_[i] = nullptr;
        vector_ctx.len_arr_[i] = 0;
        has_null = true;
      }
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  // Default implementation, decode the column data and check whether is null
  int ret = OB_SUCCESS;
  null_count = 0;
  common::ObObj cell;
  ObStorageDatum datum;
  const char *row_data = NULL;
  int64_t row_len = 0;
  int64_t row_id = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(row_index->get(row_id, row_data, row_len))) {
      LOG_WARN("get row data failed", K(ret), K(row_id));
    }
    ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(decode(const_cast<ObColumnDecoderCtx &>(ctx), datum, row_id, bs, row_data, row_len))) {
      LOG_WARN("failed to decode datum", K(ret));
    } else if (datum.is_null()) {
      null_count++;
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count_from_fixed_column(
    const ObColumnDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    const unsigned char *col_data,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(ObBitStream::get(col_data, row_id * ctx.micro_block_header_->extend_value_bit_,
        ctx.micro_block_header_->extend_value_bit_, val))) {
      LOG_WARN("Get extend value failed", K(ret), K(ctx));
    } else if (STORED_NOT_EXT != val) {
      null_count++;
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count_from_var_column(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  uint64_t val = STORED_NOT_EXT;
  const char *row_data = nullptr;
  int64_t row_len = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_id = row_ids[i];
    if (OB_FAIL(locate_row_data(ctx, row_index, row_id, row_data, row_len))) {
      LOG_WARN("Failed to get row_data offset from row index", K(ret), K(row_id));
    } else if (OB_FAIL(ObBitStream::get(
        reinterpret_cast<const unsigned char *>(row_data),
        ctx.col_header_->extend_value_index_,
        ctx.micro_block_header_->extend_value_bit_,
        val))) {
      LOG_WARN("Failed to get extend value from row data", K(ret), K(ctx));
    } else if (STORED_NOT_EXT != val) {
      null_count++;
    }
  }
  return ret;
}

int ObIColumnDecoder::get_null_count_from_extend_value(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    const char *meta_data_,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  null_count = 0;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  if (ctx.has_extend_value()) {
    if (ctx.is_fix_length() || ctx.is_bit_packing()) {
      if (OB_FAIL(get_null_count_from_fixed_column(
          ctx, row_ids, row_cap, col_data, null_count))) {
        LOG_WARN("Failed to get null count from fixed data", K(ret), K(ctx));
      }
    } else if (OB_FAIL(get_null_count_from_var_column(
        ctx, row_index, row_ids, row_cap, null_count))) {
      LOG_WARN("Failed to get null count from var data", K(ret), K(ctx));
    }
  } else {
    null_count = 0;
  }
  return ret;
}

int ObSpanColumnDecoder::decode_exception_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const int64_t ref,
    const char *exc_buf,
    const int64_t exc_buf_len,
    const int64_t vec_offset,
    sql::VectorHeader &vec_header) const
{
  int ret = OB_SUCCESS;
  const int16_t precision = decoder_ctx.obj_meta_.is_decimal_int()
      ? decoder_ctx.obj_meta_.get_stored_precision()
      : PRECISION_UNKNOWN_YET;
  VecValueTypeClass vec_tc = common::get_vec_value_tc(
      decoder_ctx.obj_meta_.get_type(),
      decoder_ctx.obj_meta_.get_scale(),
      precision);
  using VarLenTypeValueType = char[0];
  #define LOAD_VEC_BY_TYPE(ctype, decode_method) \
    ret = inner_decode_exception_vector<ctype, decode_method>(decoder_ctx, ref, exc_buf, exc_buf_len, vec_offset, vec_header);
  switch (vec_tc) {
  case VEC_TC_YEAR: {
    // uint8_t
    LOAD_VEC_BY_TYPE(uint8_t, D_INTEGER);
    break;
  }
  case VEC_TC_DATE: {
    // int32_t
    LOAD_VEC_BY_TYPE(int32_t, D_INTEGER);
    break;
  }
  case VEC_TC_INTEGER:
  case VEC_TC_DATETIME:
  case VEC_TC_TIME:
  case VEC_TC_UNKNOWN:
  case VEC_TC_INTERVAL_YM: {
    // int64_t
    LOAD_VEC_BY_TYPE(int64_t, D_INTEGER);
    break;
  }
  case VEC_TC_UINTEGER:
  case VEC_TC_BIT:
  case VEC_TC_ENUM_SET:
  case VEC_TC_DOUBLE:
  case VEC_TC_FIXED_DOUBLE: {
    // uint64_t
    LOAD_VEC_BY_TYPE(uint64_t, D_INTEGER);
    break;
  }
  case VEC_TC_FLOAT: {
    // float
    LOAD_VEC_BY_TYPE(uint32_t, D_INTEGER);
    break;
  }
  case VEC_TC_TIMESTAMP_TZ: {
    // ObOTimestampData
    LOAD_VEC_BY_TYPE(ObOTimestampData, D_DEEP_COPY);
    break;
  }
  case VEC_TC_TIMESTAMP_TINY: {
    // ObOTimestampTinyData
    LOAD_VEC_BY_TYPE(ObOTimestampTinyData, D_DEEP_COPY);
    break;
  }
  case VEC_TC_INTERVAL_DS: {
    // ObIntervalDSValue
    LOAD_VEC_BY_TYPE(ObIntervalDSValue, D_DEEP_COPY);
    break;
  }
  case VEC_TC_DEC_INT32: {
    LOAD_VEC_BY_TYPE(int32_t, D_DEEP_COPY);
    break;
  }
  case VEC_TC_DEC_INT64: {
    LOAD_VEC_BY_TYPE(int64_t, D_DEEP_COPY);
    break;
  }
  case VEC_TC_DEC_INT128: {
    LOAD_VEC_BY_TYPE(int128_t, D_DEEP_COPY);
    break;
  }
  case VEC_TC_DEC_INT256: {
    LOAD_VEC_BY_TYPE(int256_t, D_DEEP_COPY);
    break;
  }
  case VEC_TC_DEC_INT512: {
    LOAD_VEC_BY_TYPE(int512_t, D_DEEP_COPY);
    break;
  }
  default: {
    // var-length types, currently should not rely on ValueType on decode
    LOAD_VEC_BY_TYPE(VarLenTypeValueType, D_SHALLOW_COPY);
  }
  }
  #undef LOAD_VEC_BY_TYPE
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to decode inter column exception data", K(ret), K(vec_tc), K(decoder_ctx));
  }
  return ret;
}

template <typename ValueType, ObEncodingDecodeMetodType DECODE_TYPE>
int ObSpanColumnDecoder::inner_decode_exception_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const int64_t ref,
    const char *exc_buf,
    const int64_t exc_buf_len,
    const int64_t vec_offset,
    sql::VectorHeader &vec_header) const
{
  int ret = OB_SUCCESS;
  ObIVector *vector = vec_header.get_vector();

  switch (vec_header.get_format()) {
  case VEC_FIXED: {
    typedef common::ObFixedLengthFormat<ValueType> FixedFormat;
    FixedFormat *fix_vec = static_cast<FixedFormat *>(vector);
    ret = ObBitMapExcValDecodeFunc<FixedFormat, DECODE_TYPE>::decode(
        exc_buf, ref, decoder_ctx.is_bit_packing(), exc_buf_len,
        vec_offset, decoder_ctx.col_header_->get_store_obj_type(), *fix_vec);
    break;
  }
  case VEC_DISCRETE: {
    common::ObDiscreteFormat *disc_vec = static_cast<common::ObDiscreteFormat *>(vector);
    ret = ObBitMapExcValDecodeFunc<ObDiscreteFormat, DECODE_TYPE>::decode(
        exc_buf, ref, decoder_ctx.is_bit_packing(), exc_buf_len,
        vec_offset, decoder_ctx.col_header_->get_store_obj_type(), *disc_vec);
    break;
  }
  case VEC_CONTINUOUS: {
    common::ObContinuousFormat *cont_vec = static_cast<common::ObContinuousFormat *>(vector);
    ret = ObBitMapExcValDecodeFunc<ObContinuousFormat, DECODE_TYPE>::decode(
        exc_buf, ref, decoder_ctx.is_bit_packing(), exc_buf_len,
        vec_offset, decoder_ctx.col_header_->get_store_obj_type(), *cont_vec);
    break;
  }
  case VEC_UNIFORM: {
    typedef common::ObUniformFormat<false> UniformFormat;
    UniformFormat *uni_vec = static_cast<UniformFormat *>(vector);
    ret = ObBitMapExcValDecodeFunc<UniformFormat, DECODE_TYPE>::decode(
        exc_buf, ref, decoder_ctx.is_bit_packing(), exc_buf_len,
        vec_offset, decoder_ctx.col_header_->get_store_obj_type(), *uni_vec);
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector format", K(ret), K(vec_header.get_format()), K(decoder_ctx));
  }
  }
  return ret;
}

int ObSpanColumnDecoder::decode_refed_range(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex *row_index,
    const int64_t ref_start_idx,
    const int64_t ref_end_idx,
    ObVectorDecodeCtx &raw_vector_ctx) const
{
  int ret = OB_SUCCESS;
  const int64_t row_cap = ref_end_idx - ref_start_idx + 1;
  const int32_t *row_id_arr = raw_vector_ctx.row_ids_ + ref_start_idx;
  const char **ptr_arr = raw_vector_ctx.ptr_arr_ + ref_start_idx;
  uint32_t *len_arr = raw_vector_ctx.len_arr_ + ref_start_idx;
  const int64_t vec_offset = raw_vector_ctx.vec_offset_ + ref_start_idx;
  ObVectorDecodeCtx ref_range_decode_ctx(
      ptr_arr, len_arr, row_id_arr, row_cap, vec_offset, raw_vector_ctx.vec_header_);
  if (OB_FAIL(decoder_ctx.ref_decoder_->decode_vector(
      *decoder_ctx.ref_ctx_, row_index, ref_range_decode_ctx))) {
    LOG_WARN("Failed to decode vector from referenced column", K(ret),
        K(decoder_ctx), K(ref_start_idx), K(ref_end_idx));
  }
  return ret;
}

} // end of namespace oceanbase
} // end of namespace oceanbase
