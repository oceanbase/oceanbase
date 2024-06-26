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

#include "ob_integer_base_diff_decoder.h"
#include "ob_encoding_query_util.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObIntegerBaseDiffDecoder::type_;

int ObIntegerBaseDiffDecoder::decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  int ret = OB_SUCCESS;
  uint64_t val = STORED_NOT_EXT;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_) + ctx.col_header_->length_;
  int64_t data_offset = 0;

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == data || len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(len));
  } else {
    // read extend value bit
    if (ctx.has_extend_value()) {
      data_offset = ctx.micro_block_header_->row_count_ * ctx.micro_block_header_->extend_value_bit_;
      if (OB_FAIL(ObBitStream::get(col_data, row_id * ctx.micro_block_header_->extend_value_bit_,
          ctx.micro_block_header_->extend_value_bit_, val))) {
        LOG_WARN("get extend value failed", K(ret), K(bs), K(ctx));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (STORED_NOT_EXT != val) {
    set_stored_ext_value(datum, static_cast<ObStoredExtValue>(val));
  } else {
    uint32_t datum_len = 0;
    uint64_t v = 0;
    if (OB_FAIL(get_uint_data_datum_len(
        ObDatum::get_obj_datum_map_type(ctx.obj_meta_.get_type()),
        datum_len))){
      LOG_WARN("Failed to get datum len for int data", K(ret));
    } else if (ctx.is_bit_packing()) {
      if (OB_FAIL(ObBitStream::get(col_data, data_offset + row_id * header_->length_,
          header_->length_, v))) {
        LOG_WARN("get bit packing value failed", K(ret), K_(header));
      } else {
        v += base_;
        MEMCPY(const_cast<char *>(datum.ptr_), &v, datum_len);
        datum.pack_ = datum_len;
      }
    } else { // always fix length store
      data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
      MEMCPY(&v, col_data + data_offset + row_id * header_->length_, header_->length_);
      v += base_;
      MEMCPY(const_cast<char *>(datum.ptr_), &v, datum_len);
      datum.pack_ = datum_len;
    }
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::update_pointer(const char *old_block, const char *cur_block)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(old_block) || OB_ISNULL(cur_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(old_block), KP(cur_block));
  } else {
    ObIColumnDecoder::update_pointer(header_, old_block, cur_block);
  }
  return ret;
}

#define INT_DIFF_UNPACK_VALUES(ctx, row_ids, row_cap, datums, datum_len, data_offset, unpack_type) \
  int64_t row_id = 0; \
  bool has_ext_val = ctx.has_extend_value(); \
  int64_t bs_len = header_->length_ * ctx.micro_block_header_->row_count_; \
  int64_t value = 0; \
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_) \
                                  + ctx.col_header_->length_; \
  if (!has_ext_val) { \
    for (int64_t i = 0; i < row_cap; ++i) { \
      row_id = row_ids[i];  \
      value = 0; \
      ObBitStream::get<unpack_type>( \
          col_data, data_offset + row_id * header_->length_, header_->length_, \
          bs_len, value); \
      value += base_;  \
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(datums[i].ptr_), &value, datum_len); \
      datums[i].pack_ = datum_len; \
    } \
  } else { \
    for (int64_t i = 0; i < row_cap; ++i) { \
      if (datums[i].is_null()) { \
      } else { \
        row_id = row_ids[i];  \
        value = 0; \
        ObBitStream::get<unpack_type>( \
            col_data, data_offset + row_id * header_->length_, header_->length_, \
            bs_len, value); \
        value += base_;  \
        ENCODING_ADAPT_MEMCPY(const_cast<char *>(datums[i].ptr_), &value, datum_len); \
        datums[i].pack_ = datum_len; \
      } \
    } \
  }

int ObIntegerBaseDiffDecoder::batch_get_bitpacked_values(
    const ObColumnDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    const int64_t datum_len,
    const int64_t data_offset,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  int64_t packed_len = header_->length_;
  if (packed_len < 10) {
    INT_DIFF_UNPACK_VALUES(
        ctx, row_ids, row_cap, datums, datum_len,
        data_offset, ObBitStream::PACKED_LEN_LESS_THAN_10)
  } else if (packed_len < 26) {
    INT_DIFF_UNPACK_VALUES(
        ctx, row_ids, row_cap, datums, datum_len,
        data_offset, ObBitStream::PACKED_LEN_LESS_THAN_26)
  } else if (packed_len <= 64) {
    INT_DIFF_UNPACK_VALUES(
        ctx, row_ids, row_cap, datums, datum_len, data_offset, ObBitStream::DEFAULT)
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unpack size larger than 64 bit", K(ret), K(packed_len));
  }
  return ret;
}

#undef INT_DIFF_UNPACK_REFS

// Internal call, not check parameters for performance
// Potential optimization: SIMD batch add @base_ to packed delta values
int ObIntegerBaseDiffDecoder::batch_decode(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  UNUSEDx(row_index, cell_datas);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    int64_t data_offset = 0;
    const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_)
                                    + ctx.col_header_->length_;
    uint32_t datum_len = 0;
    if (ctx.has_extend_value()) {
      data_offset = ctx.micro_block_header_->row_count_
          * ctx.micro_block_header_->extend_value_bit_;
      if (OB_FAIL(set_null_datums_from_fixed_column(
          ctx, row_ids, row_cap, col_data, datums))) {
        LOG_WARN("Failed to set null datums from fixed data", K(ret), K(ctx));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_uint_data_datum_len(
        ObDatum::get_obj_datum_map_type(ctx.obj_meta_.get_type()),
        datum_len))) {
      LOG_WARN("Failed to get datum length of int/uint data", K(ret));
    } else if (ctx.is_bit_packing()) {
      if (OB_FAIL(batch_get_bitpacked_values(
          ctx, row_ids, row_cap, datum_len, data_offset, datums))) {
        LOG_WARN("Failed to batch unpack delta values", K(ret), K(ctx));
      }
    } else {
      // Fixed store data
      data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
      int64_t row_id = 0;
      uint64_t value = 0;
      for (int64_t i = 0; i < row_cap; ++i) {
        if (ctx.has_extend_value() && datums[i].is_null()) {
          // Skip
        } else {
          row_id = row_ids[i];
          value = 0;
          MEMCPY(&value, col_data + data_offset + row_id * header_->length_, header_->length_);
          value += base_;
          MEMCPY(const_cast<char *>(datums[i].ptr_), &value, datum_len);
          datums[i].pack_ = datum_len;
        }
      }
    }
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex* row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  UNUSED(row_index);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    #define FILL_VECTOR_FUNC(vector_type, has_null) \
      if (has_null) { \
        ret = inner_decode_vector<vector_type, true>(decoder_ctx, vector_ctx); \
      } else { \
        ret = inner_decode_vector<vector_type, false>(decoder_ctx, vector_ctx); \
      }
    if (VEC_UNIFORM == vector_ctx.get_format()) {
      FILL_VECTOR_FUNC(ObUniformFormat<false>, decoder_ctx.has_extend_value());
    } else if (VEC_FIXED == vector_ctx.get_format()) {
      const ObObjMeta &obj_meta = decoder_ctx.obj_meta_;
      const int16_t precision = obj_meta.is_decimal_int() ? obj_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
      VecValueTypeClass vec_tc = common::get_vec_value_tc(obj_meta.get_type(), obj_meta.get_scale(), precision);
      switch (vec_tc) {
      case VEC_TC_YEAR: {
        // uint8_t
        FILL_VECTOR_FUNC(ObFixedLengthFormat<uint8_t>, decoder_ctx.has_extend_value());
        break;
      }
      case VEC_TC_DATE:
      case VEC_TC_DEC_INT32: {
        // int32_t
        FILL_VECTOR_FUNC(ObFixedLengthFormat<int32_t>, decoder_ctx.has_extend_value());
        break;
      }
      case VEC_TC_INTEGER:
      case VEC_TC_DATETIME:
      case VEC_TC_TIME:
      case VEC_TC_UNKNOWN:
      case VEC_TC_INTERVAL_YM:
      case VEC_TC_DEC_INT64: {
        // int64_t
        FILL_VECTOR_FUNC(ObFixedLengthFormat<int64_t>, decoder_ctx.has_extend_value());
        break;
      }
      case VEC_TC_UINTEGER:
      case VEC_TC_BIT:
      case VEC_TC_ENUM_SET:
      case VEC_TC_DOUBLE:
      case VEC_TC_FIXED_DOUBLE: {
        // uint64_t
        FILL_VECTOR_FUNC(ObFixedLengthFormat<uint64_t>, decoder_ctx.has_extend_value());
        break;
      }
      case VEC_TC_FLOAT: {
        // float
        FILL_VECTOR_FUNC(ObFixedLengthFormat<uint32_t>, decoder_ctx.has_extend_value());
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector type class for fixed format", K(ret), K(vec_tc));
      }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unnexpected vector format", K(ret), K(vector_ctx));
    }
    #undef FILL_VECTOR_FUNC
  }
  return ret;
}

template<typename VectorType, bool HAS_NULL>
int ObIntegerBaseDiffDecoder::inner_decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t data_offset = 0;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_)
      + decoder_ctx.col_header_->length_;
  const sql::ObBitVector *null_bitmap = nullptr;
  uint32_t vec_data_len = 0;
  if (OB_FAIL(get_uint_data_datum_len(
      ObDatum::get_obj_datum_map_type(decoder_ctx.obj_meta_.get_type()), vec_data_len))) {
    LOG_WARN("Failed to get vec data length", K(ret), K(decoder_ctx));
  } else {
    VectorType *vector = static_cast<VectorType *>(vector_ctx.get_vector());
    if (decoder_ctx.has_extend_value()) {
      null_bitmap = sql::to_bit_vector(col_data);
      data_offset = decoder_ctx.micro_block_header_->row_count_ * decoder_ctx.micro_block_header_->extend_value_bit_;
    }
    if (decoder_ctx.is_bit_packing()) {
      bitstream_unpack unpack_func = ObBitStream::get_unpack_func(header_->length_);
      const int64_t bs_len = header_->length_ * decoder_ctx.micro_block_header_->row_count_;
      for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
        const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
        const int64_t row_id = vector_ctx.row_ids_[i];
        if (!HAS_NULL || !null_bitmap->contain(row_id)) {
          int64_t unpacked_val = 0;
          unpack_func(
              col_data,
              data_offset + row_id * header_->length_,
              header_->length_,
              bs_len,
              unpacked_val);
          unpacked_val += base_;
          vector->set_payload(curr_vec_offset, &unpacked_val, vec_data_len);
        } else {
          vector->set_null(curr_vec_offset);
        }
      }
    } else {
      // fixed
      data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
      for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
        const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
        const int64_t row_id = vector_ctx.row_ids_[i];
        if (!HAS_NULL || !null_bitmap->contain(row_id)) {
          int64_t unpacked_val = 0;
          MEMCPY(&unpacked_val, col_data + data_offset + row_id * header_->length_, header_->length_);
          unpacked_val += base_;
          vector->set_payload(curr_vec_offset, &unpacked_val, vec_data_len);
        } else {
          vector->set_null(curr_vec_offset);
        }
      }
    }
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  UNUSEDx(meta_data, row_index);
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(header_) +
      col_ctx.col_header_->length_;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Raw Decoder not inited", K(ret), K(filter));
  } else if (OB_UNLIKELY(op_type >= sql::WHITE_OP_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid op type for pushed down white filter",
             K(ret), K(op_type));
  } else if (OB_FAIL(get_is_null_bitmap_from_fixed_column(col_ctx, col_data,
                                                          pd_filter_info, result_bitmap))) {
    LOG_WARN("Failed to get is null bitmap", K(ret), K(col_ctx));
  } else {
    switch (op_type) {
    case sql::WHITE_OP_NU: {
      break;
    }
    case sql::WHITE_OP_NN: {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("Failed to flip bits for result bitmap",
            K(ret), K(result_bitmap.size()));
      }
      break;
    }
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE:
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_LE: {
      if (OB_FAIL(comparison_operator(
                  parent,
                  col_ctx,
                  col_data,
                  filter,
                  pd_filter_info,
                  result_bitmap))) {
        if (OB_UNLIKELY(OB_NOT_SUPPORTED != ret)) {
          LOG_WARN("Failed on EQ / NE operator", K(ret), K(col_ctx));
        }
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      if (OB_FAIL(bt_operator(parent, col_ctx, col_data, filter, pd_filter_info, result_bitmap))) {
        if (OB_UNLIKELY(OB_NOT_SUPPORTED != ret)) {
          LOG_WARN("Failed on BT operator", K(ret), K(col_ctx));
        }
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      if (OB_FAIL(in_operator(parent, col_ctx, col_data, filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed on IN operator", K(ret), K(col_ctx));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unexpected operation type", K(ret), K(op_type));
    }
    }
  }
  return ret;
}

// T should be int64_t or uint64_t
template <>
inline int ObIntegerBaseDiffDecoder::get_delta<uint64_t>(
    const ObObjType &obj_type, const common::ObDatum &datum, uint64_t &delta) const
{
  int ret = OB_SUCCESS;
  int64_t type_store_size = get_type_size_map()[obj_type];
  const ObObjDatumMapType datum_type = common::ObDatum::get_obj_datum_map_type(obj_type);
  const uint32_t datum_size = common::ObDatum::get_reserved_size(datum_type);
  if (OB_UNLIKELY(type_store_size < 0 || datum_size > 8)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid size for int_diff decoder", K(ret), K(type_store_size), K(datum_size));
  } else {
    uint64_t mask = INTEGER_MASK_TABLE[type_store_size];
    uint64_t datum_value = 0;
    MEMCPY(&datum_value, datum.ptr_, datum_size);
    uint64_t value = datum_value & mask;
    delta = value - base_;
  }
  return ret;
}

template <>
inline int ObIntegerBaseDiffDecoder::get_delta<int64_t>(
    const ObObjType &obj_type, const common::ObDatum &datum, uint64_t &delta) const
{
  int ret = OB_SUCCESS;
  int64_t type_store_size = get_type_size_map()[obj_type];
  const ObObjDatumMapType datum_type = common::ObDatum::get_obj_datum_map_type(obj_type);
  const uint32_t datum_size = common::ObDatum::get_reserved_size(datum_type);
  if (OB_UNLIKELY(type_store_size < 0 || datum_size > 8)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid size for int_diff decoder", K(ret), K(type_store_size), K(datum_size));
  } else {
    uint64_t mask = INTEGER_MASK_TABLE[type_store_size];
    uint64_t reverse_mask = ~mask;
    uint64_t datum_value = 0;
    MEMCPY(&datum_value, datum.ptr_, datum_size);
    uint64_t value = datum_value & mask;
    if (0 != reverse_mask && (value & reverse_mask >> 1)) {
      value |= reverse_mask;
    }
    delta = static_cast<uint64_t>(
          *reinterpret_cast<int64_t *>(&value) - *reinterpret_cast<const int64_t *>(&base_));
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::comparison_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  uint64_t delta_value = 0;
  uint64_t param_delta_value = 0;
  common::ObObjMeta filter_val_meta;
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size()
                          || NULL == col_data
                          || filter.get_datums().count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter Pushdown Operator: Invalid argument",
        K(ret), K(col_ctx), K(pd_filter_info), K(result_bitmap.size()), K(filter));
  } else if (col_ctx.obj_meta_.get_type_class() == ObFloatTC
            || col_ctx.obj_meta_.get_type_class() == ObDoubleTC) {
    // Can't compare by uint directly, support this later with float point number compare later
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("Double/Float with INT_DIFF encoding, back to retro path", K(col_ctx));
  } else if (OB_FAIL(filter.get_filter_node().get_filter_val_meta(filter_val_meta))) {
    LOG_WARN("Fail to find datum meta", K(ret), K(filter));
  } else {
    const ObDatum &ref_datum = filter.get_datums().at(0);
    ObStorageDatum base_datum;
    uint32_t base_datum_len = 0;
    if (OB_FAIL(get_uint_data_datum_len(
        ObDatum::get_obj_datum_map_type(col_ctx.obj_meta_.get_type()),
        base_datum_len))){
      LOG_WARN("Failed to get datum len for int data", K(ret));
    }
    base_datum.ptr_ = (const char *)(&base_);
    base_datum.pack_ = base_datum_len;
    ObDatumCmpFuncType cmp_func = filter.cmp_func_;
    ObObjTypeStoreClass column_sc = get_store_class_map()[col_ctx.obj_meta_.get_type_class()];
    bool  filter_obj_smaller_than_base = false;

    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    ObGetFilterCmpRetFunc get_cmp_ret = get_filter_cmp_ret_func(op_type);
    int cmp_res = 0;
    if (FAILEDx(cmp_func(ref_datum, base_datum, cmp_res))) {
      LOG_WARN("Failed to compare datum", K(ret), K(ref_datum), K(base_datum));
    } else if (FALSE_IT(filter_obj_smaller_than_base = cmp_res < 0)){
    } else if (filter_obj_smaller_than_base) {
      // Do not need to decode the data
      if (op_type == sql::WHITE_OP_GE || op_type == sql::WHITE_OP_GT || op_type == sql::WHITE_OP_NE) {
        // All rows except null value are true
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to flip all bits in bitmap", K(ret));
        }
      } else  {
        // All rows are false;
        result_bitmap.reuse();
      }
    } else {
      #define UINT_DIFF_CMP(l, r) ( l == r ? 0 : (l < r ? -1 : 1) )
      uint8_t cell_len = header_->length_;
      int64_t data_offset = 0;
      bool null_value_contained = result_bitmap.popcnt() > 0;
      bool exist_parent_filter = nullptr != parent;
      const ObObjType &ref_obj_type = filter_val_meta.get_type();
      if (col_ctx.has_extend_value()) {
        data_offset = col_ctx.micro_block_header_->row_count_
            * col_ctx.micro_block_header_->extend_value_bit_;
      }
      if (ObIntSC == column_sc) {
        if (OB_FAIL(get_delta<int64_t>(ref_obj_type, ref_datum, param_delta_value))) {
          LOG_WARN("Failed to get delta value", K(ret), K(ref_datum));
        }
      } else if (ObUIntSC == column_sc) {
        if (OB_FAIL(get_delta<uint64_t>(ref_obj_type, ref_datum, param_delta_value))) {
          LOG_WARN("Failed to get delta value", K(ret), K(ref_datum));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected Store type for int_diff decoder", K(ret), K(column_sc));
      }

      if (OB_FAIL(ret)) {
      } else if (col_ctx.is_bit_packing()) {
        int64_t row_id = 0;
        for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
          row_id = offset + pd_filter_info.start_;
          if (exist_parent_filter && parent->can_skip_filter(offset)) {
          } else if (null_value_contained && result_bitmap.test(offset)) {
            if (OB_FAIL(result_bitmap.set(offset, false))) {
              LOG_WARN("Failed to set row with null object to false",
                  K(ret), K(offset));
            }
          } else if (OB_FAIL(ObBitStream::get(
                col_data, data_offset + row_id * cell_len, cell_len, delta_value))) {
              LOG_WARN("Failed to get bit packing value", K(ret), K_(header));
          } else {
            if (get_cmp_ret(UINT_DIFF_CMP(delta_value, param_delta_value))) {
              if (OB_FAIL(result_bitmap.set(offset))) {
                LOG_WARN("Failed to set result bitmap",
                    K(ret), K(offset), K(filter));
              }
            }
          }
        }
      } else {
        data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
        int64_t row_id = 0;
        for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
          row_id = offset + pd_filter_info.start_;
          if (exist_parent_filter && parent->can_skip_filter(offset)) {
          } else if (null_value_contained && result_bitmap.test(offset)) {
            if (OB_FAIL(result_bitmap.set(offset, false))) {
              LOG_WARN("Failed to set row with null object to false",
                  K(ret), K(offset));
            }
          } else {
            MEMCPY(&delta_value, col_data + data_offset + row_id * cell_len, cell_len);
            if (get_cmp_ret(UINT_DIFF_CMP(delta_value, param_delta_value))) {
              if (OB_FAIL(result_bitmap.set(offset))) {
                LOG_WARN("Failed to set result bitmap",
                    K(ret), K(offset), K(filter));
              }
            }
          }
        }
      }
      #undef UINT_DIFF_CMP
    }
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::bt_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size()
                          || NULL == col_data
                          || filter.get_datums().count() != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument",
        K(ret), K(col_ctx), K(pd_filter_info), K(result_bitmap.size()), K(filter));
  } else if (col_ctx.obj_meta_.get_type_class() == ObFloatTC
            || col_ctx.obj_meta_.get_type_class() == ObDoubleTC) {
    // Can't compare by uint directly, support this later with float point number compare later
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("Double/Float with INT_DIFF encoding, back to retro path", K(col_ctx));
  } else if (ObUIntSC == get_store_class_map()[col_ctx.obj_meta_.get_type_class()]
        || ObIntSC == get_store_class_map()[col_ctx.obj_meta_.get_type_class()]) {
    if (OB_FAIL(traverse_all_data(parent, col_ctx, col_data,
                filter, pd_filter_info, result_bitmap,
                [](const ObDatum &cur_datum,
                   const sql::ObWhiteFilterExecutor &filter,
                   bool &result) -> int {
                  int ret = OB_SUCCESS;
                  int left_cmp_res = 0;
                  int right_cmp_res = 0;
                  ObDatumCmpFuncType cmp_func = filter.cmp_func_;
                  if (OB_FAIL(cmp_func(cur_datum, filter.get_datums().at(0), left_cmp_res))) {
                    LOG_WARN("fail to compare datums", K(ret), K(cur_datum), K(filter.get_datums().at(0)));
                  } else if (OB_FAIL(cmp_func(cur_datum, filter.get_datums().at(1), right_cmp_res))) {
                    LOG_WARN("fail to compare datums", K(ret), K(cur_datum), K(filter.get_datums().at(1)));
                  } else {
                    result = (left_cmp_res >= 0) && (right_cmp_res <= 0);
                  }
                  return ret;
                }))) {
      LOG_WARN("Failed to traverse all data in micro block", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Integer base encoding should only encode data as IntSC", K(ret), K(filter));
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::in_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter.get_datums().count() == 0
                  || result_bitmap.size() != pd_filter_info.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Pushdown in operator: Invalid arguments",
        K(ret), K(col_ctx), K(pd_filter_info), K(result_bitmap.size()), K(filter));
  } else if (OB_FAIL(traverse_all_data(parent, col_ctx, col_data,
                      filter, pd_filter_info, result_bitmap,
                      [](const ObDatum &cur_datum,
                         const sql::ObWhiteFilterExecutor &filter,
                         bool &result) -> int {
                        int ret = OB_SUCCESS;
                        if (OB_FAIL(filter.exist_in_datum_set(cur_datum, result))) {
                          LOG_WARN("Failed to check datum in hashset", K(ret), K(cur_datum));
                        }
                        return ret;
                      }))) {
    LOG_WARN("Failed to traverse all data in micro block", K(ret));
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::traverse_all_data(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    int (*lambda)(
        const ObDatum &cur_datum,
        const sql::ObWhiteFilterExecutor &filter,
        bool &result)) const
{
  int ret = OB_SUCCESS;
  uint64_t v = 0;
  uint64_t cur_int = 0;
  uint8_t cell_len = header_->length_;
  int64_t data_offset = 0;
  if (col_ctx.has_extend_value()) {
    data_offset = col_ctx.micro_block_header_->row_count_
        * col_ctx.micro_block_header_->extend_value_bit_;
  }
  if (!col_ctx.is_bit_packing()) {
    data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
  }
  bool null_value_contained = (result_bitmap.popcnt() > 0);
  bool exist_parent_filter = nullptr != parent;
  int64_t row_id = 0;
  for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
    row_id = offset + pd_filter_info.start_;
    if (exist_parent_filter && parent->can_skip_filter(offset)) {
      continue;
    } else if (null_value_contained && result_bitmap.test(offset)) {
      if (OB_FAIL(result_bitmap.set(offset, false))) {
        LOG_WARN("Failed to set row with null object to false",
            K(ret), K(offset));
      }
    } else {
      if (col_ctx.is_bit_packing()) {
        if (OB_FAIL(ObBitStream::get(col_data, data_offset + row_id * cell_len, cell_len, v))) {
          LOG_WARN("Failed to get bit packing value", K(ret), K_(header));
        }
      } else {
        MEMCPY(&v, col_data + data_offset + row_id * cell_len, cell_len);
      }
      if (OB_SUCC(ret)) {
        cur_int = base_ + v;
        ObDatum cur_datum;
        uint32_t cur_datum_len = 0;
        if (OB_FAIL(get_uint_data_datum_len(
            ObDatum::get_obj_datum_map_type(col_ctx.obj_meta_.get_type()),
            cur_datum_len))){
          LOG_WARN("Failed to get datum len for int data", K(ret));
        }
        cur_datum.pack_ = cur_datum_len;
        cur_datum.ptr_ = reinterpret_cast<char *> (&cur_int);
        // use lambda here to filter and set result bitmap
        bool result = false;
        if (FAILEDx(lambda(cur_datum, filter, result))) {
          LOG_WARN("Failed on trying to filter the row", K(ret), K(row_id), K(cur_int));
        } else if (result) {
          if (OB_FAIL(result_bitmap.set(offset))) {
            LOG_WARN("Failed to set result bitmap",
                K(ret), K(offset), K(filter));
          }
        }
      }
    }
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  const char *col_data = reinterpret_cast<const char *>(header_) + ctx.col_header_->length_;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Raw decoder is not inited", K(ret));
  } else if OB_FAIL(ObIColumnDecoder::get_null_count_from_extend_value(
      ctx,
      row_index,
      row_ids,
      row_cap,
      col_data,
      null_count)) {
    LOG_WARN("Failed to get null count", K(ctx), K(ret));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
