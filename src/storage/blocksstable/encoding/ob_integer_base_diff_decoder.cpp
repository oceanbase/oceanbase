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

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObIntegerBaseDiffDecoder::type_;

int ObIntegerBaseDiffDecoder::decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  UNUSED(ctx);
  UNUSED(row_id);
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
    set_stored_ext_value(cell, static_cast<ObStoredExtValue>(val));
  } else {
    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }
    uint64_t v = 0;
    if (ctx.is_bit_packing()) {
      if (OB_FAIL(ObBitStream::get(col_data, data_offset + row_id * header_->length_,
          header_->length_, v))) {
        LOG_WARN("get bit packing value failed", K(ret), K_(header));
      } else {
        cell.v_.uint64_ = base_ + v;
      }
    } else { // always fix length store
      data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
      MEMCPY(&v, col_data + data_offset + row_id * header_->length_, header_->length_);
      cell.v_.uint64_ = base_ + v;
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
      MEMCPY(const_cast<char *>(datums[i].ptr_), &value, datum_len); \
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
        MEMCPY(const_cast<char *>(datums[i].ptr_), &value, datum_len); \
        datums[i].pack_ = datum_len; \
      } \
    } \
  }

int ObIntegerBaseDiffDecoder::batch_get_bitpacked_values(
    const ObColumnDecoderCtx &ctx,
    const int64_t *row_ids,
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
    const int64_t *row_ids,
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

int ObIntegerBaseDiffDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
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
  } else if (OB_FAIL(get_is_null_bitmap_from_fixed_column(col_ctx, col_data, result_bitmap))) {
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
                  result_bitmap))) {
        LOG_WARN("Failed on EQ / NE operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      if (OB_FAIL(bt_operator(parent, col_ctx, col_data, filter, result_bitmap))) {
        LOG_WARN("Failed on BT operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      if (OB_FAIL(in_operator(parent, col_ctx, col_data, filter, result_bitmap))) {
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
    const common::ObObj &cell, uint64_t &delta) const
{
  int ret = OB_SUCCESS;
  int64_t type_store_size = get_type_size_map()[cell.get_type()];
  if (OB_UNLIKELY(type_store_size < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid type store size for int_diff decoder", K(ret), K(type_store_size));
  } else {
    uint64_t mask = INTEGER_MASK_TABLE[type_store_size];
    uint64_t value = cell.v_.uint64_ & mask;
    delta = value - base_;
  }
  return ret;
}

template <>
inline int ObIntegerBaseDiffDecoder::get_delta<int64_t>(
    const common::ObObj &cell, uint64_t &delta) const
{
  int ret = OB_SUCCESS;
  int64_t type_store_size = get_type_size_map()[cell.get_type()];
  if (OB_UNLIKELY(type_store_size < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid type store size for int_diff decoder", K(ret), K(type_store_size));
  } else {
    uint64_t mask = INTEGER_MASK_TABLE[type_store_size];
    uint64_t reverse_mask = ~mask;
    uint64_t value = cell.v_.uint64_ & mask;
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
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  uint64_t delta_value = 0;
  uint64_t param_delta_value = 0;
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size()
                          || NULL == col_data
                          || filter.get_objs().count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter Pushdown Operator: Invalid argument", K(ret), K(col_ctx));
  } else if (OB_UNLIKELY(col_ctx.obj_meta_.get_type() != filter.get_objs().at(0).get_type())) {
    // Filter type not match with column type, back to retro path
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("Type not match, back to retrograde path", K(col_ctx), K(filter));
  } else if (col_ctx.obj_meta_.get_type_class() == ObFloatTC
            || col_ctx.obj_meta_.get_type_class() == ObDoubleTC) {
    // Can't compare by uint directly, support this later with float point number compare later
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("Double/Float with INT_DIFF encoding, back to retro path", K(col_ctx));
  } else {
    const ObObj &ref_obj = filter.get_objs().at(0);
    ObObj base_obj;
    base_obj.copy_meta_type(col_ctx.obj_meta_);
    base_obj.v_.uint64_ = base_;
    ObObjTypeStoreClass column_sc = get_store_class_map()[col_ctx.obj_meta_.get_type_class()];
    bool filter_obj_smaller_than_base = ref_obj < base_obj;

    ObFPIntCmpOpType cmp_op_type = get_white_op_int_op_map()[filter.get_op_type()];
    if (filter_obj_smaller_than_base) {
      // Do not need to decode the data
      if ((filter_obj_smaller_than_base
              && (cmp_op_type == FP_INT_OP_GE || cmp_op_type == FP_INT_OP_GT))
          || cmp_op_type == FP_INT_OP_NE) {
        // All rows except null value are true
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to flip all bits in bitmap", K(ret));
        }
      } else  {
        // All rows are false;
        result_bitmap.reuse();
      }
    } else {
      uint8_t cell_len = header_->length_;
      int64_t data_offset = 0;
      bool null_value_contained = result_bitmap.popcnt() > 0;
      bool exist_parent_filter = nullptr != parent;
      if (col_ctx.has_extend_value()) {
        data_offset = col_ctx.micro_block_header_->row_count_
            * col_ctx.micro_block_header_->extend_value_bit_;
      }
      if (ObIntSC == column_sc) {
        if (OB_FAIL(get_delta<int64_t>(ref_obj, param_delta_value))) {
          LOG_WARN("Failed to get delta value", K(ret), K(ref_obj));
        }
      } else if (ObUIntSC == column_sc) {
        if (OB_FAIL(get_delta<uint64_t>(ref_obj, param_delta_value))) {
          LOG_WARN("Failed to get delta value", K(ret), K(ref_obj));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected Store type for int_diff decoder", K(ret), K(column_sc));
      }

      if (OB_FAIL(ret)) {
      } else if (col_ctx.is_bit_packing()) {
        for (int64_t row_id = 0;
            OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
            ++row_id) {
          if (exist_parent_filter && parent->can_skip_filter(row_id)) {
          } else if (null_value_contained && result_bitmap.test(row_id)) {
            if (OB_FAIL(result_bitmap.set(row_id, false))) {
              LOG_WARN("Failed to set row with null object to false", K(ret));
            }
          } else if (OB_FAIL(ObBitStream::get(
                col_data, data_offset + row_id * cell_len, cell_len, delta_value))) {
              LOG_WARN("Failed to get bit packing value", K(ret), K_(header));
          } else {
            bool cmp_result = fp_int_cmp<uint64_t>(delta_value, param_delta_value, cmp_op_type);
            if (cmp_result) {
              if (OB_FAIL(result_bitmap.set(row_id))) {
                LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(filter));
              }
            }
          }
        }
      } else {
        data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
        for (int64_t row_id = 0;
            OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
            ++row_id) {
          if (exist_parent_filter && parent->can_skip_filter(row_id)) {
          } else if (null_value_contained && result_bitmap.test(row_id)) {
            if (OB_FAIL(result_bitmap.set(row_id, false))) {
              LOG_WARN("Failed to set row with null object to false", K(ret));
            }
          } else {
            MEMCPY(&delta_value, col_data + data_offset + row_id * cell_len, cell_len);
            bool cmp_result = fp_int_cmp<uint64_t>(delta_value, param_delta_value, cmp_op_type);
            if (cmp_result) {
              if (OB_FAIL(result_bitmap.set(row_id))) {
                LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(filter));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::bt_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size()
                          || NULL == col_data
                          || filter.get_objs().count() != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument", K(ret), K(col_ctx));
  } else if (col_ctx.obj_meta_.get_type_class() == ObFloatTC
            || col_ctx.obj_meta_.get_type_class() == ObDoubleTC) {
    // Can't compare by uint directly, support this later with float point number compare later
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("Double/Float with INT_DIFF encoding, back to retro path", K(col_ctx));
  } else if (ObUIntSC == get_store_class_map()[filter.get_objs().at(0).get_type_class()]) {
    if (OB_FAIL(traverse_all_data(parent, col_ctx, col_data, filter, result_bitmap,
                [](uint64_t &cur_int,
                const sql::ObWhiteFilterExecutor &filter,
                bool &result) -> int {
                  result = (cur_int >= filter.get_objs().at(0).v_.uint64_)
                            && (cur_int <= filter.get_objs().at(1).v_.uint64_);
                  return OB_SUCCESS;
                }))) {
      LOG_WARN("Failed to traverse all data in micro block", K(ret));
    }
  } else if (ObIntSC == get_store_class_map()[filter.get_objs().at(0).get_type_class()]) {
    if (OB_FAIL(traverse_all_data(parent, col_ctx, col_data, filter, result_bitmap,
                [](uint64_t &cur_int,
                const sql::ObWhiteFilterExecutor &filter,
                bool &result) -> int {
                  result = (cur_int >= filter.get_objs().at(0).v_.int64_)
                            && (cur_int <= filter.get_objs().at(1).v_.int64_);
                  return OB_SUCCESS;
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
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter.get_objs().count() == 0
                  || result_bitmap.size() != col_ctx.micro_block_header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Pushdown in operator: Invalid arguments");
  } else if (OB_FAIL(traverse_all_data(parent, col_ctx, col_data, filter, result_bitmap,
                      [](uint64_t &cur_int,
                          const sql::ObWhiteFilterExecutor &filter,
                          bool &result) -> int {
                        int ret = OB_SUCCESS;
                        ObObj cur_obj(filter.get_objs().at(0));
                        cur_obj.v_.uint64_ = cur_int;
                        if (OB_FAIL(filter.exist_in_obj_set(cur_obj, result))) {
                          LOG_WARN("Failed to check object in hashset", K(ret), K(cur_obj));
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
    ObBitmap &result_bitmap,
    int (*lambda)(
        uint64_t &cur_int,
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
  for (int64_t row_id = 0;
       OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
       ++row_id) {
    if (exist_parent_filter && parent->can_skip_filter(row_id)) {
      continue;
    } else if (null_value_contained && result_bitmap.test(row_id)) {
      if (OB_FAIL(result_bitmap.set(row_id, false))) {
        LOG_WARN("Failed to set row with null object to false", K(ret));
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
        // use lambda here to filter and set result bitmap
        bool result = false;
        if (OB_FAIL(lambda(cur_int, filter, result))) {
          LOG_WARN("Failed on trying to filter the row", K(ret), K(row_id), K(cur_int));
        } else if (result) {
          if (OB_FAIL(result_bitmap.set(row_id))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(filter));
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
    const int64_t *row_ids,
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
