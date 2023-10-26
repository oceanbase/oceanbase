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
        LOG_WARN("Failed on COMPARISON operator", K(ret), K(col_ctx));
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
  ObObjTypeStoreClass column_sc = get_store_class_map()[col_ctx.obj_meta_.get_type_class()];
  if (OB_UNLIKELY(NULL == col_data
                  || result_bitmap.size() != col_ctx.micro_block_header_->row_count_ 
                  || filter.get_objs().count() != 1
                  || filter.get_op_type() > sql::WHITE_OP_NE
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter Pushdown Operator: Invalid argument", K(ret), K(col_ctx));
  } else if (OB_UNLIKELY(col_ctx.obj_meta_.get_type() != filter.get_objs().at(0).get_type())) {
    // Filter type not match with column type, back to retro path
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("Type not match, back to retrograde path", K(col_ctx), K(filter));
  } else if (OB_UNLIKELY((column_sc != ObIntSC) && (column_sc != ObUIntSC))) { 
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Integer base encoding should only encode data as IntSC or UIntSC", K(ret), K(filter));
  } else {
    ObObj base_obj;
    base_obj.copy_meta_type(col_ctx.obj_meta_);
    base_obj.v_.uint64_ = base_;
    bool filter_obj_smaller_than_base = filter.get_objs().at(0) < base_obj;

    ObFPIntCmpOpType cmp_op_type = get_white_op_int_op_map()[filter.get_op_type()];
    if (filter_obj_smaller_than_base) {
      // Do not need to decode the data
      if ((cmp_op_type == FP_INT_OP_GE || cmp_op_type == FP_INT_OP_GT) ||
          cmp_op_type == FP_INT_OP_NE) {
        // All rows except null value are true
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to flip all bits in bitmap", K(ret));
        }
      } else {
        // All rows are false;
        result_bitmap.reuse();
      }
    } else {
      // traverse and decode all data
      // do compare row by row
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(traverse_all_data(
                     parent, col_ctx, col_data, filter, result_bitmap,
                     cmp_op_type,
                     [](const ObObj &cur_obj,
                        const sql::ObWhiteFilterExecutor &filter,
                        bool &result,
                        const ObFPIntCmpOpType &cmp_op_type) -> int {
                       result = fp_int_cmp<ObObj>(cur_obj, filter.get_objs().at(0), cmp_op_type);
                       return OB_SUCCESS;
                     }))) {
        LOG_WARN("Failed to traverse all data in micro block", K(ret));
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
  ObObjTypeStoreClass column_sc = get_store_class_map()[col_ctx.obj_meta_.get_type_class()];
  const ObObj &obj_left = filter.get_objs().at(0);
  const ObObj &obj_right = filter.get_objs().at(1);
  if (OB_UNLIKELY(NULL == col_data
                  || result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() != 2
                  || filter.get_op_type() != sql::WHITE_OP_BT
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument", K(ret), K(col_ctx));
  } else if (OB_UNLIKELY((column_sc != ObIntSC) && (column_sc != ObUIntSC))) { 
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Integer base encoding should only encode data as IntSC or UIntSC", K(ret), K(filter));
  } else if (OB_UNLIKELY((col_ctx.obj_meta_.get_type() != filter.get_objs().at(0).get_type())
                         || (col_ctx.obj_meta_.get_type() != filter.get_objs().at(1).get_type()))) {
    // Filter type not match with column type or obj0 type not match with obj1
    // back to retro path
    ret = OB_NOT_SUPPORTED;
    LOG_DEBUG("Type not match, back to retrograde path", K(col_ctx), K(filter));
  } else if (obj_left > obj_right){
    // Between left bound large than right bound
    // return all-false bitmap
    result_bitmap.reuse();
    LOG_DEBUG("Hit shortcut, obj_left > obj_right, return all-false bitmap", K(obj_right), K(obj_right));
  } else {
    ObObj base_obj;
    base_obj.copy_meta_type(col_ctx.obj_meta_);
    base_obj.v_.uint64_ = base_;
    bool filter_obj_smaller_than_base = obj_right < base_obj;

    if (filter_obj_smaller_than_base) {
      // Do not need to decode the data
      // All rows are false
      result_bitmap.reuse();
      LOG_DEBUG("Hit shortcut, obj_right < base_obj, return all-false bitmap", K(obj_right), K(base_obj));
    } else {
      // traverse and decode all data
      // do compare row by row
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(traverse_all_data(
                     parent, col_ctx, col_data, filter, result_bitmap, FP_INT_OP_MAX,
                     [](const ObObj &cur_obj,
                        const sql::ObWhiteFilterExecutor &filter,
                        bool &result,
                        const ObFPIntCmpOpType &cmp_op_type) -> int {
                       result = (cur_obj >= filter.get_objs().at(0))
                                 && (cur_obj <= filter.get_objs().at(1));
                       return OB_SUCCESS;
                     }))) {
        LOG_WARN("Failed to traverse all data in micro block", K(ret));
      }
    }
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
  ObObjTypeStoreClass column_sc = get_store_class_map()[col_ctx.obj_meta_.get_type_class()];
  if (OB_UNLIKELY(NULL == col_data
                  || result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() == 0
                  || filter.get_op_type() != sql::WHITE_OP_IN
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Pushdown in operator: Invalid arguments");
  } else if (OB_UNLIKELY((column_sc != ObIntSC) && (column_sc != ObUIntSC))) { 
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Integer base encoding should only encode data as IntSC or UIntSC", K(ret), K(filter));
  } else {
    ObObj base_obj;
    base_obj.copy_meta_type(col_ctx.obj_meta_);
    base_obj.v_.uint64_ = base_;
    // use max(obj_set) compare with base
    bool filter_obj_smaller_than_base = filter.get_max_param() < base_obj;
    ObDatum *datums = nullptr;

    if (filter_obj_smaller_than_base) {
      // Do not need to decode the data
      // All rows are false
      result_bitmap.reuse();
      LOG_DEBUG("Hit shortcut, max(obj_set) < base_obj, return all-false bitmap", K(base_obj));
    } else if (OB_FAIL(filter.get_datums_from_column(datums))) {
      LOG_WARN("Failed to get datums from column for batch decode", K(ret));
    } else if (OB_ISNULL(datums)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get null datums from column for batch decode", K(ret));
    } else {
      // prepare arguments
      int64_t cur_row_id = 0;
      int64_t end_row_id = col_ctx.micro_block_header_->row_count_;
      int64_t last_start = cur_row_id;
      int64_t row_cap = 0;
      bool null_value_contained = (result_bitmap.popcnt() > 0);
      ObObj cur_obj;
      bool result = false;
      // filter by batch
      while (OB_SUCC(ret) && cur_row_id < end_row_id) {
        last_start = cur_row_id;
        row_cap = min(pd_filter_info.batch_size_, end_row_id - cur_row_id);
        cur_row_id += row_cap;
        // 1. prepare row_ids
        for (int64_t i = 0; i < row_cap; ++i) {
          pd_filter_info.row_ids_[i] = last_start + i;
        }
        // 2. batch decode
        if (OB_FAIL(batch_decode(col_ctx, NULL, pd_filter_info.row_ids_,
                                pd_filter_info.cell_data_ptrs_, row_cap,
                                datums))) {
          LOG_WARN("Failed to batch decode", K(ret), K(row_cap));
        } else {
          // 3. traverse all datums row by row
          for (int64_t row_id = last_start; OB_SUCC(ret) && row_id < cur_row_id; ++row_id) {
            const int64_t buf_id = row_id - last_start;
            if (nullptr != parent && parent->can_skip_filter(row_id)) {
              continue;
            } else if (null_value_contained && result_bitmap.test(row_id)) {
              // object in this row is null
              if (OB_FAIL(result_bitmap.set(row_id, false))) {
                LOG_WARN("Failed to set null value to false", K(ret));
              }
            } else {
              if (OB_FAIL(datums[buf_id].to_obj(cur_obj, col_ctx.obj_meta_))) {
                LOG_WARN("Failed to transform datum to obj", K(ret), K(datums[buf_id]));
              } else if (OB_FAIL(filter.exist_in_obj_set(cur_obj, result))) {
                LOG_WARN("Failed to check object in obj set", K(ret), K(cur_obj));
              } else if (result && OB_FAIL(result_bitmap.set(row_id))) {
                LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(filter));
              }
            }
          }
        }
      }
      LOG_TRACE("in_operator use batch decode successfully", K(ret), K(col_ctx.is_fix_length()));
    }
  }
  return ret;
}

int ObIntegerBaseDiffDecoder::traverse_all_data(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap,
    const ObFPIntCmpOpType &cmp_op_type,
    int (*lambda)(
        const ObObj &cur_obj,
        const sql::ObWhiteFilterExecutor &filter,
        bool &result,
        const ObFPIntCmpOpType &cmp_op_type)) const
{
  int ret = OB_SUCCESS;
  uint64_t v = 0;
  ObObj cur_obj;
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
  sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();

  // copy meta, include storage type (Int64/UInt64)
  cur_obj.copy_meta_type(col_ctx.obj_meta_);
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
        cur_obj.v_.uint64_ = base_ + v;
        // use lambda here to filter and set result bitmap
        bool result = false;
        if (OB_FAIL(lambda(cur_obj, filter, result, cmp_op_type))) {
          LOG_WARN("Failed on trying to filter the row", K(ret), K(row_id), K(cur_obj.v_.uint64_));
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
