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

#include "ob_rle_decoder.h"
#include "ob_dict_decoder.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObRLEDecoder::type_;
int ObRLEDecoder::decode(ObColumnDecoderCtx &ctx, ObObj &cell, const int64_t row_id,
    const ObBitStream &bs,
    const char *data, const int64_t len) const
{
  UNUSED(bs);
  UNUSED(data);
  UNUSED(len);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }
    // get ref value
    const int64_t pos = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_)
        .upper_bound_(meta_header_->payload_, 0, meta_header_->count_, row_id);
    const int64_t ref = ObIntArrayFuncTable::instance(meta_header_->ref_byte_)
        .at_(meta_header_->payload_ + ref_offset_, pos - 1);

    if (OB_SUCC(ret)) {
      const int64_t dict_meta_length = ctx.col_header_->length_ - meta_header_->offset_;
      if (OB_FAIL(dict_decoder_.decode(ctx.obj_meta_, cell, ref, dict_meta_length))) {
        LOG_WARN("failed to decode dict", K(ret), K(ref));
      }
    }
  }
  return ret;
}

int ObRLEDecoder::update_pointer(const char *old_block, const char *cur_block)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(old_block) || OB_ISNULL(cur_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(old_block), KP(cur_block));
  } else {
    ObIColumnDecoder::update_pointer(meta_header_, old_block, cur_block);
    if (OB_FAIL(dict_decoder_.update_pointer(old_block, cur_block))) {
      LOG_WARN("dict decoder update pointer failed", K(ret));
    }
  }
  return ret;
}

// Internal call, not check parameters for performance
// row_ids should be monotonically increasing or decreasing
int ObRLEDecoder::batch_decode(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int64_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  UNUSED(row_index);
  int ret = OB_SUCCESS;
  int64_t unused_null_cnt;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_FAIL(extract_ref_and_null_count(row_ids, row_cap, datums, unused_null_cnt))) {
    LOG_WARN("Failed to extract refs",K(ret));
  } else if (OB_FAIL(dict_decoder_.batch_decode_dict(
      ctx.col_header_->get_store_obj_type(),
      cell_datas,
      row_cap,
      ctx.col_header_->length_ - meta_header_->offset_,
      datums))) {
    LOG_WARN("Failed to batch decode RLE ref data from dict", K(ret), K(ctx));
  }
  return ret;
}

int ObRLEDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  UNUSEDx(ctx, row_index);
  int ret = OB_SUCCESS;
  null_count = 0;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_FAIL(extract_ref_and_null_count(row_ids, row_cap, nullptr, null_count))) {
    LOG_WARN("Failed to extract null count", K(ret));
  }
  return ret;
}

int ObRLEDecoder::pushdown_operator(
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
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Const decoder not inited", K(ret));
  } else if (OB_UNLIKELY(op_type >= sql::WHITE_OP_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid op type for pushed down white filter", K(ret), K(op_type));
  } else {
    switch (op_type) {
      case sql::WHITE_OP_NU:
      case sql::WHITE_OP_NN: {
        if (OB_FAIL(nu_nn_operator(parent, col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed to run NU / NN operator", K(ret), K(col_ctx));
        }
        break;
      }
      case sql::WHITE_OP_EQ:
      case sql::WHITE_OP_NE: {
        if (OB_FAIL(eq_ne_operator(parent, col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed to run EQ / NE operator", K(ret), K(col_ctx));
        }
        break;
      }
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_GT: {
        if (OB_FAIL(comparison_operator(parent, col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed to run Comparison operator", K(ret), K(col_ctx));
        }
        break;
      }
      case sql::WHITE_OP_BT: {
        if (OB_FAIL(bt_operator(parent, col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed to run BT operator", K(ret), K(filter));
        }
        break;
      }
      case sql::WHITE_OP_IN: {
        if (OB_FAIL(in_operator(parent, col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed to run IN operator", K(ret), K(filter));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Pushed down filter operator type not supported", K(ret), K(filter));
      }
    } // end of switch
  }
  return ret;
}

int ObRLEDecoder::nu_nn_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for NU / NN operator", K(ret),
             K(result_bitmap.size()), K(filter));
  } else {
    const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    if (dict_count == 0) {
      if (sql::WHITE_OP_NU == filter.get_op_type()) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to bitwise not on result_bitmap", K(ret));
        }
      }
    } else {
      if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, dict_count, FP_INT_OP_EQ, true, result_bitmap))) {
        LOG_WARN("Failed to compare ref table and set result bitmap",
            K(ret), K(dict_count), K(filter));
      } else if (sql::WHITE_OP_NN == filter.get_op_type()) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to bitwise not on result bitmnap", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRLEDecoder::eq_ne_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() != 1
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for EQ / NE operator", K(ret),
             K(result_bitmap.size()), K(filter));
  } else {
    const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    const int64_t dict_meta_length = col_ctx.col_header_->length_ - meta_header_->offset_;
    const ObObj &ref_obj = filter.get_objs().at(0);
    if (dict_count > 0) {
      ObDictDecoderIterator traverse_it = dict_decoder_.begin(&col_ctx, dict_meta_length);
      ObDictDecoderIterator end_it = dict_decoder_.end(&col_ctx, dict_meta_length);
      int64_t dict_ref = 0;
      while (OB_SUCC(ret) && traverse_it != end_it) {
        if (*traverse_it == ref_obj) {
          if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, dict_ref, FP_INT_OP_EQ, true, result_bitmap))) {
            LOG_WARN("Failed to compare reference and set result bitmap",
                K(ret), K(dict_ref), K(filter));
          }
        }
        ++traverse_it;
        ++dict_ref;
      }
    }
    if (OB_SUCC(ret) && filter.get_op_type() == sql::WHITE_OP_NE) {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("Failed to bitwise not on result bitmap", K(ret));
      } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, dict_count,
                                            FP_INT_OP_EQ, false, result_bitmap))) {
        LOG_WARN("Failed to compare reference and set result bitmap to false",
            K(ret), K(dict_count), K(filter));
      }
    }
  }
  return ret;
}

int ObRLEDecoder::comparison_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() != 1
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for comparison operator", K(ret),
             K(result_bitmap.size()), K(filter));
  } else {
    const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    const int64_t dict_meta_length = col_ctx.col_header_->length_ - meta_header_->offset_;
    const ObObj &ref_obj = filter.get_objs().at(0);
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    if (dict_count > 0) {
      bool found = false;
      ObDictDecoderIterator end_it = dict_decoder_.end(&col_ctx, dict_meta_length);
      ObDictDecoderIterator traverse_it = dict_decoder_.begin(&col_ctx, dict_meta_length);
      const int64_t ref_bitset_size = dict_count + 1;
      char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
      sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
      ref_bitset->init(ref_bitset_size);
      int64_t dict_ref = 0;
      while (traverse_it != end_it) {
        if (ObObjCmpFuncs::compare_oper_nullsafe(
              *traverse_it,
              ref_obj,
              ref_obj.get_collation_type(),
              sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type])) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++traverse_it;
        ++dict_ref;
      }
      if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, ref_bitset, result_bitmap))) {
        LOG_WARN("Failed to set result_bitmap", K(ret));
      }
    }
  }
  return ret;
}

int ObRLEDecoder::bt_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                || filter.get_objs().count() != 2
                || filter.get_op_type() != sql::WHITE_OP_BT)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid argument for BT operator",
          K(ret), K(result_bitmap.size()), K(filter.get_objs()));
  } else {
    const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    if (dict_count > 0) {
      // Unsorted dictionary
      bool found = false;
      const int64_t dict_meta_length = col_ctx.col_header_->length_ - meta_header_->offset_;
      const common::ObIArray<ObObj> &objs = filter.get_objs();
      ObDictDecoderIterator end_it = dict_decoder_.end(&col_ctx, dict_meta_length);
      ObDictDecoderIterator traverse_it = dict_decoder_.begin(&col_ctx, dict_meta_length);
      const int64_t ref_bitset_size = dict_count + 1;
      char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
      sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
      ref_bitset->init(ref_bitset_size);
      int64_t dict_ref = 0;
      while (traverse_it != end_it) {
        if (*traverse_it >= objs.at(0) && *traverse_it <= objs.at(1)) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++traverse_it;
        ++dict_ref;
      }
      if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, ref_bitset, result_bitmap))) {
        LOG_WARN("Failed to set result_bitmap", K(ret));
      }
    }
  }
  return ret;
}

int ObRLEDecoder::in_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() == 0
                  || filter.get_op_type() != sql::WHITE_OP_IN)) {
    LOG_WARN("Invalid argument for BT operator", K(ret),
             K(result_bitmap.size()), K(filter));
  } else {
    const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    if (dict_count > 0) {
      bool found = false;
      const int64_t dict_meta_length = col_ctx.col_header_->length_ - meta_header_->offset_;
      ObDictDecoderIterator end_it = dict_decoder_.end(&col_ctx, dict_meta_length);
      ObDictDecoderIterator traverse_it = dict_decoder_.begin(&col_ctx, dict_meta_length);
      const int64_t ref_bitset_size = dict_count + 1;
      char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
      sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
      ref_bitset->init(ref_bitset_size);
      int64_t dict_ref = 0;
      bool is_exist = false;
      while (OB_SUCC(ret) && traverse_it != end_it) {
        if (OB_FAIL(filter.exist_in_obj_set(*traverse_it, is_exist))) {
          LOG_WARN("Failed to check object in hashset", K(ret), K(*traverse_it));
        } else if (is_exist) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++traverse_it;
        ++dict_ref;
      }
      if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, ref_bitset, result_bitmap))) {
        LOG_WARN("Failed to set result_bitmap", K(ret));
      }
    }
  }
  return ret;
}

int ObRLEDecoder::cmp_ref_and_set_res(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const int64_t dict_ref,
    const ObFPIntCmpOpType cmp_op,
    bool flag,
    ObBitmap &result_bitmap) const
{
  UNUSED(parent);
  int ret = OB_SUCCESS;
  const ObIntArrayFuncTable &row_ids = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
  const ObIntArrayFuncTable &refs = ObIntArrayFuncTable::instance(meta_header_->ref_byte_);
  int64_t row_id;
  int64_t next_row_id;
  int64_t ref;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_header_->count_ ; ++i) {
    ref = refs.at_(meta_header_->payload_ + ref_offset_, i);
    if (fp_int_cmp<int64_t>(ref, dict_ref, cmp_op)
        && OB_LIKELY(ref < dict_decoder_.get_dict_header()->count_ || FP_INT_OP_EQ == cmp_op)) {
      row_id = row_ids.at_(meta_header_->payload_, i);
      next_row_id = i != meta_header_->count_ - 1
                          ? row_ids.at_(meta_header_->payload_, i + 1)
                          : col_ctx.micro_block_header_->row_count_;
      for (int64_t idx = row_id; OB_SUCC(ret) && idx < next_row_id; ++idx) {
        if (OB_FAIL(result_bitmap.set(idx, flag))) {
          LOG_WARN("Failed to set result_bitmap", K(ret), K(row_id), K(next_row_id), K(idx));
        }
      }
    }
  }
  return ret;
}

int ObRLEDecoder::set_res_with_bitset(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObBitVector *ref_bitset,
    ObBitmap &result_bitmap) const
{
  UNUSED(parent);
  int ret = OB_SUCCESS;
  const ObIntArrayFuncTable &row_ids = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
  const ObIntArrayFuncTable &refs = ObIntArrayFuncTable::instance(meta_header_->ref_byte_);
  int64_t row_id;
  int64_t next_row_id;
  int64_t ref;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_header_->count_ ; ++i) {
    ref = refs.at_(meta_header_->payload_ + ref_offset_, i);
    if (ref_bitset->exist(ref)) {
      row_id = row_ids.at_(meta_header_->payload_, i);
      next_row_id = i != meta_header_->count_ - 1
                          ? row_ids.at_(meta_header_->payload_, i + 1)
                          : col_ctx.micro_block_header_->row_count_;
      for (int64_t idx = row_id; OB_SUCC(ret) && idx < next_row_id; ++idx) {
        if (OB_FAIL(result_bitmap.set(idx))) {
          LOG_WARN("Failed to set result_bitmap", K(ret), K(row_id), K(next_row_id), K(idx));
        }
      }
    }
  }
  return ret;
}

int ObRLEDecoder::extract_ref_and_null_count(
    const int64_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  const ObIntArrayFuncTable &row_id_array
      = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
  const ObIntArrayFuncTable &ref_array
      = ObIntArrayFuncTable::instance(meta_header_->ref_byte_);
  const int64_t ref_count = meta_header_->count_;

  // Generate dict refs with minimum binary search call
  int64_t row_id = 0;
  int64_t ref_table_pos = 0;
  int64_t next_ref_row_id = row_id_array.at_(meta_header_->payload_, ref_table_pos);
  int64_t curr_ref = ref_array.at_(meta_header_->payload_ + ref_offset_, ref_table_pos);
  // Traverse @row_ids by direction that row_id is monotonically increasing
  bool monotonic_inc = true;
  if (row_cap > 1) {
    monotonic_inc = row_ids[1] > row_ids[0];
  }
  int64_t step = monotonic_inc ? 1 : -1;
  int64_t trav_idx = monotonic_inc ? 0 : row_cap - 1;
  int64_t trav_cnt = 0;
  const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
  while (trav_cnt < row_cap) {
    row_id = row_ids[trav_idx];
    if (ref_table_pos == ref_count || row_id < next_ref_row_id) {
    } else if (row_id == next_ref_row_id) {
      ++ref_table_pos;
      if (ref_table_pos < ref_count) {
        next_ref_row_id = row_id_array.at_(meta_header_->payload_, ref_table_pos);
      }
      curr_ref = ref_array.at_(meta_header_->payload_ + ref_offset_, ref_table_pos - 1);
    } else {
      ref_table_pos =
          row_id_array.upper_bound_(meta_header_->payload_, 0, ref_count, row_id);
      if (ref_table_pos < ref_count) {
        next_ref_row_id = row_id_array.at_(meta_header_->payload_, ref_table_pos);
      }
      curr_ref = ref_array.at_(meta_header_->payload_ + ref_offset_, ref_table_pos - 1);
    }
    if (nullptr != datums) {
      datums[trav_idx].pack_ = static_cast<uint32_t>(curr_ref);
    }
    if (curr_ref >= dict_count) {
      null_count++;
    }

    ++trav_cnt;
    trav_idx += step;
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
