/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_const_decoder.h"
#include "ob_dict_decoder.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;
const ObColumnHeader::Type ObConstDecoder::type_;

int ObConstDecoder::decode_without_dict(const ObColumnDecoderCtx &ctx, ObObj &cell) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }
    int64_t ref = meta_header_->const_ref_;
    const int64_t len = ctx.col_header_->length_ - sizeof(*meta_header_);
    const ObObjTypeClass tc = ob_obj_type_class(ctx.obj_meta_.get_type());
    if (0 == ref) {
      switch (get_store_class_map()[tc]) {
        case ObIntSC:
        case ObUIntSC: {
          cell.v_.uint64_ = 0;
          MEMCPY(&cell.v_, meta_header_->payload_, len);
          if ((cell.v_.uint64_ >> (len * CHAR_BIT - 1) & 0x1) && ObIntTC == tc) {
            cell.v_.uint64_ |=  ~INTEGER_MASK_TABLE[len];
          }
          break;
        }
        case ObNumberSC: {
          cell.nmb_desc_.desc_ = *reinterpret_cast<const uint32_t *>(meta_header_->payload_);
          cell.v_.nmb_digits_ = reinterpret_cast<uint32_t *>(
              const_cast<char *>(meta_header_->payload_) + sizeof(uint32_t));
          break;
        }
        case ObStringSC: {
          cell.val_len_ = static_cast<int32_t>(len);
          cell.v_.string_ = meta_header_->payload_;
          break;
        }
        case ObOTimestampSC: {
          ret = cell.read_otimestamp(meta_header_->payload_, cell.get_otimestamp_store_size());
          break;
        }
        case ObIntervalSC: {
          ret = cell.read_interval(meta_header_->payload_);
          break;
        }
        default: {
          ret = OB_INNER_STAT_ERROR;
          LOG_WARN("unexpected store class", K(ret), "store_type", get_store_class_map()[tc]);
        }
      }
    } else if (1 == ref) { // null
      cell.set_null();
    } else if (2 == ref) { // nope
      cell.set_ext(ObActionFlag::OP_NOP);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ref", K(ret), K(ref), "header", *meta_header_);
    }
  }
  return ret;
}

int ObConstDecoder::decode(ObColumnDecoderCtx &ctx, ObObj &cell, const int64_t row_id,
                           const ObBitStream &bs, const char *data, const int64_t len) const
{
  UNUSEDx(bs, data, len);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == meta_header_->count_) {
    if (OB_FAIL(decode_without_dict(ctx, cell))) {
      LOG_WARN("failed to decode without dict", K(ret));
    }
  } else {
    int64_t ref = 0;
    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }
    // get ref value
    const ObIntArrayFuncTable &row_ids = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
    const int64_t pos = row_ids.lower_bound_(
        meta_header_->payload_ + meta_header_->count_, 0, meta_header_->count_, row_id);
    if (pos == meta_header_->count_
        || row_id != row_ids.at_(meta_header_->payload_ + meta_header_->count_, pos)) {
      ref = meta_header_->const_ref_;
    } else {
      ref = reinterpret_cast<const uint8_t *>(meta_header_->payload_)[pos];
    }

    if (OB_SUCC(ret)) {
      const int64_t dict_meta_length = ctx.col_header_->length_ - meta_header_->offset_;
      if (OB_FAIL(dict_decoder_.decode(cell, ref, dict_meta_length))) {
        LOG_WARN("failed to decode dict", K(ret), K(ref));
      }
    }
  }
  return ret;
}

int ObConstDecoder::update_pointer(const char *old_block, const char *cur_block)
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
    if (meta_header_->count_ > 0) {
      if (OB_FAIL(dict_decoder_.update_pointer(old_block, cur_block))) {
        LOG_WARN("dict decoder update pointer failed", K(ret));
      }
    }
  }
  return ret;
}

/**
 * Internal call, not check parameters for performance
 * row_ids should be monotonically increasing or decreasing
 *
 * Potential optimization:
 *  SIMD and batch load data for microblock without exceptions
 */
int ObConstDecoder::batch_decode(
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
    LOG_WARN("Not init", K(ret));
  } else if (0 == meta_header_->count_) {
    if (OB_FAIL(batch_decode_without_dict(ctx, row_cap, datums))) {
      LOG_WARN("Failed to batch decode const encoding data without dict",
          K(ret), K(row_cap), K(ctx));
    }
  } else if (OB_FAIL(extract_ref_and_null_count(row_ids, row_cap, datums, unused_null_cnt))) {
    LOG_WARN("Failed to extract refs",K(ret));
  } else if (OB_FAIL(dict_decoder_.batch_decode_dict(
      ctx.obj_meta_.get_type(),
      cell_datas,
      row_cap,
      ctx.col_header_->length_ - meta_header_->offset_,
      datums))) {
    LOG_WARN("Failed to decode const encoding data from dict", K(ret));
  }
  return ret;
}

int ObConstDecoder::batch_decode_without_dict(
    const ObColumnDecoderCtx &ctx,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datums) || OB_UNLIKELY(0 >= row_cap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), KP(datums), K(row_cap));
  } else {
    int64_t ref = meta_header_->const_ref_;
    if (0 == ref) {
      ObObj obj_cell;
      if (OB_FAIL(decode_without_dict(ctx, obj_cell))) {
        LOG_WARN("Decode to obobj without dict failed", K(ret), K(obj_cell));
      } else if (OB_FAIL(datums[0].from_obj(obj_cell))) {
        LOG_WARN("Failed to transform from obobj to obdatum", K(ret), K(obj_cell));
      } else {
        switch (get_store_class_map()[ob_obj_type_class(ctx.obj_meta_.get_type())]) {
        case ObIntSC:
        case ObUIntSC:
        case ObOTimestampSC:
        case ObIntervalSC:
        case ObNumberSC: {
          for (int64_t i = 1; i < row_cap; ++i) {
            datums[i].pack_ = datums[0].pack_;
            MEMCPY(const_cast<char *>(datums[i].ptr_), datums[0].ptr_, datums[0].len_);
          }
          break;
        }
        case ObStringSC: {
            for (int64_t i = 1; i < row_cap; ++i) {
              datums[i].pack_ = datums[0].pack_;
              datums[i].ptr_ = datums[0].ptr_;
            }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected type", K(ret), K(ctx));
        }
        }
      }
    } else if (1 == ref) {
      for (int64_t i = 0; i < row_cap; ++i) {
        datums[i].set_null();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected ref in const encoding for batch decode", K(ret), K(row_cap), K(ctx));
    }
  }
  return ret;
}

int ObConstDecoder::get_null_count(
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
    LOG_WARN("Not init", K(ret));
  } else if (0 == meta_header_->count_) {
    int64_t ref = meta_header_->const_ref_;
    if (ref > 0) {
      null_count += row_cap;
    }
  } else if OB_FAIL(extract_ref_and_null_count(row_ids, row_cap, nullptr, null_count)) {
    LOG_WARN("Failed to extrace null count", K(ret));
  }
  return ret;
}

int ObConstDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    ObBitmap &result_bitmap) const
{
  UNUSEDx(parent, meta_data, row_index);
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Const decoder not inited", K(ret));
  } else if (OB_UNLIKELY(op_type >= sql::WHITE_OP_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid op type for pushed down white filter", K(ret), K(op_type));
  } else if (0 == meta_header_->count_) {
    // No exception
    if (OB_FAIL(const_only_operator(col_ctx, filter, result_bitmap))){
      LOG_WARN("Pushdown operator for const decoder without exception failed", K(ret));
    }
  } else {
    // Exist exceptions, exception values are stored in dict.
    switch (op_type) {
      case sql::WHITE_OP_NU:
      case sql::WHITE_OP_NN: {
        if (OB_FAIL(nu_nn_operator(col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed on running NU / NN pushed down operator",
                   K(ret), K(col_ctx), K(filter));
        }
        break;
      }
      case sql::WHITE_OP_EQ:
      case sql::WHITE_OP_NE:
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_LE: {
        if (OB_FAIL(comparison_operator(col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed on running comparison pushed down operator",
                   K(ret), K(col_ctx), K(filter));
        }
        break;
      }
      case sql::WHITE_OP_BT: {
        if (OB_FAIL(bt_operator(col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed on running BT pushed down operator", K(ret), K(col_ctx), K(filter));
        }
        break;
      }
      case sql::WHITE_OP_IN: {
        if (OB_FAIL(in_operator(col_ctx, filter, result_bitmap))) {
          LOG_WARN("Failed on running IN pushed down operator", K(ret), K(col_ctx), K(filter));
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

int ObConstDecoder::const_only_operator(
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObObj> &objs = filter.get_objs();
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for const only operator", K(ret), K(filter));
  } else {
    int64_t ref = meta_header_->const_ref_;
    ObObj const_obj;
    if (OB_UNLIKELY(ref > 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected ref", K(ret), K(ref));
    } else if (OB_FAIL(decode_without_dict(col_ctx, const_obj))){
      LOG_WARN("Failed to decode const object", K(ret), K(col_ctx));
    } else {
      if (const_obj.is_fixed_len_char_type() && nullptr != col_ctx.col_param_) {
        if (OB_FAIL(storage::pad_column(col_ctx.col_param_->get_accuracy(),
                                        *col_ctx.allocator_, const_obj))) {
          LOG_WARN("Failed to pad column", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        switch (filter.get_op_type()) {
          case sql::WHITE_OP_NU: {
            if (1 == ref) {
              if (OB_FAIL(result_bitmap.bit_not())) {
                LOG_WARN("Failed to do bitwise not on result bitmap", K(ret));
              }
            }
            break;
          }
          case sql::WHITE_OP_NN: {
            if (0 == ref) {
              if (OB_FAIL(result_bitmap.bit_not())) {
                LOG_WARN("Failed to do bitwise not on result bitmap", K(ret));
              }
            }
            break;
          }
          case sql::WHITE_OP_EQ:
          case sql::WHITE_OP_NE:
          case sql::WHITE_OP_GT:
          case sql::WHITE_OP_LT:
          case sql::WHITE_OP_GE:
          case sql::WHITE_OP_LE: {
            if (OB_UNLIKELY(objs.count() != 1 ||
                            filter.null_param_contained())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("Invalid argument", K(ret), K(filter));
            } else if (ref == 1) {
            } else if (ObObjCmpFuncs::compare_oper_nullsafe(
                    const_obj, objs.at(0),
                    const_obj.get_collation_type(),
                    sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()])) {
              if (OB_FAIL(result_bitmap.bit_not())) {
                LOG_WARN("Failed to do bitwise not on result bitmap", K(ret));
              }
            }
            break;
          }
          case sql::WHITE_OP_BT: {
            if (OB_UNLIKELY(objs.count() != 2)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("Invalid argument", K(ret),
                       K(objs), K(result_bitmap.size()), K(col_ctx));
            } else if (ref == 1) {
            } else if (const_obj >= objs.at(0)
                       && const_obj <= objs.at(1)) {
              if (OB_FAIL(result_bitmap.bit_not())) {
                LOG_WARN("Failed to do bitwise not on result bitmap", K(ret));
              }
            }
            break;
          }
          case sql::WHITE_OP_IN: {
            if (OB_UNLIKELY(objs.count() == 0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("Invalid argument", K(ret), K(objs), K(result_bitmap.size()), K(col_ctx));
            } else if (ref == 1) {
            } else {
              bool is_existed = false;
              // Check const object in hashset or not
              if (OB_FAIL(filter.exist_in_obj_set(const_obj, is_existed))) {
                LOG_WARN("Failed to check const object in hashset", K(ret));
              } else if (is_existed) {
                if (OB_FAIL(result_bitmap.bit_not())) {
                  LOG_WARN("Failed to do bitwise not on result bitmap", K(ret));
                }
              }
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("Pushed down filter operator type not supported", K(ret));
          }
        } // end of switch
      }
    }
  }
  return ret;
}

int ObConstDecoder::nu_nn_operator(
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for const only operator", K(ret), K(filter));
  } else {
    int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    const ObIntArrayFuncTable &row_ids = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
    int64_t ref;
    int64_t row_id;
    if (meta_header_->const_ref_ == dict_count) {
      // Const is null all expection values are not null
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("Failed to flip all bits in result bitmap", K(ret));
      }
      for (int64_t pos = 0; OB_SUCC(ret) && pos < meta_header_->count_; ++pos) {
        row_id = row_ids.at_(meta_header_->payload_ + meta_header_->count_, pos);
        if (OB_FAIL(result_bitmap.set(row_id, false))) {
          LOG_WARN("Failed to set result bitmap", K(ret), K(row_id));
        }
      }
    } else {
      for (int64_t pos = 0; OB_SUCC(ret) && pos < meta_header_->count_; ++pos) {
        ref = reinterpret_cast<const uint8_t*>(meta_header_->payload_)[pos];
        if (ref == dict_count) {
          row_id = row_ids.at_(meta_header_->payload_ + meta_header_->count_, pos);
          if (OB_FAIL(result_bitmap.set(row_id))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(row_id));
          }
        }
      }
    }
    if (OB_SUCC(ret) && filter.get_op_type() == sql::WHITE_OP_NN) {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("Failed to flip all bits in result bitmap", K(ret));
      }
    }
  }
  return ret;
}

int ObConstDecoder::comparison_operator(
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() != 1
                  || filter.get_op_type() > sql::WHITE_OP_NE
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for comparison operator", K(ret), K(filter), K(col_ctx));
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    const ObObj &ref_obj = filter.get_objs().at(0);
    const ObIntArrayFuncTable &row_ids = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
    const int64_t dict_meta_length = col_ctx.col_header_->length_ - meta_header_->offset_;
    int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    bool const_in_result_set = false;

    if (meta_header_->const_ref_ == dict_count) {
      // Const value is null
    } else {
      // Const value is not null
      ObDictDecoderIterator dict_iter = dict_decoder_.begin(&col_ctx, dict_meta_length);
      ObObj& const_obj = *(dict_iter + meta_header_->const_ref_);
      if (const_obj.is_fixed_len_char_type() && nullptr != col_ctx.col_param_) {
        if (OB_FAIL(storage::pad_column(col_ctx.col_param_->get_accuracy(),
                                        *col_ctx.allocator_, const_obj))) {
          LOG_WARN("Failed to pad column", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (ObObjCmpFuncs::compare_oper_nullsafe(
              const_obj,
              ref_obj,
              ref_obj.get_collation_type(),
              sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type])) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to flip all bits in result bitmap", K(ret));
        } else {
          const_in_result_set = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool found = false;
      ObDictDecoderIterator trav_it = dict_decoder_.begin(&col_ctx, dict_meta_length);
      ObDictDecoderIterator end_it = dict_decoder_.end(&col_ctx, dict_meta_length);
      const int64_t ref_bitset_size = dict_count + 1;
      char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
      sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
      ref_bitset->init(ref_bitset_size);
      int64_t dict_ref = 0;
      while (OB_SUCC(ret) && trav_it != end_it) {
        // If const value in result, set rows not in result set to false
        // Or set rows in result set to true
        if (OB_UNLIKELY(((*trav_it).is_null_oracle() && lib::is_oracle_mode())
                        || ((*trav_it).is_null() && lib::is_mysql_mode()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("There should not be null object in dictionary", K(ret));
        } else if (!const_in_result_set == ObObjCmpFuncs::compare_oper_nullsafe(
                *trav_it,
                ref_obj,
                ref_obj.get_collation_type(),
                sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type])) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++dict_ref;
        ++trav_it;
      }

      if (OB_FAIL(ret)) {
      } else if (found && OB_FAIL(set_res_with_bitset(
                  row_ids,
                  ref_bitset,
                  !const_in_result_set,
                  result_bitmap))) {
        LOG_WARN("Failed to set result bitmap", K(ret));
      } else if (const_in_result_set) {
        // Clean result bit for null rows when const value is in result set
        if (OB_FAIL(traverse_refs_and_set_res(row_ids, dict_count, false, result_bitmap))) {
          LOG_WARN("Failed to clean bitmap for null rows", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObConstDecoder::bt_operator(
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() != 2)) {
    LOG_WARN("Invalid argument for BT operator", K(ret), K(filter), K(result_bitmap.size()));
  } else {
    const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    const common::ObIArray<ObObj> &objs = filter.get_objs();
    const ObIntArrayFuncTable &row_ids = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
    const int64_t dict_meta_length = col_ctx.col_header_->length_ - meta_header_->offset_;
    bool const_in_result_set = false;

    if (meta_header_->const_ref_ == dict_count) {
    } else {
      ObDictDecoderIterator dict_iter = dict_decoder_.begin(&col_ctx, dict_meta_length);
      ObObj& const_obj = *(dict_iter + meta_header_->const_ref_);
      if (const_obj.is_fixed_len_char_type() && nullptr != col_ctx.col_param_) {
        if (OB_FAIL(storage::pad_column(col_ctx.col_param_->get_accuracy(),
                                        *col_ctx.allocator_, const_obj))) {
          LOG_WARN("Failed to pad column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (const_obj >= objs.at(0) && const_obj <= objs.at(1)) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to flip all bits in result bitmap", K(ret));
        } else {
          const_in_result_set = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool found = false;
      ObDictDecoderIterator trav_it = dict_decoder_.begin(&col_ctx, dict_meta_length);
      ObDictDecoderIterator end_it = dict_decoder_.end(&col_ctx, dict_meta_length);
      const int64_t ref_bitset_size = dict_count + 1;
      char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
      sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
      ref_bitset->init(ref_bitset_size);
      int64_t dict_ref = 0;
      while (OB_SUCC(ret) && trav_it != end_it) {
        if (OB_UNLIKELY(((*trav_it).is_null_oracle() && lib::is_oracle_mode())
                        || ((*trav_it).is_null() && lib::is_mysql_mode()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("There should not be null object in dictionary", K(ret));
        } else if (!const_in_result_set == (*trav_it >= objs.at(0) && *trav_it <= objs.at(1))) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++dict_ref;
        ++trav_it;
      }

      if (OB_FAIL(ret)) {
      } else if (found && OB_FAIL(set_res_with_bitset(
                  row_ids,
                  ref_bitset,
                  !const_in_result_set,
                  result_bitmap))) {
        LOG_WARN("Failed to set result bitmap", K(ret));
      } else if (const_in_result_set) {
        if (OB_FAIL(traverse_refs_and_set_res(row_ids, dict_count, false, result_bitmap))) {
          LOG_WARN("Failed to clean bitmap for null rows", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObConstDecoder::in_operator(
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() == 0
                  || filter.get_op_type() != sql::WHITE_OP_IN)) {
    LOG_WARN("Invalid argument for IN operator",
             K(ret), K(result_bitmap.size()), K(filter));
  } else {
    int64_t dict_count = dict_decoder_.get_dict_header()->count_;
    const ObIntArrayFuncTable &row_ids = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);
    const int64_t dict_meta_length = col_ctx.col_header_->length_ - meta_header_->offset_;
    bool const_in_result_set = false;

    if (meta_header_->const_ref_ == dict_count) {
    } else {
      ObDictDecoderIterator dict_iter = dict_decoder_.begin(&col_ctx, dict_meta_length);
      ObObj& const_obj = *(dict_iter + meta_header_->const_ref_);
      if (const_obj.is_fixed_len_char_type() && nullptr != col_ctx.col_param_) {
        if (OB_FAIL(storage::pad_column(col_ctx.col_param_->get_accuracy(),
                                        *col_ctx.allocator_, const_obj))) {
          LOG_WARN("Failed to pad column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(filter.exist_in_obj_set(const_obj, const_in_result_set))) {
        LOG_WARN("Failed to check whether const value is in set", K(ret));
      } else if (const_in_result_set) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to flip all bits for result bitmap", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      bool found = false;
      ObDictDecoderIterator trav_it = dict_decoder_.begin(&col_ctx, dict_meta_length);
      ObDictDecoderIterator end_it = dict_decoder_.end(&col_ctx, dict_meta_length);
      const int64_t ref_bitset_size = dict_count + 1;
      char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
      sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
      ref_bitset->init(ref_bitset_size);
      int64_t dict_ref = 0;
      while (OB_SUCC(ret) && trav_it != end_it) {
        bool cur_in_result_set = false;
        if (OB_UNLIKELY(((*trav_it).is_null_oracle() && lib::is_oracle_mode())
                        || ((*trav_it).is_null() && lib::is_mysql_mode()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("There should not be null object in dictionary", K(ret));
        } else if (OB_FAIL(filter.exist_in_obj_set((*trav_it), cur_in_result_set))) {
          LOG_WARN("Failed to check wheter current value is in set", K(ret));
        } else if (!const_in_result_set == cur_in_result_set) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++dict_ref;
        ++trav_it;
      }

      if (OB_FAIL(ret)) {
      } else if (found && OB_FAIL(set_res_with_bitset(
                  row_ids,
                  ref_bitset,
                  !const_in_result_set,
                  result_bitmap))) {
        LOG_WARN("Failed to set result bitmap", K(ret));
      } else if (const_in_result_set) {
        if (OB_FAIL(traverse_refs_and_set_res(row_ids, dict_count, false, result_bitmap))) {
          LOG_WARN("Failed to clean bitmap for null rows", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObConstDecoder::traverse_refs_and_set_res(
    const ObIntArrayFuncTable &row_ids,
    const int64_t dict_ref,
    const bool flag,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  int64_t ref;
  int64_t row_id;
  for (int64_t pos = 0; OB_SUCC(ret) && pos < meta_header_->count_; ++pos) {
    ref = reinterpret_cast<const uint8_t*>(meta_header_->payload_)[pos];
    if (ref == dict_ref) {
      row_id = row_ids.at_(meta_header_->payload_ + meta_header_->count_, pos);
      if (OB_FAIL(result_bitmap.set(row_id, flag))) {
        LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(flag));
      }
    }
  }
  return ret;
}

int ObConstDecoder::set_res_with_bitset(
    const ObIntArrayFuncTable &row_ids,
    const sql::ObBitVector *ref_bitset,
    const bool flag,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  int64_t ref;
  int64_t row_id;
  for (int64_t pos = 0; OB_SUCC(ret) && pos < meta_header_->count_; ++pos) {
    ref = reinterpret_cast<const uint8_t*>(meta_header_->payload_)[pos];
    if (ref_bitset->exist(ref)) {
      row_id = row_ids.at_(meta_header_->payload_ + meta_header_->count_, pos);
      if (OB_FAIL(result_bitmap.set(row_id, flag))) {
        LOG_WARN("Failed to set result bitmap", K(ret), K(row_id));
      }
    }
  }
  return ret;
}

int ObConstDecoder::extract_ref_and_null_count(
    const int64_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  const int64_t count = meta_header_->count_;
  const int64_t const_ref = meta_header_->const_ref_;
  const ObIntArrayFuncTable &row_id_arr
      = ObIntArrayFuncTable::instance(meta_header_->row_id_byte_);

  int64_t row_id = 0;
  int64_t except_table_pos = 0;
  int64_t next_except_row_id = row_id_arr.at_(meta_header_->payload_ + count, except_table_pos);

  bool monotonic_inc = true;
  if (row_cap > 1) {
    monotonic_inc = row_ids[1] > row_ids[0];
  }
  int64_t step = monotonic_inc ? 1 : -1;
  int64_t trav_idx = monotonic_inc ? 0 : row_cap - 1;
  int64_t trav_cnt = 0;
  uint32_t ref;
  const int64_t dict_count = dict_decoder_.get_dict_header()->count_;
  while (trav_cnt < row_cap) {
    row_id = row_ids[trav_idx];
    uint32_t *curr_ref = nullptr == datums ? &ref : &datums[trav_idx].pack_;
    if (except_table_pos == count || row_id < next_except_row_id) {
      *curr_ref = static_cast<uint32_t>(const_ref);
    } else if (row_id == next_except_row_id) {
      *curr_ref = reinterpret_cast<const uint8_t *>(meta_header_->payload_)[except_table_pos];
      ++except_table_pos;
      if (except_table_pos < count) {
        next_except_row_id = row_id_arr.at_(meta_header_->payload_ + count, except_table_pos);
      }
    } else {
      except_table_pos = row_id_arr.lower_bound_(
            meta_header_->payload_ + count, 0, count, row_id);
      next_except_row_id = row_id_arr.at_(meta_header_->payload_ + count, except_table_pos);
      if (except_table_pos == count || row_id != next_except_row_id) {
        *curr_ref= static_cast<uint32_t>(const_ref);
      } else {
        *curr_ref = reinterpret_cast<const uint8_t *>(meta_header_->payload_)[except_table_pos];
        ++except_table_pos;
        if (except_table_pos < count) {
          next_except_row_id = row_id_arr.at_(meta_header_->payload_ + count, except_table_pos);
        }
      }
    }
    if (*curr_ref >= dict_count) {
      null_count++;
    }
    ++trav_cnt;
    trav_idx += step;
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
