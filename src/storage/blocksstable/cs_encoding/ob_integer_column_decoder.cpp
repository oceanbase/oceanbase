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

#include "ob_integer_column_decoder.h"
#include "ob_integer_stream_decoder.h"
#include "ob_integer_stream_vector_decoder.h"
#include "ob_cs_decoding_util.h"
#include "storage/access/ob_pushdown_aggregate.h"
#include "storage/blocksstable/encoding/ob_raw_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObIntegerColumnDecoder::decode(const ObColumnCSDecoderCtx &ctx,
                                   const int32_t row_id,
                                   common::ObDatum &datum) const
{
  int ret = OB_SUCCESS;
  const ObIntegerColumnDecoderCtx &integer_ctx = ctx.integer_ctx_;
  ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
      [integer_ctx.ctx_->meta_.width_]                  /*val_store_width_V*/
      [ObRefStoreWidthV::NOT_REF]                       /*ref_store_width_V*/
      [get_width_tag_map()[integer_ctx.datum_len_]]     /*datum_width_V*/
      [integer_ctx.null_flag_]
      [integer_ctx.ctx_->meta_.is_decimal_int()];
  convert_func(integer_ctx, integer_ctx.data_, *integer_ctx.ctx_, nullptr, &row_id, 1, &datum);
  return ret;
}

int ObIntegerColumnDecoder::batch_decode(const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids, const int64_t row_cap, common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  const ObIntegerColumnDecoderCtx &integer_ctx = ctx.integer_ctx_;
  ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
      [integer_ctx.ctx_->meta_.width_]               /*val_store_width_V*/
      [ObRefStoreWidthV::NOT_REF]                    /*ref_store_width_V*/
      [get_width_tag_map()[integer_ctx.datum_len_]]  /*datum_width_V*/
      [integer_ctx.null_flag_]
      [integer_ctx.ctx_->meta_.is_decimal_int()];
  convert_func(integer_ctx, integer_ctx.data_, *integer_ctx.ctx_, nullptr, row_ids, row_cap, datums);
  return ret;
}

int ObIntegerColumnDecoder::decode_vector(
    const ObColumnCSDecoderCtx &ctx, ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  const ObIntegerColumnDecoderCtx &integer_ctx = ctx.integer_ctx_;
  if (OB_FAIL(ObIntegerStreamVecDecoder::decode_vector(integer_ctx, integer_ctx.data_,
      *integer_ctx.ctx_, nullptr, ObVecDecodeRefWidth::VDRW_NOT_REF, vector_ctx))) {
    LOG_WARN("fail to decode_vector", K(ret), K(integer_ctx), K(vector_ctx));
  }
  return ret;
}

int ObIntegerColumnDecoder::get_null_count(
    const ObColumnCSDecoderCtx &col_ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  null_count = 0;
  const ObIntegerColumnDecoderCtx &integer_ctx = col_ctx.integer_ctx_;
  const ObIntegerStreamMeta &stream_meta =integer_ctx.ctx_->meta_;
  const char *null_bitmap = nullptr;
  if (integer_ctx.has_null_bitmap()) {
    for (int64_t i = 0; i < row_cap; ++i) {
      if (ObCSDecodingUtil::test_bit(integer_ctx.null_bitmap_, row_ids[i])) {
        ++null_count;
      }
    }
  } else if (integer_ctx.is_null_replaced()) {
    const uint32_t width_size = stream_meta.get_uint_width_size();
    const uint64_t base_val = stream_meta.is_use_base() * stream_meta.base_value_;
    const uint64_t null_val = integer_ctx.null_replaced_value_ - base_val;
    uint64_t cur_val = 0;
    for (int64_t i = 0; i < row_cap; ++i) {
      MEMCPY(&cur_val, integer_ctx.data_ + row_ids[i] * width_size, width_size);
      if (cur_val == null_val) {
        ++null_count;
      }
    }
  } else {
    null_count = 0;
  }

  return ret;
}

int ObIntegerColumnDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const ObIntegerColumnDecoderCtx &integer_ctx = col_ctx.integer_ctx_;
  const int64_t row_cnt = pd_filter_info.count_;
  if (OB_UNLIKELY(row_cnt < 1 || row_cnt != result_bitmap.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cnt), K(result_bitmap.size()));
  } else {
    if (integer_ctx.has_null_bitmap()) {
      for (int64_t i = 0; OB_SUCC(ret) && (i < row_cnt); ++i) {
        const int64_t row_id = pd_filter_info.start_ + i;
        if (ObCSDecodingUtil::test_bit(integer_ctx.null_bitmap_, row_id)) {
          if (OB_FAIL(result_bitmap.set(i))) {
            LOG_WARN("fail to set bitmap", KR(ret), K(i), K(row_id));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
      switch (op_type) {
        case sql::WHITE_OP_NU:
        case sql::WHITE_OP_NN: {
          if (OB_FAIL(nu_nn_operator(integer_ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle nu_nn operator", KR(ret), K(pd_filter_info));
          }
          break;
        }
        case sql::WHITE_OP_EQ:
        case sql::WHITE_OP_NE:
        case sql::WHITE_OP_GT:
        case sql::WHITE_OP_GE:
        case sql::WHITE_OP_LT:
        case sql::WHITE_OP_LE: {
          if (OB_FAIL(comparison_operator(integer_ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle comparison operator", KR(ret), K(pd_filter_info));
          }
          break;
        }
        case sql::WHITE_OP_BT: {
          if (OB_FAIL(between_operator(integer_ctx,  parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle between operator", KR(ret), K(pd_filter_info));
          }
          break;
        }
        case sql::WHITE_OP_IN: {
          if (OB_FAIL(in_operator(integer_ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle in operator", KR(ret), K(pd_filter_info));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unexpected operation type", KR(ret), K(op_type));
        }
      }
    }
    LOG_TRACE("integer white filter pushdown", K(ret), K(integer_ctx),
        K(filter.get_op_type()), K(pd_filter_info), K(result_bitmap.popcnt()));
  }
  return ret;
}

int ObIntegerColumnDecoder::nu_nn_operator(
    const ObIntegerColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t row_start = pd_filter_info.start_;
  const int64_t row_count = pd_filter_info.count_;
  const ObIntegerStreamMeta stream_meta = ctx.ctx_->meta_;
  if (ctx.is_null_replaced()) {
    // transform 'x is nullptr' to 'x == null_replaced_value'
    const uint64_t base_value = stream_meta.is_use_base() * stream_meta.base_value_;
    const uint64_t filter_val = ctx.null_replaced_value_ - base_value;
    const uint8_t store_width_tag = stream_meta.get_width_tag();

    raw_compare_function cmp_func = RawCompareFunctionFactory::instance().get_cmp_function(
                                    false/*is_signed_data*/, store_width_tag, sql::WHITE_OP_EQ);
    if (OB_ISNULL(cmp_func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr compare function", KR(ret), K(store_width_tag));
    } else {
      cmp_func(reinterpret_cast<const unsigned char *>(ctx.data_), filter_val, result_bitmap.get_data(),
              row_start, row_start + row_count);
    }
  }

  if (OB_SUCC(ret) && (sql::WHITE_OP_NN == filter.get_op_type())) {
    if (OB_FAIL(result_bitmap.bit_not())) {
      LOG_WARN("fail to execute bit_not", KR(ret));
    }
  }
  return ret;
}

int ObIntegerColumnDecoder::comparison_operator(
    const ObIntegerColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datums_cnt = 0;
  common::ObObjMeta filter_val_meta;
  if (OB_UNLIKELY((datums_cnt = filter.get_datums().count()) != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datums_cnt));
  } else if (OB_FAIL(filter.get_filter_node().get_filter_val_meta(filter_val_meta))) {
    LOG_WARN("Fail to find datum meta", K(ret), K(filter));
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    const ObObjType filter_val_type = filter_val_meta.get_type();
    bool is_col_signed = false;
    bool is_filter_signed = false;
    uint64_t filter_val = 0;
    int64_t filter_val_size = 0;
    const ObObjType store_col_type = ctx.col_header_->get_store_obj_type();
    bool can_convert = ObCSDecodingUtil::can_convert_to_integer(store_col_type, is_col_signed);
    if (can_convert && ObCSDecodingUtil::check_datum_not_over_8bytes(
        filter_val_type, filter.get_datums().at(0), is_filter_signed, filter_val, filter_val_size)) {
      if (OB_FAIL(tranverse_integer_comparison_op(ctx, op_type, is_col_signed, filter_val, filter_val_size,
          is_filter_signed, pd_filter_info.start_, pd_filter_info.count_, parent, result_bitmap))) {
        LOG_WARN("fail to tranverse integer comparison op", KR(ret), K(ctx), K(op_type),
            K(filter_val), K(filter_val_size), K(is_col_signed));
      }
    } else {
      ObDatumCmpFuncType type_cmp_func = filter.cmp_func_;
      ObGetFilterCmpRetFunc get_cmp_ret = get_filter_cmp_ret_func(op_type);
      ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> eval =
      [&] (const ObDatum &cur_datum, const int64_t idx)
      {
	      int tmp_ret = OB_SUCCESS;
        int cmp_ret = 0;
        if (OB_TMP_FAIL(type_cmp_func(cur_datum, filter.get_datums().at(0), cmp_ret))) {
          LOG_WARN("fail to compare datums", K(tmp_ret), K(cur_datum), K(filter.get_datums()));
        } else if (get_cmp_ret(cmp_ret)) {
          if (OB_TMP_FAIL(result_bitmap.set(idx))) {
            LOG_WARN("fail to set result bitmap", KR(ret), K(idx));
          }
        }
        return tmp_ret;
      };
      if (OB_FAIL(tranverse_datum_all_op(ctx, pd_filter_info, result_bitmap, eval))) {
        LOG_WARN("fail to traverse_datum in cmp_op", KR(ret), K(ctx));
      }
    }
  }
  return ret;
}

int ObIntegerColumnDecoder::tranverse_integer_comparison_op(
    const ObIntegerColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterOperatorType &ori_op_type,
    const bool is_col_signed,
    const uint64_t filter_val,
    const int64_t filter_val_size,
    const bool is_filter_signed,
    const int64_t row_start,
    const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  sql::ObWhiteFilterOperatorType op_type = ori_op_type;
  const bool use_null_replace_val = ctx.is_null_replaced();
  const bool exist_null_bitmap = ctx.has_null_bitmap();
  const bool has_no_null = ctx.has_no_null();
  const ObIntegerStreamMeta stream_meta = ctx.ctx_->meta_;
  const uint64_t base_value = stream_meta.is_use_base() * stream_meta.base_value_;
  const uint32_t store_width_size = stream_meta.get_uint_width_size();
  const uint8_t store_width_tag = stream_meta.get_width_tag();
  const ObObjType store_col_type = ctx.col_header_->get_store_obj_type();
  uint64_t filter_base_diff = 0;
  const bool is_less_than_base = ObCSDecodingUtil::is_less_than_with_diff(
      filter_val, filter_val_size, is_filter_signed,
      base_value, 8, is_col_signed, filter_base_diff);
  bool is_exceed_range = false;
  bool need_filter_null = false;

  if (is_less_than_base) {
    // if filter_val less than base, it must be less than all row value
    if (sql::WHITE_OP_NE == op_type || sql::WHITE_OP_GT == op_type || sql::WHITE_OP_GE == op_type) {
      if (has_no_null) {
        result_bitmap.reuse(true/*is_all_true*/);
      }
      need_filter_null = !has_no_null;
    } else if (sql::WHITE_OP_EQ == op_type || sql::WHITE_OP_LT == op_type || sql::WHITE_OP_LE == op_type) {
      // whether nullptr exist or not, row_value won't <= or < or == 'filter', thus ALL_FALSE
      result_bitmap.reuse();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operator type", KR(ret), K(op_type));
    }
  } else {
    is_exceed_range = ~INTEGER_MASK_TABLE[store_width_size] & filter_base_diff;
    // filter_val larger than RANGE_MAX_VALUE, it must larger than all row value
    if (is_exceed_range) {
      if (sql::WHITE_OP_EQ == op_type || sql::WHITE_OP_GT == op_type || sql::WHITE_OP_GE == op_type) {
        // whether nullptr exist or not, row_value won't == 'filter', thus ALL_FALSE
        result_bitmap.reuse();
      } else if (sql::WHITE_OP_NE == op_type || sql::WHITE_OP_LT == op_type || sql::WHITE_OP_LE == op_type) {
        if (has_no_null) {
          // row_value != 'filter' AND not exist null, thus ALL_TRUE
          result_bitmap.reuse(true/*is_all_true*/);
        }
        need_filter_null = !has_no_null;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected operator type", KR(ret), K(op_type));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_less_than_base || is_exceed_range) {
    if (need_filter_null) {
      if (exist_null_bitmap && OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("fail to exe bit not", KR(ret));
      } else if (use_null_replace_val) {
        op_type = sql::WHITE_OP_NE;
        const uint64_t null_replaced_val_base_diff = ctx.null_replaced_value_ - base_value;
        raw_compare_function cmp_func = RawCompareFunctionFactory::instance().get_cmp_function(
                                        false/*is_signed_data*/, store_width_tag, op_type);
        cmp_func(reinterpret_cast<const unsigned char *>(ctx.data_), null_replaced_val_base_diff,
            result_bitmap.get_data(), row_start, row_start + row_count);
      }
    }
  } else {
    if (use_null_replace_val) {
      raw_compare_function_with_null cmp_func =
          RawCompareFunctionFactory::instance().get_cs_cmp_function_with_null(store_width_tag, op_type);
      const uint64_t null_replaced_val_base_diff = ctx.null_replaced_value_ - base_value;
      cmp_func(reinterpret_cast<const unsigned char *>(ctx.data_), filter_base_diff,
          null_replaced_val_base_diff, result_bitmap.get_data(), row_start, row_start + row_count);
    } else if (exist_null_bitmap) {
      if (OB_FAIL(ObCSFilterFunctionFactory::instance().integer_compare_tranverse(ctx.data_,
          store_width_size, filter_base_diff, row_start, row_count, true, op_type, parent, result_bitmap))) {
        LOG_WARN("fail to handle integer bt tranverse", KR(ret), K(filter_base_diff), K(store_width_size));
      }
    } else {
      raw_compare_function cmp_func = RawCompareFunctionFactory::instance().get_cmp_function(
                                      false/*is_signed_data*/, store_width_tag, op_type);
      if (OB_ISNULL(cmp_func)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr compare function", KR(ret), K(store_width_tag), K(op_type));
      } else {
        cmp_func(reinterpret_cast<const unsigned char *>(ctx.data_), filter_base_diff, result_bitmap.get_data(),
                row_start, row_start + row_count);
      }
    }
  }
  return ret;
}

int ObIntegerColumnDecoder::between_operator(
    const ObIntegerColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datums_cnt = 0;
  if (OB_UNLIKELY((datums_cnt = filter.get_datums().count()) != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datums_cnt));
  } else {
    bool is_signed_data = false;
    const ObObjType store_col_type = ctx.col_header_->get_store_obj_type();
    if (ObCSDecodingUtil::can_convert_to_integer(store_col_type, is_signed_data)) {
      if (OB_FAIL(tranverse_integer_between_op(ctx, parent, filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("fail to tranverse integer bt op", KR(ret), K(pd_filter_info));
      }
    } else {
      ObDatumCmpFuncType type_cmp_func = filter.cmp_func_;
      ObGetFilterCmpRetFunc get_le_cmp_ret = get_filter_cmp_ret_func(sql::WHITE_OP_LE);
      ObGetFilterCmpRetFunc get_ge_cmp_ret = get_filter_cmp_ret_func(sql::WHITE_OP_GE);
      ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> eval =
      [&] (const ObDatum &cur_datum, const int64_t idx)
      {
	      int tmp_ret = OB_SUCCESS;
        int ge_ret = 0;
        int le_ret = 0;
        if (OB_TMP_FAIL(type_cmp_func(cur_datum, filter.get_datums().at(0), ge_ret))) {
          LOG_WARN("fail to compare datums", K(tmp_ret), K(cur_datum), K(filter.get_datums()));
        } else if (!get_ge_cmp_ret(ge_ret)) {
          // skip
        } else if (OB_TMP_FAIL(type_cmp_func(cur_datum, filter.get_datums().at(1), le_ret))) {
          LOG_WARN("fail to compare datums", K(tmp_ret), K(cur_datum), K(filter.get_datums()));
        } else if (!get_le_cmp_ret(le_ret)) {
          // skip
        } else if (OB_TMP_FAIL(result_bitmap.set(idx))) {
          LOG_WARN("fail to set result bitmap", KR(tmp_ret), K(idx));
        }
        return tmp_ret;
      };
      if (OB_FAIL(tranverse_datum_all_op(ctx, pd_filter_info, result_bitmap, eval))) {
        LOG_WARN("fail to tranverse datum in bt_op", KR(ret), K(ctx));
      }
    }
  }
  return ret;
}

int ObIntegerColumnDecoder::tranverse_integer_between_op(
    const ObIntegerColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const ObIntegerStreamMeta &stream_meta = ctx.ctx_->meta_;
  const uint64_t base_value = stream_meta.is_use_base() * stream_meta.base_value_;
  const ObObjType store_col_type = ctx.col_header_->get_store_obj_type();
  const bool is_col_signed = ObCSDecodingUtil::is_signed_object_type(store_col_type);

  const sql::ObPushdownWhiteFilterNode &filter_node = filter.get_filter_node();
  ObObjType left_filter_type = filter_node.get_filter_arg_obj_type(0);
  ObObjType right_filter_type = filter_node.get_filter_arg_obj_type(1);
  bool is_left_signed = false;
  bool is_right_signed = false;
  int64_t left_filter_size = 0;
  int64_t right_filter_size = 0;
  uint64_t left_filter_val = 0;
  uint64_t right_filter_val = 0;
  // if filter dutum over 8bytes, it will be cast to the border value(int64_min/int64_max)
  ObCSDecodingUtil::check_datum_not_over_8bytes(left_filter_type, filter.get_datums().at(0), is_left_signed, left_filter_val, left_filter_size);
  ObCSDecodingUtil::check_datum_not_over_8bytes(right_filter_type, filter.get_datums().at(1), is_right_signed, right_filter_val, right_filter_size);
  // 1. left_filter > right_filter, the bt range is invalid.
  // 2. right_filter < dict_val_base, no value will between this range, cuz dict_val_base is minimum
  bool is_param_invalid = ObCSDecodingUtil::is_less_than(right_filter_val, right_filter_size, is_right_signed,
                                                         left_filter_val, left_filter_size, is_left_signed);
  uint64_t right_filter_base_diff = 0;
  bool is_right_less_than_base = ObCSDecodingUtil::is_less_than_with_diff(right_filter_val, right_filter_size,
                                  is_right_signed, base_value, 8, is_col_signed, right_filter_base_diff);
  if (is_param_invalid || is_right_less_than_base) {
    result_bitmap.reuse();
  } else {
    const uint32_t store_width_size = stream_meta.get_uint_width_size();
    const int64_t row_cnt = pd_filter_info.count_;
    const int64_t row_start = pd_filter_info.start_;

    bool need_tranverse = true;
    uint64_t left_filter_base_diff = 0;
    bool is_left_less_than_base = ObCSDecodingUtil::is_less_than_with_diff(left_filter_val, left_filter_size,
                                  is_left_signed, base_value, 8, is_col_signed, left_filter_base_diff);
    if (is_left_less_than_base) {
      left_filter_base_diff = 0; // ori_left < base, let 'left = 0'
    } else if (~INTEGER_MASK_TABLE[store_width_size] & left_filter_base_diff) {
      need_tranverse = false; // left > MAX_RANGE, no value will match
      result_bitmap.reuse();
    }

    if (need_tranverse) {
      if (~INTEGER_MASK_TABLE[store_width_size] & right_filter_base_diff) {
        right_filter_base_diff = INTEGER_MASK_TABLE[store_width_size]; // right > MAX_RANGE, right = MAX_RANGE.
      }
      uint64_t filter_vals[] = {left_filter_base_diff, right_filter_base_diff};

      if (ctx.is_null_replaced()) {
        const uint64_t null_replace_val = ctx.null_replaced_value_ - base_value;
        if (OB_FAIL(ObCSFilterFunctionFactory::instance().integer_bt_tranverse_with_null(ctx.data_, store_width_size,
            filter_vals, null_replace_val, row_start, row_cnt, parent, result_bitmap))) {
          LOG_WARN("fail to exe integer bt tranverse with null", KR(ret), K(store_width_size));
        }
      } else {
        const bool exist_null_bitmap = ctx.has_null_bitmap();
        if (OB_FAIL(ObCSFilterFunctionFactory::instance().integer_bt_tranverse(ctx.data_, store_width_size,
            filter_vals, row_start, row_cnt, exist_null_bitmap, parent, result_bitmap))) {
          LOG_WARN("fail to exe integer bt tranverse", KR(ret), K(exist_null_bitmap), K(store_width_size));
        }
      }
    }
  }
  return ret;
}

int ObIntegerColumnDecoder::in_operator(
    const ObIntegerColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datum_cnt = 0;
  if (OB_UNLIKELY((datum_cnt = (filter.get_datums().count())) < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datum_cnt));
  } else {
    bool is_col_signed = false;
    const ObObjType store_col_type = ctx.col_header_->get_store_obj_type();
    if (ObCSDecodingUtil::can_convert_to_integer(store_col_type, is_col_signed)) {
      const int64_t MAX_DATUM_CNT = 200;
      if (datum_cnt <= MAX_DATUM_CNT) {
        bool filter_vals_valid[datum_cnt];
        uint64_t filter_vals[datum_cnt];
        if (OB_FAIL(tranverse_integer_in_op(ctx, filter_vals_valid, filter_vals, parent, filter, pd_filter_info, result_bitmap))) {
          LOG_WARN("fail to tranverse integer in op", KR(ret), K(pd_filter_info), K(datum_cnt));
        }
      } else {
        bool *filter_vals_valid = static_cast<bool *>(ctx.allocator_->alloc(datum_cnt *(1 + sizeof(uint64_t))));
        uint64_t *filter_vals = reinterpret_cast<uint64_t*>(filter_vals_valid + datum_cnt);
        if (OB_FAIL(tranverse_integer_in_op(ctx, filter_vals_valid, filter_vals, parent, filter, pd_filter_info, result_bitmap))) {
          LOG_WARN("fail to tranverse integer in op", KR(ret), K(pd_filter_info), K(datum_cnt));
        }
      }
    } else {
      ObFilterInCmpType cmp_type = get_filter_in_cmp_type(pd_filter_info.count_, filter.get_datums().count(), false);
      ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> eval;
      if (cmp_type == ObFilterInCmpType::BINARY_SEARCH) {
        eval = [&] (const ObDatum &cur_datum, const int64_t idx)
        {
          int tmp_ret = OB_SUCCESS;
          bool is_exist = false;
          if (OB_TMP_FAIL(filter.exist_in_datum_array(cur_datum, is_exist))) {
            LOG_WARN("fail to check datum in array", KR(tmp_ret), K(cur_datum));
          } else if (is_exist) {
            if (OB_TMP_FAIL(result_bitmap.set(idx))) {
              LOG_WARN("fail to set result bitmap", KR(tmp_ret), K(idx));
            }
          }
          return tmp_ret;
        };
      } else if (cmp_type == ObFilterInCmpType::HASH_SEARCH) {
        eval = [&] (const ObDatum &cur_datum, const int64_t idx)
        {
          int tmp_ret = OB_SUCCESS;
          bool is_exist = false;
          if (OB_TMP_FAIL(filter.exist_in_datum_set(cur_datum, is_exist))) {
            LOG_WARN("fail to check datum in hashset", KR(tmp_ret), K(cur_datum));
          } else if (is_exist) {
            if (OB_TMP_FAIL(result_bitmap.set(idx))) {
              LOG_WARN("fail to set result bitmap", KR(tmp_ret), K(idx));
            }
          }
          return tmp_ret;
        };
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected filter in compare type", KR(ret), K(cmp_type));
      }

      if (OB_SUCC(ret) && OB_FAIL(tranverse_datum_all_op(ctx, pd_filter_info, result_bitmap, eval))) {
        LOG_WARN("fail to tranverse datum with base in in_op", KR(ret), K(ctx));
      }
    }
  }
  return ret;
}

int ObIntegerColumnDecoder::tranverse_integer_in_op(
    const ObIntegerColumnDecoderCtx &ctx,
    bool *filter_vals_valid,
    uint64_t *filter_vals,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datum_cnt = 0;
  if (OB_UNLIKELY((datum_cnt = (filter.get_datums().count())) < 1) || OB_ISNULL(filter_vals_valid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datum_cnt), KP(filter_vals_valid));
  } else {
    const int64_t row_cnt = pd_filter_info.count_;
    const int64_t row_start = pd_filter_info.start_;
    const ObIntegerStreamMeta &stream_meta = ctx.ctx_->meta_;
    const uint64_t base_value = stream_meta.is_use_base() * stream_meta.base_value_;
    const bool use_null_replace_val = ctx.is_null_replaced();
    const uint64_t null_replaced_val = ctx.null_replaced_value_ - base_value;
    const uint32_t store_width_size = stream_meta.get_uint_width_size();
    const ObObjType store_col_type = ctx.col_header_->get_store_obj_type();
    const bool is_col_signed = ObCSDecodingUtil::is_signed_object_type(store_col_type);

    MEMSET(filter_vals_valid, 1, datum_cnt);
    const sql::ObPushdownWhiteFilterNode &filter_node = filter.get_filter_node();
    for (int64_t i = 0; i < datum_cnt; ++i) {
      const ObObjType filter_type = filter_node.get_filter_arg_obj_type(i);
      uint64_t filter_val = 0;
      int64_t filter_val_size = 0;
      bool is_filter_signed = false;
      const common::ObDatum &filter_datum = filter.get_datums().at(i);
      if (ObCSDecodingUtil::check_datum_not_over_8bytes(filter_type, filter_datum, is_filter_signed, filter_val, filter_val_size)) {
        uint64_t filter_base_diff = 0;
        if (ObCSDecodingUtil::is_less_than_with_diff(filter_val, filter_val_size, is_filter_signed, base_value, 8,
            is_col_signed, filter_base_diff)) {
          filter_vals_valid[i] = false;
        } else if (~INTEGER_MASK_TABLE[store_width_size] & filter_base_diff) {
          filter_vals_valid[i] = false;
        } else {
          // filter_vals_valid[i] = ture;
          filter_vals[i] = filter_val;
        }
      } else {
        filter_vals_valid[i] = false;
      }
    }

    if (use_null_replace_val) {
      if (OB_FAIL(ObCSFilterFunctionFactory::instance().integer_in_tranverse_with_null(ctx.data_, store_width_size,
          null_replaced_val, filter_vals_valid, filter_vals, datum_cnt, row_start, row_cnt, base_value, parent, result_bitmap))) {
        LOG_WARN("fail to handle integer in tranverse with null", KR(ret), K(store_width_size));
      }
    } else {
      const bool exist_null_bitmap = ctx.has_null_bitmap();
      if (OB_FAIL(ObCSFilterFunctionFactory::instance().integer_in_tranverse(ctx.data_, store_width_size,
          filter_vals_valid, filter_vals, datum_cnt, row_start, row_cnt, base_value, exist_null_bitmap, parent, result_bitmap))) {
        LOG_WARN("fail to handle integer in tranverse", KR(ret), K(exist_null_bitmap), K(store_width_size));
      }
    }
  }
  return ret;
}

template<typename Operator>
int ObIntegerColumnDecoder::tranverse_datum_all_op(
    const ObIntegerColumnDecoderCtx &ctx,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    Operator const &eval)
{
  int ret = OB_SUCCESS;
  const int64_t row_start = pd_filter_info.start_;
  const int64_t row_count = pd_filter_info.count_;
  const ObIntegerStreamMeta &stream_meta = ctx.ctx_->meta_;
  const uint32_t store_width_size = stream_meta.get_uint_width_size();
  const bool exist_null_bitmap = ctx.has_null_bitmap();
  const bool use_null_replace = ctx.is_null_replaced();
  const uint64_t base = stream_meta.is_use_base() * stream_meta.base_value_;
  ObDatum cur_datum;
  // NU/NN will not reach here.
  if (use_null_replace) {
    const uint64_t null_replaced_val = ctx.null_replaced_value_;
    for (int64_t i = 0; (OB_SUCC(ret) && (i < row_count)); ++i) {
      const int64_t row_id = row_start + i;
      uint64_t cur_datum_val = 0;
      ENCODING_ADAPT_MEMCPY(&cur_datum_val, ctx.data_ + row_id * store_width_size, store_width_size);
      cur_datum_val += base;
      if (cur_datum_val == null_replaced_val) {
        // cur_datum is null, directly set result_bitmap
        if (OB_FAIL(result_bitmap.set(i, false))) {
          LOG_WARN("fail to set result bitmap", KR(ret), K(i));
        }
      } else {
        cur_datum.pack_ = ctx.datum_len_;
        cur_datum.ptr_ = reinterpret_cast<const char*>(&cur_datum_val);
        if (OB_FAIL(eval(cur_datum, i))) {
          LOG_WARN("fail to exe eval", KR(ret), K(i), K(cur_datum));
        }
      }
    }
  } else if (exist_null_bitmap) {
    for (int64_t i = 0; (OB_SUCC(ret) && i < row_count); ++i) {
      if (result_bitmap.test(i)) {
        // cur_datum is null, directly set result_bitmap
        if (OB_FAIL(result_bitmap.set(i, false))) {
          LOG_WARN("fail to set result bitmap", KR(ret), K(i));
        }
      } else {
        const int64_t row_id = row_start + i;
        cur_datum.pack_ = ctx.datum_len_;
        uint64_t cur_datum_val = 0;
        ENCODING_ADAPT_MEMCPY(&cur_datum_val, ctx.data_ + row_id * store_width_size, store_width_size);
        cur_datum_val += base;
        cur_datum.ptr_ = reinterpret_cast<const char*>(&cur_datum_val);
        if (OB_FAIL(eval(cur_datum, i))) {
          LOG_WARN("fail to exe eval", KR(ret), K(i), K(cur_datum));
        }
      }
    }
  } else {
    for (int64_t i = 0; (OB_SUCC(ret) && i < row_count); ++i) {
      const int64_t row_id = row_start + i;
      cur_datum.pack_ = ctx.datum_len_;
      uint64_t cur_datum_val = 0;
      ENCODING_ADAPT_MEMCPY(&cur_datum_val, ctx.data_ + row_id * store_width_size, store_width_size);
      cur_datum_val += base;
      cur_datum.ptr_ = reinterpret_cast<const char*>(&cur_datum_val);
      if (OB_FAIL(eval(cur_datum, i))) {
        LOG_WARN("fail to exe eval", KR(ret), K(i), K(cur_datum));
      }
    }
  }

  return ret;
}

int ObIntegerColumnDecoder::get_aggregate_result(
    const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObAggCell &agg_cell) const
{
  int ret = OB_SUCCESS;
  const ObIntegerColumnDecoderCtx &integer_ctx = ctx.integer_ctx_;
  bool is_col_signed = false;
  const ObObjType store_col_type = integer_ctx.col_header_->get_store_obj_type();
  const bool can_convert = ObCSDecodingUtil::can_convert_to_integer(store_col_type, is_col_signed);
  if (OB_UNLIKELY(nullptr == row_ids || row_cap <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to get aggregate result", KR(ret), KP(row_ids), K(row_cap));
  } else {
    const bool is_reverse = row_cap > 1 && row_ids[1] < row_ids[0];
    int64_t row_id_start = is_reverse ? row_ids[row_cap - 1] : row_ids[0];
    if (integer_ctx.has_null_bitmap()) {
      if (OB_FAIL(agg_cell.reserve_bitmap(row_cap))) {
        LOG_WARN("Failed to reserve memory for null bitmap", KR(ret));
      } else {
        ObBitmap &null_bitmap = agg_cell.get_bitmap();
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
          const int64_t row_id = is_reverse ? row_ids[row_cap - 1 - i] : row_ids[i];
          if (ObCSDecodingUtil::test_bit(integer_ctx.null_bitmap_, row_id) &&
              OB_FAIL(null_bitmap.set(i))) {
            LOG_WARN("Fail to set null bitmap", KR(ret), K(i), K(row_id));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(traverse_integer_in_agg(integer_ctx, is_col_signed, row_id_start, row_cap, agg_cell))){
      LOG_WARN("Failed to traverse integer to aggregate", KR(ret), K(integer_ctx), K(is_col_signed));
    }
  }
  return ret;
}

#define INT_DATUM_ASSIGN(datum, datum_width, value)               \
  switch (datum_width) {                                          \
    case ObIntegerStream::UW_1_BYTE : {                           \
      *(uint8_t*)datum.ptr_ = value;                              \
      datum.pack_ = sizeof(uint8_t);                              \
      break;                                                      \
    }                                                             \
    case ObIntegerStream::UW_2_BYTE : {                           \
      *(uint16_t*)datum.ptr_ = value;                             \
      datum.pack_ = sizeof(uint16_t);                             \
      break;                                                      \
    }                                                             \
    case ObIntegerStream::UW_4_BYTE : {                           \
    *(uint32_t*)datum.ptr_ = value;                               \
      datum.pack_ = sizeof(uint32_t);                             \
      break;                                                      \
    }                                                             \
    case ObIntegerStream::UW_8_BYTE : {                           \
    *(uint64_t*)datum.ptr_ = value;                               \
      datum.pack_ = sizeof(uint64_t);                             \
      break;                                                      \
    }                                                             \
    default : {                                                   \
      ret = OB_ERR_UNEXPECTED;                                    \
      LOG_WARN("Unexpected datum width", K(ret), K(datum_width)); \
    }                                                             \
  }

int ObIntegerColumnDecoder::traverse_integer_in_agg(
    const ObIntegerColumnDecoderCtx &ctx,
    const bool is_col_signed,
    const int64_t row_start,
    const int64_t row_count,
    storage::ObAggCell &agg_cell)
{
  int ret = OB_SUCCESS;
  const bool use_null_replace_val = ctx.is_null_replaced();
  const bool exist_null_bitmap = ctx.has_null_bitmap();
  const ObIntegerStreamMeta stream_meta = ctx.ctx_->meta_;
  const uint64_t base_value = stream_meta.is_use_base() * stream_meta.base_value_;
  const uint32_t store_width_tag = stream_meta.get_width_tag();

  bool is_less_than_base = false;
  bool is_exceed_range = false;
  if (!agg_cell.get_result_datum().is_null()) {
    const uint32_t store_width_size = stream_meta.get_uint_width_size();
    uint64_t agg_base_diff = 0;
    uint64_t agg_val = 0;
    int64_t agg_val_size = 0;
    bool is_agg_signed = false;
    ObCSDecodingUtil::get_datum_sign_and_size(agg_cell.get_obj_type(), agg_cell.get_result_datum(),
                                              is_agg_signed, agg_val, agg_val_size);
    is_less_than_base = ObCSDecodingUtil::is_less_than_with_diff(
      agg_val, agg_val_size, is_agg_signed,
      base_value, 8, is_col_signed, agg_base_diff);
    is_exceed_range = ~INTEGER_MASK_TABLE[store_width_size] & agg_base_diff;
  }

  if ((is_less_than_base && agg_cell.is_min_agg()) ||
      (is_exceed_range && agg_cell.is_max_agg())) {
    // if agg_val less than base, no need to update min
    // if agg_val larger than RANGE_MAX_VALUE, no need to update max
  } else {
    uint64_t result = 0;
    bool result_is_null = false;
    if (use_null_replace_val) {
      const uint64_t null_replaced_val_base_diff = ctx.null_replaced_value_ - base_value;
      raw_min_max_function_with_null min_max_func =
          RawAggFunctionFactory::instance().get_cs_min_max_function_with_null(store_width_tag, agg_cell.is_min_agg());
      if (OB_ISNULL(min_max_func)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null min_max function", KR(ret), K(store_width_tag), K(agg_cell));
      } else {
        min_max_func(reinterpret_cast<const unsigned char *>(ctx.data_), null_replaced_val_base_diff,
          row_start, row_start + row_count, result);
        result_is_null = result == null_replaced_val_base_diff;
      }
    } else if (exist_null_bitmap) {
      ObBitmap &null_bitmap = agg_cell.get_bitmap();
      if (null_bitmap.is_all_true()) {
        result_is_null = true;
      } else {
        raw_min_max_function_with_null_bitmap min_max_func =
            RawAggFunctionFactory::instance().get_cs_min_max_function_with_null_bitmap(store_width_tag, agg_cell.is_min_agg());
        if (OB_ISNULL(min_max_func)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null min_max function", KR(ret), K(store_width_tag), K(agg_cell));
        } else {
          min_max_func(reinterpret_cast<const unsigned char *>(ctx.data_), agg_cell.get_bitmap().get_data(),
            row_start, row_start + row_count, result);
        }
      }
    } else {
      raw_min_max_function min_max_func = RawAggFunctionFactory::instance().get_min_max_function(
                                      store_width_tag, agg_cell.is_min_agg());
      if (OB_ISNULL(min_max_func)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null min_max function", KR(ret), K(store_width_tag), K(agg_cell));
      } else {
        min_max_func(reinterpret_cast<const unsigned char *>(ctx.data_), row_start, row_start + row_count, result);
      }
    }
    if (OB_SUCC(ret) && !result_is_null) {
      result += base_value;
      ObStorageDatum storage_datum;
      if (ctx.ctx_->meta_.is_decimal_int()) {
        if (OB_FAIL(ObIntegerStreamDecoder::decimal_datum_assign(storage_datum, ctx.ctx_->meta_.precision_width_tag(), result, true))) {
          LOG_WARN("Failed to assign decimal datum", K(ret));
        }
      } else {
        INT_DATUM_ASSIGN(storage_datum, get_width_tag_map()[ctx.datum_len_], result);
      }
      if (OB_SUCC(ret) && OB_FAIL(agg_cell.eval(storage_datum))) {
        LOG_WARN("Failed to eval agg_cell", KR(ret), K(storage_datum), K(agg_cell));
      }
    }
  }
  return ret;
}
#undef INT_DATUM_ASSIGN

}
}
