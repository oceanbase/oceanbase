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

#include "ob_string_column_decoder.h"
#include "ob_string_stream_decoder.h"
#include "ob_integer_stream_decoder.h"
#include "ob_cs_encoding_util.h"
#include "ob_cs_decoding_util.h"
#include "ob_string_stream_vector_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

int ObStringColumnDecoder::decode(
  const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const
{
  int ret = OB_SUCCESS;
  const ObStringColumnDecoderCtx &string_ctx = ctx.string_ctx_;

  if (string_ctx.str_ctx_->meta_.is_fixed_len_string()) {
    ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
        [FIX_STRING_OFFSET_WIDTH_V]
        [ObRefStoreWidthV::NOT_REF]
        [string_ctx.null_flag_]
        [string_ctx.need_copy_];
    convert_func(string_ctx, string_ctx.str_data_, *string_ctx.str_ctx_,
        nullptr/*offset_data*/, nullptr/*ref_data*/, &row_id, 1, &datum);
  } else {
    ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
        [string_ctx.offset_ctx_->meta_.width_]
        [ObRefStoreWidthV::NOT_REF]
        [string_ctx.null_flag_]
        [string_ctx.need_copy_];
    convert_func(string_ctx, string_ctx.str_data_, *string_ctx.str_ctx_,
        string_ctx.offset_data_, nullptr/*ref_data*/, &row_id, 1, &datum);
  }

  return ret;
}

int ObStringColumnDecoder::batch_decode(const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids, const int64_t row_cap, common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  const ObStringColumnDecoderCtx &string_ctx = ctx.string_ctx_;
  if (string_ctx.str_ctx_->meta_.is_fixed_len_string()) {
    ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
        [FIX_STRING_OFFSET_WIDTH_V]
        [ObRefStoreWidthV::NOT_REF]
        [string_ctx.null_flag_]
        [string_ctx.need_copy_];
    convert_func(string_ctx, string_ctx.str_data_, *string_ctx.str_ctx_,
        nullptr/*offset_data*/, nullptr/*ref_data*/, row_ids, row_cap, datums);
  } else {
    ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
        [string_ctx.offset_ctx_->meta_.width_]
        [ObRefStoreWidthV::NOT_REF]
        [string_ctx.null_flag_]
        [string_ctx.need_copy_];
    convert_func(string_ctx, string_ctx.str_data_, *string_ctx.str_ctx_,
        string_ctx.offset_data_, nullptr/*ref_data*/, row_ids, row_cap, datums);
  }
  return ret;
}

int ObStringColumnDecoder::decode_vector(
    const ObColumnCSDecoderCtx &ctx, ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  const ObStringColumnDecoderCtx &string_ctx = ctx.string_ctx_;
  ObStringStreamVecDecoder::StrVecDecoderCtx vec_decoder_ctx(
    string_ctx.str_data_, string_ctx.str_ctx_, string_ctx.offset_data_, string_ctx.offset_ctx_, string_ctx.need_copy_);

  if (OB_FAIL(ObStringStreamVecDecoder::decode_vector(
    string_ctx, vec_decoder_ctx, nullptr, ObVecDecodeRefWidth::VDRW_NOT_REF, vector_ctx))) {
    LOG_WARN("fail to decode_vector", K(ret), K(vec_decoder_ctx), K(vector_ctx));
  }
  return ret;

  return ret;
}

int ObStringColumnDecoder::get_null_count(const ObColumnCSDecoderCtx &col_ctx,
    const int32_t *row_ids, const int64_t row_cap, int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_ids) || row_cap < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cap));
  } else {
    const ObStringColumnDecoderCtx &string_ctx = col_ctx.string_ctx_;
    null_count = 0;
    if (string_ctx.has_null_bitmap()) {
      for (int64_t i = 0; i < row_cap; ++i) {
        if (ObCSDecodingUtil::test_bit(string_ctx.null_bitmap_, row_ids[i])) {
          ++null_count;
        }
      }
    } else if (string_ctx.is_null_replaced()) {
      // must be not fixed length string
      const char *offset_buf = string_ctx.offset_data_;
      const uint32_t width_size = string_ctx.offset_ctx_->meta_.get_uint_width_size();
      uint64_t pre_offset = 0;
      uint64_t cur_offset = 0;
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        ENCODING_ADAPT_MEMCPY(&cur_offset, offset_buf + row_id * width_size, width_size);
        if (0 == row_id) { // handle first offset
          if (0 == cur_offset) {
            ++null_count;
          }
        } else {
          ENCODING_ADAPT_MEMCPY(&pre_offset, offset_buf + (row_id -1) * width_size, width_size);
          if (cur_offset == pre_offset) {
            ++null_count;
          }
        }
      }
    } else {
      null_count = 0;
    }
  }

  return ret;
}

//===============================filter tranverse all datum===================================//

template<typename T>
OB_INLINE int pad_datum(const ObStringColumnDecoderCtx &ctx, T &datum)
{
  return OB_ERR_UNEXPECTED;
}
template<>
OB_INLINE int pad_datum(const ObStringColumnDecoderCtx &ctx, ObStorageDatum &datum)
{
  return storage::pad_column(ctx.obj_meta_, ctx.col_param_->get_accuracy(), *ctx.allocator_, datum);
}

template<int32_t offset_width_V, int32_t null_flag_V, bool need_padding_V>
struct FilterTranverseDatum_T
{
  static int process(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const sql::ObWhiteFilterExecutor &filter,
    common::ObBitmap &result_bitmap,
    const ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> &op_handle)
  {
    int ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("impossible here", K(offset_width_V), K(null_flag_V), K(need_padding_V));
    return ret;
  }
};

template<int32_t offset_width_V, bool need_padding_V>
struct FilterTranverseDatum_T<offset_width_V, ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL, need_padding_V>
{
  static int process(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> &op_handle)
  {
    int ret = OB_SUCCESS;
    typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
    const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(ctx.offset_data_);
    const char *start = ctx.str_data_;
    int64_t row_id = 0;
    typename std::conditional<need_padding_V, ObStorageDatum, ObDatum>::type cur_datum;
    if (OB_UNLIKELY(!op_handle.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(op_handle));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      row_id = i + row_start;
      if (0 == row_id) {
        cur_datum.ptr_ = start;
        cur_datum.pack_ = offset_arr[0];
      } else {
        cur_datum.ptr_ = start + offset_arr[row_id - 1];
        cur_datum.pack_ = offset_arr[row_id] - offset_arr[row_id - 1];
      }

      if (need_padding_V && OB_FAIL(pad_datum(ctx, cur_datum))) {
        LOG_WARN("failed to pad datum", K(ret));
      } else if (OB_FAIL(op_handle(cur_datum, i))) {
        LOG_WARN("fail to handle op", KR(ret), K(i), K(ctx), K(cur_datum));
      }
    }
    return ret;
  }
};

template<int32_t offset_width_V,  bool need_padding_V>
struct FilterTranverseDatum_T<offset_width_V, ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP, need_padding_V>
{
  static int process(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> &op_handle)
  {
    int ret = OB_SUCCESS;
    typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
    const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(ctx.offset_data_);
    const char *start = ctx.str_data_;
    int64_t row_id = 0;
    typename std::conditional<need_padding_V, ObStorageDatum, ObDatum>::type cur_datum;
    if (OB_UNLIKELY(!op_handle.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(op_handle));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
      row_id = i + row_start;
      if (ObCSDecodingUtil::test_bit(ctx.null_bitmap_, row_id)) {
        cur_datum.set_null();
      } else if (0 == row_id) {
        cur_datum.ptr_ = start;
        cur_datum.pack_ = offset_arr[0];
      } else {
        cur_datum.ptr_ = start + offset_arr[row_id - 1];
        cur_datum.pack_ = offset_arr[row_id] - offset_arr[row_id - 1];
      }
      if (need_padding_V && OB_FAIL(pad_datum(ctx, cur_datum))) {
        LOG_WARN("failed to pad datum", K(ret));
      } else if (OB_FAIL(op_handle(cur_datum, i))) {
        LOG_WARN("fail to handle op", KR(ret), K(i), K(ctx), K(cur_datum));
      }
    }
    return ret;
  }
};

template<int32_t offset_width_V, bool need_padding_V>
struct FilterTranverseDatum_T<offset_width_V, ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED, need_padding_V>
{
  static int process(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> &op_handle)
  {
    int ret = OB_SUCCESS;
    typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
    const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(ctx.offset_data_);
    const char *start = ctx.str_data_;
    const char *cur_start = nullptr;
    int64_t str_len = 0;
    int64_t row_id = 0;
    typename std::conditional<need_padding_V, ObStorageDatum, ObDatum>::type cur_datum;

    if (OB_UNLIKELY(!op_handle.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(op_handle));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      row_id = i + row_start;
      if (0 == row_id) {
        cur_start = start;
        str_len = offset_arr[0];
      } else {
        cur_start = start + offset_arr[row_id - 1];
        str_len = offset_arr[row_id] - offset_arr[row_id - 1];
      }
      if (0 == str_len) { // use zero length as null
        cur_datum.set_null();
      } else {
        cur_datum.ptr_ = cur_start;
        cur_datum.pack_ = str_len;
      }
      if (need_padding_V && OB_FAIL(pad_datum(ctx, cur_datum))) {
        LOG_WARN("failed to pad datum", K(ret));
      } else if (OB_FAIL(op_handle(cur_datum, i))) {
        LOG_WARN("fail to handle op", KR(ret), K(i), K(ctx), K(cur_datum));
      }
    }
    return ret;
  }
};

// partial specialization for FilterTranverseDatum_T
template<bool need_padding_V>
struct FilterTranverseDatum_T<FIX_STRING_OFFSET_WIDTH_V,
                              ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL,
                              need_padding_V>
{
  static int process(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> &op_handle)
  {
    int ret = OB_SUCCESS;
    const char *start = ctx.str_data_;
    const char *cur_start = nullptr;
    int64_t str_len = ctx.str_ctx_->meta_.fixed_str_len_;
    int64_t row_id = 0;
    typename std::conditional<need_padding_V, ObStorageDatum, ObDatum>::type cur_datum;

    if (OB_UNLIKELY(!op_handle.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(op_handle));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
      row_id = i + row_start;
      cur_start = start + row_id * str_len;
      cur_datum.ptr_ = cur_start;
      cur_datum.pack_ = str_len;

      if (need_padding_V && OB_FAIL(pad_datum(ctx, cur_datum))) {
        LOG_WARN("failed to pad datum", K(ret));
      } else if (OB_FAIL(op_handle(cur_datum, i))) {
        LOG_WARN("fail to handle op", KR(ret), K(i), K(ctx), K(cur_datum));
      }
    }
    return ret;
  }
};

template<bool need_padding_V>
struct FilterTranverseDatum_T<FIX_STRING_OFFSET_WIDTH_V,
                              ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP,
                              need_padding_V>
{
  static int process(
      const ObStringColumnDecoderCtx &ctx,
      const int64_t row_start,
      const int64_t row_count,
      const ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> &op_handle)
  {
    int ret = OB_SUCCESS;
    const char *start = ctx.str_data_;
    const int64_t str_len = ctx.str_ctx_->meta_.fixed_str_len_;
    int64_t row_id = 0;
    typename std::conditional<need_padding_V, ObStorageDatum, ObDatum>::type cur_datum;
    if (OB_UNLIKELY(!op_handle.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(op_handle));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
      row_id = i + row_start;
      if (ObCSDecodingUtil::test_bit(ctx.null_bitmap_, row_id)) {
        cur_datum.set_null();
      } else {
        cur_datum.ptr_ = start + row_id * str_len;
        cur_datum.pack_ = str_len;
      }
      if (need_padding_V && OB_FAIL(pad_datum(ctx, cur_datum))) {
        LOG_WARN("failed to pad datum", K(ret));
      } else if (OB_FAIL(op_handle(cur_datum, i))) {
        LOG_WARN("fail to handle op", KR(ret), K(i), K(ctx), K(cur_datum));
      }
    }
    return ret;
  }
};

typedef int (*FilterTranverseDatum) (
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> &op_handle);
static ObMultiDimArray_T<FilterTranverseDatum, 5, 3, 2> filter_tranverse_datum_;

template <int32_t offset_width_V, int32_t null_flag_V, int32_t need_padding_V>
struct FilterTranverseDatumInit
{
  bool operator()()
  {
    filter_tranverse_datum_[offset_width_V][null_flag_V][need_padding_V]
      = &(FilterTranverseDatum_T<offset_width_V, null_flag_V, need_padding_V>::process);
    return true;
  }
};
static bool filter_tranverse_datum_inited = ObNDArrayIniter<FilterTranverseDatumInit, 5, 3, 2>::apply();


int ObStringColumnDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const ObStringColumnDecoderCtx &string_ctx = col_ctx.string_ctx_;
  const int64_t row_cnt = pd_filter_info.count_;
  if (OB_UNLIKELY(row_cnt < 1 || row_cnt != result_bitmap.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cnt), K(string_ctx), K(result_bitmap.size()));
  } else {
    const int64_t row_start = pd_filter_info.start_;
    const int64_t row_count = pd_filter_info.count_;
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    switch(op_type) {
      case sql::WHITE_OP_NU:
      case sql::WHITE_OP_NN: {
        if (OB_FAIL(nunn_operator(string_ctx, row_start, row_count, parent, filter, result_bitmap))) {
          LOG_WARN("fail to handle nunn operator", KR(ret), K(pd_filter_info), K(col_ctx));
        }
        break;
      }
      case sql::WHITE_OP_EQ:
      case sql::WHITE_OP_NE:
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE: {
        if (OB_UNLIKELY(filter.get_datums().count() != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(filter));
        } else if (OB_FAIL(comparison_operator(string_ctx, row_start, row_count, parent, filter,
            result_bitmap))) {
          LOG_WARN("fail to handle comparison operator", KR(ret), K(pd_filter_info), K(col_ctx));
        }
        break;
      }
      case sql::WHITE_OP_IN: {
        if (OB_UNLIKELY(filter.get_datums().count() < 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K(filter));
        } else if (OB_FAIL(in_operator(string_ctx, row_start, row_count, parent, filter, result_bitmap))) {
          LOG_WARN("fail to handle in operator", KR(ret), K(pd_filter_info), K(col_ctx));
        }
        break;
      }
      case sql::WHITE_OP_BT: {
        if (OB_FAIL(bt_operator(string_ctx, row_start, row_count, parent, filter, result_bitmap))) {
          LOG_WARN("fail to handle bt operator", KR(ret), K(pd_filter_info), K(col_ctx));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Unexpected operation type", KR(ret), K(op_type));
      }
    }
    LOG_TRACE("string white filter pushdown", K(ret), "string_ctx", col_ctx.string_ctx_,
        K(filter.get_op_type()), K(pd_filter_info), K(result_bitmap.popcnt()));
  }
  return ret;
}

int ObStringColumnDecoder::nunn_operator(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();

  if (!ctx.has_no_null()) {
    const bool is_fixed_len_str = ctx.str_ctx_->meta_.is_fixed_len_string();
    const bool need_padding = (ctx.obj_meta_.is_fixed_len_char_type() && nullptr != ctx.col_param_);

    ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> op_handle =
    [&] (const ObDatum &cur_datum, const int64_t idx)
    {
      int tmp_ret = OB_SUCCESS;
      if (cur_datum.is_null()) {
        if (OB_TMP_FAIL(result_bitmap.set(idx))) {
          LOG_WARN("fail to set", KR(tmp_ret), K(idx), K(row_start));
        }
      }
      return tmp_ret;
    };

    if (is_fixed_len_str) {
      ret = filter_tranverse_datum_[FIX_STRING_OFFSET_WIDTH_V]
                                   [ctx.null_flag_]
                                   [need_padding] (ctx, row_start, row_count, op_handle);
    } else {
      ret = filter_tranverse_datum_[ctx.offset_ctx_->meta_.width_]
                                   [ctx.null_flag_]
                                   [need_padding] (ctx, row_start, row_count, op_handle);
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("nunn_operator failed", KR(ret), K(ctx));
  } else if (sql::WHITE_OP_NN == op_type) {
    if (OB_FAIL(result_bitmap.bit_not())) {
      LOG_WARN("fail to execute bit not", KR(ret), K(ctx));
    }
  }

  return ret;
}

int ObStringColumnDecoder::comparison_operator(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  const ObDatum &filter_datum = filter.get_datums().at(0);
  const bool is_fixed_len_str = ctx.str_ctx_->meta_.is_fixed_len_string();
  const common::ObCmpOp &cmp_op = sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type];
  const bool need_padding = (ctx.obj_meta_.is_fixed_len_char_type() && nullptr != ctx.col_param_);

  ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> op_handle =
  [&] (const ObDatum &cur_datum, const int64_t idx)
  {
    int tmp_ret = OB_SUCCESS;
    bool cmp_ret = false;
    if (OB_TMP_FAIL(compare_datum(cur_datum, filter_datum, filter.cmp_func_, cmp_op, cmp_ret))) {
      LOG_WARN("Failed to compare datum", K(tmp_ret), K(cur_datum), K(filter_datum), K(cmp_op));
    } else if ((!cur_datum.is_null()) && cmp_ret) {
      if (OB_TMP_FAIL(result_bitmap.set(idx))) {
        LOG_WARN("fail to set", KR(tmp_ret), K(idx), K(row_start));
      }
    }
    return tmp_ret;
  };

  if (is_fixed_len_str) {
    ret = filter_tranverse_datum_[FIX_STRING_OFFSET_WIDTH_V]
                                 [ctx.null_flag_]
                                 [need_padding] (ctx, row_start, row_count, op_handle);
  } else {
    ret = filter_tranverse_datum_[ctx.offset_ctx_->meta_.width_]
                                 [ctx.null_flag_]
                                 [need_padding] (ctx, row_start, row_count, op_handle);
  }
  return ret;
}

int ObStringColumnDecoder::in_operator(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  const bool need_padding = (ctx.obj_meta_.is_fixed_len_char_type() && nullptr != ctx.col_param_);
  const bool is_fixed_len_str = ctx.str_ctx_->meta_.is_fixed_len_string();

  ObFilterInCmpType cmp_type = get_filter_in_cmp_type(row_count, filter.get_datums().count(), false);
  ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> op_handle;
  if (cmp_type == ObFilterInCmpType::BINARY_SEARCH) {
    op_handle = [&] (const ObDatum &cur_datum, const int64_t idx)
    {
      int tmp_ret = OB_SUCCESS;
      bool is_exist = false;
      if (cur_datum.is_null()) {
      } else if (OB_TMP_FAIL(filter.exist_in_datum_array(cur_datum, is_exist))) {
        LOG_WARN("fail to check datum in array", KR(tmp_ret), K(cur_datum));
      } else if (is_exist) {
        if (OB_TMP_FAIL(result_bitmap.set(idx))) {
          LOG_WARN("fail to set", KR(tmp_ret), K(idx), K(row_start));
        }
      }
      return tmp_ret;
    };
  } else if (cmp_type == ObFilterInCmpType::HASH_SEARCH) {
    op_handle = [&] (const ObDatum &cur_datum, const int64_t idx)
    {
      int tmp_ret = OB_SUCCESS;
      bool is_exist = false;
      if (cur_datum.is_null()) {
      } else if (OB_TMP_FAIL(filter.exist_in_datum_set(cur_datum, is_exist))) {
        LOG_WARN("fail to check datum in hashset", KR(tmp_ret), K(cur_datum));
      } else if (is_exist) {
        if (OB_TMP_FAIL(result_bitmap.set(idx))) {
          LOG_WARN("fail to set", KR(tmp_ret), K(idx), K(row_start));
        }
      }
      return tmp_ret;
    };
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected filter in compare type", KR(ret), K(cmp_type));
  }

  if (OB_FAIL(ret)) {
  } else if (is_fixed_len_str) {
    ret = filter_tranverse_datum_[FIX_STRING_OFFSET_WIDTH_V]
                                [ctx.null_flag_]
                                [need_padding] (ctx, row_start, row_count, op_handle);
  } else {
    ret = filter_tranverse_datum_[ctx.offset_ctx_->meta_.width_]
                                [ctx.null_flag_]
                                [need_padding] (ctx, row_start, row_count, op_handle);
  }
  return ret;
}

int ObStringColumnDecoder::bt_operator(
    const ObStringColumnDecoderCtx &ctx,
    const int64_t row_start,
    const int64_t row_count,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const ObDatum &left_ref_datum = filter.get_datums().at(0);
  const ObDatum &right_ref_datum = filter.get_datums().at(1);
  const bool need_padding = (ctx.obj_meta_.is_fixed_len_char_type() && nullptr != ctx.col_param_);
  const bool is_fixed_len_str = ctx.str_ctx_->meta_.is_fixed_len_string();

  ObFunction<int(const ObDatum &cur_datum, const int64_t idx)> op_handle =
  [&] (const ObDatum &cur_datum, const int64_t idx)
  {
    int tmp_ret = OB_SUCCESS;
    int left_cmp_ret = 0;
    int right_cmp_ret = 0;
    if (cur_datum.is_null()) {
      // skip
    } else if (OB_TMP_FAIL(filter.cmp_func_(cur_datum, left_ref_datum, left_cmp_ret))) {
      LOG_WARN("fail to compare datums", KR(tmp_ret), K(idx), K(cur_datum), K(left_ref_datum));
    } else if (left_cmp_ret < 0) {
      // skip
    } else if (OB_TMP_FAIL(filter.cmp_func_(cur_datum, right_ref_datum, right_cmp_ret))) {
      LOG_WARN("fail to compare datums", KR(tmp_ret), K(idx), K(cur_datum), K(right_ref_datum));
    } else if (right_cmp_ret > 0) {
      // skip
    } else if (OB_TMP_FAIL(result_bitmap.set(idx))) {
      LOG_WARN("fail to set", KR(tmp_ret), K(idx), K(row_start));
    }
    return tmp_ret;
  };
  if (is_fixed_len_str) {
    ret = filter_tranverse_datum_[FIX_STRING_OFFSET_WIDTH_V]
                                 [ctx.null_flag_]
                                 [need_padding] (ctx, row_start, row_count, op_handle);
  } else {
    ret = filter_tranverse_datum_[ctx.offset_ctx_->meta_.width_]
                                 [ctx.null_flag_]
                                 [need_padding] (ctx, row_start, row_count, op_handle);
  }
  return ret;
}

}
}
