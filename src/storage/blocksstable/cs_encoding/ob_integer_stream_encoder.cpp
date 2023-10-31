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

#include "ob_integer_stream_encoder.h"
#include "ob_cs_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace oceanbase::common;

template<int32_t store_len_V, bool has_null_V, bool has_base_V, bool is_decimal_V>
struct ConvertDatumToUint_T
{
  using StoreIntType = typename ObCSEncodingStoreTypeInference<store_len_V>::Type;
  static int process(
      ObIDatumIter &datum_iter,
      ObIntegerStreamEncoderCtx &ctx,
      const uint64_t null_replace_value,
      void *&int_arr,
      uint64_t &arr_count)
  {
    int ret = OB_SUCCESS;
    int64_t alloc_size = datum_iter.size() * sizeof(StoreIntType);
    StoreIntType *store_int_arr = nullptr;
    if (OB_ISNULL(store_int_arr = static_cast<StoreIntType*>(ctx.info_.allocator_->alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc", KR(ret), K(alloc_size));
    } else {
      int64_t i = 0;
      uint64_t curr = 0;
      const ObDatum *datum = nullptr;
      // TODO oushen, use simd gather instruntion to optimize later
      while (OB_SUCC(datum_iter.get_next(datum))) {
        uint64_t ele = 0;
        if (has_null_V && datum->is_null()) {
          // if not use null_replaced_value, null_replaced_value_ must be 0,
          // in this case, just use 0 to occupancy space
          ele = null_replace_value;
        } else if (is_decimal_V) {
          if (sizeof(int32_t) == datum->len_) {
            ele = datum->get_decimal_int32();
          } else {
            ele = datum->get_decimal_int64();
          }
        } else {
          ele = datum->get_uint64();
        }

        // base must be the first step
        if (has_base_V) {
          ele -= ctx.meta_.base_value_;
        }
        store_int_arr[i] = ele;
        i++;
      }

      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        arr_count = i;
        int_arr = store_int_arr;
      }
    }
    return ret;
  }
};

using ConvertDatumToUintFunc = int (*)(
    ObIDatumIter &datum_iter,
    ObIntegerStreamEncoderCtx &ctx,
    const uint64_t null_replace_value,
    void *&int_arr,
    uint64_t &arr_count);

static ObMultiDimArray_T<ConvertDatumToUintFunc, 4, 2, 2, 2> convert_datum_to_uint_funcs;

template <int32_t store_len_V, int32_t has_null_V, int32_t has_base_V, int32_t is_decimal_V>
struct ConvertDatumToUint_T_Init
{
  bool operator()()
  {
    convert_datum_to_uint_funcs[store_len_V][has_null_V][has_base_V][is_decimal_V]
      = &(ConvertDatumToUint_T<store_len_V, has_null_V, has_base_V, is_decimal_V>::process);
    return true;
  }
};

static bool convert_datum_to_uint_funcs_inited
    = ObNDArrayIniter<ConvertDatumToUint_T_Init, 4, 2, 2, 2>::apply();

int ObIntegerStreamEncoder::encode(
    ObIntegerStreamEncoderCtx &ctx,
    ObIDatumIter &datum_iter,
    ObMicroBufferWriter &writer)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx));
  } else {
    ctx_ = &ctx;
    uint64_t null_replace_value = 0;
    if (ctx.meta_.is_use_null_replace_value()) {
      null_replace_value = ctx.meta_.null_replaced_value_;
    } else {
      if (ctx.meta_.is_use_base()) { // make int small
        null_replace_value = ctx.meta_.base_value_;
      }
    }

    ConvertDatumToUintFunc convert_func = convert_datum_to_uint_funcs
      [ctx.meta_.width_][ctx.info_.has_null_datum_][ctx.meta_.is_use_base()][ctx.meta_.is_decimal_int()];

    void *int_arr = nullptr;
    uint64_t arr_count = 0;
    if (OB_FAIL(convert_func(datum_iter, ctx, null_replace_value, int_arr, arr_count))) {
      LOG_WARN("fail to convert to uint", KR(ret));
    } else {
      switch (ctx.meta_.get_width_tag()) {
        case ObIntegerStream::UintWidth::UW_1_BYTE : {
          if (OB_FAIL(inner_encode<uint8_t>(int_arr, arr_count, writer))) {
            LOG_WARN("fail to encode to uint8", KR(ret));
          }
          break;
        }
        case ObIntegerStream::UintWidth::UW_2_BYTE : {
          if (OB_FAIL(inner_encode<uint16_t>(int_arr, arr_count, writer))) {
            LOG_WARN("fail to encode to uint16", KR(ret));
          }
          break;
        }
        case ObIntegerStream::UintWidth::UW_4_BYTE : {
          if (OB_FAIL(inner_encode<uint32_t>(int_arr, arr_count, writer))) {
            LOG_WARN("fail to encode to uint32", KR(ret));
          }
          break;
        }
        case ObIntegerStream::UintWidth::UW_8_BYTE : {
          if (OB_FAIL(inner_encode<uint64_t>(int_arr, arr_count, writer))) {
            LOG_WARN("fail to encode to uint64", KR(ret));
          }
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected", KR(ret), KPC(ctx_));
        }
      }
    }
  }
  return ret;
}

int ObIntegerStreamEncoder::encode_stream_meta(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  char *buf = buf_writer.current();
  int64_t buf_len = buf_writer.remain_buffer_size();
  int64_t pos = 0;
  if (OB_FAIL(ctx_->meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize stream meta", K(ret), K(pos), KP(buf), K(buf_len));
  } else if (OB_FAIL(buf_writer.advance(pos))) {
    LOG_WARN("fail to advance", KR(ret), K(buf_writer), K(pos));
  }

  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
