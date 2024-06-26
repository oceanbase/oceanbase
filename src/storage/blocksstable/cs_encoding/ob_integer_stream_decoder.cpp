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

#include "ob_integer_stream_decoder.h"
#include "ob_cs_decoding_util.h"
#include "lib/wide_integer/ob_wide_integer.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace oceanbase::common;

//=================================ConvertUintToDatum_T=====================================//
ObMultiDimArray_T<ConvertUnitToDatumFunc,
                  4,
                  ObRefStoreWidthV::MAX_WIDTH_V,
                  4,
                  ObBaseColumnDecoderCtx::ObNullFlag::MAX,
                  2> convert_uint_to_datum_funcs;
template< int32_t store_len_V,
         int32_t ref_store_width_V,
         int32_t datum_width_V,
         int32_t null_flag_V,
         bool is_decimal_V>
struct ConvertUintToDatum_T
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    int ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("impossible here", K(store_len_V), K(ref_store_width_V), K(datum_width_V),
        K(null_flag_V), K(is_decimal_V), K(base_col_ctx), K(ctx));
    ::ob_abort();
  }
};

template<int32_t store_len_V, int32_t ref_store_width_V, int32_t datum_width_V, bool is_decimal_V>
struct ConvertUintToDatum_T<store_len_V,
                            ref_store_width_V,
                            datum_width_V,
                            ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF,
                            is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_store_width_V>::Type RefStoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<datum_width_V>::Type DatumIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefStoreIntType *ref_arr = reinterpret_cast<const RefStoreIntType*>(ref_data);

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < row_cap; i++) {
      common::ObDatum &datum = datums[i];
      ref = ref_arr[row_ids[i]];
      if (ref == base_col_ctx.null_replaced_ref_) {
        datum.set_null();
        // must enter next loop, row_id maybe out of range of store_uint_arr
      } else {
        value = store_uint_arr[ref] + base;
        if (is_decimal_V) {
          ObIntegerStreamDecoder::decimal_datum_assign(datum, ctx.meta_.precision_width_tag(), value, false);
        } else {
          *(DatumIntType*)(datum.ptr_) = value;
          // ignore flag_ in ObDatum, all flags not used in cs encoding
          datum.pack_ = sizeof(DatumIntType);
        }
      }
    }
  }
};


template<int32_t store_len_V, int32_t ref_store_width_V, int32_t datum_width_V, bool is_decimal_V>
struct ConvertUintToDatum_T<store_len_V,
                            ref_store_width_V,
                            datum_width_V,
                            ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL,
                            is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_store_width_V>::Type RefStoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<datum_width_V>::Type DatumIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType *>(data);
    const RefStoreIntType *ref_arr = reinterpret_cast<const RefStoreIntType *>(ref_data);

    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < row_cap; i++) {
      common::ObDatum &datum = datums[i];
      value = store_uint_arr[ref_arr[row_ids[i]]] + base;
      if (is_decimal_V) {
        ObIntegerStreamDecoder::decimal_datum_assign(datum, ctx.meta_.precision_width_tag(), value, false);
      } else {
        *(DatumIntType*)(datum.ptr_) = value;
        // ignore flag_ in ObDatum, all flags not used in cs encoding
        datum.pack_ = sizeof(DatumIntType);
      }
    }
  }
};

template<int32_t store_len_V, int32_t datum_width_V, bool is_decimal_V>
struct ConvertUintToDatum_T<store_len_V,
                            ObRefStoreWidthV::REF_IN_DATUMS,
                            datum_width_V,
                            ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF,
                            is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    UNUSEDx(ref_data, row_ids);
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<datum_width_V>::Type DatumIntType;
    StoreIntType *store_uint_arr = (StoreIntType *)(data);
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < row_cap; i++) {
      common::ObDatum &datum = datums[i];
      //row_id is stored int datum.pack_
      if (datum.pack_ == base_col_ctx.null_replaced_ref_) {
        datum.set_null();
        // must enter next loop, row_id maybe out of range of store_uint_arr
      } else {
        value = store_uint_arr[datum.pack_] + base;
        if (is_decimal_V) {
          ObIntegerStreamDecoder::decimal_datum_assign(datum, ctx.meta_.precision_width_tag(), value, false);
        } else {
          *(DatumIntType*)(datum.ptr_) = store_uint_arr[datum.pack_] + base;
          // ignore flag_ in ObDatum, all flags not used in cs encoding
          datum.pack_ = sizeof(DatumIntType);
        }
      }
    }
  }

};

template<int32_t store_len_V, int32_t datum_width_V, bool is_decimal_V>
struct ConvertUintToDatum_T<store_len_V,
                            ObRefStoreWidthV::REF_IN_DATUMS,
                            datum_width_V,
                            ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL,
                            is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    UNUSEDx(ref_data, row_ids);
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<datum_width_V>::Type DatumIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType *>(data);
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < row_cap; i++) {
      common::ObDatum &datum = datums[i];
      value = store_uint_arr[datum.pack_] + base;
      if (is_decimal_V) {
        ObIntegerStreamDecoder::decimal_datum_assign(datum, ctx.meta_.precision_width_tag(), value, false);
      } else {
        // row_id is stored int datum.pack_
        *(DatumIntType*)(datum.ptr_) = value;
        // ignore flag_ in ObDatum, all flags not used in cs encoding
        datum.pack_ = sizeof(DatumIntType);
      }
    }
  }
};

template<int32_t store_len_V, int32_t datum_width_V, bool is_decimal_V>
struct ConvertUintToDatum_T<store_len_V,
                            ObRefStoreWidthV::NOT_REF,
                            datum_width_V,
                            ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP,
                            is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<datum_width_V>::Type DatumIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType *>(data);

    int64_t row_id = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < row_cap; i++) {
      common::ObDatum &datum = datums[i];
      row_id = row_ids[i];
      if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, row_id)) {
        datum.set_null();
      } else {
        value = store_uint_arr[row_id] + base;
        if (is_decimal_V) {
          ObIntegerStreamDecoder::decimal_datum_assign(datum, ctx.meta_.precision_width_tag(), value, false);
        } else {
          *(DatumIntType*)(datum.ptr_) = value;
          // ignore flag_ in ObDatum, all flags not used in cs encoding
          datum.pack_ = sizeof(DatumIntType);
        }
      }
    }
  }
};

template<int32_t store_len_V, int32_t datum_width_V, bool is_decimal_V>
struct ConvertUintToDatum_T<store_len_V,
                            ObRefStoreWidthV::NOT_REF,
                            datum_width_V,
                            ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED,
                            is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<datum_width_V>::Type DatumIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType *>(data);

    int64_t row_id = 0;
    uint64_t value = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;

    for (int64_t i = 0; i < row_cap; i++) {
      common::ObDatum &datum = datums[i];
      value = store_uint_arr[row_ids[i]] + base;

      if (value == base_col_ctx.null_replaced_value_) {
        datum.set_null();
      } else if (is_decimal_V) {
        ObIntegerStreamDecoder::decimal_datum_assign(datum, ctx.meta_.precision_width_tag(), value, false);
      } else {
        *(DatumIntType*)(datum.ptr_) = value;
        // ignore flag_ in ObDatum, all flags not used in cs encoding
        datum.pack_ = sizeof(DatumIntType);
      }
    }
  }
};

template<int32_t store_len_V, int32_t datum_width_V, bool is_decimal_V>
struct ConvertUintToDatum_T<store_len_V,
                            ObRefStoreWidthV::NOT_REF,
                            datum_width_V,
                            ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL,
                            is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<datum_width_V>::Type DatumIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType *>(data);
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < row_cap; i++) {
      common::ObDatum &datum = datums[i];
      value = store_uint_arr[row_ids[i]] + base;
      if (is_decimal_V) {
        ObIntegerStreamDecoder::decimal_datum_assign(datum, ctx.meta_.precision_width_tag(), value, false);
      } else {
        *(DatumIntType*)(datum.ptr_) = value;
        // ignore flag_ in ObDatum, all flags not used in cs encoding
        datum.pack_ = sizeof(DatumIntType);
      }
    }
  }
};

template<int32_t store_len_V,
         int32_t ref_store_width_V,
         int32_t datum_width_V,
         int32_t null_flag_V,
         int32_t is_decimal_V>
struct ConvertUintToDatumInit
{
  bool operator()()
  {
    convert_uint_to_datum_funcs[store_len_V]
                               [ref_store_width_V]
                               [datum_width_V]
                               [null_flag_V]
                               [is_decimal_V]
      = &(ConvertUintToDatum_T<store_len_V,
                               ref_store_width_V,
                               datum_width_V,
                               null_flag_V,
                               is_decimal_V>::process);
    return true;
  }
};

static bool convert_uint_to_datum_funcs_inited
    = ObNDArrayIniter<ConvertUintToDatumInit, 4, ObRefStoreWidthV::MAX_WIDTH_V, 4, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2>::apply();

int ObIntegerStreamDecoder::decode_stream_meta(
    const ObStreamData &data,
    ObIntegerStreamDecoderCtx &ctx,
    uint16_t &stream_meta_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(ctx.meta_.deserialize(data.buf_, data.len_, pos))) {
    LOG_WARN("fail to deserializ stream meta", K(ret), K(data));
  } else {
    stream_meta_len = static_cast<uint16_t>(pos);
    LOG_DEBUG("after decode interger stream header", K(ctx), K(stream_meta_len));
  }

  return ret;
}

int ObIntegerStreamDecoder::build_decoder_ctx(
    const ObStreamData &data,
    const int64_t count,
    const ObCompressorType type,
    ObIntegerStreamDecoderCtx &ctx,
    uint16_t &stream_meta_len)
{
  ctx.count_ = count;
  ctx.compressor_type_ = type;
  return decode_stream_meta(data, ctx, stream_meta_len);
}

int ObIntegerStreamDecoder::transform_to_raw_array(
    const ObStreamData &data, ObIntegerStreamDecoderCtx &ctx, char *raw_arr_buf, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_arr_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "buf not enough", K(ret), K(ctx));
  } else if (ctx.meta_.is_8_byte_width()) {
    if (OB_FAIL(decode_all_except_base(data, ctx, (uint64_t*)raw_arr_buf, alloc))) {
      STORAGE_LOG(WARN, "fail to decode all", KR(ret), K(data), K(ctx));
    }
  } else if (ctx.meta_.is_4_byte_width()) {
    if (OB_FAIL(decode_all_except_base(data, ctx, (uint32_t*)raw_arr_buf, alloc))) {
      STORAGE_LOG(WARN, "fail to decode all", KR(ret), K(data), K(ctx));
    }
  } else if (ctx.meta_.is_2_byte_width()) {
    if (OB_FAIL(decode_all_except_base(data, ctx, (uint16_t*)raw_arr_buf, alloc))) {
      STORAGE_LOG(WARN, "fail to decode all", KR(ret), K(data), K(ctx));
    }
  } else if (ctx.meta_.is_1_byte_width()) {
    if (OB_FAIL(decode_all_except_base(data, ctx, (uint8_t*)raw_arr_buf, alloc))) {
      STORAGE_LOG(WARN, "fail to decode all", KR(ret), K(data), K(ctx));
    }
  } else {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "invalid width size", K(ret), K(ctx));
  }

  return ret;
}


}// end namespace blocksstable
} // end namespace oceanbase
