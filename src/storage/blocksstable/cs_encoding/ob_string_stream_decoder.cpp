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

#include "ob_string_stream_decoder.h"
#include "ob_cs_decoding_util.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace oceanbase::common;

template<int32_t offset_width_V,
         int32_t ref_store_width_V,
         int32_t null_flag_V,
         bool need_copy_V>
struct ConvertStringToDatum_T
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_arr_buf,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  int ret = OB_INNER_STAT_ERROR;
  LOG_ERROR("impossible here", K(offset_width_V), K(ref_store_width_V), K(null_flag_V), K(need_copy_V));
  ::ob_abort();
}
};

template<int32_t offset_width_V, int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<offset_width_V,
                              ref_store_width_V,
                              ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL,
                              need_copy_V >
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
  typedef typename ObCSEncodingStoreTypeInference<ref_store_width_V>::Type RefIntType;
  const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
  const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
  const char *cur_start = nullptr;
  int64_t str_len = 0;
  int64_t row_id = 0;
  for (int64_t i = 0; i < row_cap; ++i) {
    ObDatum &datum = datums[i];
    if (ref_store_width_V == ObRefStoreWidthV::REF_IN_DATUMS) {
      row_id = datum.pack_;
    } else if (ref_store_width_V == ObRefStoreWidthV::NOT_REF) {
      row_id = row_ids[i];
    } else {
      row_id = ref_arr[row_ids[i]];
    }
    if (0 == row_id) {
      cur_start = str_data;
      str_len = offset_arr[0];
    } else {
      cur_start = str_data + offset_arr[row_id - 1];
      str_len = offset_arr[row_id] - offset_arr[row_id - 1];
    }
    if (need_copy_V) {
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), cur_start, str_len);
      datum.pack_ = str_len;
    } else {
      datum.ptr_ = cur_start;
      datum.pack_ = str_len;
    }
  }
}
};

template<int32_t offset_width_V, int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<offset_width_V,
                              ref_store_width_V,
                              ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP,
                              need_copy_V>
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  //OB_ASSERT(ref_store_width_V == ObRefStoreWidthV::NOT_REF);
  typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
  const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
  const char *cur_start = nullptr;
  int64_t str_len = 0;

  int64_t row_id = 0;
  for (int64_t i = 0; i < row_cap; ++i) {
    ObDatum &datum = datums[i];
    row_id = row_ids[i];
    if (0 == row_id) {
      cur_start = str_data;
      str_len = offset_arr[0];
    } else {
      cur_start = str_data + offset_arr[row_id - 1];
      str_len = offset_arr[row_id] - offset_arr[row_id - 1];
    }
    if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, row_id)) {
      datum.set_null();
    } else if (need_copy_V) {
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), cur_start, str_len);
      datum.pack_ = str_len;
    } else {
      datum.ptr_ = cur_start;
      datum.pack_ = str_len;
    }
  }
}
};

template<int32_t offset_width_V, int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<offset_width_V,
                              ref_store_width_V,
                              ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED,
                              need_copy_V>
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  //OB_ASSERT(ref_store_width_V == ObRefStoreWidthV::NOT_REF);
  typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
  const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
  const char *cur_start = nullptr;
  int64_t str_len = 0;
  int64_t row_id = 0;
  for (int64_t i = 0; i < row_cap; ++i) {
    ObDatum &datum = datums[i];
    row_id = row_ids[i];
    if (0 == row_id) {
      cur_start = str_data;
      str_len = offset_arr[0];
    } else {
      cur_start = str_data + offset_arr[row_id - 1];
      str_len = offset_arr[row_id] - offset_arr[row_id - 1];
    }
    if (0 == str_len) { // use zero length as null
      datum.set_null();
    } else if (need_copy_V) {
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), cur_start, str_len);
      datum.pack_ = str_len;
    } else {
      datum.ptr_ = cur_start;
      datum.pack_ = str_len;
    }
  }
}
};

template<int32_t offset_width_V, int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<offset_width_V,
                              ref_store_width_V,
                              ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF,
                              need_copy_V>
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  //OB_ASSERT(ref_store_width_V != ObRefStoreWidthV::NOT_REF);
  typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
  typedef typename ObCSEncodingStoreTypeInference<ref_store_width_V>::Type RefIntType;
  const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
  const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
  const char *cur_start = nullptr;
  int64_t str_len = 0;
  int64_t row_id = 0;
  for (int64_t i = 0; i < row_cap; ++i) {
    ObDatum &datum = datums[i];
    if (ref_store_width_V == ObRefStoreWidthV::REF_IN_DATUMS) {
      row_id = datum.pack_;
    } else {
      row_id = ref_arr[row_ids[i]];
    }
    if (row_id == base_col_ctx.null_replaced_ref_) {
      datum.set_null();
      // not exist in offset_arr, must skip below
    } else {
      if (0 == row_id) {
        cur_start = str_data;
        str_len = offset_arr[0];
      } else {
        cur_start = str_data + offset_arr[row_id - 1];
        str_len = offset_arr[row_id] - offset_arr[row_id - 1];
      }
      if (need_copy_V) {
        ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), cur_start, str_len);
        datum.pack_ = str_len;
      } else {
        datum.ptr_ = cur_start;
        datum.pack_ = str_len;
      }
    }
  }
}
};

//==========partial specialization for FIX_STRING_OFFSET_WIDTH_V and null flag==========//
template<int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<FIX_STRING_OFFSET_WIDTH_V,
                             ref_store_width_V,
                             ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL,
                             need_copy_V>
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  typedef typename ObCSEncodingStoreTypeInference<ref_store_width_V>::Type RefIntType;
  const char *cur_start = nullptr;
  const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
  const int64_t str_len = str_ctx.meta_.fixed_str_len_;
  int64_t row_id = 0;
  for (int64_t i = 0; i < row_cap; ++i) {
    ObDatum &datum = datums[i];
    if (ref_store_width_V == ObRefStoreWidthV::NOT_REF) {
      row_id = row_ids[i];
    } else if (ref_store_width_V == ObRefStoreWidthV::REF_IN_DATUMS) {
      row_id = datum.pack_;
    } else {
      row_id = ref_arr[row_ids[i]];
    }
    cur_start = str_data + row_id * str_len;
    if (need_copy_V) {
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), cur_start, str_len);
      datum.pack_ = str_len;
    } else {
      datum.ptr_ = cur_start;
      datum.pack_ = str_len;
    }
  }
}
};

template<int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<FIX_STRING_OFFSET_WIDTH_V,
                             ref_store_width_V,
                             ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP,
                             need_copy_V>
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  //OB_ASSERT(ref_store_width_V == ObRefStoreWidthV::NOT_REF);
  const char *cur_start = nullptr;
  int64_t str_len = str_ctx.meta_.fixed_str_len_;
  int64_t row_id = 0;
  for (int64_t i = 0; i < row_cap; ++i) {
    ObDatum &datum = datums[i];
    row_id = row_ids[i];
    cur_start = str_data + row_id * str_len;
    if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, row_id)) {
      datum.set_null();
    } else if (need_copy_V) {
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), cur_start, str_len);
      datum.pack_ = str_len;
    } else {
      datum.ptr_ = cur_start;
      datum.pack_ = str_len;
    }
  }
}
};


template<int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<FIX_STRING_OFFSET_WIDTH_V,
                             ref_store_width_V,
                             ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED,
                             need_copy_V>

{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  int ret = OB_INNER_STAT_ERROR;
  LOG_ERROR("impossible here", K(str_data), K(str_ctx), K(base_col_ctx), K(ref_store_width_V));
  ::ob_abort();
}
};

template<int32_t ref_store_width_V, bool need_copy_V>
struct ConvertStringToDatum_T<FIX_STRING_OFFSET_WIDTH_V,
                             ref_store_width_V,
                             ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF,
                             need_copy_V>
{
static void process(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *str_data,
    const ObStringStreamDecoderCtx &str_ctx,
    const char *offset_data,
    const char *ref_data,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  //OB_ASSERT(ref_store_width_V != ObRefStoreWidthV::NOT_REF);
  typedef typename ObCSEncodingStoreTypeInference<ref_store_width_V>::Type RefIntType;
  const char *cur_start = nullptr;
  const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
  int64_t str_len = str_ctx.meta_.fixed_str_len_;
  int64_t row_id = 0;
  for (int64_t i = 0; i < row_cap; ++i) {
    ObDatum &datum = datums[i];
    if (ref_store_width_V == ObRefStoreWidthV::REF_IN_DATUMS) {
      row_id = datum.pack_;
    } else {
      row_id = ref_arr[row_ids[i]];
    }
    if (row_id == base_col_ctx.null_replaced_ref_) {
      datum.set_null();
      // not exist in offset_arr, must skip below
    } else {
      cur_start = str_data + row_id * str_len;
      if (need_copy_V) {
        ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), cur_start, str_len);
        datum.pack_ = str_len;
      } else {
        datum.ptr_ = cur_start;
        datum.pack_ = str_len;
      }
    }
  }
}
};

ObMultiDimArray_T<ConvertStringToDatumFunc,
    5, ObRefStoreWidthV::MAX_WIDTH_V, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2> convert_string_to_datum_funcs;

template<int32_t offset_width_V,
         int32_t ref_store_width_V,
         int32_t null_flag_V,
         int32_t need_copy_V>
struct ConvertStringToDatumInit
{
  bool operator()()
  {
    convert_string_to_datum_funcs[offset_width_V]
                                  [ref_store_width_V]
                                  [null_flag_V]
                                  [need_copy_V]
      = &(ConvertStringToDatum_T<offset_width_V,
                                 ref_store_width_V,
                                 null_flag_V,
                                 need_copy_V>::process);
    return true;
  }
};

static bool convert_string_to_datum_funcs_inited = ObNDArrayIniter<ConvertStringToDatumInit,
    5, ObRefStoreWidthV::MAX_WIDTH_V, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2>::apply();



int ObStringStreamDecoder::decode_stream_meta_(
    const ObStreamData &str_data,
    ObStringStreamDecoderCtx &ctx,
    uint16_t &str_meta_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint64_t len = str_data.len_;
  const char *buf = str_data.buf_;
  if (OB_UNLIKELY(!str_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid agrument", KR(ret), K(str_data));
  } else if (OB_FAIL(ctx.meta_.deserialize(buf, len, pos))) {
    LOG_WARN("fail to deserialize", K(ret), KP(buf), K(len));
  } else {
    str_meta_size = pos;
    LOG_DEBUG("after decode string stream meta", K(ctx), K(str_meta_size));
  }

  return ret;
}

int ObStringStreamDecoder::build_decoder_ctx(
    const ObStreamData &str_data,
    ObStringStreamDecoderCtx &ctx,
    uint16_t &str_meta_size)
{
  return decode_stream_meta_(str_data, ctx, str_meta_size);
}


} // end namespace blocksstable
} // end namespace oceanbase
