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

#define USING_LOG_PREFIX STORAGE

#include "ob_integer_stream_vector_decoder.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "ob_cs_decoding_util.h"
#include "storage/blocksstable/encoding/ob_icolumn_decoder.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

#define DECIMAL_ASSIGN(dst_ptr, dst_len, precision_width, value)           \
  switch(precision_width) {                                                \
    case ObIntegerStream::UintWidth::UW_4_BYTE : {                         \
      *(int32_t*)dst_ptr = (int64_t)(value);                               \
      dst_len= sizeof(int32_t);                                            \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_8_BYTE : {                         \
      *(int64_t*)dst_ptr = (int64_t)(value);                               \
      dst_len = sizeof(int64_t);                                           \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_16_BYTE : {                        \
      *(int128_t*)dst_ptr = (int64_t)(value);                              \
      dst_len = sizeof(int128_t);                                          \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_32_BYTE : {                        \
      *(int256_t*)dst_ptr = (int64_t)(value);                              \
      dst_len = sizeof(int256_t);                                          \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_64_BYTE : {                        \
      *(int512_t*)dst_ptr = (int64_t)(value);                              \
      dst_len = sizeof(int512_t);                                          \
      break;                                                               \
    }                                                                      \
    default : {                                                            \
      ob_abort();                                                          \
      break;                                                               \
    }                                                                      \
  }

#define GET_SRC_VALUE(value)                                                   \
  if (ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF) {                      \
    value = store_uint_arr[vector_ctx.row_ids_[i]] + base;                     \
  } else if (ref_width_V == ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF) {       \
    value = store_uint_arr[ref_arr[i]] + base;                                 \
  } else {                                                                     \
    value = store_uint_arr[ref_arr[vector_ctx.row_ids_[i]]] + base;            \
  }
#define HANDLE_VALUE_ASSIGN(dst_ptr, dst_len, precision_width, value)      \
  if (is_decimal_V) {                                                      \
    DECIMAL_ASSIGN(dst_ptr, dst_len, precision_width, value);              \
  } else {                                                                 \
    *(ValueType*)(dst_ptr) = value;                                        \
    dst_len = sizeof(ValueType);                                           \
  }

#define HANDLE_FIXED_VALUE_ASSIGN(dst_value, value)      \
 if (is_decimal_V) {                                     \
   dst_value =  (int64_t)(value);                        \
 } else {                                                \
   dst_value = value;                                    \
 }


#define GET_DICT_REF(ref)                                              \
  OB_ASSERT(ref_width_V != ObVecDecodeRefWidth::VDRW_NOT_REF);         \
  if (ref_width_V == ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF) {      \
    ref = ref_arr[i];                                                  \
  } else {                                                             \
    ref = ref_arr[vector_ctx.row_ids_[i]];                             \
  }


//============================ ConvertUintToVec_T ======================================//
template<typename VectorType, typename ValueType,
    int32_t store_len_V, int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector)
  {
    int ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("impossible here", K(store_len_V), K(ref_width_V),
        K(null_flag_V), K(is_decimal_V), K(base_col_ctx), K(ctx));
    ::ob_abort();
  }
};

// Integer stream must use deep-copy, so it don't support decoding to
// ObUniformFormat and ObContinuousFormat, these two vector types only support shallow-copy.
// In fact, for integer stream, the sql layer only use ObFixedLengthFormat.
/**********************************************************************************************
template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObUniformFormat<false>, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObUniformFormat<false> &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; i++) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      ObDatum &datum = vector.get_datum(curr_vec_offset);

      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_SRC_VALUE(value);
        HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, vector_ctx.row_ids_[i])) {
          datum.set_null();
        } else {
          value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
          HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
        if (value == base_col_ctx.null_replaced_value_) {
          datum.set_null();
        } else {
          HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        GET_DICT_REF(ref);
        if (ref == base_col_ctx.null_replaced_ref_) {
          datum.set_null(); // must enter next loop, row_id maybe out of range of store_uint_arr
        } else {
          value = store_uint_arr[ref] + base;
          HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);
        }

      } else {
        ob_abort();
      }
    }
  }
};

template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObDiscreteFormat, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObDiscreteFormat &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; i++) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      char *vec_ptr = vector.get_ptrs()[curr_vec_offset];
      ObLength &vec_len =vector.get_lens()[curr_vec_offset];

      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_SRC_VALUE(value);
        HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, vector_ctx.row_ids_[i])) {
          vector.set_null(curr_vec_offset);
        } else {
          OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
          value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
        if (value == base_col_ctx.null_replaced_value_) {
          vector.set_null(curr_vec_offset);
        } else {
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        GET_DICT_REF(ref);
        if (ref == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // must enter next loop, row_id maybe out of range of store_uint_arr
        } else {
          value = store_uint_arr[ref] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else {
        ob_abort();
      }
    }
  }
};
**********************************************************************************************************************/

template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObContinuousFormat, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObContinuousFormat &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    uint32_t curr_offset = 0;
    if (0 == vector_ctx.vec_offset_) {
      vector.get_offsets()[0] = 0;
    } else {
      curr_offset = vector.get_offsets()[vector_ctx.vec_offset_];
    }
    uint32_t vec_len = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; i++) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      const char *vec_ptr = vector.get_data() + curr_offset;

      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_SRC_VALUE(value);
        HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, vector_ctx.row_ids_[i])) {
          vector.set_null(curr_vec_offset);
        } else {
          OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
          value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
        if (value == base_col_ctx.null_replaced_value_) {
          vector.set_null(curr_vec_offset);
        } else {
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        GET_DICT_REF(ref);
        if (ref == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // must enter next loop, row_id maybe out of range of store_uint_arr
        } else {
          value = store_uint_arr[ref] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else {
        ob_abort();
      }
      curr_offset += vec_len;
      vector.get_offsets()[curr_vec_offset + 1] = curr_offset;
    }
  }
};

template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObFixedLengthFormat<ValueType>, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObFixedLengthFormat<ValueType> &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);
    ValueType *vec_value_arr = reinterpret_cast<ValueType*>(vector.get_data());

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    uint32_t vec_len = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; i++) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      ValueType &vec_value = vec_value_arr[curr_vec_offset];
      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_SRC_VALUE(value);
        HANDLE_FIXED_VALUE_ASSIGN(vec_value, value);

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, vector_ctx.row_ids_[i])) {
          vector.set_null(curr_vec_offset);
        } else {
          OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
          value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
          HANDLE_FIXED_VALUE_ASSIGN(vec_value, value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
        if (value == base_col_ctx.null_replaced_value_) {
          vector.set_null(curr_vec_offset);
        } else {
          HANDLE_FIXED_VALUE_ASSIGN(vec_value, value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        GET_DICT_REF(ref);
        if (ref == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // must enter next loop, row_id maybe out of range of store_uint_arr
        } else {
          value = store_uint_arr[ref] + base;
          HANDLE_FIXED_VALUE_ASSIGN(vec_value, value);
        }

      } else {
        ob_abort();
      }
    }
  }
};

// ================================= ObIntVecDecodeDispatcher =================================//
template<typename VectorType, typename ValueType>
class ObIntVecDecodeDispatcher final
{
public:
  static ObIntVecDecodeDispatcher &instance();
  static int decode_vector(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const ObVecDecodeRefWidth ref_width,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector)
  {
    int ret = OB_SUCCESS;
    ConvertUnitToVec convert_func = convert_to_vec_funcs_
      [ctx.meta_.width_]               /*val_store_width_V*/
      [ref_width]
      [base_col_ctx.null_flag_]
      [ctx.meta_.is_decimal_int()];
    convert_func(base_col_ctx, data, ctx, ref_data, vector_ctx, vector);
    return ret;
  }

private:
  ObIntVecDecodeDispatcher() {
    if (!func_array_inited_) {
      func_array_inited_ = ObNDArrayIniter<ConvertFuncsInit,
          4, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2>::apply();
    }
  }
  ~ObIntVecDecodeDispatcher() {}
  template<int32_t store_len_V, int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
  struct ConvertFuncsInit
  {
    bool operator()()
    {
      ObIntVecDecodeDispatcher<VectorType, ValueType>::convert_to_vec_funcs_
                               [store_len_V]
                               [ref_width_V]
                               [null_flag_V]
                               [is_decimal_V]
        = &(ConvertUintToVec_T<VectorType, ValueType, store_len_V, ref_width_V, null_flag_V, is_decimal_V>::process);
      return true;
    }
  };

  using ConvertUnitToVec = void (*)(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector);
  DISALLOW_COPY_AND_ASSIGN(ObIntVecDecodeDispatcher);

public:

  static ObMultiDimArray_T<ConvertUnitToVec,
      4, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2> convert_to_vec_funcs_;
  static bool func_array_inited_;
};

template<typename VectorType, typename ValueType>
bool ObIntVecDecodeDispatcher<VectorType, ValueType>::func_array_inited_ = false;

template<typename VectorType, typename ValueType>
ObMultiDimArray_T<typename ObIntVecDecodeDispatcher<VectorType, ValueType>::ConvertUnitToVec,
    4, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2>
    ObIntVecDecodeDispatcher<VectorType, ValueType>::convert_to_vec_funcs_;

template<typename VectorType, typename ValueType>
ObIntVecDecodeDispatcher<VectorType, ValueType> &ObIntVecDecodeDispatcher<VectorType, ValueType>::instance()
{
  static ObIntVecDecodeDispatcher<VectorType, ValueType> dispatcher;
  return dispatcher;
}


//================================= ObIntegerStreamVecDecoder ===================================//

template <typename ValueType>
int ObIntegerStreamVecDecoder::decode_vector_(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *data,
    const ObIntegerStreamDecoderCtx &ctx,
    const char *ref_data,
    const ObVecDecodeRefWidth ref_width,
    ObVectorDecodeCtx &vector_ctx)
{
#define DISPATCH_DECODE_VECTOR(VectorType, ValueType, vector)                       \
ObIntVecDecodeDispatcher<VectorType, ValueType>::instance().decode_vector(          \
    base_col_ctx, data, ctx, ref_data, ref_width, vector_ctx, vector)

  int ret = OB_SUCCESS;
  VectorFormat vec_format = vector_ctx.vec_header_.get_format();
  ObIVector *vector = vector_ctx.vec_header_.get_vector();

  switch (vec_format) {
    case VEC_FIXED : {
      ObFixedLengthFormat<ValueType> *fix_vec = static_cast<ObFixedLengthFormat<ValueType> *>(vector);
      ret = DISPATCH_DECODE_VECTOR(ObFixedLengthFormat<ValueType>, ValueType, *fix_vec);
      break;
    }
    case VEC_CONTINUOUS:{
      ObContinuousFormat *conti_vec = static_cast<ObContinuousFormat *>(vector);
      ret = DISPATCH_DECODE_VECTOR(ObContinuousFormat, ValueType,  *conti_vec);
      break;
    }
    case VEC_DISCRETE:{
      //ObDiscreteFormat *disc_vec = static_cast<ObDiscreteFormat *>(vector);
      //ret = DISPATCH_DECODE_VECTOR(ObDiscreteFormat, ValueType, *disc_vec);
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("integer stream not support decoding to ObContinuousFormat", K(ret), K(base_col_ctx), K(ctx));
      break;
    }
    case VEC_UNIFORM:{
      // ObUniformFormat<false> *uni_vec = static_cast<ObUniformFormat<false> *>(vector);
      // ret = DISPATCH_DECODE_VECTOR(ObUniformFormat<false>, ValueType, *uni_vec);
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("integer stream not support decoding to VEC_UNIFORM", K(ret), K(base_col_ctx), K(ctx));
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(vec_format));
    }
    return ret;
#undef DISPATCH_DECODE_VECTOR
}

int ObIntegerStreamVecDecoder::decode_vector(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *data,
    const ObIntegerStreamDecoderCtx &ctx,
    const char *ref_data,
    const ObVecDecodeRefWidth ref_width,
    ObVectorDecodeCtx &vector_ctx)
{
#define DECODE_VECTOR_(ValueType)                                                      \
  decode_vector_<ValueType>(base_col_ctx, data, ctx, ref_data, ref_width, vector_ctx)

  int ret = OB_SUCCESS;
  const int16_t precision = base_col_ctx.obj_meta_.is_decimal_int() ?
      base_col_ctx.obj_meta_.get_stored_precision() : PRECISION_UNKNOWN_YET;
  const VecValueTypeClass vec_tc = common::get_vec_value_tc(
      base_col_ctx.obj_meta_.get_type(), base_col_ctx.obj_meta_.get_scale(), precision);

  switch (vec_tc) {
    case VEC_TC_YEAR: {
      ret = DECODE_VECTOR_(uint8_t);
      break;
    }
    case VEC_TC_DATE:
    case VEC_TC_DEC_INT32:
    case VEC_TC_FLOAT: {
      ret = DECODE_VECTOR_(uint32_t);
      break;
    }
    case VEC_TC_INTEGER:
    case VEC_TC_DATETIME:
    case VEC_TC_TIME:
    case VEC_TC_UNKNOWN:
    case VEC_TC_INTERVAL_YM:
    case VEC_TC_DEC_INT64:

    case VEC_TC_UINTEGER:
    case VEC_TC_BIT:
    case VEC_TC_ENUM_SET:

    case VEC_TC_DOUBLE: {
      ret = DECODE_VECTOR_(uint64_t);
      break;
    }

    case VEC_TC_DEC_INT128: {
      ret = DECODE_VECTOR_(int128_t);
      break;
    }
    case VEC_TC_DEC_INT256: {
      ret = DECODE_VECTOR_(int256_t);
      break;
    }
    case VEC_TC_DEC_INT512: {
      ret = DECODE_VECTOR_(int512_t);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
#undef DECODE_VECTOR_

  if (OB_FAIL(ret)) {
    LOG_ERROR("unexpected vec value type for integer stream", K(ret), K(vec_tc), K(base_col_ctx), K(ctx));
  }
  return ret;
}

} // namspace blocksstable
} // namespace oceanbase