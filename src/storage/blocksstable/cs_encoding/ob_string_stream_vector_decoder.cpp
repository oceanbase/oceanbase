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

#include "ob_string_stream_vector_decoder.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "storage/blocksstable/encoding/ob_icolumn_decoder.h"
#include "ob_cs_decoding_util.h"


namespace oceanbase
{
using namespace common;
namespace blocksstable
{

#define GET_ROW_ID(row_id)                                                 \
  if (ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF) {                  \
    row_id = vector_ctx.row_ids_[i];                                       \
  } else if (ref_width_V == ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF) {   \
    row_id = ref_arr[i];                                                   \
  } else {                                                                 \
    row_id = ref_arr[vector_ctx.row_ids_[i]];                              \
  }

#define GET_SRC_VAR_STRING(row_id, src_ptr, src_len)                   \
  if (0 == row_id) {                                                   \
    src_ptr = str_data;                                                \
    src_len = offset_arr[0];                                           \
  } else {                                                             \
    src_ptr = str_data + offset_arr[row_id - 1];                       \
    src_len = offset_arr[row_id] - offset_arr[row_id - 1];             \
  }

#define HANDLE_STRING_ASSIGN(dst_ptr, dst_len, src_ptr, src_len)            \
  if (need_copy_V) {                                                        \
    MEMCPY(const_cast<char *>(dst_ptr), src_ptr, src_len);                  \
    dst_len = str_len;                                                      \
  } else {                                                                  \
    dst_ptr = src_ptr;                                                      \
    dst_len = str_len;                                                      \
  }

#define SHALLOW_COPY_STRING(dst_ptr, dst_len, src_ptr, src_len)   \
  dst_ptr = src_ptr;                                              \
  dst_len = str_len;


//========================== ConvertStringToVec_T =====================================//
template<typename VectorType, typename ValueType,
    int32_t offset_width_V, int32_t ref_width_V, int32_t null_flag_V, bool need_copy_V>
struct ConvertStringToVec_T
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *str_data,
      const ObStringStreamDecoderCtx &str_ctx,
      const char *offset_data,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector)
  {
    int ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("impossible here", K(base_col_ctx), K(str_ctx),  K(vector.get_format()),
        K(offset_width_V), K(ref_width_V), K(null_flag_V), K(need_copy_V));
    ::ob_abort();
  }
};

// ObUniformFormat always do shallow copy regardless need_copy_V flag.
// In fact, the object types that need to be deep copied would not use ObDiscreteFormat, except ObNumberType,
// so the copy way of ObNumberType when decode vector is different from the previous decode interface.

template<typename ValueType,
    int32_t offset_width_V, int32_t ref_width_V, int32_t null_flag_V, bool need_copy_V>
struct ConvertStringToVec_T<ObUniformFormat<false>, ValueType,
    offset_width_V, ref_width_V, null_flag_V, need_copy_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *str_data,
      const ObStringStreamDecoderCtx &str_ctx,
      const char *offset_data,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObUniformFormat<false> &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
    const char *cur_start = nullptr;
    int64_t str_len  = str_ctx.meta_.fixed_str_len_;
    int64_t row_id = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      ObDatum &datum = vector.get_datum(curr_vec_offset);

      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_ROW_ID(row_id);
        if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          SHALLOW_COPY_STRING(datum.ptr_, datum.pack_, cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          SHALLOW_COPY_STRING(datum.ptr_, datum.pack_, cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, row_id)) {
          datum.set_null();
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          SHALLOW_COPY_STRING(datum.ptr_, datum.pack_, cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          SHALLOW_COPY_STRING(datum.ptr_, datum.pack_, cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        GET_SRC_VAR_STRING(row_id, cur_start, str_len);
        if (0 == str_len) { // use zero length as null
          datum.set_null();
        } else {
          SHALLOW_COPY_STRING(datum.ptr_, datum.pack_, cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        OB_ASSERT(ref_width_V != ObVecDecodeRefWidth::VDRW_NOT_REF);
        GET_ROW_ID(row_id);
        if (row_id == base_col_ctx.null_replaced_ref_) {
          datum.set_null(); // not exist in offset_arr, must skip below
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          SHALLOW_COPY_STRING(datum.ptr_, datum.pack_, cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          SHALLOW_COPY_STRING(datum.ptr_, datum.pack_, cur_start, str_len);
        }

      } else {
        ob_abort();
      }
    }
  }

};

// ObDiscreteFormat always do shallow copy regardless need_copy_V flag.
// In fact, the object types that need to be deep copied would not use ObDiscreteFormat, except ObNumberType,
// so the copy way of ObNumberType when decode vector is different from the previous decode interface.
template<typename ValueType,
    int32_t offset_width_V, int32_t ref_width_V, int32_t null_flag_V, bool need_copy_V>
struct ConvertStringToVec_T<ObDiscreteFormat, ValueType,
    offset_width_V, ref_width_V, null_flag_V, need_copy_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *str_data,
      const ObStringStreamDecoderCtx &str_ctx,
      const char *offset_data,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObDiscreteFormat &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
    const char *cur_start = nullptr;
    int64_t str_len  = str_ctx.meta_.fixed_str_len_;
    int64_t row_id = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      char *&vec_ptr = vector.get_ptrs()[curr_vec_offset];
      ObLength &vec_len = vector.get_lens()[curr_vec_offset];
      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_ROW_ID(row_id);
        if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          SHALLOW_COPY_STRING(vec_ptr, vec_len, (char*)cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          SHALLOW_COPY_STRING(vec_ptr, vec_len, (char*)cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, row_id)) {
          vector.set_null(curr_vec_offset);
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          SHALLOW_COPY_STRING(vec_ptr, vec_len, (char*)cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          SHALLOW_COPY_STRING(vec_ptr, vec_len, (char*)cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        GET_SRC_VAR_STRING(row_id, cur_start, str_len);
        if (0 == str_len) { // use zero length as null
          vector.set_null(curr_vec_offset);
        } else {
          SHALLOW_COPY_STRING(vec_ptr, vec_len, (char*)cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        OB_ASSERT(ref_width_V != ObVecDecodeRefWidth::VDRW_NOT_REF);
        GET_ROW_ID(row_id);
        if (row_id == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // not exist in offset_arr, must skip below
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          SHALLOW_COPY_STRING(vec_ptr, vec_len, (char*)cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          SHALLOW_COPY_STRING(vec_ptr, vec_len, (char*)cur_start, str_len);
        }

      } else {
        ob_abort();
      }
    }
  }

};

// ObContinuousFormat not support shallow copy now
template<typename ValueType,
    int32_t offset_width_V, int32_t ref_width_V, int32_t null_flag_V, bool need_copy_V>
struct ConvertStringToVec_T<ObContinuousFormat, ValueType,
    offset_width_V, ref_width_V, null_flag_V, need_copy_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *str_data,
      const ObStringStreamDecoderCtx &str_ctx,
      const char *offset_data,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObContinuousFormat &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
    const char *cur_start = nullptr;
    int64_t str_len  = str_ctx.meta_.fixed_str_len_;
    int64_t row_id = 0;
    uint32_t curr_offset = 0;
    if (0 == vector_ctx.vec_offset_) {
      vector.get_offsets()[0] = 0;
    } else {
      curr_offset = vector.get_offsets()[vector_ctx.vec_offset_];
    }
    for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      char *vec_ptr = vector.get_data() + curr_offset;
      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_ROW_ID(row_id);
        if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          MEMCPY(vec_ptr, cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          MEMCPY(vec_ptr, cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, row_id)) {
          vector.set_null(curr_vec_offset);
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          MEMCPY(vec_ptr, cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          MEMCPY(vec_ptr, cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        GET_SRC_VAR_STRING(row_id, cur_start, str_len);
        if (0 == str_len) { // use zero length as null
          vector.set_null(curr_vec_offset);
        } else {
          MEMCPY(vec_ptr, cur_start, str_len);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        OB_ASSERT(ref_width_V != ObVecDecodeRefWidth::VDRW_NOT_REF);
        GET_ROW_ID(row_id);
        if (row_id == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // not exist in offset_arr, must skip below
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          cur_start = str_data + row_id * str_len;
          MEMCPY(vec_ptr, cur_start, str_len);
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          MEMCPY(vec_ptr, cur_start, str_len);
        }

      } else {
        ob_abort();
      }
      curr_offset += str_len;
      vector.get_offsets()[curr_vec_offset + 1] = curr_offset;
    }
  }

};

// ObFixedLengthFormat must need deep_copy, otherwise some error occur
template<typename ValueType,
    int32_t offset_width_V, int32_t ref_width_V, int32_t null_flag_V>
struct ConvertStringToVec_T<ObFixedLengthFormat<ValueType>, ValueType,
    offset_width_V, ref_width_V, null_flag_V, true>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *str_data,
      const ObStringStreamDecoderCtx &str_ctx,
      const char *offset_data,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObFixedLengthFormat<ValueType> &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<offset_width_V>::Type OffsetIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const OffsetIntType *offset_arr = reinterpret_cast<const OffsetIntType *>(offset_data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);
    ValueType *vec_value_arr = reinterpret_cast<ValueType*>(vector.get_data());
    const ValueType *store_value_arr = reinterpret_cast<const ValueType*>(str_data);
    const char *cur_start = nullptr;
    int64_t str_len  = str_ctx.meta_.fixed_str_len_;
    int64_t row_id = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      ValueType &vec_value = vec_value_arr[curr_vec_offset];
      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL) {
        GET_ROW_ID(row_id);
        if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          OB_ASSERT(sizeof(ValueType) == str_len);
          vec_value = store_value_arr[row_id];
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          OB_ASSERT(sizeof(ValueType) == str_len);
          vec_value = *(ValueType*)cur_start;
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_bitmap_, row_id)) {
          vector.set_null(curr_vec_offset);
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          OB_ASSERT(sizeof(ValueType) == str_len);
          vec_value = store_value_arr[row_id];
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          OB_ASSERT(sizeof(ValueType) == str_len);
          vec_value = *(ValueType*)cur_start;
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        row_id = vector_ctx.row_ids_[i];
        GET_SRC_VAR_STRING(row_id, cur_start, str_len);
        if (0 == str_len) { // use zero length as null
          vector.set_null(curr_vec_offset);
        } else {
          OB_ASSERT(sizeof(ValueType) == str_len);
          vec_value = *(ValueType*)cur_start;
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        OB_ASSERT(ref_width_V != ObVecDecodeRefWidth::VDRW_NOT_REF);
        GET_ROW_ID(row_id);
        if (row_id == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // not exist in offset_arr, must skip below
        } else if (offset_width_V == FIX_STRING_OFFSET_WIDTH_V) {
          OB_ASSERT(sizeof(ValueType) == str_len);
          vec_value = store_value_arr[row_id];
        } else {
          GET_SRC_VAR_STRING(row_id, cur_start, str_len);
          OB_ASSERT(sizeof(ValueType) == str_len);
          vec_value = *(ValueType*)cur_start;
        }

      } else {
        ob_abort();
      }
    }
  }

};

template<int32_t offset_width_V, int32_t ref_width_V, int32_t null_flag_V>
struct ConvertStringToVec_T<ObFixedLengthFormat<char[0]>, char[0], offset_width_V, ref_width_V, null_flag_V, true>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *str_data,
      const ObStringStreamDecoderCtx &str_ctx,
      const char *offset_data,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObFixedLengthFormat<char[0]> &vector)
  {
    int ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("impossible here", K(base_col_ctx), K(str_ctx), K(offset_width_V), K(ref_width_V), K(null_flag_V));
    ::ob_abort();
  }
};

// ============================ ObStrVecDecodeDispatcher =============================//
template<typename VectorType, typename ValueType>
class ObStrVecDecodeDispatcher final
{
public:
  static ObStrVecDecodeDispatcher &instance();
  static int decode_vector(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const ObStringStreamVecDecoder::StrVecDecoderCtx &vec_decode_ctx,
      const char *ref_data,
      const ObVecDecodeRefWidth ref_width,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector)
  {
    int ret = OB_SUCCESS;
    if (vec_decode_ctx.str_ctx_->meta_.is_fixed_len_string()) {
      ConvertStringToVec convert_func = convert_to_vec_funcs_
          [FIX_STRING_OFFSET_WIDTH_V]
          [ref_width]
          [base_col_ctx.null_flag_]
          [vec_decode_ctx.need_copy_];
      convert_func(base_col_ctx, vec_decode_ctx.str_data_, *vec_decode_ctx.str_ctx_,
          nullptr/*offset_data*/, ref_data, vector_ctx, vector);
    } else {
      ConvertStringToVec convert_func = convert_to_vec_funcs_
          [vec_decode_ctx.offset_ctx_->meta_.get_width_tag()]
          [ref_width]
          [base_col_ctx.null_flag_]
          [vec_decode_ctx.need_copy_];
      convert_func(base_col_ctx, vec_decode_ctx.str_data_, *vec_decode_ctx.str_ctx_,
          vec_decode_ctx.offset_data_, ref_data, vector_ctx, vector);
    }
    return ret;
  }

private:
  ObStrVecDecodeDispatcher()
  {
    if (!func_array_inited_) {
      func_array_inited_ = ObNDArrayIniter<ConvertFuncsInit,
          5, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2>::apply();
    }
  }
  ~ObStrVecDecodeDispatcher() {}
  template<int32_t offset_width_V, int32_t ref_width_V, int32_t null_flag_V, int32_t need_copy_V>
  struct ConvertFuncsInit
  {
    bool operator()()
    {
      ObStrVecDecodeDispatcher<VectorType, ValueType>::convert_to_vec_funcs_
                               [offset_width_V]
                               [ref_width_V]
                               [null_flag_V]
                               [need_copy_V]
        = &(ConvertStringToVec_T<VectorType, ValueType, offset_width_V, ref_width_V, null_flag_V, need_copy_V>::process);
      return true;
    }
  };

  using ConvertStringToVec = void (*)(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *str_data,
      const ObStringStreamDecoderCtx &str_ctx,
      const char *offset_data,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector);
  DISALLOW_COPY_AND_ASSIGN(ObStrVecDecodeDispatcher);

public:

  static ObMultiDimArray_T<ConvertStringToVec, 5/*offset_width_V*/, ObVecDecodeRefWidth::VDRW_MAX,
      ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2/*need_copy_V*/> convert_to_vec_funcs_;
  static bool func_array_inited_;
};

template<typename VectorType, typename ValueType>
bool ObStrVecDecodeDispatcher<VectorType, ValueType>::func_array_inited_ = false;

template<typename VectorType, typename ValueType>
ObMultiDimArray_T<typename ObStrVecDecodeDispatcher<VectorType, ValueType>::ConvertStringToVec,
    5/*offset_width_V*/, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX,
    2/*need_copy_V*/> ObStrVecDecodeDispatcher<VectorType, ValueType>::convert_to_vec_funcs_;



template<typename VectorType, typename ValueType>
ObStrVecDecodeDispatcher<VectorType, ValueType> &ObStrVecDecodeDispatcher<VectorType, ValueType>::instance()
{
  static ObStrVecDecodeDispatcher<VectorType, ValueType> dispatcher;
  return dispatcher;
}

//=============================== ObStringStreamVecDecoder =====================================//
template <typename ValueType>
int ObStringStreamVecDecoder::decode_vector_(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const StrVecDecoderCtx &vec_decode_ctx,
    const char *ref_data,
    const ObVecDecodeRefWidth ref_width,
    ObVectorDecodeCtx &vector_ctx)
{
#define DISPATCH_DECODE_VECTOR(VectorType, ValueType, vector)                                 \
ObStrVecDecodeDispatcher<VectorType, ValueType>::instance().decode_vector(                    \
    base_col_ctx, vec_decode_ctx, ref_data, ref_width, vector_ctx, vector)

  int ret = OB_SUCCESS;
  VectorFormat vec_format = vector_ctx.vec_header_.get_format();
  ObIVector *vector = vector_ctx.vec_header_.get_vector();

  switch (vec_format) {
    case VEC_FIXED : {
      ObFixedLengthFormat<ValueType> *fix_vec = static_cast<ObFixedLengthFormat<ValueType> *>(vector);
      ret = DISPATCH_DECODE_VECTOR(ObFixedLengthFormat<ValueType>, ValueType, *fix_vec);
      break;
    }
    case VEC_DISCRETE:{
      ObDiscreteFormat *disc_vec = static_cast<ObDiscreteFormat *>(vector);
      ret = DISPATCH_DECODE_VECTOR(ObDiscreteFormat, ValueType, *disc_vec);
      break;
    }
    case VEC_CONTINUOUS:{
      ObContinuousFormat *conti_vec = static_cast<ObContinuousFormat *>(vector);
      ret = DISPATCH_DECODE_VECTOR(ObContinuousFormat, ValueType,  *conti_vec);
      break;
    }
    case VEC_UNIFORM:{
      ObUniformFormat<false> *uni_vec = static_cast<ObUniformFormat<false> *>(vector);
      ret = DISPATCH_DECODE_VECTOR(ObUniformFormat<false>, ValueType, *uni_vec);
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(vec_format));
    }
    return ret;
#undef DISPATCH_DECODE_VECTOR
}

int ObStringStreamVecDecoder::decode_vector(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const StrVecDecoderCtx &vec_decode_ctx,
    const char *ref_data,
    const ObVecDecodeRefWidth ref_width,
    ObVectorDecodeCtx &vector_ctx)
{
#define DECODE_VECTOR_(ValueType)                                            \
  decode_vector_<ValueType>(                                                 \
      base_col_ctx, vec_decode_ctx, ref_data, ref_width, vector_ctx)

  int ret = OB_SUCCESS;
  const int16_t precision = base_col_ctx.obj_meta_.is_decimal_int() ?
      base_col_ctx.obj_meta_.get_stored_precision() : PRECISION_UNKNOWN_YET;
  const VecValueTypeClass vec_tc = common::get_vec_value_tc(
      base_col_ctx.obj_meta_.get_type(), base_col_ctx.obj_meta_.get_scale(), precision);

  switch (vec_tc) {
    case VEC_TC_TIMESTAMP_TZ: {
      ret = DECODE_VECTOR_(ObOTimestampData);
      break;
    }
    case VEC_TC_TIMESTAMP_TINY: {
      ret = DECODE_VECTOR_(ObOTimestampTinyData);
      break;
    }
    case VEC_TC_INTERVAL_DS: {
      ret = DECODE_VECTOR_(ObIntervalDSValue);
      break;
    }
    case VEC_TC_INTERVAL_YM: {
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
    case VEC_TC_DEC_INT512:
    {
      ret = DECODE_VECTOR_(int512_t);
      break;
    }

    // below types must use integer stream
    case VEC_TC_YEAR:
    case VEC_TC_DATE:
    case VEC_TC_DEC_INT32:
    case VEC_TC_FLOAT:
    case VEC_TC_INTEGER:
    case VEC_TC_DATETIME:
    case VEC_TC_TIME:
    case VEC_TC_UNKNOWN:
    //case VEC_TC_INTERVAL_YM:
    case VEC_TC_DEC_INT64:
    case VEC_TC_UINTEGER:
    case VEC_TC_BIT:
    case VEC_TC_ENUM_SET:
    case VEC_TC_DOUBLE: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected vec value type for string stream", K(ret), K(vec_tc), K(base_col_ctx), K(vec_decode_ctx));
      break;
    }

    default: {
      ret = DECODE_VECTOR_(char[0]); // var-length types, currently should not rely on ValueType on decode
      break;
    }
  }
#undef DECODE_VECTOR_

  if (OB_FAIL(ret)) {
    LOG_ERROR("fail to decode vector", K(ret), K(vec_tc), K(base_col_ctx), K(vec_decode_ctx));
  }
  return ret;
}


}
};