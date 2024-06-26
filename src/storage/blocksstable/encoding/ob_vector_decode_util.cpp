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

#include "ob_vector_decode_util.h"
#include "ob_dict_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

/*-------------------------------------load_data_to_vector----------------------------------------*/

template<typename ValueType, typename DataLocator, int STORE_TYPE, int PACKING_LEN, int DECODE_TYPE, int NULL_TYPE>
struct LoadVectorDataFunc_T<
    common::ObUniformFormat<false>,
    ValueType,
    DataLocator,
    STORE_TYPE,
    PACKING_LEN,
    DECODE_TYPE,
    NULL_TYPE>
{
  static int load_data_to_vector(
      const DataLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      common::ObUniformFormat<false> &vector)
  {
    int ret = OB_SUCCESS;
    constexpr bool is_fixed_length = STORE_TYPE != ObVecDecodeUtils::VARIABLE_LENGTH;
    constexpr int type_store_size = ObVecDecodeUtils::GetFixedDataLen<STORE_TYPE>::len_;
    ObDatum *__restrict datum_arr = vector.get_datums();
    if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t curr_vec_offset = vec_offset + i;
        ObDatum &__restrict datum = datum_arr[curr_vec_offset];
        const char *data_ptr = nullptr;
        uint32_t data_len = 0;
        data_locator.get_data(i, data_ptr, data_len);
        LoadByteAlignedData_T<ValueType, DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
            data_ptr, data_len, datum.ptr_, datum.pack_);
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t curr_vec_offset = vec_offset + i;
        ObDatum &__restrict datum = datum_arr[curr_vec_offset];
        const char *data_ptr = nullptr;
        uint32_t data_len = 0;
        bool is_null = false;
        data_locator.get_data(i, data_ptr, data_len, is_null);
        if (is_null) {
          vector.set_null(curr_vec_offset);
        } else {
          LoadByteAlignedData_T<ValueType, DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
            data_ptr, data_len, datum.ptr_, datum.pack_);
        }
      }
    }

    return ret;
  }
};

// basic discrete format
template<typename ValueType, typename DataLocator, int STORE_TYPE, int PACKING_LEN, int DECODE_TYPE, int NULL_TYPE>
struct LoadVectorDataFunc_T<
    common::ObDiscreteFormat,
    ValueType,
    DataLocator,
    STORE_TYPE,
    PACKING_LEN,
    DECODE_TYPE,
    NULL_TYPE>
{
  static int load_data_to_vector(
      const DataLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      common::ObDiscreteFormat &vector)
  {
    int ret = OB_SUCCESS;
    constexpr bool is_fixed_length = STORE_TYPE != ObVecDecodeUtils::VARIABLE_LENGTH;
    constexpr int type_store_size = ObVecDecodeUtils::GetFixedDataLen<STORE_TYPE>::len_;


    if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t curr_vec_offset = vec_offset + i;
        char *&vec_ptr = vector.get_ptrs()[curr_vec_offset];
        uint32_t *vec_len = reinterpret_cast<uint32_t *>(vector.get_lens() + curr_vec_offset);
        const char *data_ptr = nullptr;
        uint32_t data_len = 0;
        data_locator.get_data(i, data_ptr, data_len);
        LoadByteAlignedData_T<ValueType, DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
            data_ptr, data_len, const_cast<const char *&>(vec_ptr), *vec_len);
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t curr_vec_offset = vec_offset + i;
        char *&vec_ptr = vector.get_ptrs()[curr_vec_offset];
        uint32_t *vec_len = reinterpret_cast<uint32_t *>(vector.get_lens() + curr_vec_offset);
        const char *data_ptr = nullptr;
        uint32_t data_len = 0;
        bool is_null = false;
        data_locator.get_data(i, data_ptr, data_len, is_null);
        if (is_null) {
          vector.set_null(curr_vec_offset);
        } else {
          LoadByteAlignedData_T<ValueType, DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
              data_ptr, data_len, const_cast<const char *&>(vec_ptr), *vec_len);
        }
      }
    }
    return ret;
  }
};

// discrete format with fixed length shallow copy data
template<typename ValueType, int STORE_TYPE, int NULL_TYPE>
struct LoadVectorDataFunc_T<common::ObDiscreteFormat, ValueType, DataFixedLocator, STORE_TYPE, 0,
    D_SHALLOW_COPY,  NULL_TYPE>
{
  static int load_data_to_vector(
      const DataFixedLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      common::ObDiscreteFormat &vector)
  {
    int ret = OB_SUCCESS;
    const int64_t fix_len = data_locator.len_;
    const char *__restrict fixed_buf = data_locator.fixed_buf_;
    const int32_t *__restrict row_ids = data_locator.row_ids_;
    char **__restrict ptr_arr = vector.get_ptrs();
    uint32_t *__restrict len_arr = reinterpret_cast<uint32_t *>(vector.get_lens());
    if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        const int64_t curr_vec_offset = vec_offset + i;
        ptr_arr[curr_vec_offset] = const_cast<char *>(fixed_buf + row_id * fix_len);
        len_arr[curr_vec_offset] = fix_len;
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        const int64_t curr_vec_offset = vec_offset + i;
        if (data_locator.null_bitmap_->contain(row_id)) {
          vector.set_null(curr_vec_offset);
        } else {
          ptr_arr[curr_vec_offset] = const_cast<char *>(fixed_buf + row_id * fix_len);
          len_arr[curr_vec_offset] = fix_len;
        }
      }
    }
    return ret;
  }
};

// discrete format with fixed length shallow copy dict data
template<typename ValueType, int STORE_TYPE, int NULL_TYPE, typename RefType>
struct LoadVectorDataFunc_T<common::ObDiscreteFormat, ValueType, ObFixedDictDataLocator_T<RefType>,
    STORE_TYPE, 0, D_SHALLOW_COPY,  NULL_TYPE>
{
  static int load_data_to_vector(
      const ObFixedDictDataLocator_T<RefType> &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      common::ObDiscreteFormat &vector)
  {
    int ret = OB_SUCCESS;
    const int64_t fix_len = data_locator.dict_len_;
    const int32_t *__restrict row_ids = data_locator.row_ids_;
    const RefType *__restrict ref_arr = data_locator.ref_arr_;
    const char *__restrict dict_data = data_locator.dict_payload_;
    char **__restrict ptr_arr = vector.get_ptrs();
    uint32_t *__restrict len_arr = reinterpret_cast<uint32_t *>(vector.get_lens());
    if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        const int64_t ref = ref_arr[row_id];
        const int64_t curr_vec_offset = vec_offset + i;
        ptr_arr[curr_vec_offset] = const_cast<char *>(dict_data + ref * fix_len);
        len_arr[curr_vec_offset] = fix_len;
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        const int64_t ref = ref_arr[row_id];
        const int64_t curr_vec_offset = vec_offset + i;
        if (ref == data_locator.dict_cnt_) {
          vector.set_null(curr_vec_offset);
        } else {
          ptr_arr[curr_vec_offset] = const_cast<char *>(dict_data + ref * fix_len);
          len_arr[curr_vec_offset] = fix_len;
        }
      }
    }
    return ret;
  }
};

// Data dependency on offset for continuous format if need to fill data, not recommended
template<typename ValueType, typename DataLocator, int STORE_TYPE, int PACKING_LEN, int DECODE_TYPE, int NULL_TYPE>
struct LoadVectorDataFunc_T<
    common::ObContinuousFormat,
    ValueType,
    DataLocator,
    STORE_TYPE,
    PACKING_LEN,
    DECODE_TYPE,
    NULL_TYPE>
{
  static int load_data_to_vector(
      const DataLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      ObContinuousFormat &vector)
  {
    int ret = OB_SUCCESS;
    // can not shallow copy element-wise to continuous format
    constexpr int CONT_DECODE_TYPE = D_SHALLOW_COPY == DECODE_TYPE ? D_DEEP_COPY : DECODE_TYPE;
    constexpr bool is_fixed_length = STORE_TYPE != ObVecDecodeUtils::VARIABLE_LENGTH;
    constexpr int type_store_size = ObVecDecodeUtils::GetFixedDataLen<STORE_TYPE>::len_;
    uint32_t curr_offset = 0;
    if (0 == vec_offset) {
      vector.get_offsets()[0] = 0;
    } else {
      curr_offset = vector.get_offsets()[vec_offset];
    }
    for (int64_t i = 0; i < row_cap; ++i) {
      const int64_t curr_vec_offset = vec_offset + i;
      const char *vec_ptr = vector.get_data() + curr_offset;
      uint32_t vec_data_len = 0;
      const char *data_ptr = nullptr;
      uint32_t data_len = 0;
      bool is_null = false;
      if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
        data_locator.get_data(i, data_ptr, data_len);
        LoadByteAlignedData_T<ValueType, CONT_DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
            data_ptr, data_len, vec_ptr, vec_data_len);
      } else {
        data_locator.get_data(i, data_ptr, data_len, is_null);
        if (is_null) {
          vector.set_null(curr_vec_offset);
        } else {
          LoadByteAlignedData_T<ValueType, CONT_DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
              data_ptr, data_len, vec_ptr, vec_data_len);
        }
      }
      curr_offset += vec_data_len;
      vector.get_offsets()[curr_vec_offset + 1] = curr_offset;
    }

    return ret;
  }
};

template<typename ValueType, typename DataLocator, int STORE_TYPE, int PACKING_LEN, int DECODE_TYPE, int NULL_TYPE>
struct LoadVectorDataFunc_T<
    common::ObFixedLengthFormat<ValueType>,
    ValueType,
    DataLocator,
    STORE_TYPE,
    PACKING_LEN,
    DECODE_TYPE,
    NULL_TYPE>
{
  static int load_data_to_vector(
      const DataLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      common::ObFixedLengthFormat<ValueType> &vector)
  {
    int ret = OB_SUCCESS;
    constexpr bool is_fixed_length = STORE_TYPE != ObVecDecodeUtils::VARIABLE_LENGTH;
    // shallow copy or var-length data not allowed
    OB_ASSERT(D_SHALLOW_COPY != DECODE_TYPE);
    OB_ASSERT(is_fixed_length);
    constexpr uint32_t type_store_size = ObVecDecodeUtils::GetFixedDataLen<STORE_TYPE>::len_;
    constexpr uint32_t vec_store_size = sizeof(ValueType);

    for (int64_t i = 0; i < row_cap; ++i) {
      const int64_t curr_vec_offset = vec_offset + i;
      const int64_t vector_data_offset = curr_vec_offset * vec_store_size;
      const char *vec_ptr = vector.get_data() + vector_data_offset;
      uint32_t vec_data_len = 0;
      const char *data_ptr = nullptr;
      uint32_t data_len = 0;
      bool is_null = false;
      if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
        data_locator.get_data(i, data_ptr, data_len);
        LoadByteAlignedData_T<ValueType, DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
            data_ptr, data_len, vec_ptr, vec_data_len);
      } else {
        data_locator.get_data(i, data_ptr, data_len, is_null);
        if (is_null) {
          vector.set_null(curr_vec_offset);
        } else {
          LoadByteAlignedData_T<ValueType, DECODE_TYPE, is_fixed_length, PACKING_LEN, type_store_size>::load_byte_aligned_data(
              data_ptr, data_len, vec_ptr, vec_data_len);
        }
      }
    }
    return ret;
  }
};

// fixed-length format with fixed integer type data
template<typename ValueType, int STORE_TYPE, int PACKING_LEN, int NULL_TYPE>
struct LoadVectorDataFromFixedFunc_T
{

  static int load_data_to_vector(
      const DataFixedLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      common::ObFixedLengthFormat<ValueType> &vector)
  {
    int ret = OB_SUCCESS;
    // shallow copy or var-length data not allowed
    OB_ASSERT(STORE_TYPE != ObVecDecodeUtils::VARIABLE_LENGTH && STORE_TYPE != ObVecDecodeUtils::FIXED_OTHER_BYTE);
    OB_ASSERT(data_locator.len_ == PACKING_LEN);
    constexpr uint32_t type_store_size = ObVecDecodeUtils::GetFixedDataLen<STORE_TYPE>::len_;
    constexpr uint32_t vec_store_size = sizeof(ValueType);
    constexpr bool is_signed = ObVecDecodeUtils::IsSignedType<ValueType>::signed_;
    constexpr bool read_as_signed = is_signed && type_store_size == PACKING_LEN;
    typedef typename ObEncodingTypeInference<read_as_signed, ObVecDecodeUtils::GetFixedDataLengthType<PACKING_LEN>::type_>::Type StoreType;
    typedef typename ObEncodingTypeInference<is_signed, ObVecDecodeUtils::GetFixedDataLengthType<vec_store_size>::type_>::Type VecType;
    char *vec_ptr = vector.get_data() + (vec_offset * vec_store_size);
    VecType *__restrict vec_arr = reinterpret_cast<VecType *>(vec_ptr);
    const StoreType *__restrict store_arr = reinterpret_cast<const StoreType *>(data_locator.fixed_buf_);
    const int32_t *__restrict row_ids = data_locator.row_ids_;
    if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        vec_arr[i] = store_arr[row_id];
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        const int64_t curr_vec_offset = vec_offset + i;
        if (data_locator.null_bitmap_->contain(row_id)) {
          vector.set_null(curr_vec_offset);
        } else {
          vec_arr[i] = store_arr[row_id];
        }
      }
    }
    return ret;
  }
};

// fixed-length format with fixed integer type dict data
template<typename ValueType, int STORE_TYPE, int PACKING_LEN, int NULL_TYPE, typename RefType>
struct LoadVectorDataFromFixDictFunc_T
{
  static int load_data_to_vector(
      const ObFixedDictDataLocator_T<RefType> &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      common::ObFixedLengthFormat<ValueType> &vector)
  {
    int ret = OB_SUCCESS;
    // shallow copy or var-length data not allowed
    constexpr uint32_t type_store_size = ObVecDecodeUtils::GetFixedDataLen<STORE_TYPE>::len_;
    constexpr uint32_t vec_store_size = sizeof(ValueType);
    constexpr bool is_signed = ObVecDecodeUtils::IsSignedType<ValueType>::signed_;
    constexpr bool read_as_signed = is_signed && type_store_size == PACKING_LEN;
    typedef typename ObEncodingTypeInference<read_as_signed, ObVecDecodeUtils::GetFixedDataLengthType<PACKING_LEN>::type_>::Type StoreType;
    typedef typename ObEncodingTypeInference<is_signed, ObVecDecodeUtils::GetFixedDataLengthType<vec_store_size>::type_>::Type VecType;
    char *vec_ptr = vector.get_data() + (vec_offset * vec_store_size);
    VecType *__restrict vec_arr = reinterpret_cast<VecType *>(vec_ptr);
    const StoreType *__restrict store_arr = reinterpret_cast<const StoreType *>(data_locator.dict_payload_);
    const int32_t *__restrict row_ids = data_locator.row_ids_;
    const RefType *__restrict ref_arr = data_locator.ref_arr_;
    if (NULL_TYPE == ObVecDecodeUtils::NOT_HAS_NULL) {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        const RefType ref = ref_arr[row_id];
        vec_arr[i] = store_arr[ref];
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        const int64_t row_id = row_ids[i];
        const RefType ref = ref_arr[row_id];
        const int64_t curr_vec_offset = vec_offset + i;
        if (ref == data_locator.dict_cnt_) {
          vector.set_null(curr_vec_offset);
        } else {
          vec_arr[i] = store_arr[ref];
        }
      }
    }
    return ret;
  }
};

#define DEFINE_FIXED_INTEGER_WITH_BYTE_SPEC(byte) \
template<typename ValueType, int STORE_TYPE, int NULL_TYPE> \
struct LoadVectorDataFunc_T<common::ObFixedLengthFormat<ValueType>, ValueType, DataFixedLocator, STORE_TYPE, \
    byte, D_INTEGER, NULL_TYPE> \
{ \
  static int load_data_to_vector( \
      const DataFixedLocator &data_locator, \
      const int64_t row_cap, \
      const int64_t vec_offset, \
      common::ObFixedLengthFormat<ValueType> &vector) \
  { \
    return LoadVectorDataFromFixedFunc_T<ValueType, STORE_TYPE, byte, NULL_TYPE>::load_data_to_vector( \
        data_locator, row_cap, vec_offset, vector); \
  } \
}; \
template<typename ValueType, int STORE_TYPE, int NULL_TYPE, typename RefType> \
struct LoadVectorDataFunc_T<common::ObFixedLengthFormat<ValueType>, ValueType, \
    ObFixedDictDataLocator_T<RefType>, STORE_TYPE, byte, D_INTEGER, NULL_TYPE> \
{ \
  static int load_data_to_vector( \
      const ObFixedDictDataLocator_T<RefType> &data_locator, \
      const int64_t row_cap, \
      const int64_t vec_offset, \
      common::ObFixedLengthFormat<ValueType> &vector) \
  { \
    return LoadVectorDataFromFixDictFunc_T<ValueType, STORE_TYPE, byte, NULL_TYPE, RefType>::load_data_to_vector( \
        data_locator, row_cap, vec_offset, vector); \
  } \
};

DEFINE_FIXED_INTEGER_WITH_BYTE_SPEC(1)
DEFINE_FIXED_INTEGER_WITH_BYTE_SPEC(2)
DEFINE_FIXED_INTEGER_WITH_BYTE_SPEC(4)
DEFINE_FIXED_INTEGER_WITH_BYTE_SPEC(8)

#undef DEFINE_FIXED_INTEGER_WITH_BYTE_SPEC

/*-------------------------------------load vector dispatcher-------------------------------------*/

template<typename VectorType, typename ValueType, typename DataLocator>
ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator> &ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::instance()
{
  static ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator> ret;
  return ret;
}

template<typename VectorType, typename ValueType, typename DataLocator>
ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator> &ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::instance()
{
  static ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator> ret;
  return ret;
}

template<typename VectorType, typename ValueType, typename DataLocator>
ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator> &ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::instance()
{
  static ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator> ret;
  return ret;
}

#define DECLARE_INTEGER_VEC_DISPATCHER(c_type, locator_type) \
template class ObLoadIntegerVecDataDispatcher<ObFixedLengthFormat<c_type>, c_type, locator_type>; \
template class ObLoadIntegerVecDataDispatcher<ObUniformFormat<false>, c_type, locator_type>;

#define DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER(c_type, locator_type) \
template class ObLoadFixByteAlignedVecDataDispatcher<ObFixedLengthFormat<c_type>, c_type, locator_type>; \
template class ObLoadFixByteAlignedVecDataDispatcher<ObUniformFormat<false>, c_type, locator_type>;

#define DECLARE_BYTE_ALIGNED_VAR_VEC_DISPATCHER(locator_type) \
template class ObLoadVarByteAlignedVecDataDispatcher<ObUniformFormat<false>, char[0], locator_type>; \
template class ObLoadVarByteAlignedVecDataDispatcher<ObContinuousFormat, char[0], locator_type>; \
template class ObLoadVarByteAlignedVecDataDispatcher<ObDiscreteFormat, char[0], locator_type>;

#define DECLARE_BYTE_ALIGNED_VEC_DATA_DISPATCHER_LOCATOR(locator_type) \
DECLARE_INTEGER_VEC_DISPATCHER(uint64_t, locator_type) \
DECLARE_INTEGER_VEC_DISPATCHER(int64_t, locator_type) \
DECLARE_INTEGER_VEC_DISPATCHER(uint8_t, locator_type) \
DECLARE_INTEGER_VEC_DISPATCHER(int32_t, locator_type) \
DECLARE_INTEGER_VEC_DISPATCHER(uint32_t, locator_type) \
DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER(ObOTimestampData, locator_type) \
DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER(ObOTimestampTinyData, locator_type) \
DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER(ObIntervalDSValue, locator_type) \
DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER(int128_t, locator_type) \
DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER(int256_t, locator_type) \
DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER(int512_t, locator_type) \
DECLARE_BYTE_ALIGNED_VAR_VEC_DISPATCHER(locator_type)

DECLARE_BYTE_ALIGNED_VEC_DATA_DISPATCHER_LOCATOR(DataDiscreteLocator);
DECLARE_BYTE_ALIGNED_VEC_DATA_DISPATCHER_LOCATOR(DataFixedLocator);
DECLARE_BYTE_ALIGNED_VEC_DATA_DISPATCHER_LOCATOR(DataConstLoactor);
DECLARE_BYTE_ALIGNED_VEC_DATA_DISPATCHER_LOCATOR(ObFixedDictDataLocator_T<uint8_t>);
DECLARE_BYTE_ALIGNED_VEC_DATA_DISPATCHER_LOCATOR(ObFixedDictDataLocator_T<uint16_t>);

typedef ObVarDictDataLocator_T<uint8_t, uint8_t> ObVarDictDataLocator_T_1_1;
typedef ObVarDictDataLocator_T<uint8_t, uint16_t> ObVarDictDataLocator_T_1_2;
typedef ObVarDictDataLocator_T<uint16_t, uint8_t> ObVarDictDataLocator_T_2_1;
typedef ObVarDictDataLocator_T<uint16_t, uint16_t> ObVarDictDataLocator_T_2_2;
DECLARE_BYTE_ALIGNED_VAR_VEC_DISPATCHER(ObVarDictDataLocator_T_1_1);
DECLARE_BYTE_ALIGNED_VAR_VEC_DISPATCHER(ObVarDictDataLocator_T_1_2);
DECLARE_BYTE_ALIGNED_VAR_VEC_DISPATCHER(ObVarDictDataLocator_T_2_1);
DECLARE_BYTE_ALIGNED_VAR_VEC_DISPATCHER(ObVarDictDataLocator_T_2_2);

#undef DECLARE_BYTE_ALIGNED_FIXED_VEC_DISPATCHER
#undef DECLARE_BYTE_ALIGNED_VEC_DATA_DISPATCHER_LOCATOR
#undef DECLARE_BYTE_ALIGNED_VAR_VEC_DISPATCHER


template<typename VectorType, typename ValueType, typename DataLocator>
ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::ObLoadIntegerVecDataDispatcher()
{
  if (!func_array_inited_) {
    func_array_inited_ = ObNDArrayIniter<InitFunc, STORE_TYPE_CNT, PACKING_TYPE_CNT, NULL_TYPE_CNT>::apply();
  }
}


template<typename VectorType, typename ValueType, typename DataLocator>
template<int STORE_TYPE, int PACKING_TYPE_IDX, int NULL_TYPE>
bool ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>
    ::InitFunc<STORE_TYPE, PACKING_TYPE_IDX, NULL_TYPE>::operator()()
{
  if (OTHER_PACKING_TYPE_IDX == PACKING_TYPE_IDX) {
    ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_[STORE_TYPE][PACKING_TYPE_IDX][NULL_TYPE]
      = &(LoadVectorDataFunc_T<VectorType, ValueType, DataLocator, STORE_TYPE, 0, D_INTEGER, NULL_TYPE>::load_data_to_vector);
  } else {
    // for 1, 2, 4, 8 packed byte len, PACKING_TYPE_IDX equals to PACKING_TYPE
    constexpr int PACKING_LEN = ObVecDecodeUtils::GetFixedDataLen<PACKING_TYPE_IDX>::len_;
    ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_[STORE_TYPE][PACKING_TYPE_IDX][NULL_TYPE]
      = &(LoadVectorDataFunc_T<VectorType, ValueType, DataLocator, STORE_TYPE, PACKING_LEN, D_INTEGER, NULL_TYPE>::load_data_to_vector);
  }
  return true;
}

template<typename VectorType, typename ValueType, typename DataLocator>
int ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::load_byte_aligned_vec_data(
    const ObObjType stored_obj_type,
    const int64_t fixed_packing_len,
    const int null_type,
    const DataLocator &data_loactor,
    const int64_t row_cap,
    const int64_t vec_offset,
    VectorType &vector)
{
  int ret = OB_SUCCESS;

  ObObjTypeStoreClass type_sc = get_store_class_map()[ob_obj_type_class(stored_obj_type)];
  const int32_t type_store_size = ObDecimalIntSC == type_sc
      ? sizeof(ValueType)
      : get_type_size_map()[stored_obj_type];
  ObVecDecodeUtils::DataLengthType store_type = ObVecDecodeUtils::get_fixed_length_type(type_store_size);
  ObVecDecodeUtils::DataLengthType length_type = ObVecDecodeUtils::get_fixed_length_type(fixed_packing_len);
  int packing_type_idx = length_type == ObVecDecodeUtils::FIXED_OTHER_BYTE ? OTHER_PACKING_TYPE_IDX : length_type;

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(MAX_PACKING_LEN < fixed_packing_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid packing length for fixed byte aligned vector data dispatcher", K(ret), K(fixed_packing_len));
  } else if (OB_UNLIKELY(type_store_size > sizeof(ValueType))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector value type size less than type store size",
        K(ret), K(sizeof(ValueType)), K(type_store_size));
  } else if (OB_UNLIKELY(ObVecDecodeUtils::IsSignedType<ValueType>::signed_ && type_sc == ObUIntSC)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("decode vector on type converted from unsigned to signed not supported", K(ret), K(stored_obj_type));
  } else {
    ret = func_array_[store_type][packing_type_idx][null_type](data_loactor, row_cap, vec_offset, vector);
  }
  LOG_DEBUG("[Vector decode] load byte aligned integer data to fixed vector", K(ret), K(packing_type_idx), K(store_type),
      K(stored_obj_type), K(packing_type_idx), K(fixed_packing_len), K(vec_offset), K(row_cap), K(sizeof(ValueType)));
  return ret;
}

/*----------------------------------fix byte aligned dispatcher-----------------------------------*/

template<typename VectorType, typename ValueType, typename DataLocator>
ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::ObLoadFixByteAlignedVecDataDispatcher()
{
  if (!func_array_inited_) {
    func_array_inited_ = ObNDArrayIniter<InitFunc, NULL_TYPE_CNT>::apply();
  }
}


template<typename VectorType, typename ValueType, typename DataLocator>
template<int NULL_TYPE>
bool ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>
    ::InitFunc<NULL_TYPE>::operator()()
{
  ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_[NULL_TYPE]
      = &(LoadVectorDataFunc_T<VectorType, ValueType, DataLocator, ObVecDecodeUtils::FIXED_OTHER_BYTE, 0, D_DEEP_COPY, NULL_TYPE>::load_data_to_vector);
  return true;
}

template<typename VectorType, typename ValueType, typename DataLocator>
int ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::load_byte_aligned_vec_data(
    const ObObjType stored_obj_type,
    const int64_t fixed_packing_len,
    const int null_type,
    const DataLocator &data_loactor,
    const int64_t row_cap,
    const int64_t vec_offset,
    VectorType &vector)
{
  int ret = OB_SUCCESS;
  ObEncodingDecodeMetodType decode_method_type = D_MAX_DECODE_METHOD_TYPE;
  ObObjTypeStoreClass type_sc = get_store_class_map()[ob_obj_type_class(stored_obj_type)];
  if (OB_UNLIKELY(ObDecimalIntSC != type_sc && ObOTimestampSC != type_sc && ObIntervalSC != type_sc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type for fixed byte aligned vector data dispatcher", K(ret), K(stored_obj_type));
  } else {
    ret = func_array_[null_type](data_loactor, row_cap, vec_offset, vector);
  }
  return ret;
}


/*----------------------------------var byte aligned dispatcher-----------------------------------*/

template<typename VectorType, typename ValueType, typename DataLocator>
ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::ObLoadVarByteAlignedVecDataDispatcher()
{
  if (!func_array_inited_) {
    func_array_inited_ = ObNDArrayIniter<InitFunc, DECODE_TYPE_CNT, NULL_TYPE_CNT>::apply();
  }
}


template<typename VectorType, typename ValueType, typename DataLocator>
template<int DECODE_TYPE, int NULL_TYPE>
bool ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>
    ::InitFunc<DECODE_TYPE, NULL_TYPE>::operator()()
{
  ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_[DECODE_TYPE][NULL_TYPE]
      = &(LoadVectorDataFunc_T<VectorType, ValueType, DataLocator, ObVecDecodeUtils::VARIABLE_LENGTH, 0, DECODE_TYPE, NULL_TYPE>::load_data_to_vector);
  return true;
}

template<typename VectorType, typename ValueType, typename DataLocator>
int ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::load_byte_aligned_vec_data(
    const ObObjType stored_obj_type,
    const int64_t fixed_packing_len,
    const int null_type,
    const DataLocator &data_loactor,
    const int64_t row_cap,
    const int64_t vec_offset,
    VectorType &vector)
{
  int ret = OB_SUCCESS;
  ObEncodingDecodeMetodType decode_method_type = D_MAX_DECODE_METHOD_TYPE;
  switch (get_store_class_map()[ob_obj_type_class(stored_obj_type)]) {
  case ObDecimalIntSC:
  case ObOTimestampSC:
  case ObIntervalSC: {
    decode_method_type = D_DEEP_COPY;
    break;
  }
  case ObNumberSC:
  case ObStringSC:
  case ObTextSC:
  case ObJsonSC:
  case ObGeometrySC:
  case ObRoaringBitmapSC: {
    decode_method_type = D_SHALLOW_COPY;
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected store class type", K(ret));
  }
  }

  if (OB_SUCC(ret)) {
    ret = func_array_[decode_method_type][null_type](data_loactor, row_cap, vec_offset, vector);
  }
  return ret;
}


/*--------------------------------------ObVecDecodeUtils------------------------------------------*/
template <typename ValueType, typename DataLocator, int DECODE_DATA_TYPE>
int ObVecDecodeUtils::load_byte_aligned_vector(
    const ObObjType stored_obj_type,
    const int64_t fixed_packing_len,
    const int null_type,
    const DataLocator &data_locator,
    const int64_t row_cap,
    const int64_t vec_offset,
    sql::VectorHeader &vec_header)
{
  int ret = OB_SUCCESS;
  VectorFormat vec_format = vec_header.get_format();
  ObIVector *vector = vec_header.get_vector();

  switch (vec_format) {
  case VEC_FIXED: {
    typedef common::ObFixedLengthFormat<ValueType> FixedFormat;
    FixedFormat *fix_vec = static_cast<FixedFormat *>(vector);
    ret = VectorDecodeDispatcherReference<FixedFormat, ValueType, DataLocator, DECODE_DATA_TYPE>::type_
        ::instance().load_byte_aligned_vec_data(stored_obj_type, fixed_packing_len,
            null_type, data_locator, row_cap, vec_offset, *fix_vec);
    break;
  }
  case VEC_DISCRETE: {
    common::ObDiscreteFormat *disc_vec = static_cast<common::ObDiscreteFormat *>(vector);
    ret = VectorDecodeDispatcherReference<common::ObDiscreteFormat, ValueType, DataLocator, DECODE_DATA_TYPE>::type_
        ::instance().load_byte_aligned_vec_data(stored_obj_type, fixed_packing_len,
            null_type, data_locator, row_cap, vec_offset, *disc_vec);
    break;
  }
  case VEC_CONTINUOUS: {
    common::ObContinuousFormat *cont_vec = static_cast<common::ObContinuousFormat *>(vector);
    ret = VectorDecodeDispatcherReference<common::ObContinuousFormat, ValueType, DataLocator, DECODE_DATA_TYPE>::type_
        ::instance().load_byte_aligned_vec_data(stored_obj_type, fixed_packing_len,
            null_type, data_locator, row_cap, vec_offset, *cont_vec);
    break;
  }
  case VEC_UNIFORM: {
    typedef common::ObUniformFormat<false> UniformFormat;
    UniformFormat *uni_vec = static_cast<UniformFormat *>(vector);
    ret = VectorDecodeDispatcherReference<UniformFormat, ValueType, DataLocator, DECODE_DATA_TYPE>::type_
      ::instance().load_byte_aligned_vec_data(stored_obj_type, fixed_packing_len,
          null_type, data_locator, row_cap, vec_offset, *uni_vec);
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector format", K(ret), K(vec_format));
  }
  }
  return ret;
}

template <typename DataLocator>
int ObVecDecodeUtils::load_byte_aligned_vector(
    const ObObjMeta schema_obj_meta,
    const ObObjType stored_obj_type,
    const int64_t fixed_packing_len,
    const int null_type,
    const DataLocator &data_locator,
    const int64_t row_cap,
    const int64_t vec_offset,
    sql::VectorHeader &vec_header)
{
  int ret = OB_SUCCESS;
  const int16_t precision = schema_obj_meta.is_decimal_int() ? schema_obj_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
  VecValueTypeClass vec_tc = common::get_vec_value_tc(
      schema_obj_meta.get_type(),
      schema_obj_meta.get_scale(),
      precision);
  using VarLenTypeValueType = char[0];
  #define LOAD_VEC_BY_TYPE(ctype, decode_method) \
    ret = load_byte_aligned_vector<ctype, DataLocator, decode_method>(stored_obj_type, fixed_packing_len, \
        null_type, data_locator, row_cap, vec_offset, vec_header);
  switch (vec_tc) {
  case VEC_TC_YEAR: {
    // uint8_t
    LOAD_VEC_BY_TYPE(uint8_t, INTEGER_DATA_TYPE);
    break;
  }
  case VEC_TC_DATE:
  case VEC_TC_DEC_INT32: {
    // int32_t
    LOAD_VEC_BY_TYPE(int32_t, INTEGER_DATA_TYPE);
    break;
  }
  case VEC_TC_INTEGER:
  case VEC_TC_DATETIME:
  case VEC_TC_TIME:
  case VEC_TC_UNKNOWN:
  case VEC_TC_INTERVAL_YM:
  case VEC_TC_DEC_INT64: {
    // int64_t
    LOAD_VEC_BY_TYPE(int64_t, INTEGER_DATA_TYPE);
    break;
  }
  case VEC_TC_UINTEGER:
  case VEC_TC_BIT:
  case VEC_TC_ENUM_SET:
  case VEC_TC_DOUBLE:
  case VEC_TC_FIXED_DOUBLE: {
    // uint64_t
    LOAD_VEC_BY_TYPE(uint64_t, INTEGER_DATA_TYPE);
    break;
  }
  case VEC_TC_FLOAT: {
    // float
    LOAD_VEC_BY_TYPE(uint32_t, INTEGER_DATA_TYPE);
    break;
  }
  case VEC_TC_TIMESTAMP_TZ: {
    // ObOTimestampData
    LOAD_VEC_BY_TYPE(ObOTimestampData, MEDIUM_FIXED_DATA_TYPE);
    break;
  }
  case VEC_TC_TIMESTAMP_TINY: {
    // ObOTimestampTinyData
    LOAD_VEC_BY_TYPE(ObOTimestampTinyData, MEDIUM_FIXED_DATA_TYPE);
    break;
  }
  case VEC_TC_INTERVAL_DS: {
    // ObIntervalDSValue
    LOAD_VEC_BY_TYPE(ObIntervalDSValue, MEDIUM_FIXED_DATA_TYPE);
    break;
  }
  case VEC_TC_DEC_INT128: {
    LOAD_VEC_BY_TYPE(int128_t, MEDIUM_FIXED_DATA_TYPE);
    break;
  }
  case VEC_TC_DEC_INT256: {
    LOAD_VEC_BY_TYPE(int256_t, MEDIUM_FIXED_DATA_TYPE);
    break;
  }
  case VEC_TC_DEC_INT512: {
    LOAD_VEC_BY_TYPE(int512_t, MEDIUM_FIXED_DATA_TYPE);
    break;
  }
  default: {
    // var-length types, currently should not rely on ValueType on decode
    LOAD_VEC_BY_TYPE(VarLenTypeValueType, VARIABLE_DATA_TYPE);
  }
  }
  #undef LOAD_VEC_BY_TYPE

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to load byte aligned data to vector", K(ret), K(schema_obj_meta), K(stored_obj_type));
  }
  return ret;
}


}
};
