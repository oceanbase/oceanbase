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

#ifndef OCEANBASE_ENCODING_OB_VECTOR_DECODE_UTIL_H_
#define OCEANBASE_ENCODING_OB_VECTOR_DECODE_UTIL_H_

#include "ob_encoding_query_util.h"
#include "ob_icolumn_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObVecDecodeUtils
{
  enum DataLengthType
  {
    FIXED_1_BYTE = 0,
    FIXED_2_BYTE = 1,
    FIXED_4_BYTE = 2,
    FIXED_8_BYTE = 3,
    FIXED_OTHER_BYTE = 4,
    VARIABLE_LENGTH = 5,
  };

  enum NullDataType
  {
    NOT_HAS_NULL,
    NULL_BITMAP,
    NULL_REPLACED_VALUE,
  };

  template <int DATA_LEN_TYPE>
  struct GetFixedDataLen { constexpr static int len_ = 0; };
  template <> struct GetFixedDataLen<FIXED_1_BYTE> { constexpr static int len_ = 1; };
  template <> struct GetFixedDataLen<FIXED_2_BYTE> { constexpr static int len_ = 2; };
  template <> struct GetFixedDataLen<FIXED_4_BYTE> { constexpr static int len_ = 4; };
  template <> struct GetFixedDataLen<FIXED_8_BYTE> { constexpr static int len_ = 8; };

  OB_INLINE static DataLengthType get_fixed_length_type(int length)
  {
    static DataLengthType fixed_length_type_map[] = {
      FIXED_OTHER_BYTE, // 0
      FIXED_1_BYTE, // 1
      FIXED_2_BYTE, // 2
      FIXED_OTHER_BYTE,
      FIXED_4_BYTE, // 4
      FIXED_OTHER_BYTE,
      FIXED_OTHER_BYTE,
      FIXED_OTHER_BYTE,
      FIXED_8_BYTE // 8
    };
    OB_ASSERT(length <= 8);
    return fixed_length_type_map[length];
  }

  template <int DATA_LEN>
  struct GetFixedDataLengthType { constexpr static int type_ = FIXED_OTHER_BYTE; };
  template <> struct GetFixedDataLengthType<1> { constexpr static int type_ = FIXED_1_BYTE; };
  template <> struct GetFixedDataLengthType<2> { constexpr static int type_ = FIXED_2_BYTE; };
  template <> struct GetFixedDataLengthType<4> { constexpr static int type_ = FIXED_4_BYTE; };
  template <> struct GetFixedDataLengthType<8> { constexpr static int type_ = FIXED_8_BYTE; };

  template <int TYPE_STORE_SIZE>
  struct GetReverseIntegerMask { constexpr static uint64_t mask_ = 0; };
  template <> struct GetReverseIntegerMask<0> { constexpr static uint64_t mask_ = ~((uint64_t)0x0); };
  template <> struct GetReverseIntegerMask<1> { constexpr static uint64_t mask_ = ~((uint64_t)0xff); };
  template <> struct GetReverseIntegerMask<2> { constexpr static uint64_t mask_ = ~((uint64_t)0xffff); };
  template <> struct GetReverseIntegerMask<3> { constexpr static uint64_t mask_ = ~((uint64_t)0xffffff); };
  template <> struct GetReverseIntegerMask<4> { constexpr static uint64_t mask_ = ~((uint64_t)0xffffffff); };
  template <> struct GetReverseIntegerMask<5> { constexpr static uint64_t mask_ = ~((uint64_t)0xffffffffff); };
  template <> struct GetReverseIntegerMask<6> { constexpr static uint64_t mask_ = ~((uint64_t)0xffffffffffff); };
  template <> struct GetReverseIntegerMask<7> { constexpr static uint64_t mask_ = ~((uint64_t)0xffffffffffffff); };
  template <> struct GetReverseIntegerMask<8> { constexpr static uint64_t mask_ = ~((uint64_t)0xffffffffffffffff); };

  template <typename VecValueType>
  struct IsSignedType { constexpr static bool signed_ = false; };
  template <> struct IsSignedType<int8_t> { constexpr static bool signed_ = true; };
  template <> struct IsSignedType<int16_t> { constexpr static bool signed_ = true; };
  template <> struct IsSignedType<int32_t> { constexpr static bool signed_ = true; };
  template <> struct IsSignedType<int64_t> { constexpr static bool signed_ = true; };


  static constexpr int MAX_FAST_DECODE_FIXED_PACK_LEN = 8;
  static constexpr int VAR_PACKING_LEN_ARGUMENT = 0;

  template <typename DataLocator>
  static int load_byte_aligned_vector(
      const ObObjMeta schema_obj_meta,
      const ObObjType stored_obj_type,
      const int64_t fixed_packing_len,
      const int null_type,
      const DataLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      sql::VectorHeader &vec_header);

  template <typename ValueType, typename DataLocator, int DECODE_DATA_TYPE>
  static int load_byte_aligned_vector(
      const ObObjType stored_obj_type,
      const int64_t fixed_packing_len,
      const int null_type,
      const DataLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      sql::VectorHeader &vec_header);
};

template<typename ValueType, int PACKING_LEN>
struct BitUnpackData_T
{
  static int bit_unpack_data(
      const char * data,
      const int64_t offset,
      const int64_t cnt,
      ValueType &unpacked_data)
  {
    // TODO: implement ?
  }
};

struct DataDiscreteLocator
{
  explicit DataDiscreteLocator(const char **&cell_datas, uint32_t *&cell_lens)
    : cell_datas_(cell_datas), cell_lens_(cell_lens) {}
  ~DataDiscreteLocator() = default;
  inline void get_data(const int64_t idx, const char *&__restrict data, uint32_t &__restrict len) const
  {
    data = cell_datas_[idx];
    len = cell_lens_[idx];
  }
  inline void get_data(const int64_t idx, const char *&__restrict data, uint32_t &__restrict len, bool &__restrict is_null) const
  {
    data = cell_datas_[idx];
    len = cell_lens_[idx];
    is_null = nullptr == cell_datas_[idx];
  }
  const char **__restrict cell_datas_;
  uint32_t *__restrict cell_lens_;
};

struct DataFixedLocator
{
  explicit DataFixedLocator(const int32_t *&row_ids, const char *&fixed_buf, const int64_t len, const void *null_bitmap)
    : row_ids_(row_ids), fixed_buf_(fixed_buf), len_(len)
  {
    null_bitmap_ = nullptr == null_bitmap ? nullptr : sql::to_bit_vector(null_bitmap);
  }
  ~DataFixedLocator() = default;
  inline void get_data(const int64_t idx, const char *&__restrict data, uint32_t &__restrict len) const
  {
    data = fixed_buf_ + row_ids_[idx] * len_;
    len = len_;
  }
  inline void get_data(const int64_t idx, const char *&__restrict data, uint32_t &__restrict len, bool &__restrict is_null) const
  {
    get_data(idx, data, len);
    is_null = null_bitmap_->contain(row_ids_[idx]);
  }

  const int32_t *__restrict row_ids_;
  const char *__restrict fixed_buf_;
  const int64_t len_;
  const sql::ObBitVector *__restrict null_bitmap_;
};

struct DataConstLoactor
{
  explicit DataConstLoactor(const char *&const_buf, const int64_t len)
    : const_buf_(const_buf), len_(len) {}
  ~DataConstLoactor() = default;
  inline void get_data(const int64_t idx, const char *&__restrict data, uint32_t &__restrict len) const
  {
    data = const_buf_;
    len = len_;
  }
  inline void get_data(const int64_t idx, const char *&__restrict data, uint32_t &__restrict len, bool &__restrict is_null) const
  {
    get_data(idx, data, len);
    is_null = false;
  }
  const char *__restrict const_buf_;
  const int64_t len_;
};

/**
 * @brief load data to vector interface
 *
 * @tparam VectorType: specific type for vector to fill
 * @tparam ValueType: value type to fill in vector
 * @tparam DataLocator: encoding data locator type
 * @tparam STORE_TYPE: stored data packing type in encoding format
 * @tparam PACKING_LEN: data packing length type
 * @tparam DECODE_TYPE: method to fill data to vector
 * @tparam NULL_TYPE : whether data to decode might contain null value, and null representation
 */
template <typename VectorType, typename ValueType, typename DataLocator, int STORE_TYPE, int PACKING_LEN, int DECODE_TYPE, int NULL_TYPE>
struct LoadVectorDataFunc_T
{
  static int load_data_to_vector(
      const DataLocator &data_locator,
      const int64_t row_cap,
      const int64_t vec_offset,
      VectorType &vector)
  {
    int ret = OB_NOT_IMPLEMENT;
    static_assert(std::is_base_of<common::ObIVector, VectorType>::value, "VectorType must inherit from ObIVector");
    STORAGE_LOG(ERROR, "not implemented load_data_to_vector func", K(ret),
        K(STORE_TYPE), K(PACKING_LEN), K(DECODE_TYPE), K(NULL_TYPE));
    return ret;
  }
};

/**
 * @brief load byte aligned data
 *
 * @tparam DECODE_TYPE
 * @tparam IS_FIXED_LENGTH
 * @tparam PACKING_LEN
 * @tparam TYPE_STORE_SIZE
 */
template <typename ValueType, int DECODE_TYPE, bool IS_FIXED_LENGTH, int PACKING_LEN, int TYPE_STORE_SIZE>
struct LoadByteAlignedData_T
{
  // TODO the constness of parameter const char *&vec_ptr should be refined
  static void load_byte_aligned_data(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    int ret = OB_NOT_IMPLEMENT;
    static_assert(PACKING_LEN <= ObVecDecodeUtils::MAX_FAST_DECODE_FIXED_PACK_LEN, "Invalid PACKING_LEN");
    STORAGE_LOG(ERROR, "not implemented ", K(ret));
  }
};

template<typename VectorType, typename ValueType, typename DataLocator>
class ObLoadIntegerVecDataDispatcher final
{
public:
  static constexpr uint32_t STORE_TYPE_CNT = 5; // variable length not included
  static constexpr uint32_t MAX_PACKING_LEN = ObVecDecodeUtils::MAX_FAST_DECODE_FIXED_PACK_LEN > sizeof(ValueType)
      ? ObVecDecodeUtils::MAX_FAST_DECODE_FIXED_PACK_LEN
      : sizeof(ValueType);
  static_assert(MAX_PACKING_LEN <= sizeof(uint64_t), "for integer less than 8bytes");
  static constexpr uint32_t PACKING_TYPE_CNT = ObVecDecodeUtils::GetFixedDataLengthType<MAX_PACKING_LEN>::type_ + 2;
  static constexpr uint32_t OTHER_PACKING_TYPE_IDX = PACKING_TYPE_CNT - 1;
  static constexpr uint32_t NULL_TYPE_CNT = 2;
public:
  static ObLoadIntegerVecDataDispatcher &instance();
  int load_byte_aligned_vec_data(
      const ObObjType stored_obj_type,
      const int64_t fixed_packing_len,
      const int null_type,
      const DataLocator &data_loactor,
      const int64_t row_cap,
      const int64_t vec_offset,
      VectorType &vector);
private:
  ObLoadIntegerVecDataDispatcher();
  ~ObLoadIntegerVecDataDispatcher() = default;
  template <int STORE_TYPE, int PACKING_TYPE_IDX, int NULL_TYPE>
  struct InitFunc
  {
    bool operator()();
  };
  DISALLOW_COPY_AND_ASSIGN(ObLoadIntegerVecDataDispatcher);
public:
  using LoadFunc = int (*)(
      const DataLocator &data_loactor,
      const int64_t row_cap,
      const int64_t vec_offset,
      VectorType &vector);
  static ObMultiDimArray_T<LoadFunc, STORE_TYPE_CNT, PACKING_TYPE_CNT, NULL_TYPE_CNT> func_array_;
  static bool func_array_inited_;
};

template<typename VectorType, typename ValueType, typename DataLocator>
bool ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_inited_ = false;

template<typename VectorType, typename ValueType, typename DataLocator>
ObMultiDimArray_T<typename ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::LoadFunc,
    ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::STORE_TYPE_CNT,
    ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::PACKING_TYPE_CNT,
    ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::NULL_TYPE_CNT
    >ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_;


template<typename VectorType, typename ValueType, typename DataLocator>
class ObLoadFixByteAlignedVecDataDispatcher final
{
public:
  static constexpr uint32_t NULL_TYPE_CNT = 2;
public:
  static ObLoadFixByteAlignedVecDataDispatcher &instance();
  int load_byte_aligned_vec_data(
      const ObObjType stored_obj_type,
      const int64_t fixed_packing_len,
      const int null_type,
      const DataLocator &data_loactor,
      const int64_t row_cap,
      const int64_t vec_offset,
      VectorType &vector);
private:
  ObLoadFixByteAlignedVecDataDispatcher();
  ~ObLoadFixByteAlignedVecDataDispatcher() = default;
  template <int NULL_TYPE>
  struct InitFunc
  {
    bool operator()();
  };
  DISALLOW_COPY_AND_ASSIGN(ObLoadFixByteAlignedVecDataDispatcher);
public:
  using LoadFunc = int (*)(
      const DataLocator &data_loactor,
      const int64_t row_cap,
      const int64_t vec_offset,
      VectorType &vector);
  static ObMultiDimArray_T<LoadFunc,  NULL_TYPE_CNT> func_array_;
  static bool func_array_inited_;
};

template<typename VectorType, typename ValueType, typename DataLocator>
bool ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_inited_ = false;

template<typename VectorType, typename ValueType, typename DataLocator>
ObMultiDimArray_T<typename ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::LoadFunc,
    ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::NULL_TYPE_CNT
    >ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_;


template<typename VectorType, typename ValueType, typename DataLocator>
class ObLoadVarByteAlignedVecDataDispatcher final
{
public:
  static constexpr uint32_t DECODE_TYPE_CNT = D_SHALLOW_COPY + 1;
  static constexpr uint32_t NULL_TYPE_CNT = 2;
public:
  static ObLoadVarByteAlignedVecDataDispatcher &instance();
  int load_byte_aligned_vec_data(
      const ObObjType stored_obj_type,
      const int64_t fixed_packing_len,
      const int null_type,
      const DataLocator &data_loactor,
      const int64_t row_cap,
      const int64_t vec_offset,
      VectorType &vector);
private:
  ObLoadVarByteAlignedVecDataDispatcher();
  ~ObLoadVarByteAlignedVecDataDispatcher() = default;
  template <int DECODE_TYPE, int NULL_TYPE>
  struct InitFunc
  {
    bool operator()();
  };
  DISALLOW_COPY_AND_ASSIGN(ObLoadVarByteAlignedVecDataDispatcher);
public:
  using LoadFunc = int (*)(
      const DataLocator &data_loactor,
      const int64_t row_cap,
      const int64_t vec_offset,
      VectorType &vector);
  static ObMultiDimArray_T<LoadFunc, DECODE_TYPE_CNT, NULL_TYPE_CNT> func_array_;
  static bool func_array_inited_;
};

template<typename VectorType, typename ValueType, typename DataLocator>
bool ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_inited_ = false;

template<typename VectorType, typename ValueType, typename DataLocator>
ObMultiDimArray_T<typename ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::LoadFunc,
    ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::DECODE_TYPE_CNT,
    ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::NULL_TYPE_CNT
    >ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator>::func_array_;



enum DecodeVectorDataType
{
  INTEGER_DATA_TYPE = 0, // SMALL FIXED DATA TYPE
  MEDIUM_FIXED_DATA_TYPE = 1,
  VARIABLE_DATA_TYPE = 2
};
// TODO: dispatcher type should not be binded by decode method, but vector / data type, as a independent attribute
template <typename VectorType, typename ValueType, typename DataLocator, int DECODE_DATA_TYPE>
struct VectorDecodeDispatcherReference { typedef std::nullptr_t type_; };

template <typename VectorType, typename ValueType, typename DataLocator>
struct VectorDecodeDispatcherReference<VectorType, ValueType, DataLocator, INTEGER_DATA_TYPE>
{ typedef ObLoadIntegerVecDataDispatcher<VectorType, ValueType, DataLocator> type_; };
template <typename VectorType, typename ValueType, typename DataLocator>
struct VectorDecodeDispatcherReference<VectorType, ValueType, DataLocator, MEDIUM_FIXED_DATA_TYPE>
{ typedef ObLoadFixByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator> type_; };
template <typename VectorType, typename ValueType, typename DataLocator>
struct VectorDecodeDispatcherReference<VectorType, ValueType, DataLocator, VARIABLE_DATA_TYPE>
{ typedef ObLoadVarByteAlignedVecDataDispatcher<VectorType, ValueType, DataLocator> type_; };


/*-----------------------------------load_byte_aligned_integer------------------------------------*/

template <int PACKING_LEN, int TYPE_STORE_SIZE, int VEC_SIZE, bool IS_SIGNED_INTEGER_TYPE>
struct LoadByteTypeAlignedInteger_T
{
  static inline void load_byte_type_aligned_integer(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    static_assert(VEC_SIZE != 0, "Vector size can not be zero");
    constexpr bool read_as_signed_data = IS_SIGNED_INTEGER_TYPE && TYPE_STORE_SIZE == PACKING_LEN;
    typedef typename ObEncodingTypeInference<read_as_signed_data, ObVecDecodeUtils::GetFixedDataLengthType<PACKING_LEN>::type_>::Type StoreType;
    typedef typename ObEncodingTypeInference<IS_SIGNED_INTEGER_TYPE, ObVecDecodeUtils::GetFixedDataLengthType<VEC_SIZE>::type_>::Type VecType;
    *reinterpret_cast<VecType *>(const_cast<char *>(vec_ptr)) = *reinterpret_cast<const StoreType *>(cell_data);
    vec_len = sizeof(VecType);
  }
};

template<int PACKING_LEN, int TYPE_STORE_SIZE, int VEC_SIZE, bool IS_SIGNED_INTEGER_TYPE>
struct LoadByteAlignedInteger_T
{
  static inline void load_byte_aligned_integer(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    constexpr bool use_template_packing_len = PACKING_LEN <= ObVecDecodeUtils::MAX_FAST_DECODE_FIXED_PACK_LEN && PACKING_LEN != 0;
    constexpr bool might_contain_signed_data = IS_SIGNED_INTEGER_TYPE && (TYPE_STORE_SIZE == PACKING_LEN || PACKING_LEN == 0);
    constexpr uint64_t reverse_mask = ObVecDecodeUtils::GetReverseIntegerMask<TYPE_STORE_SIZE>::mask_;
    uint64_t value = 0;
    if (use_template_packing_len) {
      MEMCPY(&value, cell_data, PACKING_LEN);
    } else {
      MEMCPY(&value, cell_data, cell_len);
    }

    // TODO: optimize this complement code operation if possible
    if (might_contain_signed_data) {
      if (0 != reverse_mask && (value & (reverse_mask >> 1))) {
        value |= reverse_mask;
      }
    }

    MEMCPY(const_cast<char *>(vec_ptr), &value, VEC_SIZE);
    vec_len = VEC_SIZE;
  }
};

template<int TYPE_STORE_SIZE, int VEC_SIZE, bool IS_SIGNED_INTEGER_TYPE>
struct LoadByteAlignedInteger_T<1, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>
{
  static inline void load_byte_aligned_integer(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    LoadByteTypeAlignedInteger_T<1, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>::load_byte_type_aligned_integer(
      cell_data, cell_len, vec_ptr, vec_len);
  }
};

template<int TYPE_STORE_SIZE, int VEC_SIZE, bool IS_SIGNED_INTEGER_TYPE>
struct LoadByteAlignedInteger_T<2, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>
{
  static inline void load_byte_aligned_integer(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    LoadByteTypeAlignedInteger_T<2, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>::load_byte_type_aligned_integer(
        cell_data, cell_len, vec_ptr, vec_len);
  }
};

template<int TYPE_STORE_SIZE, int VEC_SIZE, bool IS_SIGNED_INTEGER_TYPE>
struct LoadByteAlignedInteger_T<4, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>
{
  static inline void load_byte_aligned_integer(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    LoadByteTypeAlignedInteger_T<4, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>::load_byte_type_aligned_integer(
        cell_data, cell_len, vec_ptr, vec_len);
  }
};

template<int TYPE_STORE_SIZE, int VEC_SIZE, bool IS_SIGNED_INTEGER_TYPE>
struct LoadByteAlignedInteger_T<8, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>
{
  static inline void load_byte_aligned_integer(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    LoadByteTypeAlignedInteger_T<8, TYPE_STORE_SIZE, VEC_SIZE, IS_SIGNED_INTEGER_TYPE>::load_byte_type_aligned_integer(
        cell_data, cell_len, vec_ptr, vec_len);
  }
};




/*-------------------------------------load_byte_aligned_data-------------------------------------*/


template<typename ValueType, bool IS_FIXED_LENGTH, int PACKING_LEN, int TYPE_STORE_SIZE>
struct LoadByteAlignedData_T<ValueType, D_INTEGER, IS_FIXED_LENGTH, PACKING_LEN, TYPE_STORE_SIZE>
{
  static inline void load_byte_aligned_data(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    // we regard signed attribute of store type as same with value type,
    // since type conversion from unsigned data to signed data is not allowed now
    static_assert(IS_FIXED_LENGTH == true, "need to be fixed length for integer data");
    LoadByteAlignedInteger_T<
        PACKING_LEN,
        TYPE_STORE_SIZE,
        sizeof(ValueType),
        ObVecDecodeUtils::IsSignedType<ValueType>::signed_>::load_byte_aligned_integer(cell_data, cell_len, vec_ptr, vec_len);
  }
};


template<typename ValueType, bool IS_FIXED_LENGTH, int PACKING_LEN, int TYPE_STORE_SIZE>
struct LoadByteAlignedData_T<ValueType, D_DEEP_COPY, IS_FIXED_LENGTH, PACKING_LEN, TYPE_STORE_SIZE>
{
  static inline void load_byte_aligned_data(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    constexpr bool use_template_packing_len = IS_FIXED_LENGTH
        && PACKING_LEN <= ObVecDecodeUtils::MAX_FAST_DECODE_FIXED_PACK_LEN
        && PACKING_LEN != 0;
    if (use_template_packing_len) {
      MEMCPY(const_cast<char *>(vec_ptr), cell_data, PACKING_LEN);
      vec_len = PACKING_LEN;
    } else if (std::is_same<ValueType, ObOTimestampData>::value
        || std::is_same<ValueType, ObIntervalDSValue>::value
        || std::is_same<ValueType, ObOTimestampTinyData>::value) {
      MEMCPY(const_cast<char *>(vec_ptr), cell_data, sizeof(ValueType));
      vec_len = sizeof(ValueType);
    } else {
      MEMCPY(const_cast<char *>(vec_ptr), cell_data, cell_len);
      vec_len = cell_len;
    }
  }
};

template<typename ValueType, bool IS_FIXED_LENGTH, int PACKING_LEN, int TYPE_STORE_SIZE>
struct LoadByteAlignedData_T<ValueType, D_SHALLOW_COPY, IS_FIXED_LENGTH, PACKING_LEN, TYPE_STORE_SIZE>
{
  static inline void load_byte_aligned_data(
      const char *__restrict cell_data,
      const uint32_t cell_len,
      const char *&__restrict vec_ptr,
      uint32_t &__restrict vec_len)
  {
    constexpr bool use_template_packing_len = IS_FIXED_LENGTH
        && PACKING_LEN <= ObVecDecodeUtils::MAX_FAST_DECODE_FIXED_PACK_LEN
        && PACKING_LEN != 0;
    vec_ptr = cell_data;
    if (use_template_packing_len) {
      vec_len = PACKING_LEN;
    } else {
      vec_len = cell_len;
    }
  }
};





} // namesapce blocksstable
} // namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_VECTOR_DECODE_UTIL_H_
