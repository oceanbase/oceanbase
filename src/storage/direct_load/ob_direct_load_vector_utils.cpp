/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/ob_uniform_vector.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace sql;

int ObDirectLoadVectorUtils::new_vector(VectorFormat format, VecValueTypeClass value_tc,
                                        ObIAllocator &allocator, ObIVector *&vector)
{
  int ret = OB_SUCCESS;
  vector = nullptr;
  switch (format) {
    case VEC_FIXED: {
      switch (value_tc) {
#define FIXED_VECTOR_INIT_SWITCH(value_tc)                                                        \
  case value_tc: {                                                                                \
    using VecType = RTVectorType<VEC_FIXED, value_tc>;                                            \
    static_assert(sizeof(RTVectorType<VEC_FIXED, value_tc>) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");                                  \
    vector = OB_NEWx(VecType, &allocator, nullptr, nullptr);                                      \
    break;                                                                                        \
  }
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATE);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIME);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_BIT);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATETIME);
        FIXED_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATE);
#undef FIXED_VECTOR_INIT_SWITCH
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected fixed vector value type class", KR(ret), K(value_tc));
          break;
      }
      break;
    }
    case VEC_CONTINUOUS: {
      switch (value_tc) {
#define CONTINUOUS_VECTOR_INIT_SWITCH(value_tc)                         \
  case value_tc: {                                                      \
    using VecType = RTVectorType<VEC_CONTINUOUS, value_tc>;             \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
    vector = OB_NEWx(VecType, &allocator, nullptr, nullptr, nullptr);   \
    break;                                                              \
  }
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_STRING);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_RAW);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_LOB);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_JSON);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_GEO);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_UDT);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_COLLECTION);
        CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
#undef CONTINUOUS_VECTOR_INIT_SWITCH
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected continuous vector value type class", KR(ret), K(value_tc));
          break;
      }
      break;
    }
    case VEC_DISCRETE: {
      switch (value_tc) {
#define DISCRETE_VECTOR_INIT_SWITCH(value_tc)                           \
  case value_tc: {                                                      \
    using VecType = RTVectorType<VEC_DISCRETE, value_tc>;               \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
    vector = OB_NEWx(VecType, &allocator, nullptr, nullptr, nullptr);   \
    break;                                                              \
  }
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_STRING);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_RAW);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_LOB);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_JSON);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_GEO);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_UDT);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_COLLECTION);
        DISCRETE_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
#undef DISCRETE_VECTOR_INIT_SWITCH
        default:
          ret = OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "unexpected vector value type class", KR(ret), K(value_tc));
          break;
      }
      break;
    }
    case VEC_UNIFORM: {
      ObEvalInfo *eval_info = nullptr;
      if (OB_ISNULL(eval_info = OB_NEWx(ObEvalInfo, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObEvalInfo", KR(ret));
      } else {
        MEMSET(eval_info, 0, sizeof(ObEvalInfo));
        switch (value_tc) {
#define UNIFORM_VECTOR_INIT_SWITCH(value_tc)                            \
  case value_tc: {                                                      \
    using VecType = RTVectorType<VEC_UNIFORM, value_tc>;                \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
    vector = OB_NEWx(VecType, &allocator, nullptr, eval_info);          \
    break;                                                              \
  }
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_NULL);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DATE);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_TIME);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_BIT);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_STRING);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_RAW);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_LOB);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_JSON);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_GEO);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_UDT);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_COLLECTION);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATETIME);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATE);
          UNIFORM_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
#undef UNIFORM_VECTOR_INIT_SWITCH
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector value type class", KR(ret), K(value_tc));
            break;
        }
      }
      break;
    }
    case VEC_UNIFORM_CONST: {
      ObEvalInfo *eval_info = nullptr;
      if (OB_ISNULL(eval_info = OB_NEWx(ObEvalInfo, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObEvalInfo", KR(ret));
      } else {
        MEMSET(eval_info, 0, sizeof(ObEvalInfo));
        switch (value_tc) {
#define UNIFORM_CONST_VECTOR_INIT_SWITCH(value_tc)                      \
  case value_tc: {                                                      \
    using VecType = RTVectorType<VEC_UNIFORM_CONST, value_tc>;          \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
    vector = OB_NEWx(VecType, &allocator, nullptr, eval_info);          \
    break;                                                              \
  }
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_NULL);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DATE);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_TIME);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_BIT);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_STRING);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_RAW);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_LOB);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_JSON);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_GEO);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_UDT);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_COLLECTION);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATETIME);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATE);
          UNIFORM_CONST_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
#undef UNIFORM_CONST_VECTOR_INIT_SWITCH
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector value type class", KR(ret), K(value_tc));
            break;
        }
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), K(format));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(vector)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc vector", KR(ret));
  }
  return ret;
}

int ObDirectLoadVectorUtils::prepare_vector(ObIVector *vector, const int64_t max_batch_size,
                                            ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vector || max_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vector), K(max_batch_size));
  } else {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        const ObLength length = fixed_vec->get_length();
        const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
        const int64_t data_size = length * max_batch_size;
        ObBitVector *nulls = nullptr;
        char *data = nullptr;
        if (OB_ISNULL(nulls = to_bit_vector(allocator.alloc(nulls_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
        } else if (OB_ISNULL(data = static_cast<char *>(allocator.alloc(data_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(data_size));
        } else {
          nulls->reset(max_batch_size);
          fixed_vec->set_nulls(nulls);
          fixed_vec->set_data(data);
        }
        break;
      }
      case VEC_CONTINUOUS: {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
        const int64_t offsets_size = sizeof(uint32_t) * max_batch_size;
        ObBitVector *nulls = nullptr;
        uint32_t *offsets = nullptr;
        if (OB_ISNULL(nulls = to_bit_vector(allocator.alloc(nulls_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
        } else if (OB_ISNULL(offsets = static_cast<uint32_t *>(allocator.alloc(offsets_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(offsets_size));
        } else {
          nulls->reset(max_batch_size);
          continuous_vec->set_nulls(nulls);
          continuous_vec->set_offsets(offsets);
        }
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
        const int64_t lens_size = sizeof(int32_t) * max_batch_size;
        const int64_t ptrs_size = sizeof(char *) * max_batch_size;
        ObBitVector *nulls = nullptr;
        int32_t *lens = nullptr;
        char **ptrs = nullptr;
        if (OB_ISNULL(nulls = to_bit_vector(allocator.alloc(nulls_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
        } else if (OB_ISNULL(lens = static_cast<int32_t *>(allocator.alloc(lens_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(lens_size));
        } else if (OB_ISNULL(ptrs = static_cast<char **>(allocator.alloc(ptrs_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(ptrs_size));
        } else {
          nulls->reset(max_batch_size);
          discrete_vec->set_nulls(nulls);
          discrete_vec->set_lens(lens);
          discrete_vec->set_ptrs(ptrs);
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        const int64_t datums_size = sizeof(ObDatum) * max_batch_size;
        ObDatum *datums = nullptr;
        if (OB_ISNULL(datums = static_cast<ObDatum *>(allocator.alloc(datums_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(datums_size));
        } else {
          uniform_vec->set_datums(datums);
        }
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        const int64_t datums_size = sizeof(ObDatum);
        ObDatum *datums = nullptr;
        if (OB_ISNULL(datums = static_cast<ObDatum *>(allocator.alloc(datums_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(datums_size));
        } else {
          uniform_vec->set_datums(datums);
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", KR(ret), K(format));
        break;
    }
  }
  return ret;
}

namespace
{
inline static void shallow_copy_bitmap_null_vector_base(ObBitmapNullVectorBase *src_vec,
                                                        ObBitmapNullVectorBase *dest_vec,
                                                        const int64_t batch_size)
{
  ObBitVector *nulls = dest_vec->get_nulls();
  uint16_t flag = src_vec->get_flag();
  nulls->deep_copy(*src_vec->get_nulls(), 0, batch_size);
  dest_vec->from(nulls, flag);
}

template <typename SRC_VEC, typename DEST_VEC>
inline int shallow_copy_vector_impl(SRC_VEC *src_vec, DEST_VEC *dest_vec, const int64_t batch_size);

// VEC_CONTINUOUS -> VEC_DISCRETE
template <>
inline int shallow_copy_vector_impl(ObContinuousBase *src_vec,
                                    ObDiscreteBase *dest_vec,
                                    const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  shallow_copy_bitmap_null_vector_base(src_vec, dest_vec, batch_size);
  uint32_t *offsets = src_vec->get_offsets();
  char *data = src_vec->get_data();
  ObLength *lens = dest_vec->get_lens();
  char **ptrs = dest_vec->get_ptrs();
  for (int64_t i = 0; i < batch_size; ++i) {
    ptrs[i] = data + offsets[i];
    lens[i] = offsets[i + 1] - offsets[i];
  }
  return ret;
}

// VEC_DISCRETE -> VEC_DISCRETE
template <>
inline int shallow_copy_vector_impl(ObDiscreteBase *src_vec,
                                    ObDiscreteBase *dest_vec,
                                    const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  shallow_copy_bitmap_null_vector_base(src_vec, dest_vec, batch_size);
  ObLength *src_lens = src_vec->get_lens();
  char **src_ptrs = src_vec->get_ptrs();
  ObLength *dest_lens = dest_vec->get_lens();
  char **dest_ptrs = dest_vec->get_ptrs();
  MEMCPY(dest_lens, src_lens, sizeof(ObLength) * batch_size);
  MEMCPY(dest_ptrs, src_ptrs, sizeof(char *) * batch_size);
  return ret;
}

// VEC_UNIFORM -> VEC_DISCRETE
// VEC_UNIFORM -> VEC_UNIFORM
// VEC_UNIFORM_CONST-> VEC_DISCRETE
// VEC_UNIFORM_CONST-> VEC_UNIFORM_CONST
template <bool IS_CONST>
inline int shallow_copy_vector_impl(ObUniformBase *src_vec,
                                    ObDiscreteBase *dest_vec,
                                    const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = src_vec->get_datums();
  char **ptrs = dest_vec->get_ptrs();
  ObLength *lens = dest_vec->get_lens();
  dest_vec->reset_flag();
  dest_vec->get_nulls()->reset(batch_size);
  for (int64_t i = 0; i < batch_size; ++i) {
    const ObDatum &datum = datums[IS_CONST ? 0 : i];
    if (datum.is_null()) {
      dest_vec->set_null(i);
    } else {
      ptrs[i] = const_cast<char *>(datum.ptr_);
      lens[i] = datum.len_;
    }
  }
  return ret;
}
template <bool IS_CONST>
inline int shallow_copy_vector_impl(ObUniformBase *src_vec,
                                    ObUniformBase *dest_vec,
                                    const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatum *src_datums = src_vec->get_datums();
  ObDatum *dest_datums = dest_vec->get_datums();
  *dest_vec->get_eval_info() = *src_vec->get_eval_info();
  MEMCPY(dest_datums, src_datums, sizeof(ObDatum) * (IS_CONST ? 1 : batch_size));
  return ret;
}
} // namespace

int ObDirectLoadVectorUtils::shallow_copy_vector(ObIVector *src_vec,
                                                 ObIVector *dest_vec,
                                                 const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == src_vec || nullptr == dest_vec || batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(src_vec), KP(dest_vec), K(batch_size));
  } else {
    const VectorFormat src_format = src_vec->get_format();
    const VectorFormat dest_format = dest_vec->get_format();
    switch (src_format) {
      // VEC_CONTINUOUS -> VEC_DISCRETE
      case VEC_CONTINUOUS: {
        switch (dest_format) {
          case VEC_DISCRETE:
            ret = shallow_copy_vector_impl(static_cast<ObContinuousBase *>(src_vec), static_cast<ObDiscreteBase *>(dest_vec), batch_size);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector format", KR(ret), K(src_format), K(dest_format));
            break;
        }
        break;
      }
      // VEC_DISCRETE -> VEC_DISCRETE
      case VEC_DISCRETE: {
        switch (dest_format) {
          case VEC_DISCRETE:
            ret = shallow_copy_vector_impl(static_cast<ObDiscreteBase *>(src_vec), static_cast<ObDiscreteBase *>(dest_vec), batch_size);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector format", KR(ret), K(src_format), K(dest_format));
            break;
        }
        break;
      }
      // VEC_UNIFORM -> VEC_DISCRETE
      // VEC_UNIFORM -> VEC_UNIFORM
      case VEC_UNIFORM: {
        switch (dest_format) {
          case VEC_DISCRETE:
            ret = shallow_copy_vector_impl<false>(static_cast<ObUniformBase *>(src_vec), static_cast<ObDiscreteBase *>(dest_vec), batch_size);
            break;
          case VEC_UNIFORM:
            ret = shallow_copy_vector_impl<false>(static_cast<ObUniformBase *>(src_vec), static_cast<ObUniformBase *>(dest_vec), batch_size);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector format", KR(ret), K(src_format), K(dest_format));
            break;
        }
        break;
      }
      // VEC_UNIFORM_CONST-> VEC_DISCRETE
      // VEC_UNIFORM_CONST-> VEC_UNIFORM_CONST
      case VEC_UNIFORM_CONST: {
        switch (dest_format) {
          case VEC_DISCRETE:
            ret = shallow_copy_vector_impl<true>(static_cast<ObUniformBase *>(src_vec), static_cast<ObDiscreteBase *>(dest_vec), batch_size);
            break;
          case VEC_UNIFORM_CONST:
            ret = shallow_copy_vector_impl<true>(static_cast<ObUniformBase *>(src_vec), static_cast<ObUniformBase *>(dest_vec), batch_size);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected vector format", KR(ret), K(src_format), K(dest_format));
            break;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", KR(ret), K(src_format));
        break;
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::expand_const_vector(ObIVector *const_vec,
                                                 ObIVector *dest_vec,
                                                 const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == const_vec || nullptr == dest_vec || batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(const_vec), KP(dest_vec), K(batch_size));
  } else if (OB_UNLIKELY(VEC_UNIFORM_CONST != const_vec->get_format())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not const vector", KR(ret), K(const_vec->get_format()));
  } else {
    const ObDatum &const_datum = static_cast<ObUniformBase *>(const_vec)->get_datums()[0];
    if (OB_FAIL(expand_const_datum(const_datum, dest_vec, batch_size))) {
      LOG_WARN("fail to expaned const datum", KR(ret), K(const_datum), KP(dest_vec), K(batch_size));
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::expand_const_datum(const ObDatum &const_datum,
                                                ObIVector *dest_vec,
                                                const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(const_datum.is_ext() || nullptr == dest_vec || batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(const_datum), KP(dest_vec), K(batch_size));
  } else {
    const VectorFormat dest_format = dest_vec->get_format();
    switch (dest_format) {
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(dest_vec);
        discrete_vec->reset_flag();
        discrete_vec->get_nulls()->reset(batch_size);
        if (const_datum.is_null()) {
          discrete_vec->set_has_null();
          discrete_vec->get_nulls()->set_all(batch_size);
        } else {
          ObLength *lens = discrete_vec->get_lens();
          char **ptrs = discrete_vec->get_ptrs();
          for (int64_t i = 0; i < batch_size; ++i) {
            lens[i] = const_datum.len_;
          }
          for (int64_t i = 0; i < batch_size; ++i) {
            ptrs[i] = const_cast<char *>(const_datum.ptr_);
          }
        }
        break;
      }
      case VEC_UNIFORM:
      {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(dest_vec);
        ObDatum *datums = uniform_vec->get_datums();
        for (int64_t i = 0; i < batch_size; ++i) {
          datums[i] = const_datum;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", KR(ret), K(dest_format));
        break;
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::get_payload(ObIVector *vector, const int64_t idx, bool &is_null,
                                         const char *&payload, ObLength &len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vector || idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vector), K(idx));
  } else {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        if (fixed_vec->is_null(idx)) {
          is_null = true;
        } else {
          len = fixed_vec->get_length();
          payload = fixed_vec->get_data() + len * idx;
        }
        break;
      }
      case VEC_CONTINUOUS: {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        if (continuous_vec->is_null(idx)) {
          is_null = true;
        } else {
          const uint32_t offset1 = continuous_vec->get_offsets()[idx];
          const uint32_t offset2 = continuous_vec->get_offsets()[idx + 1];
          payload = continuous_vec->get_data() + offset1;
          len = (offset2 - offset1);
        }
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        if (discrete_vec->is_null(idx)) {
          is_null = true;
        } else {
          payload = discrete_vec->get_ptrs()[idx];
          len = discrete_vec->get_lens()[idx];
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum &datum = uniform_vec->get_datums()[idx];
        if (datum.is_null()) {
          is_null = true;
        } else {
          is_null = false;
          payload = datum.ptr_;
          len = datum.len_;
        }
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        ObDatum &datum = uniform_vec->get_datums()[0];
        if (datum.is_null()) {
          is_null = true;
        } else {
          is_null = false;
          payload = datum.ptr_;
          len = datum.len_;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", KR(ret), K(format));
        break;
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::to_datum(ObIVector *vector, const int64_t idx, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  datum.reset();
  if (OB_UNLIKELY(nullptr == vector || idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vector), K(idx));
  } else {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        if (fixed_vec->is_null(idx)) {
          datum.set_null();
        } else {
          datum.len_ = fixed_vec->get_length();
          datum.ptr_ = fixed_vec->get_data() + datum.len_ * idx;
        }
        break;
      }
      case VEC_CONTINUOUS: {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        if (continuous_vec->is_null(idx)) {
          datum.set_null();
        } else {
          const uint32_t offset1 = continuous_vec->get_offsets()[idx];
          const uint32_t offset2 = continuous_vec->get_offsets()[idx + 1];
          datum.ptr_ = continuous_vec->get_data() + offset1;
          datum.len_ = (offset2 - offset1);
        }
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        if (discrete_vec->is_null(idx)) {
          datum.set_null();
        } else {
          datum.len_ = discrete_vec->get_lens()[idx];
          datum.ptr_ = discrete_vec->get_ptrs()[idx];
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        datum = uniform_vec->get_datums()[idx];
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        datum = uniform_vec->get_datums()[0];
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", KR(ret), K(format));
        break;
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::to_datums(const ObIArray<ObIVector *> &vectors, int64_t idx,
                                       ObDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == datums || vectors.count() != count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(vectors), KP(datums), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(to_datum(vectors.at(i), idx, datums[i]))) {
        LOG_WARN("fail to datum", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::to_datums(const ObIArray<ObIVector *> &vectors, int64_t idx,
                                       ObStorageDatum *datums, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || nullptr == datums || vectors.count() != count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(vectors), K(idx), KP(datums), K(count));
  } else {
    bool is_null = false;
    const char *payload = nullptr;
    ObLength length = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObIVector *vec = vectors.at(i);
      ObStorageDatum &datum = datums[i];
      if (OB_FAIL(to_datum(vec, idx, datum))) {
        LOG_WARN("fail to datum", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::set_datum(ObIVector *vector, const int64_t idx, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vector || idx < 0 || datum.is_ext())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vector), K(idx), K(datum));
  } else {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        if (datum.is_null()) {
          fixed_vec->set_null(idx);
        } else if (OB_UNLIKELY(fixed_vec->get_length() != datum.len_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected datum length", KR(ret), K(fixed_vec->get_length()), K(datum));
        } else {
          fixed_vec->unset_null(idx);
          MEMCPY(fixed_vec->get_data() + datum.len_ * idx, datum.ptr_, datum.len_);
        }
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        if (datum.is_null()) {
          discrete_vec->set_null(idx);
        } else {
          discrete_vec->unset_null(idx);
          discrete_vec->get_ptrs()[idx] = const_cast<char *>(datum.ptr_);
          discrete_vec->get_lens()[idx] = datum.len_;
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        uniform_vec->get_datums()[idx] = datum;
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        uniform_vec->get_datums()[0] = datum;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", KR(ret), K(format));
        break;
    }
  }
  return ret;
}

/**
 * tablet id vector
 */

int ObDirectLoadVectorUtils::make_const_tablet_id_vector(const ObTabletID &tablet_id,
                                                         ObIAllocator &allocator,
                                                         ObIVector *&vector)
{
  int ret = OB_SUCCESS;
  vector = nullptr;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id));
  } else {
    if (OB_FAIL(new_vector(VEC_UNIFORM_CONST, tablet_id_value_tc, allocator, vector))) {
      LOG_WARN("fail to new uniform const vector", KR(ret));
    } else {
      ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
      ObStorageDatum *storage_datum = nullptr;
      if (OB_ISNULL(storage_datum = OB_NEWx(ObStorageDatum, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObStorageDatum", KR(ret));
      } else {
        storage_datum->set_uint(tablet_id.id());
        uniform_vec->set_datums(storage_datum);
      }
    }
  }
  return ret;
}

int ObDirectLoadVectorUtils::set_tablet_id(ObIVector *vector,
                                           const int64_t batch_idx,
                                           const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vector || batch_idx < 0 || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vector), K(batch_idx), K(tablet_id));
  } else {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        const ObLength len = fixed_vec->get_length();
        if (OB_UNLIKELY(len != sizeof(uint64_t))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected fixed len", KR(ret), K(len), K(sizeof(uint64_t)));
        } else {
          reinterpret_cast<uint64_t *>(fixed_vec->get_data())[batch_idx] = tablet_id.id();
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet id vector format", KR(ret), K(format));
        break;
    }
  }
  return ret;
}

ObTabletID ObDirectLoadVectorUtils::get_tablet_id(ObFixedLengthBase *vec, const int64_t batch_idx)
{
  ObTabletID tablet_id;
  if (OB_NOT_NULL(vec)) {
    tablet_id = reinterpret_cast<const uint64_t *>(vec->get_data())[batch_idx];
  }
  return tablet_id;
}

template <bool IS_CONST>
ObTabletID ObDirectLoadVectorUtils::get_tablet_id(ObUniformBase *vec, const int64_t batch_idx)
{
  ObTabletID tablet_id;
  if (OB_NOT_NULL(vec)) {
    tablet_id = vec->get_datums()[IS_CONST ? 0 : batch_idx].get_uint();
  }
  return tablet_id;
}

ObTabletID ObDirectLoadVectorUtils::get_tablet_id(ObIVector *vec, const int64_t batch_idx)
{
  ObTabletID tablet_id;
  if (OB_NOT_NULL(vec)) {
    const VectorFormat format = vec->get_format();
    switch (format) {
      case VEC_FIXED:
        tablet_id = get_tablet_id(static_cast<ObFixedLengthBase *>(vec), batch_idx);
        break;
      case VEC_UNIFORM:
        tablet_id = get_tablet_id<false>(static_cast<ObUniformBase *>(vec), batch_idx);
        break;
      case VEC_UNIFORM_CONST:
        tablet_id = get_tablet_id<true>(static_cast<ObUniformBase *>(vec), batch_idx);
        break;
      default:
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected vector format", K(format));
        break;
    }
  }
  return tablet_id;
}

bool ObDirectLoadVectorUtils::check_all_tablet_id_is_same(ObIVector *vec, const int64_t size)
{
  bool is_same = true;
  if (OB_NOT_NULL(vec)) {
    const VectorFormat format = vec->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
        const uint64_t *ids = reinterpret_cast<const uint64_t *>(fixed_vec->get_data());
        for (int64_t i = 1; i < size; ++i) {
          if (ids[i] != ids[0]) {
            is_same = false;
            break;
          }
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vec);
        const ObDatum *datums = uniform_vec->get_datums();
        for (int64_t i = 1; i < size; ++i) {
          if (datums[i].get_uint() != datums[0].get_uint()) {
            is_same = false;
            break;
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST:
        break;
      default:
        is_same = false;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected vector format", K(format));
        break;
    }
  }
  return is_same;
}

/**
 * hidden pk vector
 */

int ObDirectLoadVectorUtils::batch_fill_hidden_pk(ObIVector *vector, const int64_t start,
                                                  const int64_t size,
                                                  ObTabletCacheInterval &pk_interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vector || start < 0 || size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vector), K(start), K(size));
  } else if (size > 0) {
    uint64_t start_value = OB_INVALID_ID;
    if (1 == size) {
      if (OB_FAIL(pk_interval.next_value(start_value))) {
        LOG_WARN("fail to get next value", KR(ret), K(pk_interval));
        ret = OB_ERR_UNEXPECTED; // rewrite error code
      }
    } else {
      ObTabletCacheInterval batch_pk;
      if (OB_FAIL(pk_interval.fetch(size, batch_pk))) {
        LOG_WARN("fail to fetch pk interval", KR(ret), K(pk_interval), K(size));
        ret = OB_ERR_UNEXPECTED; // rewrite error code
      } else if (OB_FAIL(batch_pk.get_value(start_value))) {
        LOG_WARN("fail to get value", KR(ret), K(batch_pk));
        ret = OB_ERR_UNEXPECTED; // rewrite error code
      }
    }
    if (OB_SUCC(ret)) {
      const VectorFormat format = vector->get_format();
      switch (format) {
        case VEC_FIXED: {
          ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
          if (OB_UNLIKELY(fixed_vec->get_length() != sizeof(uint64_t))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected hidden pk vector value length", KR(ret),
                     K(fixed_vec->get_length()));
          } else {
            uint64_t *pks = reinterpret_cast<uint64_t *>(fixed_vec->get_data());
            for (int64_t i = 0; i < size; ++i) {
              pks[start + i] = (start_value + i);
            }
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector format", KR(ret), K(format));
          break;
      }
    }
  }
  return ret;
}

/**
 * multi version vector
 */

int ObDirectLoadVectorUtils::make_const_multi_version_vector(const int64_t value,
                                                             ObIAllocator &allocator,
                                                             ObIVector *&vector)
{
  int ret = OB_SUCCESS;
  vector = nullptr;
  if (OB_FAIL(new_vector(VEC_UNIFORM_CONST, multi_version_value_tc, allocator, vector))) {
    LOG_WARN("fail to new uniform const vector", KR(ret));
  } else {
    ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
    ObStorageDatum *storage_datum = nullptr;
    if (OB_ISNULL(storage_datum = OB_NEWx(ObStorageDatum, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObStorageDatum", KR(ret));
    } else {
      storage_datum->set_int(value);
      uniform_vec->set_datums(storage_datum);
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
