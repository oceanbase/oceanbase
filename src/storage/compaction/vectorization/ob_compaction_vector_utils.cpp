/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/vectorization/ob_compaction_vector_utils.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_uniform_vector.h"

namespace oceanbase
{
namespace compaction
{
int ObCompactionVectorUtils::new_vector(
    sql::VectorHeader &vector_header,
    VecValueTypeClass value_tc)
{
  int ret = OB_SUCCESS;
  VectorFormat format = vector_header.get_format();
  switch (format) {
    case VEC_FIXED: {
      switch (value_tc) {
#define FIXED_VECTOR_INIT_SWITCH(value_tc)                                                        \
  case value_tc: {                                                                                \
    using VecType = RTVectorType<VEC_FIXED, value_tc>;                                            \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE,                           \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");                                  \
    new(vector_header.vector_buf_)VecType(nullptr, nullptr);                                      \
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
    case VEC_DISCRETE: {
      switch (value_tc) {
#define DISCRETE_VECTOR_INIT_SWITCH(value_tc)                                                     \
  case value_tc: {                                                                                \
    using VecType = RTVectorType<VEC_DISCRETE, value_tc>;                                         \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE,                           \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");                                  \
    new(vector_header.vector_buf_)VecType(nullptr, nullptr, nullptr);                             \
    break;                                                                                        \
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
          LOG_WARN("unexpected discrete vector value type class", KR(ret), K(value_tc));
          break;
      }
      break;
    }
    case VEC_CONTINUOUS: {
      switch (value_tc) {
#define CONTINUOUS_VECTOR_INIT_SWITCH(value_tc)                                                     \
  case value_tc: {                                                                                \
    using VecType = RTVectorType<VEC_CONTINUOUS, value_tc>;                                         \
    static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE,                           \
                  "vector size exceeds MAX_VECTOR_STRUCT_SIZE");                                  \
    new(vector_header.vector_buf_)VecType(nullptr, nullptr, nullptr);                             \
    break;                                                                                        \
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
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), K(format));
      break;
  }
  return ret;
}

int ObCompactionVectorUtils::prepare_vector(
    ObIVector *vector,
    const int64_t max_batch_size,
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
        const int64_t nulls_size = sql::ObBitVector::memory_size(max_batch_size);
        const int64_t data_size = length * max_batch_size;
        sql::ObBitVector *nulls = nullptr;
        char *data = nullptr;
        if (OB_ISNULL(nulls = sql::to_bit_vector(allocator.alloc(nulls_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
        } else if (OB_ISNULL(data = static_cast<char *>(allocator.alloc(data_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(data_size));
        } else {
          nulls->reset(max_batch_size);
          MEMSET(data, 0, data_size);
          fixed_vec->set_nulls(nulls);
          fixed_vec->set_data(data);
        }
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
        const int64_t nulls_size = sql::ObBitVector::memory_size(max_batch_size);
        const int64_t lens_size = sizeof(int32_t) * max_batch_size;
        const int64_t ptrs_size = sizeof(char *) * max_batch_size;
        sql::ObBitVector *nulls = nullptr;
        int32_t *lens = nullptr;
        char **ptrs = nullptr;
        if (OB_ISNULL(nulls = sql::to_bit_vector(allocator.alloc(nulls_size)))) {
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
          MEMSET(lens, 0, lens_size);
          MEMSET(ptrs, 0, ptrs_size);
          discrete_vec->set_nulls(nulls);
          discrete_vec->set_lens(lens);
          discrete_vec->set_ptrs(ptrs);
        }
        break;
      }
      case VEC_CONTINUOUS: {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        const int64_t nulls_size = sql::ObBitVector::memory_size(max_batch_size);
        const int64_t offsets_size = sizeof(uint32_t) * (max_batch_size + 1);
        sql::ObBitVector *nulls = nullptr;
        uint32_t *offsets = nullptr;
        if (OB_ISNULL(nulls = sql::to_bit_vector(allocator.alloc(nulls_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(nulls_size));
        } else if (OB_ISNULL(offsets = static_cast<uint32_t *>(allocator.alloc(offsets_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem", KR(ret), K(offsets_size));
        } else {
          nulls->reset(max_batch_size);
          MEMSET(offsets, 0, offsets_size);
          continuous_vec->set_nulls(nulls);
          continuous_vec->set_offsets(offsets);
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
} // namespace compaction
} // namespace oceanbase