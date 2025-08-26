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
#include "share/schema/ob_table_param.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/direct_load/ob_direct_load_batch_rows.h"

namespace oceanbase
{
namespace storage
{
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace share::schema;
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
          MEMSET(data, 0, data_size);
          fixed_vec->set_nulls(nulls);
          fixed_vec->set_data(data);
        }
        break;
      }
      case VEC_CONTINUOUS: {
        ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *>(vector);
        const int64_t nulls_size = ObBitVector::memory_size(max_batch_size);
        const int64_t offsets_size = sizeof(uint32_t) * (max_batch_size + 1);
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
          MEMSET(offsets, 0, offsets_size);
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
          MEMSET(lens, 0, lens_size);
          MEMSET(ptrs, 0, ptrs_size);
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

int ObDirectLoadVectorUtils::check_rowkey_length(const ObDirectLoadBatchRows &batch_rows,
                                                 const int64_t rowkey_column_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_rows.empty() || batch_rows.get_column_count() < rowkey_column_count ||
                  rowkey_column_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(batch_rows), K(rowkey_column_count));
  } else {
    int64_t *rowkey_len = nullptr;
    const int64_t row_count = batch_rows.size();
    if (OB_ISNULL(rowkey_len = static_cast<int64_t *>(
                    ob_malloc(sizeof(int64_t) * row_count, "TLD_CheckRK")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", KR(ret), K(rowkey_len));
    } else {
      memset(rowkey_len, 0, sizeof(int64_t) * row_count);
      for (int64_t col_idx = 0; col_idx < rowkey_column_count; col_idx++) {
        ObDirectLoadVector *vector = batch_rows.get_vectors().at(col_idx);
        vector->sum_bytes_usage(rowkey_len, row_count);
      }
    }
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; row_idx++) {
      if (rowkey_len[row_idx] > OB_MAX_VARCHAR_LENGTH_KEY) {
        ret = OB_ERR_TOO_LONG_KEY_LENGTH;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_VARCHAR_LENGTH_KEY);
        LOG_WARN("rowkey is too long", K(ret), K(row_idx), K(rowkey_len[row_idx]));
      }
    }
    if (OB_NOT_NULL(rowkey_len)) {
      ob_free(rowkey_len);
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

ObTabletID ObDirectLoadVectorUtils::get_tablet_id(ObIVector *vec, const int64_t batch_idx)
{
  ObTabletID tablet_id;
  if (OB_NOT_NULL(vec)) {
    const VectorFormat format = vec->get_format();
    switch (format) {
      case VEC_FIXED:
        tablet_id = reinterpret_cast<const uint64_t *>(
          static_cast<ObFixedLengthBase *>(vec)->get_data())[batch_idx];
        break;
      case VEC_UNIFORM:
        tablet_id = static_cast<ObUniformBase *>(vec)->get_datums()[batch_idx].get_uint();
        break;
      case VEC_UNIFORM_CONST:
        tablet_id = static_cast<ObUniformBase *>(vec)->get_datums()[0].get_uint();
        break;
      default:
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected vector format", K(format));
        break;
    }
  }
  return tablet_id;
}

bool ObDirectLoadVectorUtils::check_all_tablet_id_is_same(const uint64_t *tablet_ids,
                                                          const int64_t size)
{
  bool is_same = true;
  for (int64_t i = 1; i < size; ++i) {
    if (tablet_ids[i] != tablet_ids[0]) {
      is_same = false;
      break;
    }
  }
  return is_same;
}

bool ObDirectLoadVectorUtils::check_is_same_tablet_id(const ObTabletID &tablet_id,
                                                      ObIVector *vector, const int64_t size)
{
  bool is_same = true;
  if (nullptr != vector) {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        const uint64_t *tablet_ids = reinterpret_cast<const uint64_t *>(fixed_vec->get_data());
        for (int64_t i = 0; i < size; ++i) {
          if (tablet_ids[i] != tablet_id.id()) {
            is_same = false;
            break;
          }
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        const ObDatum *datums = uniform_vec->get_datums();
        for (int64_t i = 0; i < size; ++i) {
          if (datums[i].get_uint() != tablet_id.id()) {
            is_same = false;
            break;
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        const ObDatum &datum = uniform_vec->get_datums()[0];
        is_same = (datum.get_uint() == tablet_id.id());
        break;
      }
      default:
        is_same = false;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected vector format", K(format));
        break;
    }
  }
  return is_same;
}

bool ObDirectLoadVectorUtils::check_is_same_tablet_id(const ObTabletID &tablet_id,
                                                      ObIVector *vector, const uint16_t *selector,
                                                      const int64_t size)
{
  bool is_same = true;
  if (nullptr != vector) {
    const VectorFormat format = vector->get_format();
    switch (format) {
      case VEC_FIXED: {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
        const uint64_t *tablet_ids = reinterpret_cast<const uint64_t *>(fixed_vec->get_data());
        for (int64_t i = 0; i < size; ++i) {
          const uint16_t idx = selector[i];
          if (tablet_ids[idx] != tablet_id.id()) {
            is_same = false;
            break;
          }
        }
        break;
      }
      case VEC_UNIFORM: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        const ObDatum *datums = uniform_vec->get_datums();
        for (int64_t i = 0; i < size; ++i) {
          const uint16_t idx = selector[i];
          if (datums[idx].get_uint() != tablet_id.id()) {
            is_same = false;
            break;
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST: {
        ObUniformBase *uniform_vec = static_cast<ObUniformBase *>(vector);
        const ObDatum &datum = uniform_vec->get_datums()[0];
        is_same = (datum.get_uint() == tablet_id.id());
        break;
      }
      default:
        is_same = false;
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected vector format", K(format));
        break;
    }
  }
  return is_same;
}

bool ObDirectLoadVectorUtils::check_is_same_tablet_id(const ObTabletID &tablet_id,
                                                      const ObDatumVector &datum_vec,
                                                      const int64_t size)
{
  bool is_same = true;
  if (nullptr != datum_vec.datums_) {
    if (datum_vec.is_batch()) {
      for (int64_t i = 0; i < size; ++i) {
        if (datum_vec.datums_[i].get_uint() != tablet_id.id()) {
          is_same = false;
          break;
        }
      }
    } else {
      const ObDatum &datum = datum_vec.datums_[0];
      is_same = (datum.get_uint() == tablet_id.id());
    }
  }
  return is_same;
}

bool ObDirectLoadVectorUtils::check_is_same_tablet_id(const ObTabletID &tablet_id,
                                                      const ObDatumVector &datum_vec,
                                                      const uint16_t *selector, const int64_t size)
{
  bool is_same = true;
  if (nullptr != datum_vec.datums_) {
    if (datum_vec.is_batch()) {
      for (int64_t i = 0; i < size; ++i) {
        const uint16_t idx = selector[i];
        if (datum_vec.datums_[idx].get_uint() != tablet_id.id()) {
          is_same = false;
          break;
        }
      }
    } else {
      const ObDatum &datum = datum_vec.datums_[0];
      is_same = (datum.get_uint() == tablet_id.id());
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
