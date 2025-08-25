/**
 * Copyright (c) 2025 OceanBase
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

#include "storage/direct_load/ob_direct_load_vector.h"
#include "share/schema/ob_table_param.h"
#include "storage/direct_load/ob_direct_load_continuous_vector.h"
#include "storage/direct_load/ob_direct_load_discrete_vector.h"
#include "storage/direct_load/ob_direct_load_fixed_length_vector.h"
#include "storage/direct_load/ob_direct_load_nullable_vector.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace share::schema;

int ObDirectLoadVector::create_vector(VectorFormat format, VecValueTypeClass value_tc,
                                      bool is_nullable, const int64_t max_batch_size,
                                      ObIAllocator &allocator, ObDirectLoadVector *&dl_vector)
{
  int ret = OB_SUCCESS;
  dl_vector = nullptr;
  ObIVector *vector = nullptr;
  switch (format) {
    case VEC_FIXED:
      if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_FIXED, value_tc, allocator, vector))) {
        LOG_WARN("fail to new fixed vector", KR(ret), K(value_tc));
      } else if (OB_FAIL(
                   ObDirectLoadVectorUtils::prepare_vector(vector, max_batch_size, allocator))) {
        LOG_WARN("fail to prepare vector", KR(ret), KPC(vector));
      } else {
        ObFixedLengthBase *fixed_vector = static_cast<ObFixedLengthBase *>(vector);
        switch (value_tc) {
#define FIXED_VECTOR_INIT_SWITCH(value_tc)                                                     \
  case value_tc: {                                                                             \
    using VecValueType = RTCType<value_tc>;                                                    \
    using FixedLengthVecType = ObDirectLoadFixedLengthVector<VecValueType>;                    \
    using NullableVecType = ObDirectLoadNullableVector<FixedLengthVecType, ObFixedLengthBase>; \
    if (is_nullable) {                                                                         \
      dl_vector = OB_NEWx(NullableVecType, &allocator, fixed_vector);                          \
    } else {                                                                                   \
      dl_vector = OB_NEWx(FixedLengthVecType, &allocator, fixed_vector);                       \
    }                                                                                          \
    break;                                                                                     \
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
      }
      break;
    case VEC_CONTINUOUS:
      if (OB_FAIL(
            ObDirectLoadVectorUtils::new_vector(VEC_CONTINUOUS, value_tc, allocator, vector))) {
        LOG_WARN("fail to new continuous vector", KR(ret), K(value_tc));
      } else if (OB_FAIL(
                   ObDirectLoadVectorUtils::prepare_vector(vector, max_batch_size, allocator))) {
        LOG_WARN("fail to prepare vector", KR(ret), KPC(vector));
      } else {
        if (is_nullable) {
          using NullableVecType =
            ObDirectLoadNullableVector<ObDirectLoadContinuousVector, ObContinuousBase>;
          dl_vector = OB_NEWx(NullableVecType, &allocator, static_cast<ObContinuousBase *>(vector));
        } else {
          dl_vector = OB_NEWx(ObDirectLoadContinuousVector, &allocator,
                              static_cast<ObContinuousBase *>(vector));
        }
      }
      break;
    case VEC_DISCRETE:
      if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(VEC_DISCRETE, value_tc, allocator, vector))) {
        LOG_WARN("fail to new discrete vector", KR(ret), K(value_tc));
      } else if (OB_FAIL(
                   ObDirectLoadVectorUtils::prepare_vector(vector, max_batch_size, allocator))) {
        LOG_WARN("fail to prepare vector", KR(ret), KPC(vector));
      } else {
        if (is_nullable) {
          using NullableVecType =
            ObDirectLoadNullableVector<ObDirectLoadDiscreteVector, ObDiscreteBase>;
          dl_vector = OB_NEWx(NullableVecType, &allocator, static_cast<ObDiscreteBase *>(vector));
        } else {
          dl_vector =
            OB_NEWx(ObDirectLoadDiscreteVector, &allocator, static_cast<ObDiscreteBase *>(vector));
        }
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), K(format));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(nullptr == dl_vector)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new direct load vector", KR(ret), K(format));
  }
  return ret;
}

int ObDirectLoadVector::create_vector(const ObColDesc &col_desc, bool is_nullable,
                                      const int64_t max_batch_size, ObIAllocator &allocator,
                                      ObDirectLoadVector *&vector)
{
  int ret = OB_SUCCESS;
  const int16_t precision = col_desc.col_type_.is_decimal_int()
                              ? col_desc.col_type_.get_stored_precision()
                              : PRECISION_UNKNOWN_YET;
  VecValueTypeClass value_tc =
    get_vec_value_tc(col_desc.col_type_.get_type(), col_desc.col_type_.get_scale(), precision);
  const bool is_fixed = is_fixed_length_vec(value_tc);
  VectorFormat format = is_fixed ? VEC_FIXED : VEC_DISCRETE; // VEC_CONTINUOUS;
  if (OB_FAIL(create_vector(format, value_tc, is_nullable, max_batch_size, allocator, vector))) {
    LOG_WARN("fail to create vector", KR(ret), K(col_desc), K(value_tc), K(format), K(is_nullable));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
