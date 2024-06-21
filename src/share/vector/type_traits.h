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

#ifndef OCEANBASE_SHARE_VECTOR_TYPES_TRAIT_H_
#define OCEANBASE_SHARE_VECTOR_TYPES_TRAIT_H_

#include "common/object/ob_obj_type.h"
#include "lib/wide_integer/ob_wide_integer.h"

namespace oceanbase
{
namespace common
{
enum VectorFormat: uint8_t
{
  VEC_INVALID = 0,
  VEC_FIXED,
  VEC_DISCRETE,
  VEC_CONTINUOUS,
  VEC_UNIFORM,
  VEC_UNIFORM_CONST,
  VEC_MAX_FORMAT
};

inline bool is_uniform_format(VectorFormat format) {
  return VEC_UNIFORM_CONST == format || VEC_UNIFORM == format;
}

inline bool is_discrete_format(VectorFormat format) {
  return VEC_DISCRETE == format;
}

inline bool is_valid_format(VectorFormat format) {
  return format > VEC_INVALID && format < VEC_MAX_FORMAT;
}


namespace number {
  class ObNumber;
}
template<typename ValueType, typename BasicOp>
class ObFixedLengthVector;
template<typename BasicOp>
class ObContinuousVector;
template<typename BasicOp>
class ObDiscreteVector;
template<bool IS_CONST, typename BasicOp>
class ObUniformVector;
template<bool IS_CONST>
class ObUniformFormat;
class ObString;
class ObOTimestampData;
class ObOTimestampTinyData;
class ObIntervalDSValue;
template<VecValueTypeClass value_tc>
class VectorBasicOp;
class ObVectorBase;
class ObObj;

template <VecValueTypeClass vec_tc>
struct RTTypeTraits {};

#define DEFINE_VECTOR_TC_TRAITS(vec_tc, is_fixed, c_type) \
template <> \
struct RTTypeTraits<vec_tc> { \
  static const bool fixed = is_fixed; \
  using CType = c_type; \
};

DEFINE_VECTOR_TC_TRAITS(VEC_TC_NULL, true, char[0]);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_INTEGER, true, int64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_UINTEGER, true, uint64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_FLOAT, true, float);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_DOUBLE, true, double);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_FIXED_DOUBLE, true, double);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_NUMBER, false, number::ObNumber);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_DATETIME, true, int64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_DATE, true, int32_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_TIME, true, int64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_YEAR, true, uint8_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_EXTEND, false, ObObj);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_UNKNOWN, true, int64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_STRING, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_BIT, true, uint64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_ENUM_SET, true, uint64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_ENUM_SET_INNER, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_TIMESTAMP_TZ, true, ObOTimestampData);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_TIMESTAMP_TINY, true, ObOTimestampTinyData);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_RAW, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_INTERVAL_YM, true, int64_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_INTERVAL_DS, true, ObIntervalDSValue);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_ROWID, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_LOB, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_JSON, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_GEO, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_UDT, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_DEC_INT32, true, int32_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_DEC_INT64, true, int64_t);

DEFINE_VECTOR_TC_TRAITS(VEC_TC_DEC_INT128, true, int128_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_DEC_INT256, true, int256_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_DEC_INT512, true, int512_t);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_COLLECTION, false, ObString);
DEFINE_VECTOR_TC_TRAITS(VEC_TC_ROARINGBITMAP, false, ObString);

#undef DEFINE_VECTOR_TC_TRAITS

template <VecValueTypeClass value_tc>
using RTCType = typename RTTypeTraits<value_tc>::CType;

//********************* def RTVectorTraits ******************
template <VectorFormat format, VecValueTypeClass vec_tc>
struct RTVectorTraits {
  using VectorType = ObVectorBase;
};

template <VecValueTypeClass vec_tc>
struct RTVectorTraits<VEC_UNIFORM, vec_tc> {
  using VectorType = ObUniformVector<false, VectorBasicOp<vec_tc>>;
};

template <VecValueTypeClass vec_tc>
struct RTVectorTraits<VEC_UNIFORM_CONST, vec_tc> {
  using VectorType = ObUniformVector<true, VectorBasicOp<vec_tc>>;
};

template <VecValueTypeClass vec_tc>
struct RTVectorTraits<VEC_FIXED, vec_tc> {
  using VectorType = ObFixedLengthVector<RTCType<vec_tc>, VectorBasicOp<vec_tc>>;
};

template <VecValueTypeClass vec_tc>
struct RTVectorTraits<VEC_CONTINUOUS, vec_tc> {
  using VectorType = ObContinuousVector<VectorBasicOp<vec_tc>>;
};

template <VecValueTypeClass vec_tc>
struct RTVectorTraits<VEC_DISCRETE, vec_tc> {
  using VectorType = ObDiscreteVector<VectorBasicOp<vec_tc>>;
};

// runtime vector type
template <VectorFormat format, VecValueTypeClass value_tc>
using RTVectorType = typename RTVectorTraits<format, value_tc>::VectorType;
//********************* def RTVectorTraits end ******************
}
}
#endif // OCEANBASE_SHARE_VECTOR_TYPES_TRAIT_H_
