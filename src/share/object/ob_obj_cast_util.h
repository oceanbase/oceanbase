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

#ifndef OCEANBASE_COMMON_OB_OBJ_CAST_UTIL_
#define OCEANBASE_COMMON_OB_OBJ_CAST_UTIL_

#include "share/ob_errno.h"
#include "common/object/ob_object.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
namespace common
{

// check with given lower and upper limit.
template <typename InType, typename OutType>
OB_INLINE int numeric_range_check(const InType in_val,
                                  const OutType min_out_val,
                                  const OutType max_out_val,
                                  OutType &out_val)
{
  int ret = OB_SUCCESS;
  // Casting value from InType to OutType to prevent number overflow. 
  OutType cast_in_val = static_cast<OutType>(in_val);
  if (cast_in_val < min_out_val) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = min_out_val;
  } else if (cast_in_val > max_out_val) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = max_out_val;
  }
  return ret;
}

// explicit for int_uint check, because we need use out_val to compare with max_out_val instead
// of in_val, since we can't cast UINT64_MAX to int64.
template <>
OB_INLINE int numeric_range_check<int64_t, uint64_t>(const int64_t in_val,
                                                     const uint64_t min_out_val,
                                                     const uint64_t max_out_val,
                                                     uint64_t &out_val)
{
  int ret = OB_SUCCESS;
  if (in_val < 0) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = 0;
  } else if (out_val > max_out_val) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = max_out_val;
  }
  UNUSED(min_out_val);
  return ret;
}

// explicit for float to handle infinity
template <>
OB_INLINE int numeric_range_check<double, float>(const double in_val,
                                                     const float min_out_val,
                                                     const float max_out_val,
                                                     float &out_val)
{
  int ret = OB_SUCCESS;
  if (isinf(in_val)) {
    out_val = static_cast<float>(in_val);
  } else {
    if (in_val < min_out_val) {
      ret = OB_DATA_OUT_OF_RANGE;
      out_val = min_out_val;
    } else if (in_val > max_out_val) {
      ret = OB_DATA_OUT_OF_RANGE;
      out_val = max_out_val;
    }
  }
  return ret;
}

// check if is negative only.
template <typename OutType>
OB_INLINE int numeric_negative_check(OutType &out_val)
{
  int ret = OB_SUCCESS;
  if (out_val < static_cast<OutType>(0)) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = static_cast<OutType>(0);
  }
  return ret;
}

// explicit for number check.
template <>
OB_INLINE int numeric_negative_check<number::ObNumber>(number::ObNumber &out_val)
{
  int ret = OB_SUCCESS;
  if (out_val.is_negative()) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val.set_zero();
  }
  return ret;
}

// check upper limit only.
template <typename InType, typename OutType>
OB_INLINE int numeric_upper_check(const InType in_val,
                                  const OutType max_out_val,
                                  OutType &out_val)
{
  int ret = OB_SUCCESS;
  if (in_val > static_cast<InType>(max_out_val)) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = max_out_val;
  }
  return ret;
}

template <typename InType>
OB_INLINE int int_range_check(const ObObjType out_type,
                              const InType in_val,
                              int64_t &out_val)
{
  return numeric_range_check(in_val, INT_MIN_VAL[out_type], INT_MAX_VAL[out_type], out_val);
}

template <typename InType>
OB_INLINE int int_upper_check(const ObObjType out_type,
                              InType in_val,
                              int64_t &out_val)
{
  return numeric_upper_check(in_val, INT_MAX_VAL[out_type], out_val);
}

OB_INLINE int uint_upper_check(const ObObjType out_type, uint64_t &out_val)
{
  return numeric_upper_check(out_val, UINT_MAX_VAL[out_type], out_val);
}

template <typename InType>
OB_INLINE int uint_range_check(const ObObjType out_type,
                               const InType in_val,
                               uint64_t &out_val)
{
  return numeric_range_check(in_val, static_cast<uint64_t>(0),
                             UINT_MAX_VAL[out_type], out_val);
}

template <typename InType, typename OutType>
OB_INLINE int real_range_check(const ObObjType out_type,
                               const InType in_val,
                               OutType &out_val)
{
  return numeric_range_check(in_val, static_cast<OutType>(REAL_MIN_VAL[out_type]),
                             static_cast<OutType>(REAL_MAX_VAL[out_type]), out_val);
}

template <typename Type>
int real_range_check(const ObAccuracy &accuracy, Type &value)
{
  int ret = OB_SUCCESS;
  const ObPrecision precision = accuracy.get_precision();
  const ObScale scale = accuracy.get_scale();
  if (OB_LIKELY(precision > 0) &&
      OB_LIKELY(scale >= 0) &&
      OB_LIKELY(precision >= scale)) {
    Type integer_part = static_cast<Type>(pow(10.0, static_cast<double>(precision - scale)));
    Type decimal_part = static_cast<Type>(pow(10.0, static_cast<double>(scale)));
    Type max_value = static_cast<Type>(integer_part - 1 / decimal_part);
    Type min_value = static_cast<Type>(-max_value);
    if (OB_FAIL(numeric_range_check(value, min_value, max_value, value))) {
    } else {
      value = static_cast<Type>(rint((value - 
                                      floor(static_cast<double>(value)))* decimal_part) / 
                                      decimal_part + floor(static_cast<double>(value)));
    }
  }
  return ret;
}

inline uint64_t hex_to_uint64(const ObString &str)
{
  int32_t N = str.length();
  const uint8_t *p = reinterpret_cast<const uint8_t*>(str.ptr());
  uint64_t value = 0;
  if (OB_LIKELY(NULL != p)) {
    for (int32_t i = 0; i < N; ++i, ++p) {
      // 经过实验，mysql不做溢出检查
      value = value * 256 + *p;
    }
  }
  return value;
}

int check_convert_str_err(const char *str,
                          const char *endptr,
                          const int32_t len,
                          const int err,
                          const ObCollationType &in_cs_type);

// decimal(aka NumberType) cast to double/float precision increment. If it is an unsigned decimal,
// don’t need to increment precision, otherwise increment 1 to cover sign bit. If scale is
// equal to 0, don’t need to increment precision, otherwise increment 1 to cover dot bit.
inline int16_t decimal_to_double_precision_inc(const ObObjType type, const ObScale s)
{
  return ((type == ObUNumberType) ? 0 : 1) + ((s > 0) ? 1 : 0);
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_OBJ_CAST_UTIL_
