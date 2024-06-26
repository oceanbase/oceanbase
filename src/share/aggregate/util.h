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

#ifndef OCEANBASE_SHARE_AGGREGATE_UTIL_H_
#define OCEANBASE_SHARE_AGGREGATE_UTIL_H_

#include <type_traits>
#include <cmath>

#include "lib/number/ob_number_v2.h"
#include "common/object/ob_obj_type.h"
#include "sql/engine/ob_bit_vector.h"
#include "share/vector/ob_vector_define.h"

#define EXTRACT_MEM_ADDR(ptr) (reinterpret_cast<char *>(*reinterpret_cast<int64_t *>((ptr))))
#define STORE_MEM_ADDR(addr, dst)                                                                  \
  do {                                                                                             \
    *reinterpret_cast<int64_t *>((dst)) = reinterpret_cast<int64_t>((addr));                       \
  } while (false)

namespace oceanbase
{
namespace sql
{
  class ObEvalCtx;
  class ObExpr;
}

namespace common
{
  class ObVectorBase;
}
namespace share
{
namespace aggregate
{
using namespace oceanbase::common;

// ================
// format helper
template <VecValueTypeClass vec_tc>
using fixlen_fmt =
  typename std::conditional<is_fixed_length_vec(vec_tc), ObFixedLengthFormat<RTCType<vec_tc>>,
                            ObVectorBase>::type;

template <VecValueTypeClass vec_tc>
using discrete_fmt =
  typename std::conditional<is_discrete_vec(vec_tc), ObDiscreteFormat, ObVectorBase>::type;

template <VecValueTypeClass vec_tc>
using continuous_fmt =
  typename std::conditional<is_continuous_vec(vec_tc), ObContinuousFormat, ObVectorBase>::type;

template <VecValueTypeClass vec_tc, bool is_const>
using uniform_fmt =
  typename std::conditional<is_uniform_vec(vec_tc), ObUniformFormat<is_const>, ObVectorBase>::type;

// ================
// member function helpers
template<typename T, typename = std::void_t<>>
struct defined_collect_tmp_result: std::false_type {};

template <typename T>
struct defined_collect_tmp_result<T, std::void_t<decltype(&T::collect_tmp_result)>>
  : std::true_type {};

template <typename T, typename = std::void_t<>>
struct defined_add_param_batch: std::false_type {};

template <typename T>
struct defined_add_param_batch<T, std::void_t<decltype(&T::template add_param_batch<ObVectorBase>)>>
  : std::true_type
{};

template<typename T, bool defined = defined_collect_tmp_result<T>::value>
struct collect_tmp_result
{
  template<typename... Args>
  inline static int do_op(T &v, Args&&... args)
  {
    return v.collect_tmp_result(std::forward<Args>(args)...);
  }
};

template<typename T>
struct collect_tmp_result<T, false>
{
  template<typename... Args>
  inline static int do_op(T &v, Args&&... args)
  {
    return OB_SUCCESS;
  }
};

template<typename T, bool defined = defined_add_param_batch<T>::value>
struct add_param_batch
{
  template<typename... Args>
  inline static int do_op(T &v, Args&&... args)
  {
    return v.add_param_batch(std::forward<Args>(args)...);
  }
};

template<typename T>
struct add_param_batch<T, false>
{
  template<typename... Args>
  inline static int do_op(T &v, Args&&... args)
  {
    return OB_SUCCESS;
  }
};

template <typename T, typename = std::void_t<>>
struct defined_removal_func: std::false_type {};

template <typename T>
struct defined_removal_func<
  T, std::void_t<decltype(&T::template add_or_sub_row<ObVectorBase>)>>
  : std::true_type
{};

template<typename T, bool defined = defined_removal_func<T>::value>
struct removal_opt
{
  template<typename... Args>
  inline static int add_or_sub_row(T &v, Args&&... args)
  {
    return v.add_or_sub_row(std::forward<Args>(args)...);
  }
};

template<typename T>
struct removal_opt<T, false>
{
  template<typename... Args>
  inline static int add_or_sub_row(T &v, Args&&... args)
  {
    int ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "removal opt not suppported", K(ret));
    return ret;
  }
};

// ================
// read/writer helper

template<typename T>
struct need_extend_buf
{
  static const constexpr bool value =
    (std::is_same<T, ObString>::value || std::is_same<T, ObObj>::value);
};
template<typename T>
struct CellWriter
{
  template <typename Vector>
  inline static void set(const char *data, const int32_t data_len, Vector *vec,
                         const int64_t output_idx, char *res_buf)
  {
    OB_ASSERT(vec != NULL);
    if (need_extend_buf<T>::value) {
      MEMCPY(res_buf, data, data_len);
      vec->set_payload_shallow(output_idx, res_buf , data_len);
    } else {
      vec->set_payload(output_idx, data, data_len);
    }
  }

  template<typename Vector, typename in_type>
  inline static void cp_and_set(const in_type &in_val, Vector *vec,
                                const int64_t output_idx, char *res_buf)
  {
    UNUSEDx(res_buf);
    OB_ASSERT(vec != NULL);
    T out_val = in_val;
    vec->set_payload(output_idx, &out_val, sizeof(T));
  }
};

// ugly code, for compilation's sake
template <>
struct CellWriter<char[0]>
{
  template <typename Vector>
  inline static void set(const char *data, const int32_t data_len, Vector *vec,
                         const int64_t output_idx, char *res_buf)
  {
    UNUSEDx(data, data_len, res_buf);
    OB_ASSERT(vec != NULL);
    vec->set_null(output_idx);
  }

  template <typename Vector, typename in_type>
  inline static void cp_and_set(const in_type &in_val, Vector *vec, const int64_t output_idx,
                                char *res_buf)
  {
    UNUSEDx(in_val, res_buf);
    vec->set_null(output_idx);
  }
};

// for compact number
// using `set_number` to store number value so that cell's length can be set properly
// otherwise `hash_v2` value of cell will be inconsistent.
template<>
struct CellWriter<number::ObCompactNumber>
{
  template <typename Vector>
  inline static void set(const char *data, const int32_t data_len, Vector *vec,
                         const int64_t output_idx, char *res_buf)
  {
    UNUSED(res_buf);
    OB_ASSERT(vec != NULL);
    OB_ASSERT(data != NULL);
    OB_ASSERT(data_len > 0);
    const number::ObCompactNumber *cnum = reinterpret_cast<const number::ObCompactNumber *>(data);
    vec->set_number(output_idx, *cnum);
  }

  template <typename Vector, typename in_type>
  inline static void cp_and_set(const in_type &in_val, Vector *vec,
                                const int64_t output_idx, char *res_buf)
  {
    UNUSEDx(in_val, vec, output_idx, res_buf);
    return;
  }
};

// ================
// calculation helper
struct OverflowChecker
{
  template <typename LType, typename RType,
            class = typename std::enable_if<std::is_integral<LType>::value
                                            && std::is_integral<RType>::value>::type>
  static inline bool check_overflow(const LType &l, const RType &r, RType &res)
  {
    return __builtin_add_overflow(l, r, &res);
  }

  static inline bool check_overflow(const float l, const float r, float &res)
  {
    res = l + r;
    return (std::isinf(res) != 0);
  }

  static inline bool check_overflow(const double l, const double r, double &res)
  {
    res = l + r;
    return (std::isinf(res) != 0);
  }
  template <unsigned Bits, typename Signed>
  static inline bool check_overflow(const int64_t l, const wide::ObWideInteger<Bits, Signed> &r,
                                    wide::ObWideInteger<Bits, Signed> &res)
  {
    return OB_OPERATE_OVERFLOW == r.template add<wide::CheckOverFlow>(l, res);
  }

  template <unsigned Bits, typename Signed>
  static inline bool check_overflow(const uint64_t l, const wide::ObWideInteger<Bits, Signed> &r,
                                    wide::ObWideInteger<Bits, Signed> &res)
  {
    return OB_OPERATE_OVERFLOW == r.template add<wide::CheckOverFlow>(l, res);
  }

  template <unsigned Bits, typename Signed, unsigned Bits2, typename Signed2>
  static inline bool check_overflow(const wide::ObWideInteger<Bits, Signed> &l,
                                    const wide::ObWideInteger<Bits2, Signed2> &r,
                                    wide::ObWideInteger<Bits2, Signed2> &res)
  {
    return OB_OPERATE_OVERFLOW == r.template add<wide::CheckOverFlow>(l, res);
  }
};

template <typename L, typename R>
inline int add_overflow(const L &l, const R &r, char *res_buf, const int32_t res_len)
{
  int ret = OB_NOT_IMPLEMENT;
  SQL_LOG(WARN, "not implement", K(ret));
  return ret;
}

template <>
inline int add_overflow<float, float>(const float &l, const float &r, char *res_buf,
                                      const int32_t res_len)
{
  float &res = *reinterpret_cast<float *>(res_buf);
  if (OverflowChecker::check_overflow(l, r, res)) {
    return OB_OPERATE_OVERFLOW;
  }
  return OB_SUCCESS;
}

template <>
inline int add_overflow<double, double>(const double &l, const double &r, char *res_buf,
                                        const int32_t res_len)
{
  double &res = *reinterpret_cast<double *>(res_buf);
  // overflow checking is not needed for double
  res = l + r;
  return OB_SUCCESS;
}

template<>
inline int add_overflow<int64_t, int64_t>(const int64_t &l, const int64_t &r, char *res_buf, const int32_t res_len)
{
  int ret = OB_SUCCESS;
  int64_t tmp_r = 0;
  int64_t &res = *reinterpret_cast<int64_t *>(res_buf);
  if (OverflowChecker::check_overflow(l, r, tmp_r)) {
    ret = OB_OPERATE_OVERFLOW;
  } else {
    res = tmp_r;
  }
  return ret;
}

template<>
inline int add_overflow<uint64_t, uint64_t>(const uint64_t &l, const uint64_t &r, char *res_buf, const int32_t res_len)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_r = 0;
  uint64_t &res = *reinterpret_cast<uint64_t *>(res_buf);
  if (OverflowChecker::check_overflow(l, r, tmp_r)) {
    ret = OB_OPERATE_OVERFLOW;
  } else {
    res = tmp_r;
  }
  return ret;
}

template<>
inline int add_overflow<int32_t, int64_t>(const int32_t &l, const int64_t &r, char *res_buf, const int32_t res_len)
{
  int ret = OB_SUCCESS;
  int64_t tmp_l = l, tmp_res = 0;
  int64_t &res = *reinterpret_cast<int64_t *>(res_buf);
  if (OverflowChecker::check_overflow(tmp_l, r, tmp_res)) {
    ret = OB_OPERATE_OVERFLOW;
  } else {
    res = tmp_res;
  }
  return ret;
}

template <unsigned Bits, typename Signed>
inline int add_overflow(const wide::ObWideInteger<Bits, Signed> &l,
                        const wide::ObWideInteger<Bits, Signed> &r,
                        char *res_buf,
                        const int32_t res_len)
{
using integer = wide::ObWideInteger<Bits, Signed>;
  int ret = OB_SUCCESS;
  char tmp_res_buf[sizeof(integer)] = {0};
  integer &tmp_res = *reinterpret_cast<integer *>(tmp_res_buf);
  ret = l.template add<wide::CheckOverFlow>(r, tmp_res);
  if (OB_SUCC(ret)) {
    MEMCPY(res_buf, tmp_res_buf, sizeof(integer));
  }
  return ret;
}
template <>
inline int
add_overflow<number::ObCompactNumber, number::ObCompactNumber>(const number::ObCompactNumber &l,
                                                               const number::ObCompactNumber &r,
                                                               char *res_buf, const int32_t res_len)
{
  UNUSEDx(res_len);
  int ret = OB_SUCCESS;
  char buf_alloc[number::ObNumber::MAX_CALC_BYTE_LEN] = {0};
  ObDataBuffer local_allocator(buf_alloc, number::ObNumber::MAX_CALC_BYTE_LEN);
  number::ObNumber param1(l);
  number::ObNumber param2(r);
  number::ObNumber res_nmb;
  if (OB_FAIL(param1.add_v3(param2, res_nmb, local_allocator))) {
    SQL_LOG(WARN, "add_v3 failed", K(ret));
  } else {
    *reinterpret_cast<uint32_t *>(res_buf) = res_nmb.d_.desc_;
    MEMCPY(res_buf + sizeof(uint32_t), res_nmb.get_digits(), res_nmb.d_.len_ * sizeof(uint32_t));
  }
  return ret;
}

template<typename L, typename R>
inline int add_values(const L &l, const R &r, char *res_buf, const int32_t res_len)
{
  int ret = OB_SUCCESS;
  R &res = *reinterpret_cast<R *>(res_buf) ;
  res = r + l;
  return ret;
}

template <>
inline int
add_values<number::ObCompactNumber, number::ObCompactNumber>(const number::ObCompactNumber &l,
                                                             const number::ObCompactNumber &r,
                                                             char *res_buf, const int32_t res_len)
{
  return add_overflow(l, r, res_buf, res_len);
}

template <unsigned Bits, typename Signed>
inline int add_values(const wide::ObWideInteger<Bits, Signed> &, const number::ObCompactNumber &,
                      char *res_buf, const int32_t)
{
  int ret = OB_NOT_SUPPORTED;
  SQL_LOG(WARN, "can't add", K(ret));
  return ret;
}

template <>
inline int add_values(const int64_t &l, const number::ObCompactNumber &r, char *res_buf,
                      const int32_t res_len)
{
  int ret = OB_NOT_SUPPORTED;
  SQL_LOG(WARN, "can't add", K(ret));
  return ret;
}

template <>
inline int add_values(const uint64_t &l, const number::ObCompactNumber &r, char *res_buf,
                      const int32_t res_len)
{
  int ret = OB_NOT_SUPPORTED;
  SQL_LOG(WARN, "can't add", K(ret));
  return ret;
}

template <>
inline int add_values(const int32_t &l, const number::ObCompactNumber &r, char *res_buf,
                      const int32_t res_len)
{
  int ret = OB_NOT_SUPPORTED;
  SQL_LOG(WARN, "can't add", K(ret));
  return ret;
}

template<typename L, typename R>
inline int sub_values(const L &l, const R &r, char *res_buf, const int32_t res_len)
{
  int ret = OB_SUCCESS;
  L &res = *reinterpret_cast<L *>(res_buf) ;
  res = l - r;
  return ret;
}

template<typename Input, typename Output>
struct Caster
{
  inline static int to_type(const char *src, const int32_t src_len, const ObScale scale,
                            ObIAllocator &alloc, Output *&out_val, int32_t &out_len)
  {
    UNUSEDx(src_len, scale);
    int ret = OB_SUCCESS;
    if (OB_ISNULL(out_val = (Output *)alloc.alloc(sizeof(Output)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      *out_val = *reinterpret_cast<const Input *>(src);
      out_len = sizeof(Output);
    }
    return ret;
  }
};

template<typename Input>
struct Caster<Input, char[0]>
{
  using Output = char[0];
  inline static int to_type(const char *src, const int32_t src_len, const ObScale scale,
                            ObIAllocator &alloc, Output *&out_val, int32_t &out_len)
  {
    UNUSEDx(src, src_len, scale, alloc, out_val, out_len);
    return OB_SUCCESS;
  }
};

template<typename Integer>
struct Caster<Integer, number::ObCompactNumber>
{
  static_assert(std::is_integral<Integer>::value, "must be native integer");
  inline static int to_type(const char *src, const int32_t src_len, const ObScale scale,
                            ObIAllocator &alloc, number::ObCompactNumber *&out, int32_t &out_len)
  {
    UNUSEDx(src_len, scale);
    int ret = OB_SUCCESS;
    char local_buf[number::ObNumber::MAX_CALC_BYTE_LEN] = {0};
    ObDataBuffer tmp_alloc(local_buf, number::ObNumber::MAX_CALC_BYTE_LEN);
    number::ObNumber result_nmb;
    uint32_t *out_buf = nullptr;
    const Integer &in_val = *reinterpret_cast<const Integer *>(src);
    if (std::is_signed<Integer>::value) { // int
      int64_t tmp_v = in_val;
      if (OB_FAIL(wide::to_number(in_val, scale, tmp_alloc, result_nmb))) {
        SQL_LOG(WARN, "to_number failed", K(ret));
      }
    } else { // uint
      uint64_t tmp_v = in_val;
      if (OB_FAIL(result_nmb.from(tmp_v, tmp_alloc))) {
        SQL_LOG(WARN, "cast to number failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(out_buf = (uint32_t *)alloc.alloc(
                           sizeof(ObNumberDesc) + result_nmb.d_.len_ * sizeof(uint32_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      out = reinterpret_cast<number::ObCompactNumber *>(out_buf);
      out->desc_ = result_nmb.d_;
      MEMCPY(out->digits_, result_nmb.get_digits(), result_nmb.d_.len_ * sizeof(uint32_t));
      out_len = sizeof(ObNumberDesc) + result_nmb.d_.len_ * sizeof(uint32_t);
    }
    return ret;
  }
};

template<>
struct Caster<number::ObCompactNumber, number::ObCompactNumber>
{
  inline static int to_type(const char *src, const int32_t src_len, const ObScale scale,
                            ObIAllocator &alloc, number::ObCompactNumber *&out, int32_t &out_len)
  {
    UNUSEDx(scale, alloc);
    // shadow copy
    out_len = src_len;
    out =
      const_cast<number::ObCompactNumber *>(reinterpret_cast<const number::ObCompactNumber *>(src));
    return OB_SUCCESS;
  }
};

template<typename Integer>
struct DecintToNmb
{
  inline static int to_type(const char *src, const int32_t src_len, const ObScale scale,
                            ObIAllocator &alloc, number::ObCompactNumber *&out, int32_t &out_len)
  {
    UNUSED(src_len);
    int ret = OB_SUCCESS;
    char local_buf[number::ObNumber::MAX_CALC_BYTE_LEN] = {0};
    ObDataBuffer tmp_alloc(local_buf, number::ObNumber::MAX_CALC_BYTE_LEN);
    number::ObNumber res_nmb;
    char *tmp_buf = nullptr;
    if (OB_FAIL(wide::to_number(*reinterpret_cast<const Integer *>(src), scale, tmp_alloc,
                                res_nmb))) {
      SQL_LOG(WARN, "to_number failed", K(ret));
    } else if (OB_ISNULL(tmp_buf = (char *)alloc.alloc(sizeof(ObNumberDesc)
                                                       + res_nmb.d_.len_ * sizeof(uint32_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      out = reinterpret_cast<number::ObCompactNumber *>(tmp_buf);
      out->desc_ = res_nmb.d_;
      MEMCPY(out->digits_, res_nmb.get_digits(), res_nmb.d_.len_ * sizeof(uint32_t));
      out_len = sizeof(ObNumberDesc) + res_nmb.d_.len_ * sizeof(uint32_t);
    }
    return ret;
  }
};

template<>
struct Caster<int128_t, number::ObCompactNumber>: public DecintToNmb<int128_t> {};

template<>
struct Caster<int256_t, number::ObCompactNumber>: public DecintToNmb<int256_t> {};

template<>
struct Caster<int512_t, number::ObCompactNumber>: public DecintToNmb<int512_t> {};

// ================
// calculation types
template <VecValueTypeClass vec_tc>
using AggCalcType = typename std::conditional<vec_tc == VEC_TC_NUMBER, number::ObCompactNumber,
                                              RTCType<vec_tc>>::type;

// TODO: use small word such as uint8 for NotNullBitVecotr
struct AggBitVector: sql::ObTinyBitVector
{
  inline static int64_t word_bits() { return sql::ObTinyBitVector::WORD_BITS; }
  inline static int64_t word_size() { return sql::ObTinyBitVector::BYTES_PER_WORD; }
};

using NotNullBitVector = AggBitVector;

inline bool supported_aggregate_function(const ObItemType agg_op)
{
  switch (agg_op) {
  case T_FUN_COUNT:
  case T_FUN_MIN:
  case T_FUN_MAX:
  case T_FUN_COUNT_SUM:
  case T_FUN_SUM:
  case T_FUN_APPROX_COUNT_DISTINCT:
  case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
  case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
  case T_FUN_SYS_BIT_OR:
  case T_FUN_SYS_BIT_AND:
  case T_FUN_SYS_BIT_XOR: {
    return true;
  }
  default:
    return false;
  }
}

inline bool agg_res_not_null(const ObItemType agg_op)
{
  // TODO: add other functions
  bool ret = false;
  switch (agg_op) {
  case T_FUN_COUNT:
  case T_FUN_COUNT_SUM: {
    ret = true;
    break;
  }
  default: {
    break;
  }
  }
  return ret;
}

#define AGG_FIXED_TC_LIST     \
  VEC_TC_INTEGER,             \
  VEC_TC_UINTEGER,            \
  VEC_TC_FLOAT,               \
  VEC_TC_DOUBLE,              \
  VEC_TC_FIXED_DOUBLE,        \
  VEC_TC_DATETIME,            \
  VEC_TC_DATE,                \
  VEC_TC_TIME,                \
  VEC_TC_YEAR,                \
  VEC_TC_BIT,                 \
  VEC_TC_ENUM_SET,            \
  VEC_TC_TIMESTAMP_TZ,        \
  VEC_TC_TIMESTAMP_TINY,      \
  VEC_TC_INTERVAL_YM,         \
  VEC_TC_INTERVAL_DS,         \
  VEC_TC_DEC_INT32,           \
  VEC_TC_DEC_INT64,           \
  VEC_TC_DEC_INT128,          \
  VEC_TC_DEC_INT256,          \
  VEC_TC_DEC_INT512

#define AGG_VEC_TC_LIST       \
  VEC_TC_NULL,                \
  VEC_TC_INTEGER,             \
  VEC_TC_UINTEGER,            \
  VEC_TC_FLOAT,               \
  VEC_TC_DOUBLE,              \
  VEC_TC_FIXED_DOUBLE,        \
  VEC_TC_NUMBER,              \
  VEC_TC_DATETIME,            \
  VEC_TC_DATE,                \
  VEC_TC_TIME,                \
  VEC_TC_YEAR,                \
  VEC_TC_STRING,              \
  VEC_TC_EXTEND,              \
  VEC_TC_BIT,                 \
  VEC_TC_ENUM_SET,            \
  VEC_TC_ENUM_SET_INNER,      \
  VEC_TC_TIMESTAMP_TZ,        \
  VEC_TC_TIMESTAMP_TINY,      \
  VEC_TC_RAW,                 \
  VEC_TC_INTERVAL_YM,         \
  VEC_TC_INTERVAL_DS,         \
  VEC_TC_ROWID,               \
  VEC_TC_LOB,                 \
  VEC_TC_JSON,                \
  VEC_TC_GEO,                 \
  VEC_TC_UDT,                 \
  VEC_TC_DEC_INT32,           \
  VEC_TC_DEC_INT64,           \
  VEC_TC_DEC_INT128,          \
  VEC_TC_DEC_INT256,          \
  VEC_TC_DEC_INT512,          \
  VEC_TC_ROARINGBITMAP

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_UTIL_H_
