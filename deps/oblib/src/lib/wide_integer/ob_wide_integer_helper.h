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

#ifndef OB_WIDE_INTEGER_HELPER_H_
#define OB_WIDE_INTEGER_HELPER_H_

#include <limits>
#include "src/sql/engine/ob_bit_vector.h"

#define DISPATCH_WIDTH_TASK(width, task)                                                           \
  switch ((width)) {                                                                               \
  case sizeof(int32_t): {                                                                          \
    task(int32_t);                                                                                 \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int64_t): {                                                                          \
    task(int64_t);                                                                                 \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int128_t): {                                                                         \
    task(int128_t);                                                                                \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int256_t): {                                                                         \
    task(int256_t);                                                                                \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int512_t): {                                                                         \
    task(int512_t);                                                                                \
    break;                                                                                         \
  }                                                                                                \
  default: {                                                                                       \
    ret = OB_ERR_UNEXPECTED;                                                                       \
    COMMON_LOG(WARN, "invalid int bytes", K((width)));                                             \
  }                                                                                                \
  }

#define DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, task)                                       \
  switch ((in_width)) {                                                                            \
  case sizeof(int32_t): {                                                                          \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int32_t, int32_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int32_t, int64_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int32_t, int128_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int32_t, int256_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int32_t, int512_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int64_t): {                                                                          \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int64_t, int32_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int64_t, int64_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int64_t, int128_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int64_t, int256_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int64_t, int512_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int128_t): {                                                                         \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int128_t, int32_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int128_t, int64_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int128_t, int128_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int128_t, int256_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int128_t, int512_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int256_t): {                                                                         \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int256_t, int32_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int256_t, int64_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int256_t, int128_t);                                                                    \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int256_t, int256_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int256_t, int512_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int512_t): {                                                                         \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int512_t, int32_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int512_t, int64_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int512_t, int128_t);                                                                    \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int512_t, int256_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int512_t, int512_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  default: {                                                                                       \
    ret = OB_ERR_UNEXPECTED;                                                                       \
    COMMON_LOG(WARN, "invalid int bytes", K((in_width)));                                          \
  }                                                                                                \
  }

namespace oceanbase
{
namespace common
{
struct ObIAllocator;
namespace wide
{

// IsWideInteger
template <typename T>
struct IsWideInteger
{
  constexpr static const bool value = false;
};

template <unsigned Bits, typename Signed>
struct IsWideInteger<ObWideInteger<Bits, Signed>>
{
  constexpr static const bool value = true;
};

template <typename T>
struct IsIntegral
{
  constexpr static const bool value = std::is_integral<T>::value;
};

template <unsigned Bits, typename Signed>
struct IsIntegral<ObWideInteger<Bits, Signed>>
{
  constexpr static const bool value = true;
};

template<typename T>
struct SignedConcept
{
  constexpr static const bool value = std::is_signed<T>::value;
  using type = typename std::conditional<std::is_signed<T>::value, signed, unsigned>::type;
};

template<unsigned Bits, typename Signed>
struct SignedConcept<ObWideInteger<Bits, Signed>>
{
  constexpr static const bool value = std::is_same<Signed, signed>::value;
  using type = signed;
};

template<unsigned Bits, unsigned Bits2>
struct BiggerBits
{
  static unsigned constexpr value()
  {
    return (Bits > Bits2 ? Bits : Bits2);
  }
};

template <typename T>
struct Limits
{
  static_assert(IsIntegral<T>::value, "");
  static T min()
  {
    return std::numeric_limits<T>::min();
  }
  static T max()
  {
    return std::numeric_limits<T>::max();
  }
};

template<unsigned Bits, typename Signed>
struct Limits<ObWideInteger<Bits, Signed>>
{
  static  ObWideInteger<Bits, Signed> min()
  {
    const unsigned constexpr item_count = ObWideInteger<Bits, Signed>::ITEM_COUNT;
    ObWideInteger<Bits, Signed> res;
    if (std::is_same<Signed, signed>::value) {
      res.items_[item_count - 1] = std::numeric_limits<int64_t>::min();
    }
    return res;
  }
  static  ObWideInteger<Bits, Signed> max()
  {
    const unsigned constexpr item_count = ObWideInteger<Bits, Signed>::ITEM_COUNT;
    ObWideInteger<Bits, Signed> res;
    if (std::is_same<Signed, signed>::value) {
      res.items_[item_count - 1] = std::numeric_limits<int64_t>::max();
    } else {
      res.items_[item_count - 1] = std::numeric_limits<uint64_t>::max();
    }
    for (unsigned i = 0; i < item_count - 1; i++) {
      res.items_[i] = std::numeric_limits<uint64_t>::max();
    }
    return res;
  }
};

#define SPECIALIZE_NATIVE_LIMITS(int_type, Signed)                                                 \
  template <>                                                                                      \
  struct Limits<int_type>                                                                          \
  {                                                                                                \
    static const int_type Signed##_min_v;                                                          \
    static const int_type Signed##_max_v;                                                          \
    static int_type min()                                                                          \
    {                                                                                              \
      return Signed##_min_v;                                                                       \
    }                                                                                              \
    static int_type max()                                                                          \
    {                                                                                              \
      return Signed##_max_v;                                                                       \
    }                                                                                              \
  }

#define SPECIALIZE_LIMITS(B, Signed)                                                               \
  template <>                                                                                      \
  struct Limits<ObWideInteger<B, Signed>>                                                          \
  {                                                                                                \
    static const ObWideInteger<B, Signed> Signed##_min_v;                                          \
    static const ObWideInteger<B, Signed> Signed##_max_v;                                          \
    static ObWideInteger<B, Signed> min()                                                          \
    {                                                                                              \
      return Signed##_min_v;                                                                       \
    }                                                                                              \
    static ObWideInteger<B, Signed> max()                                                          \
    {                                                                                              \
      return Signed##_max_v;                                                                       \
    }                                                                                              \
  }

SPECIALIZE_LIMITS(128, signed);
SPECIALIZE_LIMITS(128, unsigned);
SPECIALIZE_LIMITS(256, signed);
SPECIALIZE_LIMITS(256, unsigned);
SPECIALIZE_LIMITS(512, signed);
SPECIALIZE_LIMITS(512, unsigned);
SPECIALIZE_LIMITS(1024, signed);
SPECIALIZE_LIMITS(1024, unsigned);

SPECIALIZE_NATIVE_LIMITS(int32_t, signed);
SPECIALIZE_NATIVE_LIMITS(int64_t, signed);

#undef SPECIALIZE_LIMITS

template<typename T1, typename T2>
struct CommonType
{
  template<bool B, class T, class F>
  using conditional_t = typename std::conditional<B, T, F>::type;

  using type =
      conditional_t<(sizeof(T1) < sizeof(T2)), T2,
                    conditional_t<(sizeof(T1) > sizeof(T2)), T1,
                                  conditional_t<(SignedConcept<T1>::value ||
                                                !SignedConcept<T2>::value),
                                                T2, T1>>>;
};

template<typename Integer>
struct PromotedInteger {};

template<>
struct PromotedInteger<int32_t>
{
  using type = int64_t;
};

template<>
struct PromotedInteger<int64_t>
{
  using type = ObWideInteger<128, signed>;
};

template <unsigned Bits, typename Signed>
struct PromotedInteger<ObWideInteger<Bits, Signed>> {
  static const constexpr unsigned max_bits = (1U<<10);
  using type = typename std::conditional<(Bits < max_bits / 2),
                                         ObWideInteger<Bits * 2, Signed>,
                                         ObWideInteger<max_bits, Signed>>::type;
};

class ObDecimalIntConstValue
{
public:
  // for mysql mode
  static const int512_t MYSQL_DEC_INT_MIN;
  static const int512_t MYSQL_DEC_INT_MAX;
  static const int512_t MYSQL_DEC_INT_MAX_AVAILABLE;
  static const int16_t MAX_ORACLE_SCALE_DELTA = 0 - number::ObNumber::MIN_SCALE;
  static const int16_t MAX_ORACLE_SCALE_SIZE =
    number::ObNumber::MAX_SCALE - number::ObNumber::MIN_SCALE;

  static int init_const_values(ObIAllocator &allocator, const lib::ObMemAttr &attr);
  inline static int16_t oracle_delta_scale(const int16_t in_scale)
  {
    return in_scale + MAX_ORACLE_SCALE_DELTA;
  }
  inline static int get_int_bytes_by_precision(int16_t precision)
  {
    if (precision <= 0) {
      return 0;
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_32) {
      return sizeof(int32_t);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
      return sizeof(int64_t);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
      return sizeof(int128_t);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_256) {
      return sizeof(int256_t);
    } else {
      return sizeof(int512_t);
    }
  }

  inline static int get_max_precision_by_int_bytes(int int_bytes)
  {
    if (int_bytes <= 0) {
      return 0;
    } else if (int_bytes <= sizeof(int32_t)) {
      return MAX_PRECISION_DECIMAL_INT_32;
    } else if (int_bytes <= sizeof(int64_t)) {
      return MAX_PRECISION_DECIMAL_INT_64;
    } else if (int_bytes <= sizeof(int128_t)) {
      return MAX_PRECISION_DECIMAL_INT_128;
    } else if (int_bytes <= sizeof(int256_t)) {
      return MAX_PRECISION_DECIMAL_INT_256;
    } else {
      return MAX_PRECISION_DECIMAL_INT_512;
    }
  }

  inline static int get_zero_value_byte_precision(int16_t precision, const ObDecimalInt *&decint, int32_t &int_bytes)
  {
    int ret = OB_SUCCESS;
    ObDecimalIntWideType dec_type = get_decimalint_type(precision);
    if (OB_UNLIKELY(dec_type == DECIMAL_INT_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid precision", K(precision));
    } else {
      decint = ZERO_VALUES[dec_type];
      int_bytes = ((int32_t(1)) << dec_type) * sizeof(int32_t);
    }
    return ret;
  }

  inline static const ObDecimalInt* get_max_upper(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MAX_UPPER[prec];
  }

  inline static const ObDecimalInt* get_min_lower(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MIN_LOWER[prec];
  }
  inline static const ObDecimalInt* get_max_value(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MAX_DECINT[prec];
  }

  inline static const ObDecimalInt* get_min_value(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MIN_DECINT[prec];
  }
private:
  // MYSQL_MIN
  static const ObDecimalInt *MIN_DECINT[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];

  // MYSQL_MAX
  static const ObDecimalInt *MAX_DECINT[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];
  // MYSQL_MAX_UPPER
  static const ObDecimalInt *MAX_UPPER[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];
  // MYSQL_MIN_LOWER
  static const ObDecimalInt *MIN_LOWER[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];

  static const ObDecimalInt *ZERO_VALUES[5];
};

// helper functions
inline bool is_negative(const ObDecimalInt *decint, int32_t int_bytes)
{
  switch (int_bytes) {
  case sizeof(int32_t): return (*(decint->int32_v_)) < 0;
  case sizeof(int64_t): return (*(decint->int64_v_)) < 0;
  case sizeof(int128_t): return decint->int128_v_->is_negative();
  case sizeof(int256_t): return decint->int256_v_->is_negative();
  case sizeof(int512_t): return decint->int512_v_->is_negative();
  default: return false;
  }
}

inline bool is_zero(const ObDecimalInt *decint, int32_t int_bytes)
{
  if (int_bytes == 0) {
    return true;
  }
  switch (int_bytes) {
  case sizeof(int32_t): return *(decint->int32_v_) == 0;
  case sizeof(int64_t): return *(decint->int64_v_) == 0;
  case sizeof(int128_t): return *(decint->int128_v_) == 0;
  case sizeof(int256_t): return *(decint->int256_v_) == 0;
  case sizeof(int512_t): return *(decint->int512_v_) == 0;
  default: return false;
  }
}

inline int negate(const ObDecimalInt *decint, int32_t int_bytes, ObDecimalInt *&out_decint,
                  int32_t out_bytes, ObIAllocator &allocator)
{
#define DO_NEG_VAL(in_type, out_type)\
  *reinterpret_cast<out_type *>(out_decint) = -(*reinterpret_cast<const in_type *>(decint));

  int ret = OB_SUCCESS;
  out_decint = nullptr;
  if (out_bytes == 0 || int_bytes == 0) {
    // do nothing
  } else if (OB_ISNULL(out_decint = (ObDecimalInt *)allocator.alloc(out_bytes))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    DISPATCH_INOUT_WIDTH_TASK(int_bytes, out_bytes, DO_NEG_VAL);
  }
  return ret;
#undef DO_NEG_VAL
}

template <typename T>
int scale_up_decimalint(const T &x, unsigned scale, ObDecimalIntBuilder &res)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  // here we use int512 to store tmp result, because scale up may overflow, and then we need copy
  // current result to bigger integer, and then continue calculating. use int512 to avoid copy and
  // complicated overflow calculating
  int ret = OB_SUCCESS;
  int512_t result = x;
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        result = result * pows[i]; // this may also overflow, ignore it
        scale -= (1<<i);
      }
    }
    if (scale != 0) {
      result = result * 10;
      scale -= 1;
    }
  }
  if (result <= wide::Limits<int32_t>::max() && result >= wide::Limits<int32_t>::min()) {
    res.from(result, sizeof(int32_t));
  } else if (result <= wide::Limits<int64_t>::max() && result >= wide::Limits<int64_t>::min()) {
    res.from(result, sizeof(int64_t));
  } else if (result <= wide::Limits<int128_t>::max() && result >= wide::Limits<int128_t>::min()) {
    res.from(result, sizeof(int128_t));
  } else if (result <= wide::Limits<int256_t>::max() && result >= wide::Limits<int256_t>::min()) {
    res.from(result, sizeof(int256_t));
  } else {
    res.from(result);
  }
  return ret;
}

template <typename T>
int scale_down_decimalint_for_round(const T &x, unsigned scale, ObDecimalIntBuilder &res)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  static const int64_t carry_threshholds[5] = {5, 50, 5000, 50000000, 5000000000000000};
  int ret = OB_SUCCESS;
  T result = x;
  if (x < 0) { result = -result; }
  T remain;
  bool last_carry = false;
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && result != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        remain = result % pows[i];
        result = result / pows[i];
        last_carry = (remain >= carry_threshholds[i]);
        scale -= (1 << i);
      }
    }
    if (scale != 0) {
      remain = result % 10;
      result = result / 10;
      last_carry = (remain >= 5);
      scale -= 1;
    }
  }
  if (scale != 0) { last_carry = false; }
  if (last_carry) { result++; }
  if (x < 0) { result = -result; }
  res.from(result);
  return ret;
}

template <typename T>
int scale_down_decimalint_for_trunc(const T &x, unsigned scale, ObDecimalIntBuilder &res)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  T result = x;
  if (x < 0) { result = -result; }
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && result != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        result = result / pows[i];
        scale -= (1 << i);
      }
    }
    if (scale != 0) {
      result = result / 10;
      scale -= 1;
    }
  }
  if (x < 0) { result = -result; }
  res.from(result);
  return OB_SUCCESS;
}

template <typename T>
inline int scale_down_decimalint(const T &x, unsigned scale, const bool is_trunc, ObDecimalIntBuilder &res)
{
  int ret = OB_SUCCESS;
  if (is_trunc) {
    scale_down_decimalint_for_trunc(x, scale, res);
  } else {
    scale_down_decimalint_for_round(x, scale, res);
  }

  return OB_SUCCESS;
}

int common_scale_decimalint(const ObDecimalInt *decint, const int32_t int_bytes,
                            const ObScale in_scale, const ObScale out_scale,
                            ObDecimalIntBuilder &res, const bool is_trunc = false);

int check_range_valid_int64(
    const ObDecimalInt *decint, const int32_t int_bytes,
    bool &is_valid_int64, int64_t &res_val); // scale is regarded as 0

int check_range_valid_uint64(
    const ObDecimalInt *decint, const int32_t int_bytes,
    bool &is_valid_uint64, uint64_t &res_val); // scale is regarded as 0

template<typename Hash, typename Obj>
struct PartitionHash
{
  using HashMethod = Hash;
  static int calculate(const Obj &val, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    constexpr static uint32_t SIGN_BIT_MASK = (1 << 31);
    const uint32_t *data = reinterpret_cast<const uint32_t *>(val.get_decimal_int());
    int32_t last = val.get_int_bytes() / sizeof(uint32_t) - 1;
    // find minimum length of uint32_t values to represent `val`:
    // if data[last] ==  UINT32_MAX && data[last - 1]'s highest bit is 1, last--
    // else if data[last] == 0 && data[last - 1]'s highest bit is 0, last--
    //
    // this way, val can be easily recovered appending 0/UINT32_MAX values
    if (last <= 0) {
      // do nothing
    } else if (data[last] == UINT32_MAX) {
      while (last > 0 && data[last] == UINT32_MAX && (data[last - 1] & SIGN_BIT_MASK)) {
        last--;
      }
    } else if (data[last] == 0) {
      while (last > 0 && data[last] == 0 && ((data[last - 1] & SIGN_BIT_MASK) == 0)) {
        last--;
      }
    }
    res = HashMethod::hash(data, (last + 1) * sizeof(uint32_t), seed);
    return ret;
  }

  static void hash_batch(uint64_t *hash_values, Obj *vals, const bool is_batch_datum,
                         const sql::ObBitVector &skip, const int64_t size,
                         const uint64_t *seeds, const bool is_batch_seed)
  {
    if (is_batch_datum && !is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const Obj, true>(vals), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    } else if (is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const Obj, true>(vals), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else if (!is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const Obj, false>(vals), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else {
      do_hash_batch(hash_values, VectorIter<const Obj, false>(vals), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    }
  }
private:
  template <typename DATUM_VEC, typename SEED_VEC>
  static void do_hash_batch(uint64_t *hash_values, const DATUM_VEC &datum_vec,
                            const sql::ObBitVector &skip, const int64_t size,
                            const SEED_VEC &seed_vec)
  {
    sql::ObBitVector::flip_foreach(
      skip, size, [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        ret = calculate(datum_vec[idx], seed_vec[idx], hash_values[idx]);
        return ret;
      });
  }
  template<typename T, bool is_vec>
  struct VectorIter
  {
    explicit VectorIter(T *vec): vec_(vec) {}
    T &operator[](const int64_t idx) const
    {
      return is_vec ? vec_[idx] : vec_[0];
    }
    T *vec_;
  };
};

} // end namespace wide
} // end namespace common
} // end namespace oceanbase
#endif // !OB_WIDE_INTEGER_HELPER_H_
