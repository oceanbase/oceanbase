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

#ifndef OCEANBASE_LIB_NUMBER_OB_NUMBER_V2_H_
#define OCEANBASE_LIB_NUMBER_OB_NUMBER_V2_H_
#include <stdint.h>
#include <algorithm>
#include "lib/ob_define.h"
#include "lib/worker.h"
#include "lib/utility/utility.h"
#include "common/data_buffer.h"
#include "common/ob_accuracy.h"

#define ITEM_SIZE(ptr) sizeof((ptr)[0])

namespace oceanbase
{

namespace sql
{
  // forward declaration for all friends
  class ObAggregateProcessor;
}

namespace common
{
static const int64_t MAX_FLOAT_PRINT_SIZE = 64;
static const int64_t MAX_DOUBLE_PRINT_SIZE = 64;

namespace number
{
// Poly: [a, b, c, d] = a * Base^3
//                    + b * Base^2
//                    + c * Base^1
//                    + d * Base^0;
// T should implement:
// uint64_t at(const int64_t idx) const;
// uint64_t base() const;
// int64_t size() const;
// int set(const int64_t idx, const uint64_t digit);
// int ensure(const int64_t size);
// T ref(const int64_t start, const int64_t end) const;
// int assign(const T &other, const int64_t start, const int64_t end);
// int64_t to_string(char* buffer, const int64_t length) const;
//
////////////////////////////////////////////////////////////////////////////////////////////////////

static const uint64_t MAX_BASE = 0x0000000100000000;

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T>
int poly_mono_mul(
    const T &multiplicand,
    const uint64_t multiplier,
    T &product);

template <class T>
int poly_mono_div(
    const T &dividend,
    const uint64_t divisor,
    T &quotient,
    uint64_t &remainder);

template <class T>
int poly_poly_add(
    const T &augend,
    const T &addend,
    T &sum);

template <class T>
int poly_poly_sub(
    const T &minuend,
    const T &subtrahend,
    T &remainder,
    bool &negative,
    const bool truevalue = true);

template <class T>
int poly_poly_mul(
    const T &multiplicand,
    const T &multiplier,
    T &product);

//template <class T>
//int poly_poly_div(
//    const T &dividend,
//    const T &divisor,
//    T &quotient,
//    T &remainder);

////////////////////////////////////////////////////////////////////////////////////////////////////
#define MAX_FMT_STR_LEN 64
struct ObNumberFmtModel
{
  bool has_x_ = false;
  bool has_d_ = false;               //has D/d/. format element:decimal point
  bool has_sign_ = false;            //has S/s format element：sign
  bool has_currency_ = false;        //has currency element: only support dollar for now
  bool has_b_ = false;               //has B/b format element:Returns blanks for the integer part
                                     //of a fixed-point number when the integer part is zero
  bool has_comma_ = false;
  int16_t dc_position_ = -1;         //dot character
  int16_t sign_position_ = -1;       //position of sign: 1 first; 2 last
  int16_t fmt_len_ = 0;
  char *fmt_str_;
};

// Compact number
struct ObCompactNumber
{
  ObCompactNumber() = delete;
  ObNumberDesc desc_;
  uint32_t digits_[0];

  bool is_zero() const { return 0 == desc_.len_; }

  TO_STRING_KV(K_(digits), K(desc_.desc_));
};

static_assert(sizeof(ObCompactNumber) == sizeof(uint32_t), "wrong compact number size");

class ObCalcVector;
class ObNumber final
{
  friend class oceanbase::sql::ObAggregateProcessor;
  class IAllocator
  {
  public:
    virtual ~IAllocator() {};
    virtual uint32_t *alloc(const int64_t num) = 0;
    virtual uint32_t *alloc(const int64_t num, const lib::ObMemAttr &attr) = 0;
  };
  template <class T>
  class TAllocator : public IAllocator
  {
  public:
    explicit TAllocator(T &allocator) : allocator_(allocator) {};
    uint32_t *alloc(const int64_t num) { return (uint32_t *)allocator_.alloc(num); }
    uint32_t *alloc(const int64_t num, const lib::ObMemAttr &attr) { return (uint32_t *)allocator_.alloc(num, attr); }
  private:
    T &allocator_;
  };

public:
  typedef ObNumberDesc Desc;
  static const uint64_t BASE = 1000000000;
  static const uint64_t MAX_VALUED_DIGIT = BASE - 1;
  static const uint64_t BASE2 = 1000000000000000000;
  static const int64_t DIGIT_LEN = 9;
  static const char *DIGIT_FORMAT;
  static const char *BACK_DIGIT_FORMAT[DIGIT_LEN];
  static const ObPrecision MIN_PRECISION = 0;
  static const ObPrecision MAX_PRECISION = 65;
  static const ObScale MIN_SCALE = -84;
  static const ObScale MAX_SCALE = 127;
  static const ObScale MAX_TOTAL_SCALE = MAX_SCALE + MAX_PRECISION + 1;
  // 5 valid digits, another: 1 for round, 1 for negative end flag
  static const int64_t MAX_STORE_LEN = 9;
  static const int64_t MAX_APPEND_LEN = 3;// '+'/'-', '.', '\0'
  // number存储需要消耗的空间，6个int32_t的digit空间，1个int32_t的Desc空间
  static const int64_t MAX_BYTE_LEN = sizeof(uint32_t) * MAX_STORE_LEN + sizeof(Desc::desc_);
  static const ObScale FLOATING_SCALE = 72;  // 8 valid digits
  static const int64_t MAX_INTEGER_EXP = 14; // {9.999...999,count=38}e134 14*9-1=134
  static const int64_t MAX_DECIMAL_EXP = 15; // {1}e-135
  static const int64_t MAX_CALC_LEN = 32;
  static const uint8_t NEGATIVE = 0;
  static const uint8_t POSITIVE = 1;
  static const uint8_t EXP_ZERO = 0x40;
  static const int64_t MAX_PRINTABLE_SIZE = 256;
  static const int32_t OB_MAX_INTEGER_DIGIT = 3;
  static const int32_t OB_MAX_DECIMAL_DIGIT = MAX_STORE_LEN; // attention ... show be equal to MAX_STORE_LEN + 1
  static const int32_t OB_CALC_BUFFER_SIZE = OB_MAX_DECIMAL_DIGIT + 1;
  static const int32_t OB_DIV_BUFFER_SIZE = OB_CALC_BUFFER_SIZE * 2;
  static const int32_t OB_MULT_BUFFER_SIZE = OB_CALC_BUFFER_SIZE * 2 + 1;
  static const int32_t OB_REM_BUFFER_SIZE = 32; // REM need to change both operands to integer, maximum length = 15 + 15
  static const uint64_t CARRY_BOUND = 5000000000000000000;
  static const int64_t MAX_SCI_SIZE = 126;    /* compatible with Oracle */
  static const int64_t MIN_SCI_SIZE = -130;    /* compatible with Oracle */
  static const int64_t SCI_NUMBER_LENGTH = 40;
  static const int64_t MAX_FAST_SUM_AGG_NUMBER_LENGTH = 25 + MAX_APPEND_LEN;
  static const int POSITIVE_EXP_BOUNDARY = 0xc0;
  static const int NEGATIVE_EXP_BOUNDARY = 0x40;
  static const int MIN_ROUND_DIGIT_COUNT[];//8/4
  static const int MAX_DIGIT_COUNT = MAX_CALC_LEN * DIGIT_LEN;
  static const int MAX_NUMBER_ALLOC_BUFF_SIZE = MAX_TOTAL_SCALE + 1; //large then ObNumber::MAX_TOTAL_SCALE
  static const char FLOATING_ZEROS[FLOATING_SCALE + 1];
  static const int64_t MAX_CALC_BYTE_LEN = sizeof(uint32_t) * OB_CALC_BUFFER_SIZE;
  static const ObNumber &get_positive_one();
  static const ObNumber &get_positive_zero_dot_five();
  static const ObNumber &get_zero();
  static const ObNumber &get_pi();
public:
  ObNumber() : d_(0), digits_(0)
  {
    d_.sign_ = POSITIVE;
  }
  ObNumber(const uint32_t desc, uint32_t *digits)
    : d_(desc), digits_(digits) {}
  ObNumber(const number::ObCompactNumber &num)
    : d_(num.desc_.desc_), digits_(const_cast<uint32_t *>(&(num.digits_[0]))) {}

  NEED_SERIALIZE_AND_DESERIALIZE;
public:
  //v1: original edition
  //v2: algorithmic optimization edition
  //v3: engineering optimization edition

  int sanity_check()
  {
    int ret = OB_SUCCESS;
    if (d_.len_ > OB_MAX_DECIMAL_DIGIT) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "Invalid digit len %u", d_.len_);
    } else if (d_.len_ > 0 && digits_ != nullptr) {
      for (auto i = 0;  OB_SUCC(ret) && i < d_.len_; i++) {
        if (digits_[i] >= BASE) {
          ret = OB_ERR_UNEXPECTED;
          _OB_LOG(WARN, "Invalid value %u", digits_[i]);
        } else {
          _OB_LOG(DEBUG, "Digit value %u", digits_[i]);
        }
      }
    }
    return ret;
  }
  template <class T>
  int from(const int64_t value, T &allocator);
  template <class T>
  int from(const uint64_t value, T &allocator);
  template <class T>
  int from(const char *str, T &allocator, int16_t *precision = NULL, int16_t *scale = NULL, const bool do_rounding = true);
  template <class T>
  int from(const char *str, const int64_t length, T &allocator, int16_t *precision = NULL, int16_t *scale = NULL, const bool do_rounding = true);
  template <class T>
  int from_v3(const char *str, const int64_t length, T &allocator, int16_t *precision = NULL, int16_t *scale = NULL);
  template <class T>
  int from_v2(const char *str, const int64_t length, T &allocator, int16_t *precision = NULL, int16_t *scale = NULL);
  template <class T>
  int from_v1(const char *str, const int64_t length, T &allocator, int16_t *precision = NULL, int16_t *scale = NULL);
  template <class T>
  int from(const char *str, const int64_t length, T &allocator, const lib::ObMemAttr &attr, int16_t *precision = NULL, int16_t *scale = NULL);
  template <class T>
  int from(const char *str, const int64_t length, T &allocator, number::ObNumberFmtModel *fmt, int16_t *precision = NULL, int16_t *scale = NULL);
  template <class T>
  int from(const int16_t precision, const int16_t scale, const char *str, const int64_t length,
           T &allocator);
  template <class T>
  int from_sci(const char *str, const int64_t length, T &allocator, int16_t *precision, int16_t *scale, const bool do_rounding = true);
  template <class T>
  int from(const uint32_t desc, const ObCalcVector &vector, T &allocator);
  template <class T>
  int from(const ObNumber &other, T &allocator);
  inline void shadow_copy(const ObNumber &other);
  int deep_copy(const ObNumber &other, IAllocator &allocator);
  int deep_copy_v3(const ObNumber &other, ObIAllocator &allocator);
  OB_INLINE int encode(char *buf, const int64_t buf_size, int64_t &pos, ObNumber *out = NULL) const;
  inline int decode(const char *buf, const int64_t buf_size, int64_t &pos);

  OB_INLINE void assign(const uint32_t desc, uint32_t *digits);
  int round_v1(const int64_t scale);
  inline int round_v2(const int64_t scale, const bool for_oracle_to_char = false);
  inline int round(const int64_t scale, const bool for_oracle_to_char = false)
  {
    return round_v3(scale, for_oracle_to_char);
  }

  template <class T>
  int from_sci_opt(const char *str, const int64_t length, T &allocator,
                   int16_t *precision = NULL, int16_t *scale = NULL,
                   const bool do_rounding = true);
  inline int round_v3(const int64_t scale, const bool for_oracle_to_char = false);
  // used when cast number to string in oracle mode
  inline int round_for_sci(const int64_t scale, const bool for_oracle_to_char = false);
  int round_precision(const int64_t precision);
  int floor(const int64_t scale);
  int ceil(const int64_t scale);
  int check_and_round(const int64_t precision, const int64_t scale);
  int trunc(const int64_t scale);
//  int to_mysql_binary(char *buffer, const int64_t length, int64_t &pos);

  template <class T>
  int add(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int add_v1(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int add_v2(const ObNumber &other, ObNumber &value, T &allocator) const;
  //strict_mode = true, means only alloc necessary size. otherwise alloc max fixed-length
  int add_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
             const bool strict_mode = true, const bool do_rounding = true) const;

  static inline int add_v3(const ObNumber &left, const ObNumber &right, ObNumber &result,
          ObIAllocator &allocator);
  template <class T>
  int add(const T param1, const T param2, ObNumber &result, ObIAllocator &allocator) const;
  template <class T>
  int sub(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int sub_v2(const ObNumber &other, ObNumber &value, T &allocator) const;
  //strict_mode = true, means only alloc necessary size. otherwise alloc max fixed-length
  int sub_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
             const bool strict_mode = true, const bool do_rounding = true) const;
  static inline int sub_v3(const ObNumber &left, const ObNumber &right, ObNumber &result,
                           ObIAllocator &allocator);
  template <class T>
  int sub_v1(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int mul(const ObNumber &other, ObNumber &value, T &allocator, const bool do_rounding = true) const;
  template <class T>
  int mul_v1(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int mul_v2(const ObNumber &other, ObNumber &value, T &allocator) const;
  int mul_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
      const bool strict_mode = true, const bool do_rounding = true) const;

  static inline int mul_v3(const ObNumber &left, const ObNumber &right, ObNumber &result,
          ObIAllocator &allocator);

  template <class T>
  int div(const ObNumber &other, ObNumber &value, T &allocator,
          const int32_t qscale = OB_MAX_DECIMAL_DIGIT, const bool do_rounding = true) const;
  //template <class T>
  //int div_v1(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int div_v2(const ObNumber &other, ObNumber &value, T &allocator) const;
  int div_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator,
      const int32_t qscale = OB_MAX_DECIMAL_DIGIT, const bool do_rounding = true) const;
  template <class T>
  int rem(const ObNumber &other, ObNumber &value, T &allocator) const;
  int round_remainder(const ObNumber &other, ObNumber &value, ObIAllocator &allocator) const;
  // template <class T>
  // int rem_v1(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int rem_v2(const ObNumber &other, ObNumber &value, T &allocator) const;
  int rem_v3(const ObNumber &other, ObNumber &value, ObIAllocator &allocator) const;
  template <class T>
  int negate(ObNumber &value, T &allocator) const;
  int sqrt(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int ln(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int e_power(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int power(const int64_t exponent, ObNumber &value,
            ObIAllocator &allocator, const bool do_rounding = true) const;
  int power(const ObNumber &exponent, ObNumber &value,
            ObIAllocator &allocator, const bool do_rounding = true) const;
  int log(const ObNumber &base, ObNumber &value,
          ObIAllocator &allocator, const bool do_rounding = true) const;
  inline ObNumber negate() const __attribute__((always_inline));
  inline ObNumber abs() const __attribute__((always_inline));
  int width_bucket(const ObNumber &start, const ObNumber &end, const ObNumber &bucket, ObNumber &value, ObIAllocator &allocator) const;
  int sinh(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int cosh(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int tanh(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int asin(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int acos(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int atan(ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;
  int atan2(const ObNumber &other, ObNumber &value, ObIAllocator &allocator, const bool do_rounding = true) const;

  int sin(ObNumber &out, ObIAllocator &allocator, const bool do_rounding = true) const;
  int cos(ObNumber &out, ObIAllocator &allocator, const bool do_rounding = true) const;
  int tan(ObNumber &out, ObIAllocator &allocator, const bool do_rounding = true) const;

  static int compare(const number::ObNumber::Desc &this_desc,
                     const uint32_t *this_digits,
                     const number::ObNumber::Desc &other_desc,
                     const uint32_t *other_digits);
  inline static int compare_v2(const number::ObNumber::Desc &this_desc,
                        const uint32_t *this_digits,
                        const number::ObNumber::Desc &other_desc,
                        const uint32_t *other_digits);
  static bool is_equal(const number::ObNumber::Desc &this_desc,
                       const uint32_t *this_digits,
                       const number::ObNumber::Desc &other_desc,
                       const uint32_t *other_digits);
  inline static bool is_equal_v2(const number::ObNumber::Desc &this_desc,
                       const uint32_t *this_digits,
                       const number::ObNumber::Desc &other_desc,
                       const uint32_t *other_digits);
  inline int compare_v1(const ObNumber &other) const __attribute__((always_inline))
  {
    int ret = compare(this->d_, this->digits_, other.d_, other.digits_);
//    OB_LOG(DEBUG, "current info", KPC(this), K(other), K(ret), K(common::lbt()));
    return ret;
  }
  inline int compare(const ObNumber &other) const __attribute__((always_inline))
  {
    OB_LOG(DEBUG, "current info", KPC(this), K(other));
    return compare_v2(this->d_, this->digits_, other.d_, other.digits_);
  }
  inline bool is_equal(const ObNumber &other) const __attribute__((always_inline))
  {
//    OB_LOG(DEBUG, "current info", KPC(this), K(other));
    return is_equal_v2(this->d_, this->digits_, other.d_, other.digits_);
  }
  inline bool operator<(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator<=(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator>(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator>=(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator==(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator!=(const ObNumber &other) const __attribute__((always_inline));
  inline int abs_compare(const ObNumber &other) const __attribute__((always_inline));

  inline int compare(const int64_t &other) const __attribute__((always_inline));
  inline bool is_equal(const int64_t &other) const __attribute__((always_inline));
  inline bool operator<(const int64_t &other) const __attribute__((always_inline));
  inline bool operator<=(const int64_t &other) const __attribute__((always_inline));
  inline bool operator>(const int64_t &other) const __attribute__((always_inline));
  inline bool operator>=(const int64_t &other) const __attribute__((always_inline));
  inline bool operator==(const int64_t &other) const __attribute__((always_inline));
  inline bool operator!=(const int64_t &other) const __attribute__((always_inline));
  inline int abs_compare_sys(const uint64_t &other,
                             int64_t exp) const  __attribute__((always_inline));
  inline int is_in_uint(int64_t exp,
                        bool &is_uint,
                        uint64_t &num) const __attribute__((always_inline));;

  inline int compare(const uint64_t &other) const __attribute__((always_inline));
  inline bool is_equal(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator<(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator<=(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator>(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator>=(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator==(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator!=(const uint64_t &other) const __attribute__((always_inline));

  OB_INLINE static bool is_negative_number(const ObNumber::Desc &desc);
  OB_INLINE static bool is_zero_number(const ObNumber::Desc &desc);
  OB_INLINE static int64_t get_number_length(const ObNumber::Desc &desc);

  OB_INLINE void set_zero();
  OB_INLINE void set_one();
  OB_INLINE bool is_negative() const;
  OB_INLINE bool is_zero() const;
  OB_INLINE bool is_decimal() const;
  OB_INLINE bool is_integer() const;
  OB_INLINE bool has_decimal() const;

  inline static int uint32cmp(const uint32_t *s1, const uint32_t *s2,
                              const int64_t n) __attribute__((always_inline));
  inline static int uint32cmp_v2(const uint32_t *s1, const uint32_t *s2,
                                 const int64_t n) __attribute__((always_inline));
  inline static bool uint32equal(const uint32_t *s1, const uint32_t *s2,
                                 const int64_t n) __attribute__((always_inline));
  inline static bool uint32equal_v2(const uint32_t *s1, const uint32_t *s2,
                                    const int64_t n) __attribute__((always_inline));
  inline static uint32_t remove_back_zero(const uint32_t digit,
                                          int64_t &count) __attribute__((always_inline));
  inline static int64_t get_digit_len(const uint32_t v) __attribute__((always_inline));
  inline static int64_t get_digit_len_v2(const uint32_t v) __attribute__((always_inline));

  int64_t to_string(char *buffer, const int64_t length) const;
  int format_v1(char *buf, const int64_t buf_len, int64_t &pos, int16_t scale) const;
  inline int64_t get_max_format_length() const
  {
    int64_t ret_len = MAX_APPEND_LEN;
    if (!is_zero()) {
      const int32_t exp_val = get_decode_exp(d_);
      if (exp_val >= 0) {
        ret_len += std::max(exp_val + 1, static_cast<int32_t>(d_.len_)) * DIGIT_LEN;
      } else {
        ret_len += (0 - exp_val + static_cast<int32_t>(d_.len_)) * DIGIT_LEN;
      }
    }
    return ret_len;
  }
  inline bool fast_sum_agg_may_overflow () const
  {
    return get_max_format_length() > ObNumber::MAX_FAST_SUM_AGG_NUMBER_LENGTH;
  }
  inline int64_t get_deep_copy_size() const
  {
    return sizeof(int32_t) + (is_zero() ?  0 : d_.len_ * sizeof(int32_t));
  }
  int format_v2(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      int16_t scale,
      const bool need_to_sci = false) const;
  inline int format_int64(char *buf, int64_t &pos, const int16_t scale, bool &is_finish) const;
  inline int format(char *buf, const int64_t buf_len, int64_t &pos, int16_t scale) const
  {
//      return format_v1(buf, buf_len, pos, scale);
    return format_v2(buf, buf_len, pos, scale);
  }
  int format_with_oracle_limit(char *buf, const int64_t buf_len, int64_t &pos, int16_t scale) const;
  const char *format() const;

  OB_INLINE uint32_t get_desc_value() const;
  OB_INLINE const ObNumber::Desc& get_desc() const;
  uint32_t *get_digits() const;
  int64_t get_length() const;
  int64_t get_cap() const;
  bool is_valid_uint64(uint64_t &uint64) const;
  bool is_valid_int64(int64_t &int64) const;
  bool is_valid_int() const;
  bool is_int_parts_valid_int64(int64_t &int_parts, int64_t &decimal_parts) const;
  int extract_valid_int64_with_trunc(int64_t &value) const;
  int extract_valid_uint64_with_trunc(uint64_t &value) const;
  int extract_valid_int64_with_round(int64_t &value) const;
  int extract_valid_uint64_with_round(uint64_t &value) const;
  static int64_t get_decode_exp(const uint32_t desc) __attribute__((always_inline));
  void set_digits(uint32_t *digits) { digits_ = digits; }
  int normalize_(uint32_t *digits, const int64_t length);
  int normalize_v2_(const bool from_calc = true, const bool only_normalize_tailer = false);
  int normalize_v3_(const bool from_calc = true, const bool only_normalize_tailer = false);
  int round_even_number();
  int64_t get_scale() const;
  // 2 number multiplication fast path
  // formular format: a(2 digits number) * b(1 digit number):
  //  - a range: [1, 999999999].[000000001, 999999999]
  //  - b range: [1, 999999999] or 0.[000000001, 999999999]
  OB_INLINE static bool fast_mul_1d_mul_2d(const uint32_t *multiplicand_digits,
                                        const uint32_t *multiplier_digits,
                                        const uint32_t multiplicand_desc,
                                        const uint32_t multiplier_desc,
                                        int64_t res_len, int64_t res_exp,
                                        uint32_t *res_digit, Desc &res_desc);
  // 2 number multiplication fast path
  // formular format: a(1 digit number) * b(1 digit number):
  //  - a range: [1, 999999999] or 0.[000000001, 999999999]
  //  - b range: [1, 999999999] or 0.[000000001, 999999999]
  OB_INLINE static bool fast_mul_1d_mul_1d(const uint32_t *multiplicand_digits,
                                        const uint32_t *multiplier_digits,
                                        const uint32_t multiplicand_desc,
                                        const uint32_t multiplier_desc,
                                        int64_t res_len, int64_t res_exp,
                                        uint32_t *res_digit, Desc &res_desc);
  // 2 number multiplication fast path
  // formular format: a(2 digits number) * b(2 digits number):
  //  - a range: [1, 999999999].[000000001, 999999999]
  //  - b range: [1, 999999999].[000000001, 999999999]
  OB_INLINE static bool fast_mul_2d_mul_2d(const uint32_t *multiplicand_digits,
                                        const uint32_t *multiplier_digits,
                                        const uint32_t multiplicand_desc,
                                        const uint32_t multiplier_desc,
                                        int64_t res_len, int64_t res_exp,
                                        uint32_t *res_digit, Desc &res_desc);

  // multiplication fast path entrance
  OB_INLINE static bool try_fast_mul(ObNumber &l_num, ObNumber &r_num,
                                     uint32_t *res_digit, Desc &res_desc);
  // 2 posivtive number sum fast path
  // formular format: a(1 digit number) + b(1 digit number):
  //  - a range: [1, 999999999] or 0.[000000001, 999999999]
  //  - b range: [1, 999999999] or 0.[000000001, 999999999]
  OB_INLINE static bool try_fast_add(ObNumber &l_num, ObNumber &r_num,
                                     uint32_t *res_digit, Desc &res_desc);
  // 2 posivtive number minus fast path
  // formular format: a(1 digit integer ) - b(1 digit decimal):
  //  - a range: [1, 999999999]
  //  - b range: 0.[000000001, 999999999]
  OB_INLINE static bool try_fast_minus(ObNumber &l_num, ObNumber &r_num,
                                     uint32_t *res_digit, Desc &res_desc);



protected:
  int find_point_range_(const char *str, const int64_t length,
    int64_t &start_idx, int64_t &floating_point, int64_t &end_idx, bool &negative,
    int32_t &warning, int16_t *precision, int16_t *scale);
  int construct_digits_(const char *str, const int64_t start_idx, const int64_t floating_point,
      const int64_t end_idx, uint32_t *digits, int64_t &exp, int64_t &len);

  template <class IntegerT>
  int from_integer_(IntegerT integer_val, IAllocator &allocator);
  int from_(const int64_t value, IAllocator &allocator);
  int from_(const uint64_t value, IAllocator &allocator);
  int from_(const char *str,
            IAllocator &allocator,
            int16_t *precision = NULL,
            int16_t *scale = NULL,
            const bool do_rounding = true);
  int from_v1_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
            ObNumberFmtModel *fmt, int16_t *precision = NULL, int16_t *scale = NULL,
            const lib::ObMemAttr *attr = NULL);
  int from_v2_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
               ObNumberFmtModel *fmt, int16_t *precision = NULL, int16_t *scale = NULL,
               const lib::ObMemAttr *attr = NULL, const bool do_rounding = true);
  int from_v3_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
               ObNumberFmtModel *fmt, int16_t *precision = NULL, int16_t *scale = NULL,
               const lib::ObMemAttr *attr = NULL, const bool do_rounding = true);
  inline int from_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
            ObNumberFmtModel *fmt, int16_t *precision = NULL, int16_t *scale = NULL,
            const lib::ObMemAttr *attr = NULL, const bool do_rounding = true)
  {
//      return from_old_(str, length, allocator, warning, fmt, precision, scale, attr);
    return (NULL == fmt ? from_v3_(str, length, allocator, warning, fmt, precision, scale, attr, do_rounding)
            : from_v2_(str, length, allocator, warning, fmt, precision, scale, attr, do_rounding));
  }
  int from_(const int16_t precision, const int16_t scale, const char *str, const int64_t length,
            IAllocator &allocator);
  int from_(const uint32_t desc, const ObCalcVector &vector, IAllocator &allocator);
  int from_v2_(const uint32_t desc, const ObCalcVector &vector, IAllocator &allocator);
  int from_(const ObNumber &other, IAllocator &allocator);
  int from_(const ObNumber &other, uint32_t *digits);
  int from_sci_(const char *str, const int64_t length, IAllocator &allocator, int &warning, int16_t *precision, int16_t *scale, const bool do_rounding);
  int deep_copy_to_allocator_(ObIAllocator &allocator);
  int add_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  template <typename T>
  void normalize_digit_(T digits, int32_t loc) const;
  int calc_desc_and_check_(const uint32_t base_desc, Desc &desc, int64_t exp, uint8_t len) const;
  inline static void calc_positive_num_desc(Desc &desc, const int64_t exp,
                                              const uint8_t len);
  inline static int calc_desc_and_check(const uint32_t base_desc, const int64_t exp, const uint8_t len,
                                        Desc &desc, const bool is_oracle_mode);
  int add_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int sub_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int sub_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int mul_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int mul_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator,
              const bool do_rounding = true) const;
  int div_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int div_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator, const int32_t qscale = OB_MAX_DECIMAL_DIGIT,
              const bool do_rounding = true) const;
  //int rem_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int rem_v2_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int round_remainder_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int negate_(ObNumber &value, IAllocator &allocator) const;
  int negate_v2_(ObNumber &value, IAllocator &allocator) const;//without check
  int negate_v3_(ObNumber &value, ObIAllocator &allocator) const;//without check
  int sqrt_first_guess_(ObNumber &value, ObIAllocator &allocator) const;
  bool is_integer(int32_t &expr_value) const;
  static int32_t get_decode_exp(const ObNumber::Desc &desc) __attribute__((always_inline));
  bool is_integer(const int32_t expr_value) const;
  bool is_int32(const int32_t expr_value) const;
  bool is_int64_without_expr(const int32_t expr_value) const;
  bool is_int64_with_expr(const int32_t expr_value) const;

  int taylor_series_sin_(const ObNumber &transformed_x, ObNumber &out, ObIAllocator &allocator, const ObNumber &neg_pi, const ObNumber &pi) const;
  int taylor_series_cos_(const ObNumber &transformed_x, ObNumber &out, ObIAllocator &allocator, const ObNumber &range_low, const ObNumber &range_high) const;
  int get_npi_(int64_t n, ObNumber& out, ObIAllocator &alloc, const bool do_rounding = true) const;
  int get_npi_(double n, ObNumber& out, ObIAllocator &alloc, const bool do_rounding = true) const;
  int simple_factorial_for_sincos_(int64_t start, ObIAllocator &allocator, ObNumber &result) const;
  int to_sci_str_(common::ObString &num_str, char *buf, const int64_t buf_len, int64_t &pos) const;
protected:
  uint32_t *alloc_(IAllocator &allocator, const int64_t num, const lib::ObMemAttr *attr = NULL);
  int check_precision_(const int64_t precision, const int64_t scale);
  int round_scale_(const int64_t scale, const bool using_floating_scale);
  int round_scale_v2_(const int64_t scale, const bool using_floating_scale,
      const bool for_oracle_to_char,
      int16_t *res_precision = NULL, int16_t *res_scale = NULL);
  int round_scale_v3_(const int64_t scale, const bool using_floating_scale,
      const bool for_oracle_to_char,
      int16_t *res_precision = NULL, int16_t *res_scale = NULL);
  inline bool need_round_after_arithmetic(const bool is_oracle_mode) const
  { return d_.len_ >= MIN_ROUND_DIGIT_COUNT[is_oracle_mode]; }
  int round_scale_oracle_(const int64_t scale, const bool using_floating_scale,
      int16_t *res_precision = NULL, int16_t *res_scale = NULL);
  int round_integer_(
      const int64_t scale,
      uint32_t *integer_digits,
      int64_t &integer_length);
  int round_decimal_(
      const int64_t scale,
      uint32_t *decimal_digits,
      int64_t &decimal_length);
  int trunc_scale_(const int64_t scale, const bool using_floating_scale);
  int trunc_integer_(const int64_t scale, uint32_t *integer_digits, int64_t &integer_length);
  int trunc_decimal_(const int64_t scale, uint32_t *decimal_digits, int64_t &decimal_length);
  int rebuild_digits_(
      const uint32_t *integer_digits,
      const int64_t integer_length,
      const uint32_t *decimal_digits,
      const int64_t decimal_length);
  inline static void exp_shift_(const int64_t shift, Desc &desc) __attribute__((always_inline));
  inline static int64_t exp_integer_align_(const Desc d1,
                                           const Desc d2) __attribute__((always_inline));
  inline static int64_t get_extend_length_remainder_(const Desc d1,
                                                     const Desc d2) __attribute__((always_inline));
  inline static int64_t get_decimal_extend_length_(const Desc d) __attribute__((always_inline));
  inline static int exp_cmp_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_max_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_mul_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_div_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_rem_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static bool is_ge_1_(const Desc d) __attribute__((always_inline));
  inline static bool is_lt_1_(const Desc d) __attribute__((always_inline));
  inline static int exp_check_(const ObNumber::Desc &desc, const bool is_oracle_mode = false) __attribute__((always_inline));
  inline static bool is_valid_sci_tail_(const char *str,
                                        const int64_t length,
                                        const int64_t e_pos) __attribute__((always_inline));
public:
  bool is_int64() const;
  int cast_to_int64(int64_t &value) const;
  inline int cast_from_int64(const int64_t value, IAllocator &allocator)
  {
    return from_integer_(value, allocator);
  }
public:
  Desc d_;
protected:
  uint32_t *digits_;
private:
  int check_range(bool *is_valid_uint64, bool *is_valid_int64,
                   uint64_t &int_parts, uint64_t &decimal_parts) const;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class StackAllocator
{
public:
  StackAllocator() : is_using_(false) {};
public:
  uint32_t *alloc(const int64_t num)
  {
    uint32_t *ret = NULL;
    if (!is_using_
        && num <= (int64_t)(ObNumber::MAX_STORE_LEN * ITEM_SIZE(buffer_))) {
      ret = buffer_;
      is_using_ = true;
    }
    return ret;
  };
  uint32_t *alloc(const int64_t num, const lib::ObMemAttr &attr)
  {
    UNUSED(attr);
    return alloc(num);
  };
private:
  uint32_t buffer_[ObNumber::MAX_STORE_LEN];
  bool is_using_;
};

class ObIntegerBuilder
{
public:
  ObIntegerBuilder();
  ~ObIntegerBuilder();
public:
  inline int push(const uint8_t d, const bool reduce_zero);
  inline int push_digit(const uint32_t d, const bool reduce_zero);
  inline int64_t get_exp() const;
  inline int64_t get_length() const;
  inline const uint32_t *get_digits() const;
  inline int64_t length() const;
  inline void reset();
private:
  int64_t exp_;
  int64_t digit_pos_;
  int64_t digit_idx_;
  uint32_t digits_[ObNumber::MAX_CALC_LEN];
};

class ObDecimalBuilder
{
public:
  ObDecimalBuilder();
  ~ObDecimalBuilder();
public:
  inline int push(const uint8_t d, const bool reduce_zero);
  inline int push_digit(const uint32_t d, const bool reduce_zero);
  inline int64_t get_exp() const;
  inline int64_t get_length() const;
  inline const uint32_t *get_digits() const;
  inline int64_t length() const;
  inline void reset();
private:
  int64_t exp_;
  int64_t digit_pos_;
  int64_t digit_idx_;
  uint32_t digits_[ObNumber::MAX_CALC_LEN];
};

enum FmtPosition
{
  FmtFirst = 1,
  FmtLast = 2,
};

class ObNumberBuilder
{
public:
  ObNumberBuilder() : number_(), ib_(), db_() {}
  ~ObNumberBuilder() {}

public:
  int build(const char *str,
            int64_t length,
            int &warning,
            ObNumberFmtModel *fmt = NULL,
            int16_t *precision = NULL,
            int16_t *scale = NULL);
  int build_v2(const char *str,
            int64_t length,
            int &warning,
            ObNumberFmtModel *fmt = NULL,
            int16_t *precision = NULL,
            int16_t *scale = NULL);
  int64_t get_exp() const;
  int64_t get_length() const;
  void reset();
private:
  int find_point_(
      const char *str,
      int64_t &length,
      int64_t &integer_start,
      int64_t &integer_end,
      int64_t &decimal_start,
      bool &negative,
      bool &integer_zero,
      bool &decimal_zero,
      int &warning);
  int find_point_v2_(
      const char *str,
      int64_t &length,
      int64_t &integer_start,
      int64_t &integer_end,
      int64_t &decimal_start,
      bool &negative,
      bool &integer_zero,
      bool &decimal_zero,
      int &warning);
  //with format string
  int find_point_(
      const char *str,
      ObNumberFmtModel *fmt,
      char* new_str,
      int64_t &length,
      int64_t &integer_start,
      int64_t &integer_end,
      int64_t &decimal_start,
      bool &negative,
      bool &integer_zero,
      bool &decimal_zero,
      int64_t &comma_cnt);
  int build_integer_(const char *str, const int64_t integer_start, const int64_t integer_end,
                     const bool reduce_zero);
  int build_integer_(const char *str, const int64_t integer_start, const int64_t integer_end,
                     const bool reduce_zero, ObNumberFmtModel* fmt);
  int build_hex_integer_(const char *str, const int64_t integer_start, const int64_t integer_end,
                         const bool reduce_zero, ObNumberFmtModel* fmt);
  int build_decimal_(const char *str, const int64_t length, const int64_t decimal_start,
                     const bool reduce_zero);
  int build_integer_v2_(const char *str, const int64_t integer_start, const int64_t integer_end,
                     const bool reduce_zero, const bool integer_zero, int &warning);
  int build_integer_v2_(const char *str, const int64_t integer_start, const int64_t integer_end,
                     const bool reduce_zero, const bool integer_zero, int &warning, ObNumberFmtModel *fmt);
  int build_decimal_v2_(const char *str, const int64_t length, const int64_t decimal_start,
                     const bool reduce_zero, const bool decimal_zero, int &warning);
  // converts hexadecimal characters to decimal digits
  int hex_to_num_(char c, int32_t &val) const;
  // multiply hex string and the result is stored in the str
  // @param [in] multiplier
  // @param [out] str hex string multiplicand
  // @param [out] len str length
  // @return OB_SUCCESS if succeed, other error code if error occurs.
  int multiply_(int32_t multiplier, char *str, int32_t &len) const;
  // adding two hex strings and the result is stored in the second string
  // @param [in] str1 first hex string
  // @param [in] str1_len str1 length
  // @param [out] str2 second hex string
  // @param [out] str2_len str2 length
  // @return OB_SUCCESS if succeed, other error code if error occurs.
  int add_hex_str_(const char *str1, int32_t str1_len,
                   char *str2, int32_t &str2_len) const;
  // convert hexadecimal string to decimal string
  // @param [in] hex_str hex string
  // @param [in] hex_len hex_str length
  // @param [out] dec_str decimal string
  // @param [out] dec_len dec_str length
  // @return OB_SUCCESS if succeed, other error code if error occurs.
  int hex_to_dec_(const char *hex_str, int32_t hex_len,
                  char *dec_str, int32_t &dec_len) const;
public:
  ObNumber number_;
private:
  ObIntegerBuilder ib_;
  ObDecimalBuilder db_;
};

class ObDigitIterator final
{
public:
  //A,B.C,D
  //A--head integer
  //B--body integer
  //C--body decimal
  //D--tail decimal
  enum NextDigitEnum {
    ND_HEAD_INTEGER = 0,
    ND_BODY_INTEGER,
    ND_BODY_DECIMAL,
    ND_TAIL_DECIMAL,
    ND_END
  };

  ObDigitIterator() : iter_idx_(0), iter_len_(0), iter_exp_(0), number_() {}
  inline void assign(const uint32_t desc, uint32_t *digits);
  inline void reset_iter();
  inline int get_digit(const int64_t idx, uint32_t &ret_digit) const;
  int get_next_digit(uint32_t &digit, bool &from_integer, bool &last_decimal);
  NextDigitEnum get_next_digit(uint32_t &digit);

private:
  int64_t iter_idx_;
  int64_t iter_len_;
  int64_t iter_exp_;
  ObNumber number_;
};

void ObDigitIterator::reset_iter()
{
  iter_idx_ = 0;
  iter_len_ = number_.d_.len_;
  iter_exp_ = ObNumber::get_decode_exp(number_.d_.desc_);
}

void ObDigitIterator::assign(const uint32_t desc, uint32_t *digits)
{
  number_.assign(desc, digits);
  reset_iter();
}

int ObDigitIterator::get_digit(const int64_t idx, uint32_t &ret_digit) const
{
  int ret = OB_SUCCESS;
  ret_digit = 0;
  if (OB_ISNULL(number_.get_digits())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "the pointer is null", K(ret), K(number_.get_digits()));
  } else {
    ret_digit = number_.get_digits()[idx];
    if (OB_UNLIKELY(ObNumber::BASE <= ret_digit)) {
      const ObNumber &nmb = number_;
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "the param is invalid", K(ret), K(idx), K(nmb), "digits_ptr", number_.get_digits(), "digits_len", iter_len_);
    }
  }
  return ret;
}

class ObCalcVector
{
public:
  ObCalcVector();
  ~ObCalcVector();
  ObCalcVector(const ObCalcVector &other);
  ObCalcVector &operator =(const ObCalcVector &other);
public:
  int init(const uint32_t desc, uint32_t *digits);
public:
  uint64_t at(const int64_t idx) const;
  uint64_t base() const;
  void set_base(const uint64_t base);
  int64_t size() const;
  uint32_t *get_digits();
  int set(const int64_t idx, const uint64_t digit);
  int ensure(const int64_t size);
  int resize(const int64_t size);
  ObCalcVector ref(const int64_t start, const int64_t end) const;
  int assign(const ObCalcVector &other, const int64_t start, const int64_t end);
  int64_t to_string(char *buffer, const int64_t length) const;
  int normalize();
private:
  uint64_t base_;
  int64_t length_;
  uint32_t *digits_;
  uint32_t buffer_[ObNumber::MAX_CALC_LEN];
};

class ObTRecover
{
public:
  ObTRecover(ObNumber &orig,
             int cond,
             int &observer) : orig_(orig),
                              recover_(orig),
                              cond_(cond),
                              observer_(observer)
  {
    memcpy(digits_, orig_.get_digits(), orig_.get_length() * sizeof(uint32_t));
  };
  ~ObTRecover()
  {
    if (cond_ != observer_) {
      recover_ = orig_;
      memcpy(orig_.get_digits(), digits_, orig_.get_length() * sizeof(uint32_t));
    }
  };
private:
  const ObNumber orig_;
  ObNumber &recover_;
  uint32_t digits_[ObNumber::MAX_CALC_BYTE_LEN];
  const int cond_;
  int &observer_;
};


//////////////////////////////////////////////////////////////////////////////////////
// struct for quick calculation in poly-poly-div function

class ObDivArray final // small-eidian storage
{
public:
  ObDivArray() : len_(0), zero_cnt_(0) {}

  void from(uint32_t *digits, int len, int size) {
    size = std::max(size, len);
    len_ = size;
    zero_cnt_ = size;
    for (int32_t i = 0, j = size - 1; i < len; ++i, --j) {
      digits_[j] = digits[i];
      if (digits_[j] != 0) {
        zero_cnt_ = j;
      }
    }
    MEMSET(digits_, 0, (size - len) * sizeof(int64_t));
    --zero_cnt_;
  }

  inline void get_uint32_digits(uint32_t *digits, const int32_t size)
  {
    if (size >= len_) {
      int64_t *tmp_digits = digits_ + len_ - 1;
      for (int32_t i = 0; i < len_; ++i) {
        *digits++ = static_cast<uint32_t>(*tmp_digits--);
      }
    }
  }

  inline bool is_zero()
  {
    bool bret = true;
    const int64_t *tmp_digits = digits_ + 0;
    for (int32_t i = 0; i < len_; ++i) {
      if (*tmp_digits++ != 0) {
        bret = false;
      }
    }
    return bret;
  }

  inline void copy_from(ObDivArray &source, const int32_t loc)
  {
    MEMCPY(digits_ + loc, source.digits_ + 0, source.len_ * sizeof(int64_t));
  }

  inline void copy_to(const int32_t l, const int32_t r, ObDivArray &out)
  {
    out.len_ = r - l;
    MEMCPY(out.digits_ + 0, digits_ + l, out.len_ * sizeof(int64_t));
  }

  inline void set(const int64_t loc, const int64_t value)
  {
    digits_[loc] = value;
  }

//  void print() {
//    printf("(%d) : ", len_);
//    for (int i=len_ - 1; i>=0; --i)
//      if (i == len_ - 1) printf("%d ", digits_[i]);
//      else printf("%09d ", digits_[i]);
//    printf("\n");
//  }

  inline bool is_neg()
  {
    bool bret = false;
    for (int32_t i = len_ - 1; i >= 0; --i) {
      if (digits_[i] != 0) {
        bret = (digits_[i] < 0);
        break;
      }
    }
    return bret;
  }

  inline bool is_neg_v2()
  {
    bool bret = false;
    const int64_t *tmp_digits = digits_ + len_ - 1;
    for (int32_t i = 0; i < len_; ++i, --tmp_digits) {
      if (*tmp_digits != 0) {
        bret = (*tmp_digits < 0);
        break;
      }
    }
    return bret;
  }

  void add(const ObDivArray &b, ObDivArray &c) {
    const int32_t this_len = len_;
    c.len_ = (len_ > b.len_ ? this_len : b.len_);
    int64_t carry = 0;
    for (int32_t i = 0; i < c.len_; ++i) {
      const int64_t this_digit = digits_[i];
      c.digits_[i] = carry;
      if (i < this_len) {
        c.digits_[i] += this_digit;
      }
      if (i < b.len_) {
        c.digits_[i] += b.digits_[i];
      }

      if (c.digits_[i] >= BASE) {
        carry = c.digits_[i] / BASE;
        c.digits_[i] -= carry * BASE;
      } else {
        carry = 0;
      }
    }
    while (carry != 0) {
      const int64_t old_carry = carry;
      carry /= BASE;
      c.digits_[c.len_] = old_carry - carry * BASE;
      ++c.len_;
    }
  }

  void add_v2(const ObDivArray &b, ObDivArray &c) {
    const int32_t this_len = len_;
    c.len_ = (len_ > b.len_ ? this_len : b.len_);
    int64_t carry = 0;
    for (int32_t i = 0; i < c.len_; ++i) {
      const int64_t this_digit = digits_[i];
      int64_t &c_digit = c.digits_[i];
      c_digit = carry;
      if (i < this_len) {
        c_digit += this_digit;
      }
      if (i < b.len_) {
        c_digit += b.digits_[i];
      }

      if (c_digit >= BASE) {
        carry = c_digit / BASE;
        c_digit -= carry * BASE;
      } else {
        carry = 0;
      }
    }
    while (carry != 0) {
      const int64_t old_carry = carry;
      carry /= BASE;
      c.digits_[c.len_] = old_carry - carry * BASE;
      ++c.len_;
    }
  }

  void sub(const ObDivArray &b, ObDivArray &c) {
    const int32_t this_len = len_;
    c.len_ = (len_ > b.len_ ? len_ : b.len_);
    int64_t borrow = 0;
    for (int32_t i = 0; i < c.len_; ++i) {
      const int64_t this_digit = digits_[i];
      c.digits_[i] = borrow;
      if (i < this_len) {
        c.digits_[i] += this_digit;
      }
      if (i < b.len_) {
        c.digits_[i] -= b.digits_[i];
      }
      if (c.digits_[i] < 0) {
        c.digits_[i] += BASE;
        borrow = -1;
      } else {
        borrow = 0;
      }
    }
    if (borrow == -1) {
      c.digits_[c.len_] = -1;
      ++c.len_;
    }
  }

  void sub_v2(const ObDivArray &b, ObDivArray &c) {
    const int32_t this_len = len_;
    c.len_ = (len_ > b.len_ ? len_ : b.len_);
    int64_t borrow = 0;
    for (int32_t i = 0; i < c.len_; ++i) {
      const int64_t this_digit = digits_[i];
      int64_t &c_digit = c.digits_[i];
      c_digit = borrow;
      if (i < this_len) {
        c_digit += this_digit;
      }
      if (i < b.len_) {
        c_digit -= b.digits_[i];
      }
      if (c_digit < 0) {
        c_digit += BASE;
        borrow = -1;
      } else {
        borrow = 0;
      }
    }
    if (borrow == -1) {
      c.digits_[c.len_] = -1;
      ++c.len_;
    }
  }


  void mul(const int64_t b, ObDivArray &c) {
    c.len_ = len_;
    int64_t carry = 0;
    for (int32_t i = 0; i < c.len_; ++i) {
      int64_t temp = digits_[i] * b + carry;
      if (temp >= BASE) {
        carry = temp / BASE;
        c.digits_[i] = temp - carry * BASE;
      } else {
        c.digits_[i] = temp;
        carry = 0;
      }
    }
    while (carry != 0) {
      const int64_t old_carry = carry;
      carry /= BASE;
      c.digits_[c.len_] = old_carry - carry * BASE;
      ++ c.len_;
    }
  }

  void div(const int32_t b, ObDivArray &c) {
    int64_t remainder = 0;
    c.len_ = len_;
    for (int32_t i = len_ - 1; i >= 0; --i) {
      remainder = remainder * BASE + digits_[i];
      c.digits_[i] = remainder / b;
      remainder -= c.digits_[i] * b;
    }
  }

  void knuth_div(ObDivArray a, ObDivArray b, ObDivArray &q, ObDivArray &r)
  {
    int n = b.len_;
    int m = a.len_ - b.len_ + 1;
    int d = BASE / (b.digits_[b.len_ - 1] + 1);
//    int zerotag = a.zero_cnt_;
    a.mul(d, a);
    b.mul(d, b);

    if ((m + n) >= a.len_) {
      a.digits_[m + n] = 0;
    }
    if ((m + n - 1) >= a.len_) {
      a.digits_[m + n - 1] = 0;
    }

    ObDivArray tmp_ct;
    ObDivArray tmp_cf;
    ObDivArray tmp_da_qq;
//    a.print();
//    b.print();
    q.len_ = m;
    for (int32_t j = m; j >= 0; --j) {
      // D3;
      const int64_t tmp_value = a.digits_[j + n] * BASE + a.digits_[j + n - 1] ;
      int64_t qq = tmp_value / b.digits_[n - 1];
      int64_t rr = tmp_value % b.digits_[n - 1];
//      printf("\nround %d-%lld %lld %lld %lld %lld\n",j,  qq, rr, tmp_value, a.digits_[j + n], a.digits_[j + n - 1]);
      if (qq == BASE
          || (n >= 2 && qq * b.digits_[n - 2] > BASE * rr + a.digits_[j + n - 2])) {
        --qq;
        rr += b.digits_[n - 1];
      }
      // D4;
      a.copy_to(j, j + n + 1, tmp_ct);
//      b.print();

      b.mul(qq, tmp_da_qq);
//      tmp_da_qq.print();

      tmp_ct.sub(tmp_da_qq, tmp_cf);
//      tmp_ct.print();
//      tmp_cf.print();

      if (tmp_cf.is_neg()) {
        tmp_cf.add(b, tmp_cf);
//        tmp_cf.print();
        --qq;
      }
      a.copy_from(tmp_cf, j);
      q.set(j, qq);
//      printf("\nround %d-%lld %lld %lld %lld %lld\n",j,  qq, rr, tmp_value, a.digits_[j + n], a.digits_[j + n - 1]);
//      q.print();
//      if (rr == 0 && j > 0 && j + n - 2 <= zerotag) {
//        --j;
//        while (j >= 0) {
//          q.set(j, 0);
//          --j;
//        }
//        break;
//      }
//      q.print();

    }
    q.len_ = m;
    a.copy_to(0, n, r);
  }

  void knuth_d4(int64_t *msua, int64_t *msub, int64_t *msuc, const int length, int64_t &qq)
  {
    /* calculate c[0..length] = a[0..length] - b[0..length] * q */
    int64_t *astart = msua - length + 1;
    int64_t *bstart = msub - length + 1;
    int64_t *cstart = msuc - length + 1;
    int64_t borrow = 0;
    for (int i = 0; i < length; ++i, ++astart, ++bstart, ++cstart) {
//      printf("%d %d %lld %lld %lld\n",length, i, *astart, *bstart, qq);
      int64_t tmp_value = (*astart) - (*bstart) * qq + borrow;
//      printf("%d %d %lld\n",length, i, tmp_value);
      if (tmp_value < 0 || tmp_value >= BASE) {
        borrow = (tmp_value + 1) / BASE - 1;
        (*cstart) = tmp_value - borrow * BASE;
      } else {
        (*cstart) = tmp_value;
        borrow = 0;
      }
//      printf("%d %d %lld %lld %lld\n",length, i, *cstart, borrow, tmp_value);
    }

    /* if c < 0, set c += b and negative = true */
    if (borrow < 0) {
      --qq;
      (*(astart - 1)) += borrow * BASE;
      bstart = msub - length + 1;
      cstart = msuc - length + 1;
//      printf("borrow %d %lld %lld\n",length, borrow, *(astart - 1));
      borrow = 0;
      for (int i = 0; i < length; ++i, ++cstart, ++bstart) {
//        printf("borrow %d %d %lld %lld\n",length, i, (*cstart), (*bstart));

        (*cstart) += (*bstart) + borrow;
        if (*cstart >= BASE) {
          borrow = (*cstart) / BASE;
          (*cstart) -= borrow * BASE;
        } else {
          borrow = 0;
        }
//        printf("borrow %d %d %lld %lld\n",length, i, borrow, *cstart);
      }
    }
  }


  /*
   * knuth algorithm 除法步骤：
   * 给定非负整数(b进制) u[m+n-1 .. 0] 和 v[n-1 .. 0],其中 v[n-1]!=0 且 n>1
   * 计算b进制的 商|_u/v_| = q[m..0] 和 余数u mod v = r[n-1 .. 0]
   *
   * D1. [规格化]
   *      对 u 和 v 同时乘以 d 满足 v[n-1]*d >= b/2,这里取 d = |_b / (v[n-1] + 1)_|
   *      这样计算出来的 q 是不变的，余数 r 需要除以 d 才能得到
   * D2. [初始化 j]
   *      置 j = m
   * D3. [计算 qq]
   *      置 qq = |_(u[j+n] * b +u[j+n-1]) / v[n-1]_|, rr = (u[j+n] * b +u[j+n-1]) % v[n-1]
   *      测试是否 qq == b 或者 qq*v[n-2] > b*rr+u[j+n-2]，如果是 则qq -= 1， r += v[n-1]
   * D4 & D6. [乘和减 + 往回加]
   *      u[j+n .. j] = u[j+n .. j] - qq * v[n-1 .. 0]
   *      如果 u 为负数，将当前位置加上 v 且置 qq -= 1
   * D5. [测试余数]
   *      q[j] = q
   * D7. [对 j 进行循环]
   *      j -= 1, 如果 j >= 0 返回到 D3
   * D8. [不规格化]
   *      q[m .. 0] 就是所求商，所求余数为 u[n-1..0] / d
   */
  void knuth_div_v2(ObDivArray &a, ObDivArray &b, ObDivArray &q, ObDivArray &r) {
    int n = b.len_;
    int m = a.len_ - b.len_ + 1;

    // D1;
    int d = BASE / (b.digits_[b.len_ - 1] + 1);
    a.mul(d, a);
    b.mul(d, b);

    if ((m + n) >= a.len_) {
      a.digits_[m + n] = 0;
    }
    if ((m + n - 1) >= a.len_) {
      a.digits_[m + n - 1] = 0;
    }

    b.digits_[n] = 0;
    int64_t *aptr = a.digits_ + (m + n);
    int64_t *bptr = b.digits_ + (n);
    const int64_t bmsu = *(bptr - 1); // bmsu : Most Significant Unit of array B ... b[n-1]
    const int64_t bsmsu = (n >= 2 ? *(bptr - 2) : 0);// bsmsu : Second Most Significant Unit of array B ... b[n-2]

//    a.print();
//    b.print();
    q.len_ = m;

    // D2;
    for (int32_t j = m; j >= 0; --j, --aptr) {
      // D3;
      const int64_t tmp_value = (*aptr) * BASE + (*(aptr - 1)) ;
      int64_t qq = tmp_value / bmsu;
      int64_t rr = tmp_value - qq * bmsu;
//      printf("\nround %d-%lld %lld %lld %lld %lld\n",j,  qq, rr, tmp_value, *aptr, *(aptr - 1));
      if (qq == BASE
          || (n >= 2 && qq * bsmsu > BASE * rr + (*(aptr - 2)))) {
        --qq;
        rr += bmsu;
      }

      // D4 & D6.
      if (qq != 0) {
        knuth_d4(aptr, bptr, aptr, n + 1, qq);
      }

      // D5.
      q.set(j, qq);

//      printf("\nround %d-%lld %lld %lld %lld %lld\n",j,  qq, rr, tmp_value, *aptr, *(aptr - 1));
//      q.print();
      // Optimization.
      // 如果当前余数为 0 并且后面被除数都是 0, 说明当前已经整除,剩余商都是 0
//      if (rr == 0 && j > 0 && (j + n - 2) <= zerotag) {
//        --j;
//        while (j >= 0) {
//          q.set(j, 0);
//          --j;
//        }
//        break;
//      }
//      q.print();
    }
    // D8.
    /* set q */
    q.len_ = m;
    /* set r = a[0..n] */
    a.copy_to(0, n, r);
  }

  void div(const ObDivArray &b, ObDivArray &q) {
    ObDivArray r;
    knuth_div(*this, b, q, r);
  }

  void div_v2(ObDivArray &b, ObDivArray &q) {
    ObDivArray r;
    knuth_div_v2(*this, b, q, r);
  }

  void rem(const ObDivArray &b, ObDivArray &r) {
    ObDivArray q;
    knuth_div(*this, b, q, r);
    int32_t d = BASE / (b.digits_[b.len_ - 1] + 1);
    r.div(d, r);
  }

  void rem_v2(ObDivArray &b, ObDivArray &r) {
    ObDivArray q;
    knuth_div_v2(*this, b, q, r);
    int32_t d = BASE / (b.digits_[b.len_ - 1] + 1);
    r.div(d, r);
  }

public:
  static const int64_t BASE = 1000000000;

  int32_t len_;
  int32_t zero_cnt_;
  int64_t digits_[ObNumber::OB_REM_BUFFER_SIZE];
};

////////////////////////////////////////////////////////////////////////////////////////////////////

inline ObNumber ObNumber::negate() const
{
  ObNumber ret = *this;
  if (ret.is_zero()) {
    ret.set_zero();
  } else {
    if (POSITIVE == ret.d_.sign_) {
      ret.d_.sign_ = NEGATIVE;
    } else {
      ret.d_.sign_ = POSITIVE;
    }
    ret.d_.exp_ = (0x7f & (~ret.d_.exp_)) + 1;
  }
  return ret;
}

inline ObNumber ObNumber::abs() const
{
  ObNumber ret = *this;
  if (NEGATIVE == ret.d_.sign_) {
    ret.d_.sign_ = POSITIVE;
    ret.d_.exp_ = (0x7f & (~ret.d_.exp_)) + 1;
  }
  return ret;
}

int ObNumber::abs_compare(const ObNumber &other) const
{
  int ret = 0;
  if (!is_negative()) {
    if (!other.is_negative()) {
      ret = compare(other);
    } else {
      ret = compare(other.negate());
    }
  } else {
    if (other.is_negative()) {
      ret = -compare(other);
    } else {
      ret = negate().compare(other);
    }
  }
  return ret;
}

template <class T>
int ObNumber::from(const int64_t value, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(value, ta);
}

template <class T>
int ObNumber::from(const uint64_t value, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(value, ta);
}

template <class T>
int ObNumber::from(const char *str, T &allocator, int16_t *precision, int16_t *scale, const bool do_rounding)
{
  TAllocator<T> ta(allocator);
  return from_(str, ta, precision, scale, do_rounding);
}

template <class T>
int ObNumber::from(const char *str,
                   const int64_t length,
                   T &allocator,
                   int16_t *precision,
                   int16_t *scale,
                   const bool do_rounding)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_(str, length, ta, warning, NULL, precision, scale, NULL, do_rounding);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

template <class T>
int ObNumber::from_v1(const char *str,
                   const int64_t length,
                   T &allocator,
                   int16_t *precision,
                   int16_t *scale)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_v1_(str, length, ta, warning, NULL, precision, scale, NULL);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

template <class T>
int ObNumber::from_v2(const char *str,
                   const int64_t length,
                   T &allocator,
                   int16_t *precision,
                   int16_t *scale)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_v2_(str, length, ta, warning, NULL, precision, scale, NULL);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

template <class T>
int ObNumber::from_v3(const char *str,
                   const int64_t length,
                   T &allocator,
                   int16_t *precision,
                   int16_t *scale)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_v3_(str, length, ta, warning, NULL, precision, scale, NULL);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

template <class T>
int ObNumber::from_sci(const char *str,
                   const int64_t length,
                   T &allocator,
                   int16_t *precision,
                   int16_t *scale,
                   const bool do_rounding)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_sci_(str, length, ta, warning, precision, scale, do_rounding);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

template <class T>
int ObNumber::from_sci_opt(const char *str,
                           const int64_t length,
                           T &allocator,
                           int16_t *precision,
                           int16_t *scale,
                           const bool do_rounding)
{
  int ret = OB_SUCCESS;
  const common::ObString tmp_string(length, str);
  if (!tmp_string.empty() && (NULL != tmp_string.find('e') || NULL != tmp_string.find('E'))) {
    ret = from_sci(str, length, allocator, precision, scale, do_rounding);
  } else {
    ret = from(str, length, allocator, precision, scale, do_rounding);
  }
  return ret;
}

template <class T>
int ObNumber::from(const char *str,
                   const int64_t length,
                   T &allocator,
                   const lib::ObMemAttr &attr,
                   int16_t *precision,
                   int16_t *scale)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_(str, length, ta, warning, NULL, precision, scale, &attr);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}


template <class T>
int ObNumber::from(const char *str,
                   const int64_t length,
                   T &allocator,
                   ObNumberFmtModel *fmt,
                   int16_t *precision,
                   int16_t *scale)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_(str, length, ta, warning, fmt, precision, scale);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

template <class T>
int ObNumber::from(const int16_t precision, const int16_t scale, const char *str,
                   const int64_t length, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(precision, scale, str, length, ta);
}

template <class T>
int ObNumber::from(const uint32_t desc, const ObCalcVector &vector, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(desc, vector, ta);
}

template <class T>
int ObNumber::from(const ObNumber &other, T &allocator)
{
  TAllocator<T> ta(allocator);
  return deep_copy(other, ta);
}

template <class T>
int ObNumber::add_v1(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return add_(other, value, ta);
}

template <class T>
int ObNumber::add_v2(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return add_v2_(other, value, ta);
}

inline int ObNumber::add_v3(const ObNumber &left, const ObNumber &right, ObNumber &result,
                  ObIAllocator &allocator)
{
  return left.add_v3(right, result, allocator);
}

template <class T>
int ObNumber::add(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return add_v2_(other, value, ta);
}


template <class IntegerT>
int ObNumber::add(const IntegerT param1, const IntegerT param2, ObNumber &result,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber param_result_nmb;
  char buf_alloc[ObNumber::MAX_CALC_BYTE_LEN * 3];
  ObDataBuffer local_allocator(buf_alloc, ObNumber::MAX_CALC_BYTE_LEN * 3);
  ObNumber param1_nmb;
  ObNumber param2_nmb;
  if (OB_FAIL(param1_nmb.from(param1, local_allocator))) {
    LIB_LOG(WARN, "number add failed", K(ret), K(param1));
  } else if (OB_FAIL(param2_nmb.from(param2, local_allocator))) {
    LIB_LOG(WARN, "number add failed", K(ret), K(param2));
  } else if (OB_FAIL(param1_nmb.add_v3(param2_nmb, param_result_nmb, local_allocator))) {
    LIB_LOG(WARN, "number add failed", K(ret), K(param1_nmb), K(param2_nmb));
  } else if (OB_FAIL(add_v3(param_result_nmb, result, allocator))) {
    LIB_LOG(WARN, "number add failed", K(ret), KPC(this), K(param_result_nmb));
  } else {
    LIB_LOG(DEBUG, "succ add", K(param1), K(param2), KPC(this), K(param_result_nmb), K(result));
  }
  return ret;
}

inline int ObNumber::sub_v3(const ObNumber &left, const ObNumber &right, ObNumber &result,
                  ObIAllocator &allocator)
{
  return left.sub_v3(right, result, allocator);
}

template <class T>
int ObNumber::sub_v1(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return sub_(other, value, ta);
}

template <class T>
int ObNumber::sub_v2(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return sub_v2_(other, value, ta);
}

template <class T>

int ObNumber::sub(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return sub_v2_(other, value, ta);

}

inline int ObNumber::mul_v3(const ObNumber &left, const ObNumber &right, ObNumber &result,
                  ObIAllocator &allocator)
{
  return left.mul_v3(right, result, allocator);
}

template <class T>
int ObNumber::mul(const ObNumber &other, ObNumber &value, T &allocator,
                  const bool do_rounding) const
{
  TAllocator<T> ta(allocator);
  return mul_v2_(other, value, ta, do_rounding);
}

template <class T>
int ObNumber::mul_v1(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return mul_(other, value, ta);
}

template <class T>
int ObNumber::mul_v2(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return mul_v2_(other, value, ta);
}

//template <class T>
//int ObNumber::div_v1(const ObNumber &other, ObNumber &value, T &allocator) const
//{
//  TAllocator<T> ta(allocator);
//  return div_(other, value, ta);
//}

template <class T>
int ObNumber::div_v2(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return div_v2_(other, value, ta);
}

template <class T>
int ObNumber::div(const ObNumber &other, ObNumber &value, T &allocator, const int32_t qscale,
                  const bool do_rounding) const
{
  TAllocator<T> ta(allocator);
  return div_v2_(other, value, ta, qscale, do_rounding);
}

//template <class T>
//int ObNumber::rem_v1(const ObNumber &other, ObNumber &value, T &allocator) const
//{
//  TAllocator<T> ta(allocator);
//  return rem_(other, value, ta);
//}

template <class T>
int ObNumber::rem_v2(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return rem_v2_(other, value, ta);
}

template <class T>
int ObNumber::rem(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return rem_v2_(other, value, ta);
}

template <class T>
int ObNumber::negate(ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return negate_(value, ta);
}

bool ObNumber::operator<(const ObNumber &other) const
{
  return (compare(other) < 0);
}

bool ObNumber::operator<=(const ObNumber &other) const
{
  return (compare(other) <= 0);
}

bool ObNumber::operator>(const ObNumber &other) const
{
  return (compare(other) > 0);
}

bool ObNumber::operator>=(const ObNumber &other) const
{
  return (compare(other) >= 0);
}

bool ObNumber::operator==(const ObNumber &other) const
{
  return  is_equal(other);
}

bool ObNumber::operator!=(const ObNumber &other) const
{
  return !is_equal(other);
}

int ObNumber::abs_compare_sys(const uint64_t &other, int64_t exp) const
{
  int cmp = 0;
  if (exp < 0) {
    cmp = (0 == other) ? 1 : -1;
  } else {
   /* uint64_t int_part = static_cast<uint64_t>(digits_[0]);
    switch (exp) {
      case 0 : cmp = (int_part >= other) ? ((int_part > other) ? 1 : ((len_ > 1) ? 1 : 0)) : -1; break;
      case 1 : {
        int_part = int_part * BASE + digits_[1];
        cmp = (int_part >= other) ? ((int_part > other) ? 1 : ((len_ > 2) ? 1 : 0)) : -1; break;
      }
      case 2 : {
        uint64_t part = other / (BASE * BASE);
        int64_t value = digits_[0] - part;
        if (0 == value) {
          int_part = digits_[1] * BASE + digits_[2];
          value = int_part - (other % (BASE * BASE));
          cmp = (value >= 0) ? ((value > 0) ? 1 : ((len_ > 3) ? 1 : 0)) : -1; break;
        } else {
          cmp = (value < 0) ? -1 : 1;
          break;
        }
      }
      default : cmp = -1;
    }   */
    //  首先判断值是否number值是否超过uint值域，不超过，则将uint转换为number，直接进行比较操作
    int64_t exp_u = (other >= BASE2) ? 2 : (other >= BASE) ? 1 : 0;
    if (exp != exp_u) {
      cmp = exp > exp_u ? 1 : -1;
    } else {
      bool is_uint = true;
      uint64_t num = 0;
      if (OB_SUCCESS == is_in_uint(exp, is_uint, num)) {
        if (is_uint) {
           cmp = num > other ? 1 : num == other ? 0 : -1;
          if (0 == cmp) {
            cmp = d_.len_ > exp + 1 ? 1 : 0;
          }
        } else {
          cmp = 1;
        }
      }
    }
  }
  return cmp;
}

int ObNumber::is_in_uint(int64_t exp, bool &is_uint, uint64_t &num) const
{
  //18  446744073   709551615
  int ret = OB_SUCCESS;
  int64_t len = d_.len_;
  is_uint = true;
  num = 0;
  if (exp > 2) {
    is_uint = false;
  } else {
    if (OB_ISNULL(digits_)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(ERROR, "the pointer is null");
    } else {
      if (2 == exp) {
        static const uint32_t unum[3] = {18, 446744073, 709551615};
        for (int i = 0 ; i < min(len , exp + 1); ++i) {
          if (digits_[i] != unum[i]) {
            is_uint = digits_[i] < unum[i];
            break;
          }
        }
      }
      if (is_uint) {
        for (int i = 0; exp >= 0 ; --exp, ++i, --len) {
          if (len > 0) {
            num = num * BASE + digits_[i];
          } else {
            num = num * BASE ;       //超过digits的长度，最后应该补零
          }
        }
      }
    }
  }
  return ret;
}


bool ObNumber::is_equal(const int64_t &other) const
{
  return (0 == compare(other));
}

int ObNumber::compare(const int64_t &other) const
{
  int cmp = 0;
  if (is_zero()) {
    cmp = (other < 0) ? 1 : ((0 == other) ? 0 : -1);
  } else {
    if (is_negative()) {
      //这里处理int64_t最小值问题
      cmp = (other >= 0) ? -1 : - abs_compare_sys(static_cast<uint64_t>((other + 1)* (-1))
                                                  + static_cast<uint64_t>(1), EXP_ZERO - d_.exp_);
    } else {
      cmp = (other <= 0) ? 1 : abs_compare_sys(static_cast<uint64_t>(other), d_.exp_ - EXP_ZERO);
    }
  }
  return cmp;
}

bool ObNumber::operator<(const int64_t &other) const
{
  return (compare(other) < 0);
}

bool ObNumber::operator<=(const int64_t &other) const
{
  return (compare(other) <= 0);
}

bool ObNumber::operator>(const int64_t &other) const
{
  return (compare(other) > 0);
}

bool ObNumber::operator>=(const int64_t &other) const
{
  return (compare(other) >= 0);
}

bool ObNumber::operator==(const int64_t &other) const
{
  return (0 == compare(other));
}

bool ObNumber::operator!=(const int64_t &other) const
{
  return (0 != compare(other));
}

bool ObNumber::is_equal(const uint64_t &other) const
{
  return (0 == compare(other));
}

int ObNumber::compare(const uint64_t &other) const
{
  int cmp = 0;
  if (is_zero()) {
    cmp = (0 == other) ? 0 : -1;
  } else {
    if (is_negative()) {
      cmp = -1;
    } else {
      cmp = abs_compare_sys(other, d_.exp_ - EXP_ZERO);
    }
  }
  return cmp;
}

bool ObNumber::operator<(const uint64_t &other) const
{
  return (compare(other) < 0);
}

bool ObNumber::operator<=(const uint64_t &other) const
{
  return (compare(other) <= 0);
}

bool ObNumber::operator>(const uint64_t &other) const
{
  return (compare(other) > 0);
}

bool ObNumber::operator>=(const uint64_t &other) const
{
  return (compare(other) >= 0);
}

bool ObNumber::operator==(const uint64_t &other) const
{
  return (0 == compare(other));
}

bool ObNumber::operator!=(const uint64_t &other) const
{
  return (0 != compare(other));
}

inline int ObNumber::uint32cmp(const uint32_t *s1, const uint32_t *s2, const int64_t n)
{
  int cret = 0;
  if (n > 0 && (OB_ISNULL(s1) || OB_ISNULL(s2))) {
    LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "the poniter is null");
  } else {
    for (int64_t i = 0; i < n; ++i) {
      if (s1[i] < s2[i]) {
        cret = -1;
        break;
      } else if (s1[i] > s2[i]) {
        cret = 1;
        break;
      }
    }
  }
  return cret;
}

inline int ObNumber::uint32cmp_v2(const uint32_t *s1, const uint32_t *s2, const int64_t n)
{
  int cret = 0;
  for (int64_t i = 0; i < n; ++i) {
    if (s1[i] > s2[i]) {
      cret = 1;
      break;
    } else if (s1[i] == s2[i]) {
      continue;
    } else {
      cret = -1;
      break;
    }
  }
  return cret;
}

inline bool ObNumber::uint32equal(const uint32_t *s1, const uint32_t *s2, const int64_t n)
{
  bool is_equal = true;
  for (int64_t i = 0; i < n; ++i) {
    if (s1[i] != s2[i]) {
      is_equal = false;
      break;
    }
  }
  return is_equal;
}

inline bool ObNumber::uint32equal_v2(const uint32_t *s1, const uint32_t *s2, const int64_t n)
{
  return 0 == memcmp(s1, s2, n * sizeof(uint32_t));
}

inline bool ObNumber::is_equal_v2(const number::ObNumber::Desc &this_desc,
  const uint32_t *this_digits, const number::ObNumber::Desc &other_desc,
  const uint32_t *other_digits)
{
  bool bret = false;
  if (this_desc.se_ == other_desc.se_ && this_desc.len_ == other_desc.len_) {
    bret = uint32equal_v2(this_digits, other_digits, this_desc.len_) ;
  }
  return bret;
}

inline int ObNumber::compare_v2(const number::ObNumber::Desc &this_desc,
  const uint32_t *this_digits, const number::ObNumber::Desc &other_desc,
  const uint32_t *other_digits)
{
  int ret = 0;
  if (this_desc.se_ == other_desc.se_) {
    if (this_desc.len_ == other_desc.len_) {
      ret = uint32cmp_v2(this_digits, other_digits, this_desc.len_);
    } else if (this_desc.len_ < other_desc.len_) {
      if (0 == (ret = uint32cmp_v2(this_digits, other_digits, this_desc.len_))) {
        ret = -1;
      }
    } else {
      if (0 == (ret = uint32cmp_v2(this_digits, other_digits, other_desc.len_))) {
        ret = 1;
      }
    }
    if (NEGATIVE == this_desc.sign_) {
      ret = -ret;
    }
  } else if (this_desc.se_ < other_desc.se_) {
    ret = -1;
  } else {
    ret = 1;
  }
  return ret;
}

inline void ObNumber::shadow_copy(const ObNumber &other)
{
  if (&other == this) {
    // assign to self
  } else {
    d_ = other.d_;
    digits_ = other.digits_;
  }
}

inline int ObNumber::encode(char *buf, const int64_t buf_size,
    int64_t &pos, ObNumber *out/* = NULL*/) const
{
  int ret = OB_SUCCESS;
  const int64_t copy_size = d_.len_ * static_cast<int64_t>(sizeof(uint32_t));

  if (OB_UNLIKELY(pos + sizeof(uint32_t) + copy_size > buf_size)) {
    ret = OB_BUF_NOT_ENOUGH;
//    LIB_LOG(ERROR, "buff is not enough", K(buf), K(copy_size), K(buf_size), K(pos), K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_NULL_VALUE;
    LIB_LOG(ERROR, "buff is null", KP(buf), K(copy_size), K(buf_size), K(pos), K(ret));
  } else {
    ObNumber::Desc *number_desc = reinterpret_cast<ObNumber::Desc*> (buf + pos);
    number_desc->desc_ = d_.desc_;
    number_desc->reserved_ = 0;
    number_desc->flag_ = 0;
    pos += sizeof(uint32_t);

    if (copy_size > 0) {
      MEMCPY(buf + pos, digits_, copy_size);
      if (NULL != out) {
        out->d_.desc_ = number_desc->desc_;
        out->digits_ = reinterpret_cast<uint32_t *>(buf + pos);
      }
      pos += copy_size;
    }
  }
  return ret;
}

inline int ObNumber::decode(const char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_size - pos < static_cast<int64_t>(sizeof(uint32_t)))) {
    ret = OB_BUF_NOT_ENOUGH;
//    LIB_LOG(ERROR, "buff is not enough", K(buf), K(buf_size - pos), K(buf_size), K(pos), K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_NULL_VALUE;
    LIB_LOG(ERROR, "buff is not enough", KP(buf), K(buf_size - pos), K(buf_size), K(pos), K(ret));
  } else {
    MEMCPY(&d_.desc_, buf + pos, sizeof(uint32_t));
    pos += static_cast<int64_t>(sizeof(uint32_t));
    d_.reserved_ = 0;
    digits_ = NULL;
    if (d_.len_ > 0) {
      const int64_t copy_size = d_.len_ * static_cast<int64_t>(sizeof(uint32_t));
      if (OB_UNLIKELY(buf_size - pos < copy_size)) {
        ret = OB_BUF_NOT_ENOUGH;
        LIB_LOG(WARN, "buff is not enough", K(copy_size), K(buf_size - pos), K(buf_size), K(pos), K(ret));
      } else {
        digits_ = (uint32_t *)(buf + pos);
        pos += copy_size;
      }
    }
  }
  return ret;
}

inline int ObNumber::round_v2(const int64_t scale, const bool for_oracle_to_char/*false*/)
{
  int ret = OB_SUCCESS;
  if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(digits_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(scale < MIN_SCALE) || OB_UNLIKELY(scale > MAX_SCALE)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid param", K(scale), K(ret));
  } else if (OB_FAIL(round_scale_v2_(scale, false, for_oracle_to_char))) {
    LIB_LOG(WARN, "fail to round_scale_oracle_", KPC(this), K(ret));
  }
  return ret;
}

inline int ObNumber::round_v3(const int64_t scale, const bool for_oracle_to_char/*false*/)
{
  int ret = OB_SUCCESS;
  if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(digits_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(scale < MIN_SCALE) || OB_UNLIKELY(scale > MAX_SCALE)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid param", K(scale), K(ret));
  } else if (OB_FAIL(round_scale_v3_(scale, false, for_oracle_to_char))) {
    LIB_LOG(WARN, "fail to round_scale_oracle_", KPC(this), K(ret));
  }
  return ret;
}

inline int ObNumber::round_for_sci(const int64_t scale, const bool for_oracle_to_char/*false*/)
{
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "only for oracle mode", K(ret));
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(digits_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(scale < MIN_SCI_SIZE) || OB_UNLIKELY(scale > (-MIN_SCI_SIZE))) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid param", K(scale), K(ret));
  } else if (OB_FAIL(round_scale_v3_(scale, false, for_oracle_to_char))) {
    LIB_LOG(WARN, "fail to round_scale_oracle_", KPC(this), K(ret));
  }
  return ret;
}

inline uint32_t ObNumber::remove_back_zero(const uint32_t digit, int64_t &count)
{
  uint32_t ret = digit;
  count = 0;
  while (0 != ret
         && 0 == ret % 10) {
    ret = ret / 10;
    ++count;
  }
  return ret;
}

inline int64_t ObNumber::get_digit_len(const uint32_t d)
{
  int64_t len = 0;
  if (BASE <= d) {
    LIB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "d is out of range");
  } else {
    len = (d >= 100000000) ? 9 :
          (d >= 10000000) ? 8 :
          (d >= 1000000) ? 7 :
          (d >= 100000) ? 6 :
          (d >= 10000) ? 5 :
          (d >= 1000) ? 4 :
          (d >= 100) ? 3 :
          (d >= 10) ? 2 :
          1;
  }

  return len;
}

inline void ObNumber::exp_shift_(const int64_t shift, Desc &desc)
{
  int64_t exp = desc.exp_;
  if (NEGATIVE == desc.sign_) {
    exp = (0x7f & ~exp);
    exp += 1;
    exp += shift;
    exp = (0x7f & ~exp);
    exp += 1;
  } else {
    if (0 != exp) {
      exp += shift;
    }
  }
  desc.exp_ = (0x7f) & exp;
}

inline int64_t ObNumber::exp_integer_align_(const Desc d1, const Desc d2)
{
  int64_t ret = 0;

  if (d1.desc_ != d2.desc_) {
    int64_t exp1 = d1.exp_;
    int64_t exp2 = d2.exp_;
    int64_t len1 = d1.len_;
    int64_t len2 = d2.len_;
    if (NEGATIVE == d1.sign_) {
      exp1 = (0x7f & ~exp1);
      exp1 += 1;
    }
    if (NEGATIVE == d2.sign_) {
      exp2 = (0x7f & ~exp2);
      exp2 += 1;
    }
    if (0 != exp1) {
      exp1 -= EXP_ZERO;
    }
    if (0 != exp2) {
      exp2 -= EXP_ZERO;
    }
    int64_t min_exp1 = labs(std::min(exp1 - len1 + 1, (int64_t)0));
    int64_t min_exp2 = labs(std::min(exp2 - len2 + 1, (int64_t)0));
    ret = std::max(min_exp1, min_exp2);
  }

  return ret;
}

inline int64_t ObNumber::get_extend_length_remainder_(const Desc d1, const Desc d2)
{
  int64_t ret = 0;

  if (d1.desc_ != d2.desc_) {
    int64_t exp1 = d1.exp_;
    int64_t exp2 = d2.exp_;
    int64_t len1 = d1.len_;
    int64_t len2 = d2.len_;
    if (NEGATIVE == d1.sign_) {
      exp1 = (0x7f & ~exp1);
      exp1 += 1;
    }
    if (NEGATIVE == d2.sign_) {
      exp2 = (0x7f & ~exp2);
      exp2 += 1;
    }
    if (0 != exp1) {
      exp1 -= EXP_ZERO;
    }
    if (0 != exp2) {
      exp2 -= EXP_ZERO;
    }

    int64_t ext_len1 = (0 <= (exp1 - len1 + 1)) ? (exp1 + 1) : len1;
    int64_t ext_len2 = (0 <= (exp2 - len2 + 1)) ? (exp2 + 1) : len2;
    ret = ext_len1 - ext_len2;
  }

  return ret;
}

inline int64_t ObNumber::get_decimal_extend_length_(const Desc d)
{
  int64_t ret = 0;

  int64_t exp = d.exp_;
  int64_t len = d.len_;
  if (NEGATIVE == d.sign_) {
    exp = (0x7f & ~exp);
    exp += 1;
  }
  if (0 != exp) {
    exp -= EXP_ZERO;
  }
  ret = exp - len + 1;
  ret = (0 <= ret) ? 0 : -ret;

  return ret;
}

inline int ObNumber::exp_cmp_(const Desc d1, const Desc d2)
{
  int ret = 0;

  if (d1.desc_ != d2.desc_) {
    int64_t exp1 = d1.exp_;
    int64_t exp2 = d2.exp_;
    if (NEGATIVE == d1.sign_) {
      exp1 = (0x7f & ~exp1);
      exp1 += 1;
    }
    if (NEGATIVE == d2.sign_) {
      exp2 = (0x7f & ~exp2);
      exp2 += 1;
    }
    if (0 != exp1) {
      exp1 = labs(exp1 - EXP_ZERO);
    }
    if (0 != exp2) {
      exp2 = labs(exp2 - EXP_ZERO);
    }

    ret = (int)(exp1 - exp2);
  }

  return ret;
}

inline ObNumber::Desc ObNumber::exp_max_(const Desc d1, const Desc d2)
{
  Desc ret;
  int cmp_ret = exp_cmp_(d1, d2);
  if (0 <= cmp_ret) {
    ret = d1;
  } else {
    ret = d2;
  }
  return ret;
}

inline ObNumber::Desc ObNumber::exp_mul_(const Desc d1, const Desc d2)
{
  Desc ret;
  ret.desc_ = 0;

  int64_t exp1 = d1.exp_;
  int64_t exp2 = d2.exp_;
  if (NEGATIVE == d1.sign_) {
    exp1 = (0x7f & ~exp1);
    exp1 += 1;
  }
  exp1 -= EXP_ZERO;
  if (NEGATIVE == d2.sign_) {
    exp2 = (0x7f & ~exp2);
    exp2 += 1;
  }
  exp2 -= EXP_ZERO;

  ret.exp_ = 0x7f & (uint8_t)(exp1 + exp2 + EXP_ZERO);
  if (d1.sign_ != d2.sign_) {
    ret.sign_ = NEGATIVE;
    ret.exp_ = (0x7f & ~ret.exp_);
    ++ret.exp_;
  } else {
    ret.sign_ = POSITIVE;
  }
  return ret;
}

inline ObNumber::Desc ObNumber::exp_div_(const Desc d1, const Desc d2)
{
  Desc ret;
  ret.desc_ = 0;

  int64_t exp1 = d1.exp_;
  int64_t exp2 = d2.exp_;
  if (NEGATIVE == d1.sign_) {
    exp1 = (0x7f & ~exp1);
    exp1 += 1;
  }
  exp1 -= EXP_ZERO;
  if (NEGATIVE == d2.sign_) {
    exp2 = (0x7f & ~exp2);
    exp2 += 1;
  }
  exp2 -= EXP_ZERO;

  ret.exp_ = 0x7f & (uint8_t)(exp1 - exp2 + EXP_ZERO);
  if (d1.sign_ != d2.sign_) {
    ret.sign_ = NEGATIVE;
    ret.exp_ = (0x7f & ~ret.exp_);
    ++ret.exp_;
  } else {
    ret.sign_ = POSITIVE;
  }
  return ret;
}

inline ObNumber::Desc ObNumber::exp_rem_(const Desc d1, const Desc d2)
{
  Desc ret = d2;
  ret.sign_ = d1.sign_;
  if (d1.sign_ != d2.sign_) {
    ret.exp_ = (0x7f & ~ret.exp_);
    ++ret.exp_;
  }
  return ret;
}

inline bool ObNumber::is_lt_1_(const Desc d)
{
  bool bret = false;
  if ((NEGATIVE == d.sign_ && EXP_ZERO < d.exp_)
      || (POSITIVE == d.sign_ && 0 < d.exp_ && EXP_ZERO > d.exp_)) {
    bret = true;
  }
  return bret;
}

inline bool ObNumber::is_ge_1_(const Desc d)
{
  bool bret = false;
  if ((NEGATIVE == d.sign_ && EXP_ZERO >= d.exp_)
      || (POSITIVE == d.sign_ && EXP_ZERO <= d.exp_)) {
    bret = true;
  }
  return bret;
}

inline int ObNumber::exp_check_(const ObNumber::Desc &desc, const bool is_oracle_mode/*false*/)
{
  int ret = OB_SUCCESS;
  const int64_t exp = labs(desc.exp_ - EXP_ZERO);
  if (OB_UNLIKELY(is_ge_1_(desc) && (is_oracle_mode ? (MAX_INTEGER_EXP <= exp) : (MAX_INTEGER_EXP < exp)))) {
    ret = OB_INTEGER_PRECISION_OVERFLOW;
  } else if (OB_UNLIKELY(MAX_DECIMAL_EXP < exp
      && is_lt_1_(desc))) {
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
  }
  return ret;
}

inline int ObNumber::calc_desc_and_check(const uint32_t base_desc, const int64_t exp, const uint8_t len,
    Desc &desc, const bool is_oracle_mode)
{
  desc.desc_ = base_desc;
  desc.len_ = len;
  desc.reserved_ = 0;
  desc.exp_ = 0x7f & (uint8_t)(EXP_ZERO + exp);
  if (desc.sign_ == NEGATIVE) {
    desc.exp_ = (0x7f & (~desc.exp_));
    ++desc.exp_;
  }
  return exp_check_(desc, is_oracle_mode);
}

// ATTENTION: performance critical, no defensive check.
// The caller SHOULD check exp_ ahead of calling the routine.
// Otherwise, call calc_desc_and_check instead.
inline void ObNumber::calc_positive_num_desc(Desc &desc, int64_t exp, uint8_t len)
{
  desc.sign_ = ObNumber::POSITIVE;
  desc.len_ = len;
  desc.reserved_ = 0;
  desc.exp_ = 0x7f & (uint8_t)(ObNumber::EXP_ZERO + exp);
}

inline int64_t ObNumber::get_decode_exp(const uint32_t desc)
{
  Desc d;
  d.desc_ = desc;
  int64_t e = d.exp_;
  if (NEGATIVE == d.sign_
      && EXP_ZERO != d.exp_) {
    e = (0x7f & ~e);
    e += 1;
  }
  e = e - EXP_ZERO;
  return e;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T>
int poly_mono_mul(
    const T &multiplicand,
    const uint64_t multiplier,
    T &product)
{
  int ret = OB_SUCCESS;

  uint64_t base = multiplicand.base();
  int64_t multiplicand_size = multiplicand.size();
  int64_t product_size = product.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base <= multiplier
      || base != product.base()
      || 0 >= multiplicand_size
      || (multiplicand_size + 1) != product_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    uint64_t carry = 0;
    int64_t multiplicand_idx = multiplicand_size - 1;
    int64_t product_idx = product_size - 1;
    while (product_idx >= 0) {
      uint64_t tmp = carry;
      if (multiplicand_idx >= 0) {
        tmp += (multiplicand.at(multiplicand_idx) * multiplier);
        --multiplicand_idx;
      }
      if (base <= tmp) {
        carry = tmp / base;
        tmp = tmp % base;
      } else {
        carry = 0;
      }
      if (OB_FAIL(product.set(product_idx, tmp))) {
        LIB_LOG(WARN, "set to product fail", K(ret), K(product_idx));
        break;
      }
      --product_idx;
    }
  }
  return ret;
}

template <class T>
int poly_mono_div(
    const T &dividend,
    const uint64_t divisor,
    T &quotient,
    uint64_t &remainder)
{
  int ret = OB_SUCCESS;

  uint64_t base = dividend.base();
  int64_t dividend_size = dividend.size();
  int64_t quotient_size = quotient.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base <= divisor
      || base != quotient.base()
      || 0 >= dividend_size
      || dividend_size != quotient_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    uint64_t carry = 0;
    int64_t dividend_idx = 0;
    int64_t quotient_idx = 0;
    while (dividend_idx < dividend_size) {
      carry = carry * base + dividend.at(dividend_idx);
      ++dividend_idx;

      uint64_t tmp = carry / divisor;
      carry = carry % divisor;
      if (OB_FAIL(quotient.set(quotient_idx, tmp))) {
        LIB_LOG(WARN, "set to quotient fail", K(ret), K(quotient_idx));
        break;
      }
      ++quotient_idx;
    }
    remainder = carry;
  }
  return ret;
}

template <class T>
int poly_poly_add(
    const T &augend,
    const T &addend,
    T &sum)
{
  int ret = OB_SUCCESS;

  uint64_t base = augend.base();
  int64_t augend_size = augend.size();
  int64_t addend_size = addend.size();
  int64_t add_size = std::max(augend_size, addend_size) + 1;
  int64_t sum_size = sum.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base != addend.base()
      || base != sum.base()
      || 0 >= augend_size
      || 0 >= addend_size
      || add_size != sum_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid", K(base), K(addend.base()), K(sum.base()), K(augend_size), K(addend_size), K(add_size), K(sum_size));
  } else {
    uint64_t carry = 0;
    int64_t augend_idx = augend_size - 1;
    int64_t addend_idx = addend_size - 1;
    int64_t sum_idx = sum_size - 1;
    while (sum_idx >= 0) {
      uint64_t tmp = carry;
      if (augend_idx >= 0) {
        tmp += augend.at(augend_idx);
        --augend_idx;
      }
      if (addend_idx >= 0) {
        tmp += addend.at(addend_idx);
        --addend_idx;
      }
      if (base <= tmp) {
        tmp -= base;
        carry = 1;
      } else {
        carry = 0;
      }
      if (OB_FAIL(sum.set(sum_idx, tmp))) {
        LIB_LOG(WARN, "set to sum fail", K(ret), K(sum_idx));
        break;
      }
      --sum_idx;
    }
  }
  return ret;
}

template <class T>
int poly_poly_sub(
    const T &minuend,
    const T &subtrahend,
    T &remainder,
    bool &negative,
    const bool truevalue/* = true*/)
{
  int ret = OB_SUCCESS;

  uint64_t base = minuend.base();
  int64_t minuend_size = minuend.size();
  int64_t subtrahend_size = subtrahend.size();
  int64_t sub_size = std::max(minuend_size, subtrahend_size);
  int64_t remainder_size = remainder.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base != subtrahend.base()
      || base != remainder.base()
      || 0 >= minuend_size
      || 0 >= subtrahend_size
      || sub_size != remainder_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    uint64_t borrow = 0;
    int64_t minuend_idx = minuend_size - 1;
    int64_t subtrahend_idx = subtrahend_size - 1;
    int64_t remainder_idx = remainder_size - 1;
    while (remainder_idx >= 0) {
      uint64_t tmp_subtrahend = borrow;
      if (subtrahend_idx >= 0) {
        tmp_subtrahend += subtrahend.at(subtrahend_idx);
        --subtrahend_idx;
      }
      uint64_t tmp_minuend = 0;
      if (minuend_idx >= 0) {
        tmp_minuend += minuend.at(minuend_idx);
        --minuend_idx;
      }
      if (tmp_minuend < tmp_subtrahend) {
        tmp_minuend += base;
        borrow = 1;
      } else {
        borrow = 0;
      }
      tmp_minuend -= tmp_subtrahend;
      if (OB_FAIL(remainder.set(remainder_idx, tmp_minuend))) {
        LIB_LOG(WARN, "set to remainder fail", K(ret), K(tmp_minuend));
        break;
      }
      --remainder_idx;
    }

    negative = (0 != borrow);
    if (OB_SUCCESS == ret
        && negative
        && truevalue) {
      uint64_t borrow = 0;
      for (int64_t i = remainder_size - 1; i >= 0; --i) {
        uint64_t tmp = borrow + remainder.at(i);
        if (0 < tmp) {
          tmp = base - tmp;
          borrow = 1;
        } else {
          borrow = 0;
        }
        remainder.set(i, tmp);
      }
    }
  }
  return ret;
}

template <class T>
int recursion_set_product(
    const uint64_t base,
    const uint64_t value,
    const int64_t start_idx,
    T &product)
{
  int ret = OB_SUCCESS;

  uint64_t carry = value;
  int64_t carry_idx = 0;
  while (0 != carry) {
    carry += product.at(start_idx - carry_idx);
    uint64_t tmp = carry % base;
    carry = carry / base;

    if (OB_FAIL(product.set(start_idx - carry_idx, tmp))) {
      LIB_LOG(WARN, "set to product fail",
              K(ret), K(start_idx), K(carry_idx), K(tmp), K(carry));
      break;
    }
    ++carry_idx;
  }
  return ret;
}

template <class T>
int poly_poly_mul(
    const T &multiplicand,
    const T &multiplier,
    T &product)
{
  int ret = OB_SUCCESS;

  uint64_t base = multiplicand.base();
  int64_t multiplicand_size = multiplicand.size();
  int64_t multiplier_size = multiplier.size();
  int64_t mul_size = multiplicand_size + multiplier_size;
  int64_t product_size = product.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base != multiplier.base()
      || base != product.base()
      || 0 >= multiplicand_size
      || 0 >= multiplier_size
      || mul_size != product_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    // [a, b, c] * [d, e, f]
    // = (a*B^2 + b*B^1 + c*B^0) * (d*B^2 + e^B^1 + f*B^0)
    // = a*d*B^4 + (a*e+b*d)*B^3 + (a*f+b*e+c*d)*B^2 + (b*f+c*e)*B^1 + c*f*B^0
    for (int64_t i = 0; i < product_size; ++i) {
      product.set(i, 0);
    }
    for (int64_t multiplicand_idx = multiplicand_size - 1, product_idx = product_size - 1;
         OB_SUCCESS == ret && multiplicand_idx >= 0;
         --multiplicand_idx, --product_idx) {
      for (int64_t multiplier_idx = multiplier_size - 1; multiplier_idx >= 0; --multiplier_idx) {
        int64_t cur_product_idx = product_idx - (multiplier_size - 1 - multiplier_idx);
        uint64_t tmp_product = multiplicand.at(multiplicand_idx) * multiplier.at(multiplier_idx);

        if (OB_FAIL(recursion_set_product(base, tmp_product, cur_product_idx, product))) {
          LIB_LOG(WARN, "set product fail", K(tmp_product), K(product_idx));
          break;
        }
      }
    }
  }
  return ret;
}

//template <class T>
//uint64_t knuth_calc_delta(
//    const int64_t j,
//    T &u,
//    T &v)
//{
//  uint64_t qq = 0;
//  uint64_t base = u.base();
//
//  if (u.at(j) == v.at(1)) {
//    qq = base - 1;
//  } else {
//    qq = (u.at(j) * base + u.at(j + 1)) / v.at(1);
//  }
//
//  while (2 < v.size()) {
//    uint64_t left = v.at(2) * qq;
//    uint64_t right = (u.at(j) * base + u.at(j + 1) - qq * v.at(1)) * base + u.at(j + 2);
//    if (left > right) {
//      qq -= 1;
//    } else {
//      break;
//    }
//  }
//  LIB_LOG(DEBUG, "after D3 ",
//          K(j), K(qq));
//  return qq;
//}

template <class T>
int knuth_probe_quotient(
    const int64_t j,
    T &u,
    const T &v,
    uint64_t &qq)
{
  int ret = OB_SUCCESS;
  uint64_t base = u.base();
  int64_t n = v.size() - 1;

  // D4
  T qq_mul_v;
  qq_mul_v.set_base(base);
  T u_sub;
  u_sub.set_base(base);
  bool negative = false;
  bool truevalue = false;
  if (OB_FAIL(qq_mul_v.ensure(v.size() + 1))) {
    _OB_LOG(WARN, "ensure qq_mul_v fail, ret=%d size=%ld", ret, v.size() + 1);
  } else if (OB_FAIL(poly_mono_mul(v, qq, qq_mul_v))) {
    _OB_LOG(WARN, "%lu mul %s u=%s fail, ret=%d", qq, to_cstring(v), to_cstring(u), ret);
  } else if (OB_FAIL(u_sub.ensure(n + 1))) {
    _OB_LOG(WARN, "ensure u_sub fail, ret=%d size=%ld", ret, n + 1);
  } else if (OB_FAIL(poly_poly_sub(u.ref(j, j + n), qq_mul_v.ref(1, n + 1), u_sub,
                                                negative, truevalue))) {
    _OB_LOG(WARN, "%s sub %s fail, ret=%d", to_cstring(u.ref(j, j + n)),
            to_cstring(qq_mul_v), ret);
  } else if (!negative
             && OB_FAIL(u.assign(u_sub, j, j + n))) {
    _OB_LOG(WARN, "assign %s to u[%ld,%ld] fail, ret=%d", to_cstring(u_sub), j, j + n, ret);
  } else {
    // do nothing
  }
  LIB_LOG(DEBUG, "after D4",
          K(j), K(u), K(v), K(qq), K(negative),
          K(qq_mul_v), K(u_sub), K(ret));


  // D6
  if (negative
      && OB_SUCCESS == ret) {
    qq -= 1;
    T u_add;
    u_add.set_base(base);
    if (OB_FAIL(u_add.ensure(n + 2))) {
      _OB_LOG(WARN, "ensure u_add fail, ret=%d size=%ld", ret, n + 2);
    } else if (OB_FAIL(poly_poly_add(u_sub, v, u_add))) {
      _OB_LOG(WARN, "%s add %s fail, ret=%d", to_cstring(u_sub), to_cstring(v), ret);
    } else if (OB_FAIL(u.assign(u_add.ref(1, n + 1), j, j + n))) {
      _OB_LOG(WARN, "assign %s to u[%ld,%ld] fail, ret=%d",
              to_cstring(u_add.ref(1, n + 1)), j, j + n, ret);
    } else {
      // do nothing
    }
    LIB_LOG(DEBUG, "after D6 ",
            K(j), K(u), K(v), K(qq), K(u_add), K(ret));
  }

  return ret;
}

//template <class T>
//int poly_poly_div(
//    const T &dividend,
//    const T &divisor,
//    T &quotient,
//    T &remainder)
//{
//  int ret = OB_SUCCESS;
//
//  uint64_t base = dividend.base();
//  int64_t dividend_size = dividend.size();
//  int64_t divisor_size = divisor.size();
//  int64_t div_size = dividend_size - divisor_size + 1;
//  int64_t quotient_size = quotient.size();
//  int64_t remainder_size = remainder.size();
//
//  int64_t n = divisor_size;
//  int64_t m = dividend_size - n;
//  T u;
//  T v;
//  T &q = quotient;
//  u.set_base(base);
//  v.set_base(base);
//
//  if (OB_UNLIKELY(MAX_BASE < base
//      || base != divisor.base()
//      || base != quotient.base()
//      || base != remainder.base()
//      || 0 >= divisor_size
//      || divisor_size >= dividend_size
//      || div_size != quotient_size
//      || divisor_size != remainder_size)) {
//    ret = OB_INVALID_ARGUMENT;
//    LIB_LOG(ERROR, "the input param is invalid");
//  } else {
//    // Knuth Algo:
//    // D1
//    uint64_t d = base / (divisor.at(0) + 1);
//
//    if (OB_FAIL(u.ensure(dividend_size + 1))) {
//      _OB_LOG(WARN, "ensure dividend fail ret=%d size=%ld", ret, dividend_size + 1);
//    } else if (OB_FAIL(v.ensure(divisor_size + 1))) {
//      _OB_LOG(WARN, "ensure divisor fail ret=%d size=%ld", ret, divisor_size + 1);
//    }
//    if (OB_FAIL(poly_mono_mul(dividend, d, u))) {
//      _OB_LOG(WARN, "%s mul %lu fail, ret=%d", to_cstring(dividend), d, ret);
//    } else if (OB_FAIL(poly_mono_mul(divisor, d, v))) {
//      _OB_LOG(WARN, "%s mul %lu fail, ret=%d", to_cstring(divisor), d, ret);
//    } else {
//      LIB_LOG(DEBUG, "Knuth Algo start",
//              K(dividend), K(divisor), K(d), K(u), K(v), K(n), K(m));
//
//      // D2
//      int64_t j = 0;
//      while (true) {
//        // D3
//        uint64_t qq = knuth_calc_delta(j, u, v);
//
//        // D4, D6
//        if (OB_FAIL(knuth_probe_quotient(j, u, v, qq))) {
//          break;
//        }
//
//        // D5
//        q.set(j, qq);
//        LIB_LOG(DEBUG, "after D5",
//                K(j), K(q));
//
//        // D7
//        j += 1;
//        if (m < j) {
//          break;
//        }
//      }
//
//      if (OB_SUCC(ret)) {
//        uint64_t r = 0;
//        if (OB_FAIL(poly_mono_div(u.ref(m + 1, m + n), d, remainder, r))) {
//          _OB_LOG(WARN, "%s div %lu fail, ret=%d", to_cstring(u.ref(m + 1, m + n)), d, ret);
//        }
//      }
//
//      _OB_LOG(DEBUG, "Knuth Algo end, %s / %s = %s ... %s",
//              to_cstring(dividend), to_cstring(divisor),
//              to_cstring(quotient), to_cstring(remainder));
//    }
//  }
//  return ret;
//}

inline void ObNumber::set_zero()
{
  d_.len_ = 0;
  d_.sign_ = POSITIVE;
  d_.exp_ = 0;
}

inline bool ObNumber::is_negative() const
{
  return (NEGATIVE == d_.sign_);
}

inline bool ObNumber::is_zero() const
{
  return (0 == d_.len_);
}

inline bool ObNumber::is_decimal() const
{
  bool bret = false;
  if (POSITIVE == d_.sign_) {
    bret = (0 > (d_.exp_ - EXP_ZERO));
  } else {
    bret = (0 < (d_.exp_ - EXP_ZERO));
  }
  return bret;
}

inline bool ObNumber::has_decimal() const
{
  const int32_t exp = get_decode_exp(d_);
  return (exp < 0 || (exp + 1) < d_.len_);
}

inline bool ObNumber::is_integer() const
{
  bool bret = false;
  int int_len = 0;
  if (is_zero()) {
    bret = true;
  } else if (OB_ISNULL(digits_) || OB_UNLIKELY(0 >= d_.len_)) {
    bret = false;
    _OB_LOG_RET(ERROR, common::OB_NOT_INIT, "not init");
  } else {
    if (POSITIVE == d_.sign_) {
      int_len = d_.se_ - POSITIVE_EXP_BOUNDARY;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else if (NEGATIVE == d_.sign_) {
      int_len = NEGATIVE_EXP_BOUNDARY - d_.se_;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else { /* Do nothing */ }
    if (int_len < 0 || (int_len > 0 && int_len < d_.len_)) {
      bret = false;
    } else {
      bret = true;
    }
  }
  return bret;
}

inline int32_t ObNumber::get_decode_exp(const ObNumber::Desc &desc)
{
  return (POSITIVE == desc.sign_ ? (desc.se_ - POSITIVE_EXP_BOUNDARY) : (NEGATIVE_EXP_BOUNDARY - desc.se_));
}

inline bool ObNumber::is_integer(const int32_t expr_value) const
{
  bool bret = false;
  if (is_zero()) {
    bret = true;
  } else if (OB_ISNULL(digits_) || OB_UNLIKELY(0 >= d_.len_)) {
    bret = false;
    _OB_LOG_RET(ERROR, common::OB_NOT_INIT, "not init");
  } else if (expr_value < 0 || (expr_value + 1 < d_.len_)) {
    bret = false;
  } else {
    bret = true;
  }
  return bret;
}

inline bool ObNumber::is_int32(const int32_t expr_value) const
{
  return (1 == d_.len_ && 0 == expr_value);
}

inline bool ObNumber::is_int64_without_expr(const int32_t expr_value) const
{
  return (2 == d_.len_ && 1 == expr_value);
}

inline bool ObNumber::is_int64_with_expr(const int32_t expr_value) const
{
  return (1 == d_.len_ && 1 == expr_value);
}

inline uint32_t ObNumber::get_desc_value() const
{
  return d_.desc_;
}

inline const ObNumber::Desc& ObNumber::get_desc() const
{
  return d_;
}

inline uint32_t *ObNumber::get_digits() const
{
  return digits_;
}

inline int64_t ObNumber::get_length() const
{
  return d_.len_;
}

template <typename T>
void ObNumber::normalize_digit_(T digits, int32_t max_idx) const
{
  if (digits[max_idx] >= BASE / 10 * 5) {
    for (int32_t i = max_idx - 1; i >= 0; --i) {
      if (digits[i] + 1 == BASE) {
        digits[i] = 0;
      } else {
        ++digits[i];
        break;
      }
    }
  }
}

OB_INLINE bool ObNumber::is_negative_number(const ObNumber::Desc &desc)
{
  return NEGATIVE == desc.sign_;
}

OB_INLINE bool ObNumber::is_zero_number(const ObNumber::Desc &desc)
{
  return 0 == desc.len_;
}

OB_INLINE int64_t ObNumber::get_number_length(const ObNumber::Desc &desc)
{
  return desc.len_;
}

OB_INLINE void ObNumber::assign(const uint32_t desc, uint32_t *digits)
{
  //不用检查digits是否为null， 0就是null
  d_.desc_ = desc;
  digits_ = digits;
}

OB_INLINE int64_t ObNumber::get_scale() const
{
  int64_t decimal_count = 0;
  if (!is_zero()) {
    const int64_t expr_value = get_decode_exp(d_);
    //xxx_length means xx digit array length
    const int64_t integer_length = (expr_value >= 0 ? (expr_value + 1) : 0);
    //e.g. 1,000000000,000000000,   digits=[1], expr_value=2, len_=1,
    //integer_length=3, valid_integer_length=1
    // const int64_t valid_integer_length = (d_.len_ > integer_length ? integer_length : d_.len_);

    //len_ > integer_length means have decimal
    const int64_t valid_decimal_length = (d_.len_ > integer_length ? (d_.len_ - integer_length) : 0);
    //e.g. 0.000000000 000000001, digits=[1], expr_value=-2, len_=1, decimal_length=2,
    //valid_decimal_length=1
    const int64_t decimal_length = valid_decimal_length
                                   + (0 == integer_length ? (0 - expr_value - 1) : 0);

    int64_t tail_decimal_zero_count = 0;
    if (valid_decimal_length > 0) {
      remove_back_zero(digits_[d_.len_ - 1], tail_decimal_zero_count);
    }
    decimal_count = decimal_length * DIGIT_LEN - tail_decimal_zero_count;
    LIB_LOG(DEBUG, "get_scale", KPC(this), K(expr_value),
            K(integer_length), K(valid_decimal_length), K(decimal_length),
            K(tail_decimal_zero_count), K(decimal_count));
  }
  return decimal_count;
}

OB_INLINE bool ObNumber::fast_mul_1d_mul_2d(const uint32_t *multiplicand_digits,
                                          const uint32_t *multiplier_digits,
                                          uint32_t multiplicand_desc,
                                          uint32_t multiplier_desc,
                                          int64_t res_len, int64_t res_exp,
                                          uint32_t *res_digit,
                                          Desc &res_desc)
{
  bool fast_mul_done = false;
  // pattern detection
  if (multiplier_desc == NUM_DESC_2DIGITS_POSITIVE_DECIMAL &&
      (multiplicand_desc == NUM_DESC_1DIGIT_POSITIVE_INTEGER ||
       multiplicand_desc == NUM_DESC_1DIGIT_POSITIVE_FRAGMENT)) {
    const uint64_t tmp_multiplicand_di =
        static_cast<uint64_t>(*multiplicand_digits);
    auto low_digit = tmp_multiplicand_di * multiplier_digits[1];
    auto high_digit = tmp_multiplicand_di * multiplier_digits[0];
    int64_t digits_len = 0;
    // formalize result digits
    uint64_t carry = 0;
    if (low_digit >= ObNumber::BASE) {
      carry = low_digit / ObNumber::BASE;
      low_digit = low_digit % ObNumber::BASE;
      res_len -= (low_digit == 0);
    }
    high_digit += carry;
    if (high_digit >= ObNumber::BASE) {
      carry = high_digit / ObNumber::BASE;
      high_digit = high_digit % ObNumber::BASE;
      ++res_exp;
      res_len -= (high_digit == 0 && low_digit == 0);
      digits_len = res_len;
      // performance critical: set the tailing digits even they are 0, no
      // overflow risk
      res_digit[0] = static_cast<uint32_t>(carry);
      res_digit[1] = static_cast<uint32_t>(high_digit);
      res_digit[2] = static_cast<uint32_t>(low_digit);
    } else {
      digits_len = --res_len;
      // performance critical: set the tailing digits even they are 0, no
      // overflow risk
      res_digit[0] = static_cast<uint32_t>(high_digit);
      res_digit[1] = static_cast<uint32_t>(low_digit);
    }
    ObNumber::calc_positive_num_desc(res_desc, res_exp, (uint8_t)digits_len);
    fast_mul_done = true;
  }

  return fast_mul_done;
}

OB_INLINE bool ObNumber::fast_mul_1d_mul_1d(const uint32_t *multiplicand_digits,
                                            const uint32_t *multiplier_digits,
                                            uint32_t multiplicand_desc,
                                            uint32_t multiplier_desc,
                                            int64_t res_len, int64_t res_exp,
                                            uint32_t *res_digit,
                                            Desc &res_desc)
{
  bool fast_mul_done = false;
  // pattern detection
  if ((multiplicand_desc == NUM_DESC_1DIGIT_POSITIVE_INTEGER ||
       multiplicand_desc == NUM_DESC_1DIGIT_POSITIVE_FRAGMENT) &&
      (multiplier_desc == NUM_DESC_1DIGIT_POSITIVE_FRAGMENT ||
       multiplier_desc == NUM_DESC_1DIGIT_POSITIVE_INTEGER)) {
    auto res_digit_value =
        static_cast<uint64_t>(*multiplicand_digits) * multiplier_digits[0];
    int64_t digits_len = 0;
    // formalize result digits
    uint64_t carry = 0;
    if (res_digit_value >= ObNumber::BASE) {
      carry = res_digit_value / ObNumber::BASE;
      res_digit_value = res_digit_value % ObNumber::BASE;
      // same as: if (res_digit_value == 0) { res_len = sum_len - 1} else {
      // res_len = sum_len}
      res_len -= (res_digit_value == 0);
      digits_len = res_len;
      ++res_exp;
      // performance critical: set the tailing digits even they are 0, no
      // overflow risk
      res_digit[0] = static_cast<uint32_t>(carry);
      res_digit[1] = static_cast<uint32_t>(res_digit_value);
    } else {
      digits_len = --res_len;
      res_digit[0] = static_cast<uint32_t>(res_digit_value);
    }

    ObNumber::calc_positive_num_desc(res_desc, res_exp, (uint8_t)digits_len);
    fast_mul_done = true;
  }

  return fast_mul_done;
}

OB_INLINE bool ObNumber::fast_mul_2d_mul_2d(const uint32_t *multiplicand_digits,
                                           const uint32_t *multiplier_digits,
                                           uint32_t multiplicand_desc,
                                           uint32_t multiplier_desc,
                                           int64_t res_len, int64_t res_exp,
                                           uint32_t *res_digit,
                                           Desc &res_desc)
{
  int fast_mul_done = false;
  // pattern detection
  if (multiplicand_desc == NUM_DESC_2DIGITS_POSITIVE_DECIMAL &&
      multiplier_desc == NUM_DESC_2DIGITS_POSITIVE_DECIMAL) {
    const auto multiplicand_high_di =
        static_cast<uint64_t>(*multiplicand_digits++);
    const auto multiplicand_low_di =
        static_cast<uint64_t>(*multiplicand_digits);
    auto low_digit = multiplicand_low_di * multiplier_digits[1];
    auto mid_digit = multiplicand_low_di * multiplier_digits[0];
    mid_digit += multiplicand_high_di * multiplier_digits[1];
    auto high_digit = multiplicand_high_di * multiplier_digits[0];
    int64_t digits_len = 0;

    // formalize result digits
    uint64_t carry = 0;
    if (low_digit >= ObNumber::BASE) {
      carry = low_digit / ObNumber::BASE;
      low_digit = low_digit % ObNumber::BASE;
      res_len -= (low_digit == 0);
    }

    mid_digit += carry;
    if (mid_digit >= ObNumber::BASE) {
      carry = mid_digit / ObNumber::BASE;
      mid_digit = mid_digit % ObNumber::BASE;
      res_len -= (mid_digit == 0 && low_digit == 0);
    } else {
      carry = 0;
    }

    high_digit += carry;
    if (high_digit >= ObNumber::BASE) {
      carry = high_digit / ObNumber::BASE;
      high_digit = high_digit % ObNumber::BASE;
      ++res_exp;
      res_len -= (high_digit == 0 && mid_digit == 0 && low_digit == 0);
      digits_len = res_len;
      // performance critical: set the tailing digits even they are 0, no
      // overflow risk
      res_digit[0] = static_cast<uint32_t>(carry);
      res_digit[1] = static_cast<uint32_t>(high_digit);
      res_digit[2] = static_cast<uint32_t>(mid_digit);
      res_digit[3] = static_cast<uint32_t>(low_digit);
    } else {
      digits_len = --res_len;
      // performance critical: set the tailing digits even they are 0, no
      // overflow risk
      res_digit[0] = static_cast<uint32_t>(high_digit);
      res_digit[1] = static_cast<uint32_t>(mid_digit);
      res_digit[2] = static_cast<uint32_t>(low_digit);
    }

    ObNumber::calc_positive_num_desc(res_desc, res_exp, (uint8_t)digits_len);
    fast_mul_done = true;
  }
  return fast_mul_done;
}

OB_INLINE bool ObNumber::try_fast_mul(ObNumber &l_num, ObNumber &r_num,
                                      uint32_t *res_digit, Desc &res_desc)
{
  bool is_fast_panel = false;
  uint32_t multiplicand_desc = l_num.d_.desc_;
  uint32_t multiplier_desc = r_num.d_.desc_;
  const uint32_t *multiplicand_digits = l_num.digits_;
  const uint32_t *multiplier_digits = r_num.digits_;
  int64_t multiplicand_len = l_num.d_.len_;
  int64_t multiplier_len = r_num.d_.len_;

  if (l_num.d_.len_ > r_num.d_.len_) {
    multiplicand_digits = r_num.digits_;
    multiplier_digits = l_num.digits_;
    multiplicand_len = r_num.d_.len_;
    multiplier_len = l_num.d_.len_;
    multiplicand_desc = r_num.d_.desc_;
    multiplier_desc = l_num.d_.desc_;
  }
  int64_t res_exp = l_num.get_decode_exp(multiplicand_desc) +
                    l_num.get_decode_exp(multiplier_desc);
  int64_t res_len = multiplier_len + multiplicand_len;

  if (fast_mul_1d_mul_2d(multiplicand_digits, multiplier_digits,
                            multiplicand_desc, multiplier_desc, res_len,
                            res_exp, res_digit, res_desc)) {

    is_fast_panel = true;
    // LOG_DEBUG("mul speedup", K(l_num.format()),
    // K(r_num.format()), K(res_num.format()));
  } else if (fast_mul_1d_mul_1d(multiplicand_digits, multiplier_digits,
                                   multiplicand_desc, multiplier_desc, res_len,
                                   res_exp, res_digit, res_desc)) {
    is_fast_panel = true;
    // LOG_DEBUG("mul speedup", K(l_num.format()),
    // K(r_num.format()), K(res_num.format()));
  } else if (fast_mul_2d_mul_2d(multiplicand_digits, multiplier_digits,
                                   multiplicand_desc, multiplier_desc, res_len,
                                   res_exp, res_digit, res_desc)) {
    is_fast_panel = true;
  }
  return is_fast_panel;
}

OB_INLINE bool ObNumber::try_fast_add(ObNumber &l_num, ObNumber &r_num,
                                      uint32_t *res_digit, Desc &res_desc)
{
  bool is_fast_panel = true;
  uint32_t sum_frag_val = 0;
  uint32_t sum_int_val = 0;

  if (l_num.d_.is_1d_positive_integer()) {
    sum_int_val = l_num.get_digits()[0];
  } else if (l_num.d_.is_1d_positive_fragment()) {
    sum_frag_val = l_num.get_digits()[0];
  } else {
    is_fast_panel = false;
  }

  if (r_num.d_.is_1d_positive_integer()) {
    sum_int_val += r_num.get_digits()[0];
  } else if (r_num.d_.is_1d_positive_fragment()) {
    sum_frag_val += r_num.get_digits()[0];
  } else {
    is_fast_panel = false;
  }

  if (is_fast_panel) {
    // generate res_desc
    res_desc.len_ = 2;
    res_desc.exp_ = common::max(l_num.d_.exp_, r_num.d_.exp_);
    auto carry = 0;
    if (sum_frag_val >= BASE) {
      carry = 1;
      sum_frag_val -= BASE;
    }

    res_desc.len_ -= (sum_frag_val == 0);
    sum_int_val += carry;
    if (sum_int_val >= BASE) {
      sum_int_val -= BASE;
      res_desc.len_ -= (sum_int_val == 0 && sum_frag_val == 0);
      res_desc.exp_++;
      res_desc.len_++;
      res_digit[0] = 1;
      res_digit[1] = sum_int_val;
      res_digit[2] = sum_frag_val; // set by default, no zero check
    } else {
      if (sum_int_val) {
        res_digit[0] = sum_int_val;
        res_digit[1] = sum_frag_val; // set by default, no zero check
        if (sum_int_val == carry && carry == 1) {
          res_desc.exp_++;
        }
      } else {
        res_digit[0] = sum_frag_val;
        res_desc.len_--;
      }
    }
    res_desc.sign_ = POSITIVE;
    res_desc.reserved_ = 0; // must assign 0 explicitly here, due to optimization in sql.
  }

  return is_fast_panel;
}

OB_INLINE bool ObNumber::try_fast_minus(ObNumber &l_num, ObNumber &r_num,
                                      uint32_t *res_digit, Desc &res_desc)
{
  bool is_fast_panel = false;
  uint64_t res_frag_val = 0;
  uint64_t res_int_val = 0;
  uint64_t carry = BASE;

  // TODO qubin.qb: support more sophisticated cases in further release.
  // E.G: minus between positive integer(2 - 1) or  postive decimal(e.g: 1.32 - 1.1).
  // So far only support cases like 2 - 0.3
  if (l_num.d_.is_1d_positive_integer() && r_num.d_.is_1d_positive_fragment()) {
    is_fast_panel = true;
    res_int_val = l_num.get_digits()[0] - 1;
    res_frag_val = carry - r_num.get_digits()[0];
    res_desc.desc_ = NUM_DESC_2DIGITS_POSITIVE_DECIMAL;
  }

  if (is_fast_panel) {
    if (res_int_val == 0) {
      res_desc.exp_--;
      res_desc.len_--;
      res_digit[0] = static_cast<uint32_t> (res_frag_val);
    } else {
      res_digit[0] = static_cast<uint32_t> (res_int_val);
      res_digit[1] = static_cast<uint32_t> (res_frag_val);
    }
    res_desc.len_ -= (res_frag_val == 0);
    res_desc.reserved_ = 0;
  }
  return is_fast_panel;
}

class ObNumberCalc
{
/* 用法说明：计算 res = (v0 + v1 - v2) * v3
    ObNumber res;
    ObNumberCalc calc(v0, allocator);
    if (OB_FAIL(ctx.add(v1).sub(v2).mul(v3).get_result(res))) {
      LOG_WARN("fail calc value", K(ret));
    }

  内存说明： 无论是否参与过运算，res 使用 allocator 分配的内存
*/
public:
  ObNumberCalc(const ObNumber &init_v, common::ObIAllocator &allocator)
    : last_ret_(common::OB_SUCCESS), allocator_(allocator)
  {
    last_ret_ = res_.from(init_v, allocator_);
  }

  ObNumberCalc &add(int64_t v)
  {
    if (common::OB_SUCCESS == last_ret_) {
      ObNumber tmp;
      if (common::OB_SUCCESS == (last_ret_ = tmp.from(v, allocator_))) {
        last_ret_ = res_.add(tmp, res_, allocator_);
      }
    }
    return *this;
  }
  ObNumberCalc &sub(int64_t v)
  {
    if (common::OB_SUCCESS == last_ret_) {
      ObNumber tmp;
      if (common::OB_SUCCESS == (last_ret_ = tmp.from(v, allocator_))) {
        last_ret_ = res_.sub(tmp, res_, allocator_);
      }
    }
    return *this;
  }
  ObNumberCalc &mul(int64_t v)
  {
    if (common::OB_SUCCESS == last_ret_) {
      ObNumber tmp;
      if (common::OB_SUCCESS == (last_ret_ = tmp.from(v, allocator_))) {
        last_ret_ = res_.mul(tmp, res_, allocator_);
      }
    }
    return *this;
  }

  ObNumberCalc &add(const ObNumber &v)
  {
    if (common::OB_SUCCESS == last_ret_) {
      last_ret_ = res_.add(v, res_, allocator_);
    }
    return *this;
  }
  ObNumberCalc &sub(const ObNumber &v)
  {
    if (common::OB_SUCCESS == last_ret_) {
      last_ret_ = res_.sub(v, res_, allocator_);
    }
    return *this;
  }
  ObNumberCalc &mul(const ObNumber &v)
  {
    if (common::OB_SUCCESS == last_ret_) {
      last_ret_ = res_.mul(v, res_, allocator_);
    }
    return *this;
  }
  int get_result(ObNumber &res)
  {
    if (common::OB_SUCCESS == last_ret_) {
      res.shadow_copy(res_); // 浅拷贝，引用 allocator 中内存
    }
    return last_ret_;
  }
private:
  int last_ret_;
  ObNumber res_;
  // TODO: 这里内存还可以继续优化，最多只需要使用 2 个定长 buffer 就可以
  // 完成所有串联运算，而不需要每次运算都从 allocator 新分配一个 buffer
  // 考虑到一般场景下运算的操作数个数较少，暂不做这个优化
  common::ObIAllocator &allocator_;
};

}
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_LIB_NUMBER_OB_NUMBER_V2_H_
