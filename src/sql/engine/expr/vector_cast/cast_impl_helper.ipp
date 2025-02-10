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
#include "sql/engine/expr/vector_cast/vector_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "share/object/ob_obj_cast_util.h"

#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
#define CAST_ARG_LIST_DECL ObObjType in_type, ObCollationType in_cs_type, ObScale in_scale, ObPrecision in_prec,\
                           ObObjType out_type, ObCollationType out_cs_type, ObScale out_scale, ObPrecision out_prec,\
                           ObEvalCtx &ctx
#define CAST_ARG_DECL in_type, in_cs_type, in_scale, in_prec,\
                      out_type, out_cs_type, out_scale, out_prec,\
                      ctx

class CastFnBase
{
public:
  CastFnBase(ObObjType in_type, ObCollationType in_cs_type, ObScale in_scale, ObPrecision in_prec,
             ObObjType out_type, ObCollationType out_cs_type, ObScale out_scale, ObPrecision out_prec,
             ObEvalCtx &ctx)
            : in_type_(in_type), in_cs_type_(in_cs_type), in_scale_(in_scale), in_prec_(in_prec),
              out_type_(out_type), out_cs_type_(out_cs_type), out_scale_(out_scale), out_prec_(out_prec),
              ctx_(ctx) {}
protected:
  ObObjType in_type_;
  ObCollationType in_cs_type_;
  ObScale in_scale_;
  ObPrecision in_prec_;
  ObObjType out_type_;
  ObCollationType out_cs_type_;
  ObScale out_scale_;
  ObPrecision out_prec_;
  ObEvalCtx &ctx_;
};

struct CastHelperImpl
{
public:
  static const int64_t INT_TC_PRECISION = INT64_MAX;
  template<typename Calc, typename ArgVec, typename ResVec>
  OB_INLINE static int batch_cast(Calc calc, const ObExpr &expr,
                                  ArgVec *arg_vec, ResVec *res_vec,
                                  ObBitVector &eval_flags,
                                  const ObBitVector &skip,
                                  const EvalBound &bound) {
    int ret = OB_SUCCESS;
    bool no_skip_no_null = bound.get_all_rows_active()
                     && eval_flags.accumulate_bit_cnt(bound) == 0
                     && !arg_vec->has_null();
    if (no_skip_no_null) {
      for (int idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        ret = calc(expr, idx);
      }
      eval_flags.set_all(bound.start(), bound.end());
    } else {
      for (int idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else if (arg_vec->is_null(idx)) {
          res_vec->set_null(idx);
          eval_flags.set(idx);
          continue;
        }
        ret = calc(expr, idx);
        eval_flags.set(idx);
      }
    }
    return ret;
  }

public:
  constexpr static const uint64_t power_of_10[] =
  {
      1LL,
      10LL,
      100LL,
      1000LL,
      10000LL,
      100000LL,
      1000000LL,
      10000000LL,
      100000000LL,
      1000000000LL,
      10000000000LL,
      100000000000LL,
      1000000000000LL,
      10000000000000LL,
      100000000000000LL,
      1000000000000000LL,
      10000000000000000LL,
      100000000000000000LL,
      1000000000000000000LL,
      10000000000000000000ULL
  };
  OB_INLINE static uint64_t pow10(int16_t idx)
  {
    return (idx >= 0 && idx <= 19) ? power_of_10[idx] : power_of_10[19];
  }

public:
  OB_INLINE static int64_t fast_strtoll(const ObString &s,
                                        const int64_t precision,
                                        bool &succ)
  {
    uint64_t res = 0;
    succ = s.length() <= 18 && s.length() > 0;
    char *str = const_cast<char *> (s.ptr());
    if (!succ) {
      if (s.length() == 19) {
        if (*str < '9') { // <=> *str == '-' || *str == '+' || (*str >= '0' && *str < '9')
          succ = true;
        }
      } else if (s.length() == 20) {
        if ((*str == '-' || *str == '+') && *(str + 1) < '9') {
          succ = true;
        }
      }
    }
    if (succ && s.length() >= precision) {
      succ = false;
    }
    if (succ) {
      bool neg = false;
      if (*str == '-' || *str == '+') {
        neg = (*str == '-');
        ++str;
      }
      for (; succ && str < s.ptr() + s.length(); str++) {
        if (*str < '0' || *str > '9') {
          succ = false;
        } else {
          res = res * 10 + (*str - '0');
        }
      }
      if (neg) {
        res *= -1;
      }
    }
    return res;
  }

  ///@fn extract integer part of decimalint, will round up
  ///@param [in] scale_factor scale_type heavily influence performance.
  /// 1. divide of wide integer is much slower than native one;
  /// 2. even the cost of 'if(sf < 10^19)' should be avoided,
  ///    since it might be operator of wide integer;
  /// 3. that is, when dividing, scale_factor should be uint64_t.
  ///@param [in] scale >= 0, negative scale for decimalint is illegal
  template<typename T, typename scale_type, class = typename std::enable_if<sizeof(T) < sizeof(int128_t)>::type>
  static inline int64_t scale_to_integer(T &lhs,
                                         scale_type &scale_factor,
                                         int16_t scale,
                                         bool round_up = true) {
    uint64_t sf = static_cast<int64_t>(scale_factor);
    int64_t q = lhs;
    if (OB_LIKELY(sf > 1)) {
      bool is_neg = lhs < 0;
      if (is_neg) { q = -lhs; }
      int64_t r = q % sf;
      q = q / sf;
      round_up &= (r >= (sf>>1));
      if (round_up) { q = q + 1; }
      if (is_neg) { q = -q; }
    }
    return q;
  }
  template<unsigned Bits, typename scale_type>
  static inline int64_t scale_to_integer(const wide::ObWideInteger<Bits> &lhs,
                                         const scale_type &scale_factor,
                                         int16_t scale,
                                         bool round_up = true) {
    int64_t q = 0;
    int64_t decint = static_cast<int64_t>(lhs);
    if (OB_UNLIKELY(scale == 0)) {
      q = (lhs < INT64_MIN) ? INT64_MIN : (lhs > INT64_MAX ? INT64_MAX : decint);
    } else if (OB_LIKELY(INT64_MIN <= lhs && lhs <= INT64_MAX)) {
      uint64_t sf = pow10(scale);
      q = scale_to_integer(decint, sf, scale, round_up);
    } else {
      static const uint64_t constexpr DIGITS10_BASE = 10000000000000000000ULL; // 10^19
      if (scale > 19) {
        wide::ObWideInteger<Bits> num = lhs / DIGITS10_BASE;
        scale_type sf = get_scale_factor<scale_type>(scale - 19);
        q = scale_to_integer(num, sf, scale - 19, round_up);
      } else {
        bool is_neg = (lhs < 0);
        wide::ObWideInteger<Bits> num = (is_neg ? -lhs : lhs);
        wide::ObWideInteger<Bits> remain;
        uint64_t sf = pow10(scale);
        wide::ObWideInteger<Bits>::_impl::template
            divide<wide::IgnoreOverFlow>(num, sf, num, remain);
        round_up &= (remain >= (sf >> 1));
        if (round_up) { num = num + 1; }
        if (is_neg) { num = -num; }
        q = static_cast<int64_t>(num);
        q = (num < INT64_MIN) ? INT64_MIN : (num > INT64_MAX ? INT64_MAX : q);
      }
    }
    return q;
  }

  template<typename T, typename scale_type, class = typename std::enable_if<sizeof(T) < sizeof(int128_t)>::type>
  static inline uint64_t scale_to_unsigned_integer(T &lhs,
                                                   scale_type &scale_factor,
                                                   int16_t scale,
                                                   bool round_up = true) {
    uint64_t sf = static_cast<int64_t>(scale_factor);
    uint64_t q = lhs;
    if (OB_LIKELY(sf > 1)) {
      int64_t r = q % sf;
      q = q / sf;
      round_up &= (r >= (sf>>1));
      if (round_up) { q = q + 1; }
    }
    return q;
  }
  template<unsigned Bits, typename scale_type>
  static inline uint64_t scale_to_unsigned_integer(const wide::ObWideInteger<Bits> &lhs,
                                                   const scale_type &scale_factor,
                                                   int16_t scale,
                                                   bool round_up = true) {
    uint64_t q = 0;
    uint64_t decint = static_cast<int64_t>(lhs);
    if (OB_UNLIKELY(scale == 0)) {
      q = (lhs > UINT64_MAX ? UINT64_MAX : decint);
    } else if (OB_LIKELY(lhs <= UINT64_MAX)) {
      uint64_t sf = pow10(scale);
      q = scale_to_unsigned_integer(decint, sf, scale, round_up);
    } else {
      static const uint64_t constexpr DIGITS10_BASE = 10000000000000000000ULL; // 10^19
      wide::ObWideInteger<Bits> num;
      if (scale > 19) {
        num = lhs / DIGITS10_BASE;
        scale_type sf = get_scale_factor<scale_type>(scale - 19);
        q = scale_to_unsigned_integer(num, sf, scale - 19, round_up);
      } else {
        wide::ObWideInteger<Bits> remain;
        uint64_t sf = pow10(scale);
        wide::ObWideInteger<Bits>::_impl::template
            divide<wide::IgnoreOverFlow>(lhs, sf, num, remain);
        round_up &= (remain >= (sf >> 1));
        if (round_up) { num = num + 1; }
        q = (num > UINT64_MAX ? UINT64_MAX : static_cast<int64_t>(num));
      }
    }
    return q;
  }

  ///@fn 根据传入的标记对res做舍入、溢出的处理
  ///@description: 基于common_scale_decimalint进行的修改，保留
  ///  common_scale_decimalint中根据cast_mode对舍入、溢出的差异化处理
  ///  sizeof(calc_type) >= sizeof(out_type)
  template<typename out_type, typename calc_type>
  OB_INLINE static int decimal_overflow_roundup_check(
      const ObCastMode cast_mode, out_type min_val, out_type max_val, bool is_neg,
      bool overflow, bool round_up, bool has_extra_decimals,
      calc_type &res)
  {
    int ret = OB_SUCCESS;
    if (CM_IS_CONST_TO_DECIMAL_INT(cast_mode)) {
      if (overflow) { res = is_neg ? min_val : max_val; }
      if (has_extra_decimals) {
        if ((cast_mode & CM_CONST_TO_DECIMAL_INT_UP) != 0 && !is_neg && res != max_val) { res = res + 1; }
        if ((cast_mode & CM_CONST_TO_DECIMAL_INT_DOWN) != 0 && is_neg && res != min_val) { res = res - 1; }
        if ((cast_mode & CM_CONST_TO_DECIMAL_INT_EQ) != 0) {
          res = is_neg ? min_val : max_val;
        }
      }
      if (res < min_val || res > max_val) {
        res = is_neg ? min_val : max_val;
      }
    } else if (CM_IS_COLUMN_CONVERT(cast_mode) || CM_IS_EXPLICIT_CAST(cast_mode)) {
      if (overflow) { res = is_neg ? min_val : max_val; }
      if (round_up) { res = is_neg ? res - 1 : res + 1; }
      if (res < min_val || res > max_val) {
        res = is_neg ? min_val : max_val;
      }
    } else {  // implicit
      if (round_up) { res = is_neg ? res - 1 : res + 1; }
    }
    return ret;
  }

  template<typename calc_type>
  static inline int parse_decimalint_to_datetime(
      calc_type decint, calc_type scale_factor, int16_t scale,
      const ObCastMode cast_mode, ObTimeConvertCtx cvrt_ctx,
      int64_t &int_part, int64_t &dec_part)
  {
    int ret = OB_SUCCESS;
    if (scale < 6) {  // 20010528.1430 -> 20010528, 143000 000
      if (OB_UNLIKELY(decint > INT64_MAX)) {
        ret = OB_INVALID_DATE_VALUE;
        SQL_LOG(WARN, "datetime integer is out of range", K(ret), K(decint));
      } else {
        uint64_t sf = pow10(scale);
        int64_t int64 = static_cast<int64_t>(decint);
        int_part = int64 / sf;
        dec_part = (int64 % sf) * pow10(9 - scale);
      }
    } else {  // 20010528.1430307654321 -> 20010528, 143031 000
      calc_type sf = get_scale_factor<calc_type>(scale);
      int_part = CastHelperImpl::scale_to_integer(decint, sf, scale, false);
      calc_type sf_minus6 = get_scale_factor<calc_type>(scale - 6);
      if (scale < 19) {
        int64_t remain = decint - sf * int_part;
        dec_part = CastHelperImpl::scale_to_integer(remain, sf_minus6, scale - 6);
      } else {
        calc_type remain = decint - sf * int_part;
        dec_part = CastHelperImpl::scale_to_integer(remain, sf_minus6, scale - 6);
      }
      dec_part *= 1000;
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(dec_part != 0
                      && ((0 == int_part && cvrt_ctx.is_timestamp_)
                          || (int_part >= 100 && int_part <= 99999999)))) {
        if (CM_IS_COLUMN_CONVERT(cast_mode) && !CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_INVALID_DATE_VALUE;
          SQL_LOG(WARN, "invalid date value", K(ret), K(int_part), K(dec_part), K(cast_mode));
        } else {
          dec_part = 0;
        }
      }
    }
    return ret;
  }

  static inline int vector_copy_string(const ObExpr &expr, ObEvalCtx &ctx, int64_t idx,
                                       int64_t in_len, const char *in_ptr,
                                       int64_t &out_len, char *&out_ptr,
                                       const int64_t align_offset = 0) {
    int ret = OB_SUCCESS;
    out_ptr = NULL;
    out_len = in_len + align_offset;
    if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, out_len, idx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      MEMCPY(out_ptr + align_offset, in_ptr, in_len);
      MEMSET(out_ptr, 0, align_offset);
    }
    return ret;
  }

  ///@brief common_copy_string_zf里面CM_IS_ZERO_FILL分支,
  /// get_res_mem一次batch都是申请同一片空间，会导致向量化计算结果被覆盖，需要修改.
  /// 此外，为了加速向量化，把非数据相关开销都尽量移出去了，并且
  /// 该函数将仅实现将局部变量的字符串copy到str_res_mem的任务.
  static inline int vector_copy_string_zf(const ObExpr &expr, ObEvalCtx &ctx, int64_t idx,
                                          int64_t in_len, const char *in_ptr, ObScale out_scale,
                                          int64_t &out_len, char *&out_ptr,
                                          const int64_t align_offset = 0) {
    int ret = OB_SUCCESS;
    out_ptr = NULL;
    out_len = static_cast<uint8_t>(out_scale);
    if (CM_IS_ZERO_FILL(expr.extra_) && out_len > in_len) {
      if (OB_ISNULL(out_ptr = expr.get_str_res_mem(ctx, out_len, idx))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        int64_t zf_len = out_len - in_len;
        MEMMOVE(out_ptr + zf_len, in_ptr, in_len);
        MEMSET(out_ptr, '0', zf_len);
      }
    } else {
      if (OB_FAIL(vector_copy_string(expr, ctx, idx, in_len, in_ptr, out_len, out_ptr, align_offset))) {
        SQL_LOG(WARN, "common_copy_string failed", K(ret), K(in_ptr), K(expr), K(ctx));
      }
    }
    return ret;
  }
};


OB_DECLARE_AVX512_SPECIFIC_CODE(

///@fn Fastly convert simple long strings into integers with simd.
/// Only for strings with a length between 5 and 16 characters,
/// containing only leading plus or minus signs and digits)
inline static bool parse_16_chars(const char* string, uint len, int64_t &res) noexcept
{
  bool neg = false;
  char *str = const_cast<char *>(string);
  bool succ = false;
  if (len >= 5 && len <= 16) {
    if (*str == '+' || (neg=(*str == '-'))) { str ++, len --; }

    auto chunk = _mm_lddqu_si128(reinterpret_cast<const __m128i*>(str));
    __m128i is_digit = _mm_and_si128(
            _mm_cmpgt_epi8(chunk, _mm_set1_epi8('/')),
            _mm_cmplt_epi8(chunk, _mm_set1_epi8(':'))
        );
    uint32_t mask = (1 << len) - 1;

    if ((_mm_movemask_epi8(is_digit) & mask) == mask) { // all digits
      chunk = chunk - _mm_set1_epi8('0');
      switch (len) {  // for length<16, need align
        case 4:  chunk = _mm_slli_si128(chunk, 12); break;
        case 5:  chunk = _mm_slli_si128(chunk, 11); break;
        case 6:  chunk = _mm_slli_si128(chunk, 10); break;
        case 7:  chunk = _mm_slli_si128(chunk,  9); break;
        case 8:  chunk = _mm_slli_si128(chunk,  8); break;
        case 9:  chunk = _mm_slli_si128(chunk,  7); break;
        case 10: chunk = _mm_slli_si128(chunk,  6); break;
        case 11: chunk = _mm_slli_si128(chunk,  5); break;
        case 12: chunk = _mm_slli_si128(chunk,  4); break;
        case 13: chunk = _mm_slli_si128(chunk,  3); break;
        case 14: chunk = _mm_slli_si128(chunk,  2); break;
        case 15: chunk = _mm_slli_si128(chunk,  1); break;
        default: break;  //16
      }
      chunk = _mm_maddubs_epi16(chunk, _mm_set_epi8(1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10));
      chunk = _mm_madd_epi16(chunk, _mm_set_epi16(1, 100, 1, 100, 1, 100, 1, 100));
      chunk = _mm_packus_epi32(chunk, chunk);
      chunk = _mm_madd_epi16(chunk, _mm_set_epi16(0, 0, 0, 0, 1, 10000, 1, 10000));
      res = ((chunk[0] & 0xffffffff) * 100000000) + (chunk[0] >> 32);
      if (neg) res = -res;
      succ = true;
    }
  }
  return succ;
}

)

} // end sql
} // namespace oceanbase
