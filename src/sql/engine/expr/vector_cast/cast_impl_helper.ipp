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
