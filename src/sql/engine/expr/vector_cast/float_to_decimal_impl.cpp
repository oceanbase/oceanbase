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
#include "lib/charset/ob_dtoa.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_helper.h"
#include "share/object/ob_obj_cast.h"
#include "share/object/ob_obj_cast_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/vector_cast/float_to_decimal_impl.h"
namespace oceanbase
{
namespace sql
{

/*
  Cast float value to decimal int value.

  Inspired by "How to Print Floating-Point Numbers Accurately" by
  Guy L. Steele, Jr. and Jon L. White [Proc. ACM SIGPLAN '90, pp. 112-126].

  Process for mysql mode:
    1. Get the long double value lx = abs(x), if x = -15, then lx = 15.
    2. Get the decimal_cnt of lx and scale the lx to [1.0, 10.0) with it,
       if lx = 15, then we need decimal_cnt = 1 cause (15 / 10^1 = 1.5).
    3. Init the eps = 2^(63-3), which represents the max error of lx
       it can be calculated with the method mentioned in the paper:
       "Printing Floating-Point Numbers Quickly and Accurately with Integers".
    4. Get the margin_low and margin_high of lx, which represents the 
       margin between lx and the nearest double value.
       if lx = 15, then its real_exponent is -49, margin = 1.0 * 2^-49.
    5. Cast the lx to decimal value with Steele & White's algorithm,
       which only generates the digits for rounding correctly.

  Process for oracle mode:
    1. - 3. are the same as mysql mode.
    4. Generate max digits of type_precision and then fix them up.
 */
int ObFloatToDecimal::float2decimal(double x, const bool is_oracle_mode, ob_gcvt_arg_type arg_type,
                                    const ObPrecision target_precision, const ObScale target_scale,
                                    const ObCastMode cast_mode, ObDecimalIntBuilder &dec_builder,
                                    const ObUserLoggingCtx *user_logging_ctx,
                                    ObDecimalInt *&decint){
  int ret = OB_SUCCESS;
  dec_builder.set_zero(sizeof(int512_t));
  /* if is_oracle_mode and arg_type is OB_GCVT_ARG_FLOAT, type_precision is MAX_DIGITS_FLOAT
    else type_precision is MAX_DIGITS_DOUBLE */
  uint16_t type_precision = ((OB_GCVT_ARG_FLOAT == arg_type) && is_oracle_mode) ?
                                  MAX_DIGITS_FLOAT : MAX_DIGITS_DOUBLE;
  bool is_column_convert = CM_IS_COLUMN_CONVERT(cast_mode);
  if (FLOAT80_PRECISION != std::numeric_limits<long double>::digits10) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!std::isfinite(x))) {
    ret = OB_INVALID_NUMERIC;
  } else if (0 == x) {
    dec_builder.from(0);
  } else {
    int64_t res = 0, is_neg = 0;
    DoubleUnion union_x;
    union_x.double_value = x;
    is_neg = union_x.is_negative();
    //binary_cnt is the number of binary digits of x, decimal_cnt is the number of decimal digits of x minus 1
    //for example, x = 15, binary_cnt = 4, decimal_cnt = 2 - 1
    int32_t binary_cnt = 0, real_exponent = 0, decimal_cnt = 0;
    //lx is long double type value of abs(x), eps is the max error of lx
    long double lx = 0, margin = 0, eps = 0;
    LongDoubleUnion union_lx, union_margin, union_eps;
    /* init lx = abs(x) */
    union_lx.double_val = x;
    if (is_neg) union_lx.uint32_val[2] &= ~0x8000; // clear sign bit
    lx = union_lx.double_val;
    /* get the binary digits cnt and real_exponent of x
    x can be represented as x = (mantissa + 2^52) * 2^(exponent - 1023 - 52)
    so the binary_cnt = 52, real_exponent = exponent - 1023 - 52 */
    const uint64_t mantissa = union_x.get_mantissa();
    const int64_t exponent = union_x.get_exponent();
    if (0 != exponent) {
      binary_cnt = DOUBlE_MANTISSA_BITS;
      real_exponent = exponent - DOUBLE_EXPONENT_BIAS - DOUBlE_MANTISSA_BITS;
    } else { //subnormal double case
      binary_cnt = mantissa ? 64 - __builtin_clzl(mantissa) : 0;
      real_exponent = 1 - DOUBLE_EXPONENT_BIAS - DOUBlE_MANTISSA_BITS;
    }
    /* Get the decimal_cnt with binary_cnt and real_exponent,
    decimal_cnt is used to scale the value x to [1.0, 10.0).
    For example, if x = 25, then we need decimal_cnt = 1 cause (25 / 10^1 = 2.5).
    In Such case, binary_cnt = 52, real_exponent = (1027-1023-52) = -48,
    decimal_cnt = low[(52 - 48) * LOG10_2- EPSILON] = 1 */
    decimal_cnt = static_cast<int32_t>(static_cast<double>
                          (binary_cnt + real_exponent) * LOG10_2 - EPSILON);
    /* In some cases, the decimal_cnt is 1 less than expected, so we need to adjust it
    For example, if x = 15, binary_cnt = 52, real_exponent = -49, decimal_cnt = 0 */
    if (lx > 1.0l) {
      long double scale = get_scale_factor<long double>(decimal_cnt);
      lx /= scale;
      // margin /= scale;
      if (lx >= 10.0l) {
        lx /= 10.0l;
        // margin /= 10.0l;
        decimal_cnt++;
      }
    } else {
      long double scale = get_scale_factor<long double>(-decimal_cnt);
      lx *= scale;
      // margin *= scale;
      if (lx < 1.0l) {
        lx *= 10.0l;
        // margin *= 10.0l;
        decimal_cnt--;
      }
    }
    /* check if the value is out of range */
    if (decimal_cnt >= target_precision - target_scale) {
      int32_t check_int_bytes = wide::ObDecimalIntConstValue::
                                get_int_bytes_by_precision(target_precision);
      if (is_neg) {
        dec_builder.from(wide::ObDecimalIntConstValue::
                                get_min_value(target_precision),check_int_bytes);
      } else {
        dec_builder.from(wide::ObDecimalIntConstValue::
                                get_max_value(target_precision),check_int_bytes);
      }
      if (is_column_convert) {
        // For column convert casting, an additional check will be applied to check if the decimal
        // overflows, so the result decimal needs to be a bigger/smaller value for column_convert to
        // catch overflow.
        const int32_t overflow_step = is_neg ? -1 : 1;
#define ADD_OVERFLOW_STEP(int_type) \
  *reinterpret_cast<int_type *>(dec_builder.get_buffer()) += overflow_step;
        DISPATCH_WIDTH_TASK(check_int_bytes, ADD_OVERFLOW_STEP);
#undef DO_ADD_STEP_TO_OVERFLOW
      }
    } else if (decimal_cnt < -target_scale - 1) {
      dec_builder.from(0);
    } else {
      /* The margin between lx and the nearest double value */
      union_margin.double_val = 1.0l;
      union_margin.uint32_val[2] += real_exponent;
      margin = union_margin.double_val;
      if(decimal_cnt > 0) {
        margin /= get_scale_factor<long double>(decimal_cnt);
      } else {
        margin *= get_scale_factor<long double>(-decimal_cnt);
      }
      /* eps represents the maximum error of lx */
      union_eps.double_val = 10.0l;
      union_eps.uint32_val[2] -= LONG_DOUBLE_MANTISSA_BITS; //equals to 10*2^-63
      eps = union_eps.double_val;
      /* scale represents the current scale of lx
         if scale = 1, means lx = x * 10^1 */
      ObScale scale = -decimal_cnt;
      if (!is_oracle_mode) {
        bool low = false;
        bool high = false;
        long double low_calc = 0.0l;
        long double high_calc = 0.0l;
        long double margin_low = margin;
        long double margin_high = margin;
        uint16_t item = 0;
        if (0 == mantissa) margin_low /= 2.0l;
        if (0 == exponent) {
          margin_low *= 2.0l;
          margin_high *= 2.0l;
        }
        for (int i = 0 ; ; ) {
          item = static_cast<uint16_t>(lx);
          lx -= item;
          res += item;
          low_calc = 2.0l * lx - margin_low;
          high_calc = 2.0l * lx + margin_high - 2.0l;
          if (std::abs(low_calc) <= eps || 
              (std::abs(high_calc) <= eps)) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          low = low_calc < 0; // 2.0l * lx < margin_low;
          high = high_calc > 0; // 2.0l * lx > 2.0l - margin_high;
          if (low || high || ++i >= type_precision) break;
          eps *= 10.0l;
          lx *= 10.0l;
          margin_low *= 10.0l;
          margin_high *= 10.0l;
          res *= 10;
          ++scale;
        }
        if (low && high) {
          if (std::abs(0.5l - lx) <= eps) 
            ret = OB_ERR_UNEXPECTED;
          if(lx > 0.5l) res++;
        } else if (high) {
          res++;
        } else if (low) {
          // do nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      } else { // if is_oracle_mode
        eps *= get_scale_factor<long double>(type_precision - 1);
        uint16_t item = 0;
        for (int i = 1;; i++) {
          item = static_cast<uint16_t>(lx);
          res += item;
          lx -= item;
          if ((0 == lx) || (i == type_precision)) break;
          res *= 10;
          lx *= 10.0l;
          ++scale;
        }
        if (lx > 0.5l + eps) {
          res++;
        } else if (lx < 0.5l - eps) {
          // do nothing
        } else{
          ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_SUCC(ret)) {
        int512_t temp_res = 0;
        /* if scale is less than target_scale, we need to continue scale up the value */
        if (scale <= target_scale){
          temp_res = res;
          while (target_scale - scale > 0) {
            const int16_t step = MIN(target_scale - scale, MAX_PRECISION_DECIMAL_INT_64);
            temp_res = temp_res * get_scale_factor<int64_t>(step);
            scale += step;
          }
        /* else we need to scale down the value and round it */
        } else {
          if (!is_oracle_mode && is_column_convert) {
            sql::ObDataTypeCastUtil::log_user_error_warning(user_logging_ctx, OB_ERR_DATA_TRUNCATED,
                                                            ObString(""), ObString(""), cast_mode);
          }
          res /= get_scale_factor<int64_t>(MIN(scale - target_scale - 1, INT64_PRECISION));
          res += 5; //for rounding
          res /= 10;
          temp_res = res;
        }
        if (is_neg) temp_res = -temp_res;
        dec_builder.from(temp_res, sizeof(int512_t));
      }
    }
  }
  decint = reinterpret_cast<ObDecimalInt *>(dec_builder.get_buffer());
  return ret;
}
}
}