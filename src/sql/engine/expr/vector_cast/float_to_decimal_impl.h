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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_VECTOR_CAST_FLOAT_TO_DECIMAL_
#define OCEANBASE_SQL_ENGINE_EXPR_VECTOR_CAST_FLOAT_TO_DECIMAL_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObFloatToDecimal final
{
public:
constexpr static uint16_t MAX_DIGITS_FLOAT = 9;
constexpr static uint16_t MAX_DIGITS_DOUBLE = 17;
constexpr static uint16_t FLOAT80_PRECISION = 18;
constexpr static uint16_t INT64_PRECISION = 18;
constexpr static uint16_t DOUBlE_MANTISSA_BITS = 52;
constexpr static uint16_t DOUBLE_EXPONENT_BITS = 11;
constexpr static uint16_t DOUBLE_EXPONENT_BIAS = 1023;
constexpr static uint16_t LONG_DOUBLE_MANTISSA_BITS = 63;
constexpr static uint64_t DOUBLE_EXPONENT_MASK = 0x7FF;
constexpr static uint64_t DOUBLE_MANTISSA_MASK = 0xFFFFFFFFFFFFFull;
constexpr static double LOG10_2 = 0.30103;
constexpr static double EPSILON = 1e-6;

typedef union
{
  bool is_negative() const { return (integer_value >> (DOUBlE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS)) != 0; }
  uint32_t get_exponent() const { return (integer_value >> DOUBlE_MANTISSA_BITS) & DOUBLE_EXPONENT_MASK; }
  uint64_t get_mantissa() const { return integer_value & DOUBLE_MANTISSA_MASK; }
  double double_value;
  uint64_t integer_value;
} DoubleUnion;

typedef union{
  long double double_val;
  uint32_t uint32_val[4];
} LongDoubleUnion;

static int float2decimal(double x, const bool is_oracle_mode, ob_gcvt_arg_type arg_type,
                         const ObPrecision target_precision, const ObScale target_scale,
                         const ObCastMode cast_mode, ObDecimalIntBuilder &dec_builder,
                         const ObUserLoggingCtx *user_logging_ctx, ObDecimalInt *&decint);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_VECTOR_CAST_FLOAT_TO_DECIMAL_ */