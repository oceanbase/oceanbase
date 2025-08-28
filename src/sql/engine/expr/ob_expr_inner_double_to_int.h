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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INNER_DOUBLE_TO_INT_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INNER_DOUBLE_TO_INT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
//This expression is used to extract double to int range.
//inner_double_to_int(val)
//for example: inner_double_to_int(9.223372036854776e+18) = 9223372036854775296 
//             inner_double_to_int(9.223372036854776e+18) = INT64_MAX
class ObExprInnerDoubleToInt : public ObFuncExprOperator
{
public:
  explicit  ObExprInnerDoubleToInt(common::ObIAllocator &alloc);
  virtual ~ObExprInnerDoubleToInt() {};

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_inner_double_to_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int convert_double_to_int64_range(double d, int64_t &start, int64_t &end);

  static int convert_double_to_uint64_range(double d, uint64_t &start, uint64_t &end);

  static int add_double_bit_1(double d, double &out);

  static int sub_double_bit_1(double d, double &out);

  static int double_to_number(double in_val, ObIAllocator &alloc,
                                  number::ObNumber &number);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerDoubleToInt) const;
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INNER_DOUBLE_TO_INT_
