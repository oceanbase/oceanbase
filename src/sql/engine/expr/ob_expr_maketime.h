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

#ifndef OB_SQL_ENGINE_EXPR_MAKETIME_
#define OB_SQL_ENGINE_EXPR_MAKETIME_

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
// Returns a time value calculated from the hour, minute, and second arguments.
// The 'second' argument can have a fractional part (with scale of 6).
//
// MAKETIME(hour,minute,second)
// e.g
// mysql> SELECT MAKETIME(12,15,30);
//        -> '12:15:30'
class ObExprMakeTime : public ObFuncExprOperator
{
public:
  explicit  ObExprMakeTime(common::ObIAllocator &alloc);
  virtual ~ObExprMakeTime();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_maketime(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
  static int eval_batch_maketime(const ObExpr &expr, ObEvalCtx &ctx,
                                 const ObBitVector &skip, const int64_t batch_size);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  // Parse ObNumber object (arg: sec) and split it into integer/quotient part and remainder part.
  // Fetch the integer part.
  // E.G:
  //   sec = 5.1234565
  // =>
  //   quotient = 5
  static int fetch_second_quotient(int64_t &quotient, common::number::ObNumber &sec);
  // Parse ObNumber object sec and split it into integer/quotient part and remainder
  // part(with scale 6). Transform the remainder part to integer and do rounding.
  // E.G:
  //   sec = 5.1234565
  // =>
  //   remainder = 123457
  static int fetch_second_remainder(const int64_t quotient, int64_t &remainder,
                                      common::ObIAllocator &alloc, common::number::ObNumber &sec);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprMakeTime);
};

inline int ObExprMakeTime::calc_result_type3(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprResType &type2,
                                      ObExprResType &type3,
                                      common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;
  type.set_type(common::ObTimeType);
  // According to Mysql User Manual(from 5.x to 8.0), the return value is TIME type with scale 6.
  // https://dev.mysql.com/doc/refman/8.0/en/time.html 
  type.set_scale(common::MAX_SCALE_FOR_TEMPORAL);
  type1.set_calc_type(common::ObInt32Type);
  type2.set_calc_type(common::ObInt32Type);
  type3.set_calc_type(common::ObNumberType);
  return ret;
}
}
}

#endif /* OB_SQL_ENGINE_EXPR_MAKETIME_ */
