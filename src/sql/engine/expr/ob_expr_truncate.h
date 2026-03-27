/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_TRUNCATE_H_
#define _OB_EXPR_TRUNCATE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTruncate: public ObFuncExprOperator
{
public:
  explicit ObExprTruncate(common::ObIAllocator &alloc);
  virtual ~ObExprTruncate() {};

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;

  static int do_trunc_decimalint(
      const int16_t in_prec, const int16_t in_scale,
      const int16_t out_prec, const int64_t trunc_scale, const int64_t out_scale,
      const ObDatum &in_datum, ObDecimalIntBuilder &res_val);

  static int calc_trunc_decimalint(
      const int16_t in_prec, const int16_t in_scale,
      const int16_t out_prec, const int64_t trunc_scale, const int16_t out_scale,
      const ObDatum &in_datum, ObDatum &res_datum);

  static int set_trunc_val(common::ObObj &result,
                    common::number::ObNumber &nmb,
                    common::ObExprCtx &expr_ctx,
                    common::ObObjType res_type);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:

  DISALLOW_COPY_AND_ASSIGN(ObExprTruncate);
};
} // namespace sql
} // namespace oceanbase

#endif // _OB_EXPR_TRUNCATE_H_
