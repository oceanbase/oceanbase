/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OB_EXPR_PERIOD_DIFF_H_
#define _OCEANBASE_SQL_OB_EXPR_PERIOD_DIFF_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase
{
namespace sql
{
class ObExprPeriodDiff : public ObFuncExprOperator
{
public:
  explicit  ObExprPeriodDiff(common::ObIAllocator &alloc);
  virtual ~ObExprPeriodDiff();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &left,
                                ObExprResType &right,
                                common::ObExprTypeCtx &type_ctx) const;
  template <typename T>
  static int calc(T &result, const T &left, const T &right);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_perioddiff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprPeriodDiff);
};

inline int ObExprPeriodDiff::calc_result_type2(ObExprResType &type,
                                               ObExprResType &left,
                                               ObExprResType &right,
                                               common::ObExprTypeCtx &type_ctx) const
{
  type.set_int();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  left.set_calc_type(common::ObUInt64Type);
  right.set_calc_type(common::ObUInt64Type);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  return common::OB_SUCCESS;
}

class ObExprPeriodAdd : public ObFuncExprOperator
{
public:
  explicit  ObExprPeriodAdd(common::ObIAllocator &alloc);
  virtual ~ObExprPeriodAdd();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &left,
                                ObExprResType &right,
                                common::ObExprTypeCtx &type_ctx) const;
  template <typename T>
  static int calc(T &result, const T &left, const T &right);
  virtual common::ObCastMode get_cast_mode() const { return CM_STRING_INTEGER_TRUNC;}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_periodadd(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprPeriodAdd);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_PERIOD_DIFF_H_
