/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXPR_CALC_UROWID_H
#define OB_EXPR_CALC_UROWID_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCalcURowID: public ObExprOperator
{
public:
  explicit ObExprCalcURowID(common::ObIAllocator &alloc);
  virtual ~ObExprCalcURowID();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_statck,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int calc_urowid(const ObExpr &rt_expr,
                         ObEvalCtx &eva_ctx,
                         ObDatum &expr_datum);
private:
  static bool all_pk_is_null(int64_t version, ObIArray<ObObj> &pk_vals);
  DISALLOW_COPY_AND_ASSIGN(ObExprCalcURowID);
};
} // end namespace
} // end namespace oceanbase

#endif // !OB_EXPR_CALC_UROWID_H
