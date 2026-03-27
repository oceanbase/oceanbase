/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_SPIV_VALUE_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_SPIV_VALUE_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSpivValue : public ObFuncExprOperator
{
public:
  explicit ObExprSpivValue(common::ObIAllocator &alloc);
  virtual ~ObExprSpivValue() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_resultN(common::ObObj &result,
                          const common::ObObj *objs_array,
                          int64_t param_num,
                          common::ObExprCtx &expr_ctx) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN;}
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx,
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr) const override;
  static int generate_spiv_value(
          const ObExpr &raw_ctx,
          ObEvalCtx &eval_ctx,
          ObDatum &expr_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprSpivValue);
};
}
}
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_SPIV_VALUE_H_ */

