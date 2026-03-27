/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_GET_MYSQL_ROUTINE_PARAMETER_TYPE_STR_H
#define _OB_EXPR_GET_MYSQL_ROUTINE_PARAMETER_TYPE_STR_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprGetMySQLRoutineParameterTypeStr : public ObExprOperator
{
public:
  explicit  ObExprGetMySQLRoutineParameterTypeStr(common::ObIAllocator &alloc);
  virtual ~ObExprGetMySQLRoutineParameterTypeStr();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int get_mysql_routine_parameter_type_str(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprGetMySQLRoutineParameterTypeStr);
};
}
}
#endif