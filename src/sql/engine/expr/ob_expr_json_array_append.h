/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_APPEND_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_APPEND_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonArrayAppend : public ObFuncExprOperator
{
public:
  explicit ObExprJsonArrayAppend(common::ObIAllocator &alloc);
  explicit ObExprJsonArrayAppend(common::ObIAllocator &alloc,
                                ObExprOperatorType type,
                                const char *name,
                                int32_t param_num,
                                ObValidForGeneratedColFlag valid_for_generated_col,
                                int32_t dimension);
  virtual ~ObExprJsonArrayAppend();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx) const override;

  static int eval_json_array_append(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonArrayAppend);
private:
  static const ObString name_;
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_APPEND_H_