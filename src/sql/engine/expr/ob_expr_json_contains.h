/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for json_contains.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_CONTAINS_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_CONTAINS_H_

#include "lib/json_type/ob_json_tree.h"
#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonContains : public ObFuncExprOperator
{
public:
  explicit ObExprJsonContains(common::ObIAllocator &alloc);
  virtual ~ObExprJsonContains();
  virtual int calc_result_typeN(ObExprResType& type,
                              ObExprResType* types,
                              int64_t param_num, 
                              common::ObExprTypeCtx& type_ctx)
                              const override;
  static int eval_json_contains(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonContains);
  static int json_contains(ObIJsonBase* json_target, ObIJsonBase* json_candidate, bool *result);
  static int json_contains_object(ObIJsonBase* json_target, ObIJsonBase* json_candidate, bool *result);
  static int json_contains_array(ObIJsonBase* json_target, ObIJsonBase* json_candidate, bool *result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_CONTAINS_H_