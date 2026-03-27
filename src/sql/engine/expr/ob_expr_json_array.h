/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonArray : public ObFuncExprOperator
{
public:
  enum {
    OPT_NULL_ID,
    OPT_TYPE_ID,
    OPT_STRICT_ID,
    OPT_MAX_ID,
  };

  explicit ObExprJsonArray(common::ObIAllocator &alloc);
  virtual ~ObExprJsonArray();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx) const override;
                          
  static int eval_json_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_ora_json_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonArray);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_ARRAY_H_