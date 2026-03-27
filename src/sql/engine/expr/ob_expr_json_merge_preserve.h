/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file is for define of func json_merge_preserve
 */


#ifndef OCEANBASE_SQL_OB_EXPR_JSON_MERGE_PRESERVE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_MERGE_PRESERVE_H_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonMergePreserve : public ObFuncExprOperator
{
public:
  explicit ObExprJsonMergePreserve(common::ObIAllocator &alloc);
  explicit ObExprJsonMergePreserve(common::ObIAllocator &alloc, ObExprOperatorType type, 
                                   const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprJsonMergePreserve();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num, 
                                common::ObExprTypeCtx& type_ctx) const override;

  static int eval_json_merge_preserve(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonMergePreserve);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_MERGE_PRESERVE_H_