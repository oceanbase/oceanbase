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
 * This file contains implementation for st_astext.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_ASTEXT_
#define OCEANBASE_SQL_OB_EXPR_ST_ASTEXT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSTAsText : public ObFuncExprOperator
{
public:
  explicit ObExprSTAsText(common::ObIAllocator &alloc,
                          ObExprOperatorType type,
                          const char *name,
                          int32_t param_num,
                          int32_t dimension);
  explicit ObExprSTAsText(common::ObIAllocator &alloc);
  virtual ~ObExprSTAsText() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_st_astext(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &res);
  static int eval_st_astext_common(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   ObDatum &res,
                                   const char *func_name);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTAsText);
};

class ObExprSTAsWkt : public ObExprSTAsText
{
public:
  explicit ObExprSTAsWkt(common::ObIAllocator &alloc)
    : ObExprSTAsText(alloc,
                     T_FUN_SYS_ST_ASWKT,
                     N_ST_ASWKT,
                     MORE_THAN_ZERO,
                     NOT_ROW_DIMENSION) {}
  virtual ~ObExprSTAsWkt() {}
  static int eval_st_astext(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObDatum &res);
  int cg_expr(ObExprCGCtx &expr_cg_ctx,
              const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTAsWkt);
};


} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_ASTEXT_