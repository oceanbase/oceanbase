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
 * This file contains implementation for rb_and, rb_or, rb_xor, rb_andnot,
 * rb_and_null2empty,rb_or_null2empty, rb_andnot_null2empty.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_CALC_
#define OCEANBASE_SQL_OB_EXPR_RB_CALC_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "lib/roaringbitmap/ob_rb_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprRbCalc : public ObFuncExprOperator
{
public:
  explicit ObExprRbCalc(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name);
  virtual ~ObExprRbCalc();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_rb_calc(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          ObDatum &res,
                          ObRbOperation op,
                          bool is_null2empty = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbCalc);
};

class ObExprRbAnd : public ObExprRbCalc
{
public:
  explicit ObExprRbAnd(common::ObIAllocator &alloc);
  virtual ~ObExprRbAnd();
  static int eval_rb_and(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbAnd);
};

class ObExprRbOr : public ObExprRbCalc
{
public:
  explicit ObExprRbOr(common::ObIAllocator &alloc);
  virtual ~ObExprRbOr();
  static int eval_rb_or(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbOr);
};

class ObExprRbXor : public ObExprRbCalc
{
public:
  explicit ObExprRbXor(common::ObIAllocator &alloc);
  virtual ~ObExprRbXor();
  static int eval_rb_xor(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbXor);
};

class ObExprRbAndnot : public ObExprRbCalc
{
public:
  explicit ObExprRbAndnot(common::ObIAllocator &alloc);
  virtual ~ObExprRbAndnot();
  static int eval_rb_andnot(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbAndnot);
};

class ObExprRbAndNull2empty : public ObExprRbCalc
{
public:
  explicit ObExprRbAndNull2empty(common::ObIAllocator &alloc);
  virtual ~ObExprRbAndNull2empty();
  static int eval_rb_and_null2empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbAndNull2empty);
};

class ObExprRbOrNull2empty : public ObExprRbCalc
{
public:
  explicit ObExprRbOrNull2empty(common::ObIAllocator &alloc);
  virtual ~ObExprRbOrNull2empty();
  static int eval_rb_or_null2empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbOrNull2empty);
};

class ObExprRbAndnotNull2empty : public ObExprRbCalc
{
public:
  explicit ObExprRbAndnotNull2empty(common::ObIAllocator &alloc);
  virtual ~ObExprRbAndnotNull2empty();
  static int eval_rb_andnot_null2empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbAndnotNull2empty);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_CALC_