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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_calc.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprRbCalc::ObExprRbCalc(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name)
    : ObFuncExprOperator(alloc, type, name, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprRbCalc::~ObExprRbCalc()
{
}

int ObExprRbCalc::calc_result_type2(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprResType &type2,
                                         common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (!(type1.is_roaringbitmap() ||  type1.is_hex_string())) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid left roaringbitmap data type provided.", K(ret), K(type1.get_type()), K(type1.get_collation_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (ob_is_null(type2.get_type())) {
    // do nothing
  } else if (!(type2.is_roaringbitmap() || type2.is_hex_string())) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid right roaringbitmap data type provided.", K(ret), K(type2.get_type()), K(type2.get_collation_type()));
  }
  if (OB_SUCC(ret)) {
    type.set_roaringbitmap();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObRoaringBitmapType]).get_length());
  }
  return ret;
}

int ObExprRbCalc::eval_rb_calc(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObRbOperation op, bool is_null2empty)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));
  ObExpr *rb1_arg = expr.args_[0];
  ObExpr *rb2_arg = expr.args_[1];
  bool is_rb1_null = false;
  bool is_rb2_null = false;
  bool is_res_null = false;
  ObRoaringBitmap *rb1 = nullptr;
  ObRoaringBitmap *rb2 = nullptr;
  ObString rb_res;
  if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap(ctx, rb1_arg, rb1, is_rb1_null))) {
    LOG_WARN("failed to get left input roaringbitmap", K(ret));
  } else if (is_rb1_null && !is_null2empty) {
    is_res_null = true;
  } else if (is_rb1_null && is_null2empty
            && OB_ISNULL(rb1 = OB_NEWx(ObRoaringBitmap, &tmp_allocator, (&tmp_allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
  } else if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap(ctx, rb2_arg, rb2, is_rb2_null))) {
    LOG_WARN("failed to get right input roaringbitmap", K(ret));
  } else if (is_rb2_null  && !is_null2empty) {
    is_res_null = true;
  } else if (is_rb2_null && is_null2empty
            && OB_ISNULL(rb2 = OB_NEWx(ObRoaringBitmap, &tmp_allocator, (&tmp_allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
  } else if (OB_FAIL(rb1->value_calc(rb2, op))) {
    LOG_WARN("failed to calcutlate roaringbitmap value_and", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (is_res_null) {
    res.set_null();
  } else if (OB_FAIL(ObRbUtils::rb_serialize(tmp_allocator, rb_res, rb1))) {
    LOG_WARN("failed to serialize roaringbitmap", K(ret));
  } else if (OB_FAIL(ObRbExprHelper::pack_rb_res(expr, ctx, res, rb_res))) {
    LOG_WARN("fail to pack roaringbitmap res", K(ret));
  }
  ObRbUtils::rb_destroy(rb1);
  ObRbUtils::rb_destroy(rb2);
  return ret;
}

ObExprRbAnd::ObExprRbAnd(common::ObIAllocator &alloc)
    : ObExprRbCalc(alloc, T_FUN_SYS_RB_AND, N_RB_AND)
{
}
ObExprRbAnd::~ObExprRbAnd()
{
}
int ObExprRbAnd::eval_rb_and(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc(expr, ctx, res, ObRbOperation::AND))) {
    LOG_WARN("failed to eval roaringbitmap and calculation", K(ret));
  }
  return ret;
}
int ObExprRbAnd::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAnd::eval_rb_and;
  return OB_SUCCESS;
}

ObExprRbOr::ObExprRbOr(common::ObIAllocator &alloc)
    : ObExprRbCalc(alloc, T_FUN_SYS_RB_OR, N_RB_OR)
{
}
ObExprRbOr::~ObExprRbOr()
{
}
int ObExprRbOr::eval_rb_or(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc(expr, ctx, res, ObRbOperation::OR))) {
    LOG_WARN("failed to eval roaringbitmap or calculation", K(ret));
  }
  return ret;
}
int ObExprRbOr::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbOr::eval_rb_or;
  return OB_SUCCESS;
}

ObExprRbXor::ObExprRbXor(common::ObIAllocator &alloc)
    : ObExprRbCalc(alloc, T_FUN_SYS_RB_XOR, N_RB_XOR)
{
}
ObExprRbXor::~ObExprRbXor()
{
}
int ObExprRbXor::eval_rb_xor(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc(expr, ctx, res, ObRbOperation::XOR))) {
    LOG_WARN("failed to eval roaringbitmap xor calculation", K(ret));
  }
  return ret;
}
int ObExprRbXor::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbXor::eval_rb_xor;
  return OB_SUCCESS;
}

ObExprRbAndnot::ObExprRbAndnot(common::ObIAllocator &alloc)
    : ObExprRbCalc(alloc, T_FUN_SYS_RB_ANDNOT, N_RB_ANDNOT)
{
}
ObExprRbAndnot::~ObExprRbAndnot()
{
}
int ObExprRbAndnot::eval_rb_andnot(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc(expr, ctx, res, ObRbOperation::ANDNOT))) {
    LOG_WARN("failed to eval roaringbitmap andnot calculation", K(ret));
  }
  return ret;
}
int ObExprRbAndnot::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAndnot::eval_rb_andnot;
  return OB_SUCCESS;
}

ObExprRbAndNull2empty::ObExprRbAndNull2empty(common::ObIAllocator &alloc)
    : ObExprRbCalc(alloc, T_FUN_SYS_RB_AND_NULL2EMPTY, N_RB_AND_NULL2EMPTY)
{
}
ObExprRbAndNull2empty::~ObExprRbAndNull2empty()
{
}
int ObExprRbAndNull2empty::eval_rb_and_null2empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc(expr, ctx, res, ObRbOperation::AND, true))) {
    LOG_WARN("failed to eval roaringbitmap and calculation", K(ret));
  }
  return ret;
}
int ObExprRbAndNull2empty::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAndNull2empty::eval_rb_and_null2empty;
  return OB_SUCCESS;
}

ObExprRbOrNull2empty::ObExprRbOrNull2empty(common::ObIAllocator &alloc)
    : ObExprRbCalc(alloc, T_FUN_SYS_RB_OR_NULL2EMPTY, N_RB_OR_NULL2EMPTY)
{
}
ObExprRbOrNull2empty::~ObExprRbOrNull2empty()
{
}
int ObExprRbOrNull2empty::eval_rb_or_null2empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc(expr, ctx, res, ObRbOperation::OR, true))) {
    LOG_WARN("failed to eval roaringbitmap or calculation", K(ret));
  }
  return ret;
}
int ObExprRbOrNull2empty::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbOrNull2empty::eval_rb_or_null2empty;
  return OB_SUCCESS;
}

ObExprRbAndnotNull2empty::ObExprRbAndnotNull2empty(common::ObIAllocator &alloc)
    : ObExprRbCalc(alloc, T_FUN_SYS_RB_ANDNOT_NULL2EMPTY, N_RB_ANDNOT_NULL2EMPTY)
{
}
ObExprRbAndnotNull2empty::~ObExprRbAndnotNull2empty()
{
}
int ObExprRbAndnotNull2empty::eval_rb_andnot_null2empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc(expr, ctx, res, ObRbOperation::ANDNOT, true))) {
    LOG_WARN("failed to eval roaringbitmap andnot calculation", K(ret));
  }
  return ret;
}
int ObExprRbAndnotNull2empty::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAndnotNull2empty::eval_rb_andnot_null2empty;
  return OB_SUCCESS;
}






} // namespace sql
} // namespace oceanbase