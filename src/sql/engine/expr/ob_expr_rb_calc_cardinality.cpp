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
 * This file contains implementation for rb_and_cardinality, rb_or_cardinality, rb_xor_cardinality,
 * rb_andnot_cardinality, rb_and_null2empty_cardinality, rb_or_null2empty_cardinality,
 * rb_andnot_null2empty_cardinality.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_calc_cardinality.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprRbCalcCardinality::ObExprRbCalcCardinality(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name)
    : ObFuncExprOperator(alloc, type, name, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprRbCalcCardinality::~ObExprRbCalcCardinality()
{
}
int ObExprRbCalcCardinality::calc_result_type2(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprResType &type2,
                                         common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (!(type1.is_roaringbitmap() || type1.is_hex_string())) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid left roaringbitmap data type provided.", K(ret), K(type1.get_type()), K(type1.get_collation_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (ob_is_null(type2.get_type())) {
    // do nothing
  } else if (!(type2.is_roaringbitmap() ||  type2.is_hex_string())) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid right roaringbitmap data type provided.", K(ret), K(type2.get_type()), K(type2.get_collation_type()));
  }
  if (OB_SUCC(ret)) {
    type.set_uint64();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt64Type].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt64Type].precision_);
  }
  return ret;
}
int ObExprRbCalcCardinality::eval_rb_calc_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, ObRbOperation op, bool is_null2empty)
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
  uint64_t cardinality = 0;
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
  }

  if (OB_FAIL(ret)) {
  } else if (is_res_null) {
    res.set_null();
  } else {
    ObRbUtils::calc_cardinality(rb1, rb2, cardinality, op);
    res.set_uint(cardinality);
  }
  ObRbUtils::rb_destroy(rb1);
  ObRbUtils::rb_destroy(rb2);
  return ret;
}

ObExprRbAndCardinality::ObExprRbAndCardinality(common::ObIAllocator &alloc)
    : ObExprRbCalcCardinality(alloc, T_FUN_SYS_RB_AND_CARDINALITY, N_RB_AND_CARDINALITY)
{
}
ObExprRbAndCardinality::~ObExprRbAndCardinality()
{
}
int ObExprRbAndCardinality::eval_rb_and_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc_cardinality(expr, ctx, res, ObRbOperation::AND))) {
    LOG_WARN("failed to eval roaringbitmap and cardinality calculation", K(ret));
  }
  return ret;
}
int ObExprRbAndCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAndCardinality::eval_rb_and_cardinality;
  return OB_SUCCESS;
}

ObExprRbOrCardinality::ObExprRbOrCardinality(common::ObIAllocator &alloc)
    : ObExprRbCalcCardinality(alloc, T_FUN_SYS_RB_OR_CARDINALITY, N_RB_OR_CARDINALITY)
{
}
ObExprRbOrCardinality::~ObExprRbOrCardinality()
{
}
int ObExprRbOrCardinality::eval_rb_or_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc_cardinality(expr, ctx, res, ObRbOperation::OR))) {
    LOG_WARN("failed to eval roaringbitmap or cardinality calculation", K(ret));
  }
  return ret;
}
int ObExprRbOrCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbOrCardinality::eval_rb_or_cardinality;
  return OB_SUCCESS;
}

ObExprRbXorCardinality::ObExprRbXorCardinality(common::ObIAllocator &alloc)
    : ObExprRbCalcCardinality(alloc, T_FUN_SYS_RB_XOR_CARDINALITY, N_RB_XOR_CARDINALITY)
{
}
ObExprRbXorCardinality::~ObExprRbXorCardinality()
{
}
int ObExprRbXorCardinality::eval_rb_xor_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc_cardinality(expr, ctx, res, ObRbOperation::XOR))) {
    LOG_WARN("failed to eval roaringbitmap xor cardinality calculation", K(ret));
  }
  return ret;
}
int ObExprRbXorCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbXorCardinality::eval_rb_xor_cardinality;
  return OB_SUCCESS;
}

ObExprRbAndnotCardinality::ObExprRbAndnotCardinality(common::ObIAllocator &alloc)
    : ObExprRbCalcCardinality(alloc, T_FUN_SYS_RB_ANDNOT_CARDINALITY, N_RB_ANDNOT_CARDINALITY)
{
}
ObExprRbAndnotCardinality::~ObExprRbAndnotCardinality()
{
}
int ObExprRbAndnotCardinality::eval_rb_andnot_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc_cardinality(expr, ctx, res, ObRbOperation::ANDNOT))) {
    LOG_WARN("failed to eval roaringbitmap andnot cardinality calculation", K(ret));
  }
  return ret;
}
int ObExprRbAndnotCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAndnotCardinality::eval_rb_andnot_cardinality;
  return OB_SUCCESS;
}

ObExprRbAndNull2emptyCardinality::ObExprRbAndNull2emptyCardinality(common::ObIAllocator &alloc)
    : ObExprRbCalcCardinality(alloc, T_FUN_SYS_RB_AND_NULL2EMPTY_CARDINALITY, N_RB_AND_NULL2EMPTY_CARDINALITY)
{
}
ObExprRbAndNull2emptyCardinality::~ObExprRbAndNull2emptyCardinality()
{
}
int ObExprRbAndNull2emptyCardinality::eval_rb_and_null2empty_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc_cardinality(expr, ctx, res, ObRbOperation::AND, true))) {
    LOG_WARN("failed to eval roaringbitmap and cardinality calculation", K(ret));
  }
  return ret;
}
int ObExprRbAndNull2emptyCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAndNull2emptyCardinality::eval_rb_and_null2empty_cardinality;
  return OB_SUCCESS;
}

ObExprRbOrNull2emptyCardinality::ObExprRbOrNull2emptyCardinality(common::ObIAllocator &alloc)
    : ObExprRbCalcCardinality(alloc, T_FUN_SYS_RB_OR_NULL2EMPTY_CARDINALITY, N_RB_OR_NULL2EMPTY_CARDINALITY)
{
}
ObExprRbOrNull2emptyCardinality::~ObExprRbOrNull2emptyCardinality()
{
}
int ObExprRbOrNull2emptyCardinality::eval_rb_or_null2empty_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc_cardinality(expr, ctx, res, ObRbOperation::OR, true))) {
    LOG_WARN("failed to eval roaringbitmap or cardinality calculation", K(ret));
  }
  return ret;
}
int ObExprRbOrNull2emptyCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbOrNull2emptyCardinality::eval_rb_or_null2empty_cardinality;
  return OB_SUCCESS;
}

ObExprRbAndnotNull2emptyCardinality::ObExprRbAndnotNull2emptyCardinality(common::ObIAllocator &alloc)
    : ObExprRbCalcCardinality(alloc, T_FUN_SYS_RB_ANDNOT_NULL2EMPTY_CARDINALITY, N_RB_ANDNOT_NULL2EMPTY_CARDINALITY)
{
}
ObExprRbAndnotNull2emptyCardinality::~ObExprRbAndnotNull2emptyCardinality()
{
}
int ObExprRbAndnotNull2emptyCardinality::eval_rb_andnot_null2empty_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(eval_rb_calc_cardinality(expr, ctx, res, ObRbOperation::ANDNOT, true))) {
    LOG_WARN("failed to eval roaringbitmap andnot cardinality calculation", K(ret));
  }
  return ret;
}
int ObExprRbAndnotNull2emptyCardinality::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbAndnotNull2emptyCardinality::eval_rb_andnot_null2empty_cardinality;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase