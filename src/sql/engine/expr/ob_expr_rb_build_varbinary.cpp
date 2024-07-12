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
 * This file contains implementation for rb_build_varbinary.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_build_varbinary.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "lib/roaringbitmap/ob_rb_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprRbBuildVarbinary::ObExprRbBuildVarbinary(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_BUILD_VARBINARY, N_RB_BUILD_VARBINARY, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbBuildVarbinary::~ObExprRbBuildVarbinary()
{
}

int ObExprRbBuildVarbinary::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (type1.is_string_type()) {
    type1.set_calc_type(ObLongTextType);
    type1.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type1.set_collation_type(CS_TYPE_BINARY);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid type provided.", K(ret), K(type1.get_type()));
  }

  if (OB_SUCC(ret)) {
    type.set_roaringbitmap();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObRoaringBitmapType]).get_length());
  }
  return ret;
}

int ObExprRbBuildVarbinary::eval_rb_build_varbinary(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));
  ObExpr *rb_arg = expr.args_[0];
  bool is_null_result = false;
  bool is_rb_null = false;
  ObString rb_bin = nullptr;
  ObString res_rb_bin = nullptr;

  if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap_bin(ctx, rb_arg, rb_bin, is_rb_null))) {
    LOG_WARN("fail to get input roaringbitmap", K(ret));
  } else if (is_rb_null || rb_bin == nullptr) {
    is_null_result = true;
  } else if (OB_FAIL(ObRbUtils::build_binary(tmp_allocator, rb_bin, res_rb_bin))) {
    LOG_WARN("failed to build rb binary", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (OB_FAIL(ObRbExprHelper::pack_rb_res(expr, ctx, res, res_rb_bin))) {
    LOG_WARN("fail to pack roaringbitmap res", K(ret));
  }

  return ret;
}

int ObExprRbBuildVarbinary::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbBuildVarbinary::eval_rb_build_varbinary;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase