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
 * This file contains implementation for rb_build_empty.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_build_empty.h"
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
ObExprRbBuildEmpty::ObExprRbBuildEmpty(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_BUILD_EMPTY, N_RB_BUILD_EMPTY, 0, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbBuildEmpty::~ObExprRbBuildEmpty()
{
}


int ObExprRbBuildEmpty::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_roaringbitmap();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObRoaringBitmapType]).get_length());
  return OB_SUCCESS;
}

int ObExprRbBuildEmpty::eval_rb_build_empty(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &res)
{
  int ret = OB_SUCCESS;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));

  ObString rb_bin;
  ObRoaringBitmap *rb_empty;
  if (OB_ISNULL(rb_empty = OB_NEWx(ObRoaringBitmap, &tmp_allocator, (&tmp_allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
  } else if (OB_FAIL(ObRbUtils::rb_serialize(tmp_allocator, rb_bin, rb_empty))) {
    LOG_WARN("failed to serialize empty roaringbitmap", K(ret));
  } else if (OB_FAIL(ObRbExprHelper::pack_rb_res(expr, ctx, res, rb_bin))) {
    LOG_WARN("fail to pack roaringbitmap res", K(ret));
  }
  ObRbUtils::rb_destroy(rb_empty);
  return ret;
}

int ObExprRbBuildEmpty::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbBuildEmpty::eval_rb_build_empty;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase