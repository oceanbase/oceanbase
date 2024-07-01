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
 * This file contains implementation for rb_is_empty.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_is_empty.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "lib/roaringbitmap/ob_rb_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprRbIsEmpty::ObExprRbIsEmpty(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_IS_EMPTY, N_RB_IS_EMPTY, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbIsEmpty::~ObExprRbIsEmpty()
{
}

int ObExprRbIsEmpty::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (!(type1.is_roaringbitmap() || type1.is_hex_string())) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid roaringbitmap data type provided.", K(ret), K(type1.get_type()), K(type1.get_collation_type()));
  }
  if (OB_SUCC(ret)) {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  return ret;
}

int ObExprRbIsEmpty::eval_rb_is_empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));
  ObExpr *rb_arg = expr.args_[0];
  bool is_rb_null = false;
  ObRoaringBitmap *rb = nullptr;
  ObDatum *rb_datum = nullptr;
  if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap(ctx, rb_arg, rb, is_rb_null))) {
    LOG_WARN("fail to get input roaringbitmap", K(ret));
  } else if (is_rb_null) {
    res.set_null();
  } else {
    uint64_t cardinality = rb->get_cardinality();
    if (cardinality == 0) {
      res.set_bool(true);
    } else {
      res.set_bool(false);
    }
  }
  ObRbUtils::rb_destroy(rb);
  return ret;
}

int ObExprRbIsEmpty::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbIsEmpty::eval_rb_is_empty;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase