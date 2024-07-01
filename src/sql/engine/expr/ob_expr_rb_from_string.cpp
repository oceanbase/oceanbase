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
 * This file contains implementation for rb_from_string.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_from_string.h"
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
ObExprRbFromString::ObExprRbFromString(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_FROM_STRING, N_RB_FROM_STRING, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbFromString::~ObExprRbFromString()
{
}

int ObExprRbFromString::calc_result_type1(ObExprResType &type,
                                          ObExprResType &type1,
                                          common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (!ob_is_string_type(type1.get_type())
              || ObCharset::is_cs_nonascii(type1.get_collation_type())
              || ObHexStringType == type1.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid type provided.", K(ret), K(type1.get_type()));
  }

  if (OB_SUCC(ret)) {
    type.set_roaringbitmap();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObRoaringBitmapType]).get_length());
  }
  return ret;
}

int ObExprRbFromString::eval_rb_from_string(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));
  ObDatum *datum = NULL;
  bool is_null_result = false;
  ObString rb_str;
  ObString rb_bin;
  ObRoaringBitmap *rb = NULL;

  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("failed to eval argument", K(ret));
  } else if (datum->is_null()) {
    is_null_result = true;
  } else {
    rb_str = datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator,
                                                          *datum,
                                                          expr.args_[0]->datum_meta_,
                                                          expr.args_[0]->obj_meta_.has_lob_header(),
                                                          rb_str))) {
      LOG_WARN("failed to get real string data", K(ret), K(rb_str));
    } else if (OB_FAIL(ObRbUtils::rb_from_string(tmp_allocator, rb_str, rb))) {
      LOG_WARN("failed to build roaringbitmap from string", K(ret), K(rb_str));
    } else if (OB_FAIL(ObRbUtils::rb_serialize(tmp_allocator, rb_bin, rb))) {
      LOG_WARN("failed to serialize roaringbitmap", K(ret));
    }
    ObRbUtils::rb_destroy(rb);
  }
  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (OB_FAIL(ObRbExprHelper::pack_rb_res(expr, ctx, res, rb_bin))) {
    LOG_WARN("fail to pack roaringbitmap res", K(ret));
  }

  return ret;
}

int ObExprRbFromString::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbFromString::eval_rb_from_string;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase