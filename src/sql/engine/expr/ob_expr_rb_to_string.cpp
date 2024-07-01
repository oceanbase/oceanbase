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
 * This file contains implementation for rb_to_string.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_to_string.h"
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
ObExprRbToString::ObExprRbToString(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_TO_STRING, N_RB_TO_STRING, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbToString::~ObExprRbToString()
{
}


int ObExprRbToString::calc_result_type1(ObExprResType &type,
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
    type.set_type(ObLongTextType);
    type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
  }
  return ret;
}

int ObExprRbToString::eval_rb_to_string(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));
  ObExpr *arg = expr.args_[0];
  bool is_rb_null = false;
  ObDatum *datum = nullptr;
  ObString rb_bin;
  ObString rb_str;

  if (OB_FAIL(arg->eval(ctx, datum))) {
    LOG_WARN("eval roaringbitmap args failed", K(ret));
  } else if (datum->is_null()) {
    is_rb_null = true;
  } else if (OB_FALSE_IT(rb_bin = datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator,
                                                               *datum,
                                                               arg->datum_meta_,
                                                               arg->obj_meta_.has_lob_header(),
                                                               rb_bin))) {
    LOG_WARN("failed to get real string data", K(ret), K(rb_bin));
  }

  if (OB_FAIL(ret)) {
  } else if (is_rb_null) {
    res.set_null();
  } else if (OB_FAIL(ObRbUtils::rb_to_string(tmp_allocator, rb_bin, rb_str))) {
    LOG_WARN("failed to print roaringbitmap to string", K(ret));
  } else {
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
    if (OB_FAIL(str_result.init(rb_str.length()))) {
      LOG_WARN("failed to init result", K(ret), K(rb_str.length()));
    } else if (OB_FAIL(str_result.append(rb_str.ptr(), rb_str.length()))) {
      LOG_WARN("failed to append realdata", K(ret), K(rb_str));
    } else {
      str_result.set_result();
    }
  }
  return ret;
}

int ObExprRbToString::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbToString::eval_rb_to_string;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase