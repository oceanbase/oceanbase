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
 * This file contains implementation for rb_to_varbinary.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_to_varbinary.h"
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
ObExprRbToVarbinary::ObExprRbToVarbinary(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_TO_VARBINARY, N_RB_TO_VARBINARY, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbToVarbinary::~ObExprRbToVarbinary()
{
}


int ObExprRbToVarbinary::calc_result_typeN(ObExprResType &type,
                                           ObExprResType *types,
                                           int64_t param_num,
                                           common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (ob_is_null(types[0].get_type())) {
    // do nothing
  } else if (types[0].is_roaringbitmap()) {
    types[0].set_calc_collation_type(CS_TYPE_BINARY);
    types[0].set_calc_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("invalid roaringbitmap data type provided.", K(ret), K(types[0].get_type()), K(types[0].get_collation_type()));
  }
  if (OB_SUCC(ret) && param_num == 2 && !types[1].is_string_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("second argument should be string type", K(ret), K(types[1]));
  }
  if (OB_SUCC(ret)) {
    type.set_type(ObLongTextType);
    type.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type.set_collation_type(CS_TYPE_BINARY);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
  }
  return ret;
}

int ObExprRbToVarbinary::eval_rb_to_varbinary(const ObExpr &expr,
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
  ObString expected_format;
  ObString res_bin;

  if (OB_FAIL(arg->eval(ctx, datum))) {
    LOG_WARN("eval roaringbitmap args failed", K(ret));
  } else if (datum->is_null()) {
    is_rb_null = true;
  } else if (OB_FALSE_IT(rb_bin = datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                         tmp_allocator,
                         *datum,
                         arg->datum_meta_,
                         arg->obj_meta_.has_lob_header(),
                         rb_bin))) {
    LOG_WARN("fail to get real string data", K(ret), K(rb_bin));
  } else if (expr.arg_cnt_ == 1) {
    res_bin = rb_bin;
  } else {
    ObExpr *format_arg = expr.args_[1];
    ObDatum *format_datum = nullptr;
    if (OB_FAIL(format_arg->eval(ctx, format_datum))) {
      LOG_WARN("eval roaringbitmap args failed", K(ret));
    } else if (format_datum->is_null()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported expected format", K(ret), K(expected_format));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "expected format expect 'roaring' is");    } else if (OB_FALSE_IT(expected_format = format_datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                           tmp_allocator,
                           *format_datum,
                           format_arg->datum_meta_,
                           format_arg->obj_meta_.has_lob_header(),
                           expected_format))) {
      LOG_WARN("fail to get real string data", K(ret), K(expected_format));
    } else if (expected_format.case_compare("roaring") != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported expected format", K(ret), K(expected_format));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "expected format expect 'roaring' is");
    } else if (OB_FAIL(ObRbUtils::binary_format_convert(tmp_allocator, rb_bin, res_bin))) {
      LOG_WARN("failed to convert binary to roaring format", K(ret), K(rb_bin));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_rb_null) {
    res.set_null();
  } else {
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &res);
    if (OB_FAIL(str_result.init(res_bin.length()))) {
      LOG_WARN("fail to init result", K(ret), K(res_bin.length()));
    } else if (OB_FAIL(str_result.append(res_bin.ptr(), res_bin.length()))) {
      LOG_WARN("failed to append realdata", K(ret), K(res_bin));
    } else {
      str_result.set_result();
    }
  }
  return ret;
}

int ObExprRbToVarbinary::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbToVarbinary::eval_rb_to_varbinary;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase