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
 * This file contains implementation for rb_select.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_select.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"
#include "lib/roaringbitmap/ob_rb_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprRbSelect::ObExprRbSelect(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RB_SELECT, N_RB_SELECT, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbSelect::~ObExprRbSelect()
{
}

int ObExprRbSelect::calc_result_typeN(ObExprResType &type,
                                   ObExprResType *types,
                                   int64_t param_num,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 6)) {
    const ObString name(N_RB_SELECT);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    if (ob_is_null(types[i].get_type())) {
      // do nothing
    } else if (i == 0) { // rb input
      if (!(types[i].is_roaringbitmap() || types[i].is_hex_string())) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid roaringbitmap data type provided.", K(ret), K(types[i].get_type()), K( types[i].get_collation_type()));
      }
    } else if (i == 3) { // bool param
      if (!types[i].is_tinyint()) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid roaringbitmap data type provided.", K(ret), K(types[i].get_type()));
      }
    } else { // unsigned bigint params
      if (ob_is_integer_type(types[i].get_type()) && !types[i].is_tinyint()) {
        types[i].set_calc_type(ObUInt64Type);
      } else if (ob_is_string_type(types[i].get_type()) && !types[i].is_hex_string()) {
        // types[i].set_calc_collation_utf8();
        types[i].set_calc_type(ObUInt64Type);
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid data type provided.", K(ret), K( types[i].get_type()), K( types[i].get_collation_type()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObCastMode cast_mode = type_ctx.get_cast_mode();
    cast_mode &= ~CM_WARN_ON_FAIL; // make cast return error when fail
    cast_mode |= CM_STRING_INTEGER_TRUNC; // make cast check range when string to int
    type_ctx.set_cast_mode(cast_mode); // cast mode only do work in new sql engine cast frame.
    type.set_roaringbitmap();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObRoaringBitmapType]).get_length());
  }
  return ret;
}

int ObExprRbSelect::eval_rb_select(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &res)
{
  int ret = OB_SUCCESS;

  ObRoaringBitmap *rb = NULL;
  ObRoaringBitmap *res_rb = NULL;
  ObString res_rb_bin;
  uint64_t limit = 0;
  uint64_t offset = 0;
  bool reverse = false;
  uint64_t range_start = 0;
  uint64_t range_end = UINT64_MAX;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObRbExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "ROARINGBITMAP"));

  bool is_null_result = false;
  bool is_empty_bitmap = false;
  int num_args = expr.arg_cnt_;
  // eval input params
  for (int64_t i = 1; OB_SUCC(ret) && !is_null_result && i < num_args; ++i) {
    ObDatum *datum = NULL;
    ObExpr *arg = expr.args_[i];
    ObObjType type = arg->datum_meta_.type_;
    if (ob_is_null(type)) {
      is_null_result = true;
    } else if (OB_FAIL(arg->eval(ctx, datum))) {
      LOG_WARN("fail to eval arg", K(ret), K(arg), K(i));
    } else if (datum->is_null()) {
      is_null_result = true;
    }
    if (OB_FAIL(ret) || is_null_result) {
    } else if (i == 1) {
      limit = datum->get_uint();
    } else if (i == 2) {
      offset = datum->get_uint();
    } else if (i == 3) {
      reverse = datum->get_tinyint() == 0 ? false : true;
    } else if (i == 4) {
      range_start = datum->get_uint();
    } else if (i == 5) {
      range_end = datum->get_uint();
      if (range_end > 0) {
        range_end = range_end - 1;
      } else {
        is_empty_bitmap = true;
      }
    }
  }

  if (OB_FAIL(ret) || is_null_result || is_empty_bitmap) {
  } else {
    ObExpr *rb_arg = expr.args_[0];
    if (OB_FAIL(ObRbExprHelper::get_input_roaringbitmap(ctx, tmp_allocator, rb_arg, rb, is_null_result))) {
      LOG_WARN("failed to get input roaringbitmap", K(ret));
    } else if (is_null_result) {
      // do nonthing
    } else if (limit == 0 || range_start > range_end) {
      is_empty_bitmap =  true;
    } else if (OB_ISNULL(res_rb = OB_NEWx(ObRoaringBitmap, &tmp_allocator, (&tmp_allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create alloc memory to result roaringbitmap", K(ret));
    } else if (OB_FAIL(rb->subset(res_rb, limit, offset, reverse, range_start, range_end))) {
      LOG_WARN("failed to subset roaringbitmap", K(ret));
    } else if (OB_FAIL(ObRbUtils::rb_serialize(tmp_allocator, res_rb_bin, res_rb))) {
      LOG_WARN("failed to serialize roaringbitmap", K(ret));
    }
  }
  ObRbUtils::rb_destroy(rb);
  ObRbUtils::rb_destroy(res_rb);

  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (is_empty_bitmap && OB_FAIL(ObRbUtils::build_empty_binary(tmp_allocator, res_rb_bin))) {
    LOG_WARN("fail to build empty rb binary", K(ret));
  } else if (OB_FAIL(ObRbExprHelper::pack_rb_res(expr, ctx, res, res_rb_bin))) {
    LOG_WARN("fail to pack roaringbitmap res", K(ret));
  }

  return ret;
}

int ObExprRbSelect::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbSelect::eval_rb_select;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase