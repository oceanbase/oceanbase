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
 * This file contains implementation for json_depth.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_func_helper.h"
#include "ob_expr_json_depth.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonDepth::ObExprJsonDepth(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_DEPTH, N_JSON_DEPTH, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonDepth::~ObExprJsonDepth()
{
}

int ObExprJsonDepth::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx); 
  INIT_SUCC(ret);
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);

  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_DEPTH))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  }
  
  return ret;
}

int ObExprJsonDepth::eval_json_depth(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  // get json doc
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[0];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObIJsonBase *json_doc = NULL;
  bool is_null_result = false;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type == ObNullType || json_datum->is_null()) {
    is_null_result = true;
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0,
                                                    json_doc, is_null_result))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else {
    // do nothing
  }

  // set result
  if (OB_FAIL(ret)) {
    LOG_WARN("json_depth failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else {
    res.set_int32(json_doc->depth());
  }

  return ret;
}

int ObExprJsonDepth::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_depth;
  return OB_SUCCESS;
}


}
}