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
 */

#define USING_LOG_PREFIX SQL_ENG
#include <string.h>
#include "sql/engine/expr/ob_expr_empty_lob.h"
#include "sql/session/ob_sql_session_info.h"
#include "objit/common/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

// empty_clob
ObExprEmptyClob::ObExprEmptyClob(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_EMPTY_CLOB, N_EMPTY_CLOB, 0, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprEmptyClob::~ObExprEmptyClob()
{
}

int ObExprEmptyClob::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
  type.set_clob();
  type.set_length(0);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_collation_type(nls_param.nls_collation_);
  return OB_SUCCESS;
}

int ObExprEmptyClob::eval_empty_clob(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (expr.obj_meta_.has_lob_header()) {
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
    if (OB_FAIL(str_result.init(0))) {
      LOG_WARN("init lob result failed");
    } else {
      str_result.set_result();
    }
  } else {
    expr_datum.set_string(NULL, 0);
  }
  return ret;
}

int ObExprEmptyClob::cg_expr(
    ObExprCGCtx &op_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprEmptyClob::eval_empty_clob;
  return OB_SUCCESS;
}


// empty_blob
ObExprEmptyBlob::ObExprEmptyBlob(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_EMPTY_BLOB, N_EMPTY_BLOB, 0, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprEmptyBlob::~ObExprEmptyBlob()
{
}

int ObExprEmptyBlob::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_blob();
  type.set_length(0);
  type.set_collation_type(CS_TYPE_BINARY);
  return OB_SUCCESS;
}

int ObExprEmptyBlob::eval_empty_blob(
    const ObExpr &expr,
    ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (expr.obj_meta_.has_lob_header()) {
    ObTextStringDatumResult str_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
    if (OB_FAIL(str_result.init(0))) {
      LOG_WARN("init lob result failed");
    } else {
      str_result.set_result();
    }
  } else {
    expr_datum.set_string(NULL, 0);
  }
  return ret;
}

int ObExprEmptyBlob::cg_expr(
    ObExprCGCtx &op_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprEmptyBlob::eval_empty_blob;
  return OB_SUCCESS;
}

} // end of sql
} // end of oceanbase
