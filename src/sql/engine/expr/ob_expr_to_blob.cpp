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
#include "sql/engine/expr/ob_expr_to_blob.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_item_type.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprToBlob::ObExprToBlob(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_TO_BLOB, N_TO_BLOB, 1)
{}

ObExprToBlob::~ObExprToBlob()
{}

int ObExprToBlob::calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (ob_is_null(text.get_type())) {
    type.set_null();
  } else if (ob_is_blob(text.get_type(), text.get_collation_type()) || ob_is_raw(text.get_type()) ||
             ob_is_string_tc(text.get_type())) {
    type.set_blob();
    type.set_collation_type(CS_TYPE_BINARY);
    if (ob_is_string_tc(text.get_type())) {
      text.set_calc_type(common::ObRawType);
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("wrong type of argument in function to_blob", K(ret), K(text));
  }

  return ret;
}

int ObExprToBlob::calc_result1(common::ObObj& result, const ObObj& text, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;

  if (text.is_null()) {
    result.set_null();
  } else if (text.is_raw() || ob_is_blob(text.get_type(), text.get_collation_type())) {
    ObString temp_str = text.get_string();
    result.set_lob_value(ObLongTextType, temp_str.ptr(), temp_str.length());
    result.set_collation_type(CS_TYPE_BINARY);
  } else {
    // won't come here
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong type of argument in function to_blob", K(ret), K(text.get_type()), K(text));
  }

  return ret;
}

int ObExprToBlob::eval_to_blob(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt or arg res type", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (arg->is_null()) {
    res.set_null();
  } else {
    res.set_datum(*arg);
  }
  return ret;
}

int ObExprToBlob::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_to_blob;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
