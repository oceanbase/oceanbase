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

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_char_to_rowid.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprCharToRowID::ObExprCharToRowID(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_CHAR_TO_ROWID, N_CHAR_TO_ROWID, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprCharToRowID::~ObExprCharToRowID()
{
}

int ObExprCharToRowID::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_urowid();
  type.set_length(type1.get_length());
  type1.set_calc_meta(type.get_obj_meta());
  return ret;
}

int ObExprCharToRowID::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_char_to_rowid;
  return ret;
}

int ObExprCharToRowID::eval_char_to_rowid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    expr_datum = *arg;
    LOG_DEBUG("calc char to rowid done", KPC(arg), K(expr_datum));
  }
  return ret;
}

} /* sql */
} /* oceanbase */
