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
#include "ob_expr_acos.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprAcos::ObExprAcos(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ACOS, N_ACOS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

ObExprAcos::~ObExprAcos()
{
}

int ObExprAcos::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  common::ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, type1, type_ctx);
}

DEF_CALC_TRIGONOMETRIC_EXPR(acos, arg > 1 || arg < -1, OB_ERR_ARGUMENT_OUT_OF_RANGE);

int ObExprAcos::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_acos_expr;
  return ret;
}
} //namespace sql
} //namespace oceanbase
