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

#include "sql/engine/expr/ob_expr_nvl2_oracle.h"

namespace oceanbase {
namespace sql {
using namespace oceanbase::common;

ObExprNvl2Oracle::ObExprNvl2Oracle(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_NVL2, N_NVL2, 3, NOT_ROW_DIMENSION)
{}

ObExprNvl2Oracle::~ObExprNvl2Oracle()
{}

int ObExprNvl2Oracle::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type1);
  // nvl2 reference: https://docs.oracle.com/database/121/SQLRF/functions132.htm#SQLRF00685
  // the expr1 can have any data type
  // the type of expr1 and expr2 can't be LONG, but now ob doesn't support the LONG type.
  return ObExprOracleNvl::calc_nvl_oralce_result_type(type, type2, type3, type_ctx);
}

int ObExprNvl2Oracle::calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
    const common::ObObj& obj3, common::ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  if (obj1.is_null_oracle()) {
    result = obj3;
  } else {
    result = obj2;
  }
  return ret;
}

int ObExprNvl2Oracle::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprNvlUtil::calc_nvl_expr2;
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
