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

#include "ob_expr_part_id.h"
#include "share/ob_i_sql_expression.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

ObExprPartId::ObExprPartId(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PART_ID, N_PART_ID, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

int ObExprPartId::calc_result_type1(ObExprResType &type,
                                    ObExprResType &type1,
                                    common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;
  if (!type1.is_uint64()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("type1 is not int", K(type1));
  } else {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    type.set_result_flag(NOT_NULL_FLAG);
  }
  return ret;
}

int ObExprPartId::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = &ObExprPartId::eval_part_id;
  return ret;
}

int ObExprPartId::eval_part_id(const ObExpr &, ObEvalCtx &, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  expr_datum.set_int(0);
  return ret;
}




} //end of sql
} //end of oceanbase
