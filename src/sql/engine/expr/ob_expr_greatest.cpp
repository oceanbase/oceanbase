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

#include "sql/engine/expr/ob_expr_greatest.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_less_than.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/oblog/ob_log.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprGreatest::ObExprGreatest(ObIAllocator &alloc)
    : ObExprLeastGreatest(alloc,
                           T_FUN_SYS_GREATEST,
                           N_GREATEST,
                           MORE_THAN_ZERO)
{
}

//same type params
int ObExprGreatest::calc_greatest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    ret = ObExprLeastGreatest::calc_oracle(expr, ctx, expr_datum, false);
  } else {
    ret = ObExprLeastGreatest::calc_mysql(expr, ctx, expr_datum, false);
  }
  return ret;
}

}
}
