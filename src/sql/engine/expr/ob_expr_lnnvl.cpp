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
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_lnnvl.h"
#include "share/object/ob_obj_cast.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
}
}


ObExprFuncLnnvl::ObExprFuncLnnvl(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_LNNVL, N_LNNVL, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFuncLnnvl::~ObExprFuncLnnvl()
{
}

int ObExprFuncLnnvl::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_lnnvl;
  return ret;
}

int ObExprFuncLnnvl::eval_lnnvl(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (arg->is_null()) {
    expr_datum.set_bool(true);
  } else {
    expr_datum.set_bool(!arg->get_tinyint());
  }
  return ret;
}
