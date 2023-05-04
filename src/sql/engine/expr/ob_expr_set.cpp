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
#include "sql/engine/expr/ob_expr_set.h"
#include "objit/common/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"
#include "pl/ob_pl_allocator.h"
#include "sql/engine/expr/ob_expr_multiset.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
}
}

ObExprSet::ObExprSet(ObIAllocator &alloc)
  :ObFuncExprOperator(alloc, T_FUN_SYS_SET, N_SET, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSet::~ObExprSet()
{
}

int ObExprSet::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OP_SET) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
  }
  return ret;
}

int ObExprSet::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (type1.is_ext()) {
      type.set_ext();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      type.set_calc_type(type1.get_calc_type());
      type.set_result_flag(NOT_NULL_FLAG);
      type.set_udt_id(type1.get_udt_id());
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
              K(ret), K(type1));
    }
  } else {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt",
              K(ret), K(type1));
  }
  return ret;
}


int ObExprSet::cg_expr(
    ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  CK (1 == rt_expr.arg_cnt_);
  OX (rt_expr.eval_func_ = calc_set);
  return ret;
}

int ObExprSet::calc_set(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSEDx(expr, ctx, expr_datum);
  return ret;
}

