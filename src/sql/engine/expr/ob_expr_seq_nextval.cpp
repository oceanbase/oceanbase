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

#include "sql/engine/expr/ob_expr_seq_nextval.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObExprSeqNextval::ObExprSeqNextval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SEQ_NEXTVAL, N_SEQ_NEXTVAL, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}


ObExprSeqNextval::~ObExprSeqNextval()
{
}


int ObExprSeqNextval::calc_result_type1(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (!type1.is_uint64()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input param should be uint64_t type", K(type1), K(ret));
  } else {
    type.set_number();
    type.set_scale(0);
    type.set_precision(OB_MAX_NUMBER_PRECISION);
    type.set_result_flag(NOT_NULL_FLAG);
  }
  return ret;
}

int ObExprSeqNextval::cg_expr(
  ObExprCGCtx &expr_cg_ctx,
  const ObRawExpr &raw_expr,
  ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid arg num", K(ret), K(rt_expr.arg_cnt_));
  } else {
    rt_expr.eval_func_ = calc_sequence_nextval;
  }
  return ret;
}

int ObExprSeqNextval::calc_sequence_nextval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *arg_datum = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("failed to eval expr", K(ret));
  } else {
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    common::number::ObNumber num;
    ObNumStackOnceAlloc tmp_alloc;
    uint64_t tenant_id = session->get_effective_tenant_id();
    int64_t seq_id = arg_datum->get_int();
    ObSequenceValue value;
    if (OB_FAIL(session->get_sequence_value(tenant_id, seq_id, value))) {
      LOG_WARN("failed to get sequence value from session",
               K(tenant_id), K(seq_id), K(ret));
    } else if (OB_FAIL(num.from(value.val(), tmp_alloc))) {
      LOG_WARN("fail deep copy value", K(ret));
    } else {
      res.set_number(num);
    }
    LOG_DEBUG("trace sequence nextval", K(num), K(ret));
  }
  return ret;
}

}//end namespace sql
}//end namespace oceanbase
