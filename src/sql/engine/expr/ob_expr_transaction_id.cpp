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
#include "sql/engine/expr/ob_expr_transaction_id.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{


ObExprTransactionId::ObExprTransactionId(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_TRANSACTION_ID, N_TRANSACTION_ID, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTransactionId::~ObExprTransactionId()
{
}

int ObExprTransactionId::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  if (is_oracle_mode()) {
    const ObAccuracy &acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[common::ORACLE_MODE][common::ObNumberType];
    type.set_number();
    type.set_scale(acc.get_scale());
    type.set_precision(acc.get_precision());
  } else {
    const ObAccuracy &acc = common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObUInt64Type];
    type.set_uint64();
    type.set_scale(acc.get_scale());
    type.set_precision(acc.get_precision());
    type.set_result_flag(NOT_NULL_FLAG);
  }
  return OB_SUCCESS;
}

int ObExprTransactionId::eval_transaction_id(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    const transaction::ObTxDesc *txdesc = session_info->get_tx_desc();
    const int64_t tx_id = txdesc ? txdesc->get_tx_id().get_id() : 0;
    if (ObUInt64Type == expr.datum_meta_.type_) { // mysql mode
      expr_datum.set_uint(tx_id);
    } else { // oracle mode
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(tx_id, tmp_alloc))) {
        LOG_WARN("copy number fail", K(ret));
      } else {
        expr_datum.set_number(num);
      }
    }
  }
  return ret;
}

int ObExprTransactionId::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprTransactionId::eval_transaction_id;
  return OB_SUCCESS;
}


}/* ns sql*/
}/* ns oceanbase */
