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
#include "sql/engine/expr/ob_expr_current_scn.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "src/sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
#include "src/storage/tx/ob_tx_api.h"
namespace sql
{
ObExprCurrentScn::ObExprCurrentScn(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CURRENT_SCN, N_CURRENT_SCN, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprCurrentScn::~ObExprCurrentScn()
{
}

int ObExprCurrentScn::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
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

int ObExprCurrentScn::eval_current_scn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    share::SCN current_scn;
    ObTransService *txs = MTL(transaction::ObTransService*);
    int64_t query_timeout = 0;
    session->get_query_timeout(query_timeout);
    int64_t expire_ts = session->get_query_start_time() + query_timeout;
    if (OB_FAIL(txs->get_read_snapshot_version(expire_ts, current_scn))) {
      LOG_WARN("get read snapshot version", K(ret));
    } else if (ObUInt64Type == expr.datum_meta_.type_) {
      expr_datum.set_uint(current_scn.get_val_for_sql());
    } else {
      uint64_t scn_version = current_scn.get_val_for_sql();
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber num;
      if (OB_FAIL(num.from(scn_version, tmp_alloc))) {
        LOG_WARN("copy number fail", K(ret));
      } else {
        expr_datum.set_number(num);
      }
    }
  }
  return ret;
}

int ObExprCurrentScn::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCurrentScn::eval_current_scn;
  return OB_SUCCESS;
}

} //namespace sql
} //namespace oceanbase
