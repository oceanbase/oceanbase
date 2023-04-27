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

#include "sql/engine/expr/ob_expr_pl_seq_nextval.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/sequence/ob_sequence_cache.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObExprPLSeqNextval::ObExprPLSeqNextval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PL_SEQ_NEXT_VALUE, N_PL_SEQ_NEXTVAL, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE)
{
}


ObExprPLSeqNextval::~ObExprPLSeqNextval()
{
}


int ObExprPLSeqNextval::calc_result_type1(ObExprResType &type,
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
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].precision_);
    type.set_result_flag(NOT_NULL_FLAG);
  }
  return ret;
}

int ObExprPLSeqNextval::get_schema_guard(ObExecContext *exec_ctx, 
                                         share::schema::ObSchemaGetterGuard *&schema_guard) const
{
  int ret = OB_SUCCESS;
  schema_guard = NULL;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task executor context is NULL", K(ret), KP(exec_ctx));
  } else if (OB_ISNULL(schema_guard = exec_ctx->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  }
  return ret;
}

int ObExprPLSeqNextval::cg_expr(
    ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  CK (1 == rt_expr.arg_cnt_);
  OX (rt_expr.eval_func_ = eval_pl_seq_next_val);
  return ret;
}

int ObExprPLSeqNextval::eval_pl_seq_next_val(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  share::ObSequenceCache &seq_cache = share::ObSequenceCache::get_instance();
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  ObDatum *datum = NULL;
  CK (OB_NOT_NULL(session));
  CK (1 == expr.arg_cnt_);
  OZ (expr.args_[0]->eval(ctx, datum));
  CK (OB_NOT_NULL(datum));
  CK (OB_NOT_NULL(schema_guard = ctx.exec_ctx_.get_sql_ctx()->schema_guard_));
  if (OB_FAIL(ret)) {
  } else {
    uint64_t tenant_id = session->get_effective_tenant_id();
    int64_t seq_id = datum->get_int();
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    const share::schema::ObSequenceSchema *seq_schema = nullptr;
    if (OB_FAIL(schema_guard->get_sequence_schema(tenant_id,
                                                  seq_id,
                                                  seq_schema))) {
      LOG_WARN("fail get sequence schema", K(seq_id), K(ret));
    } else {
      ObSequenceValue seq_value;
      if (OB_FAIL(seq_cache.nextval(*seq_schema,
                                    alloc_guard.get_allocator(),
                                    seq_value))) {
        LOG_WARN("fail get nextval for seq", K(tenant_id), K(seq_id), K(ret));
      } else if (OB_FAIL(session->set_sequence_value(tenant_id, seq_id, seq_value))) {
        LOG_WARN("save seq_value to session as currval for later read fail",
                  K(tenant_id), K(seq_id), K(seq_value), K(ret));
      } else { /*do nothing*/ }
      if (OB_SUCC(ret)) {
        common::number::ObNumber tmp;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(tmp.from(seq_value.val(), tmp_alloc))){
          LOG_WARN("fail deep copy value", K(ret), K(seq_id));
        } else {
          expr_datum.set_number(tmp);
        }
      }
    }
  }
  return ret;
}

}//end namespace sql
}//end namespace oceanbase
