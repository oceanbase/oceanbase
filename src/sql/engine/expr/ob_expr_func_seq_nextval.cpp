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

#include "sql/engine/expr/ob_expr_func_seq_nextval.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/sequence/ob_sequence_cache.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace sql {
ObExprFuncSeqNextval::ObExprFuncSeqNextval(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MYSQL_SEQ_NEXTVAL, N_MYSQL_SEQ_NEXTVAL, 2, NOT_ROW_DIMENSION)
{}

ObExprFuncSeqNextval::~ObExprFuncSeqNextval()
{}

int ObExprFuncSeqNextval::calc_result_type2(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (!type1.is_varchar() || !type2.is_varchar()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input param should be varchar type", K(ret), K(type1), K(type2));
  } else {
    type.set_number();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].scale_);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType].precision_);
  }
  return ret;
}

int ObExprFuncSeqNextval::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(expr_ctx.my_session_));
  } else {
    ObSQLSessionInfo& session = *expr_ctx.my_session_;
    common::number::ObNumber res_num;
    ObNumStackOnceAlloc cal_allocator;
    common::ObIAllocator& res_allocator = *expr_ctx.calc_buf_;
    uint64_t tenant_id = session.get_effective_tenant_id();
    const ObString& db_name = obj1.get_string();
    const ObString& seq_name = obj2.get_string();
    ObSequenceValue value;

    ObExecContext& exec_ctx = *expr_ctx.exec_ctx_;
    ObTaskExecutorCtx* task_ctx = NULL;
    share::schema::ObMultiVersionSchemaService* schema_service = NULL;
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObSequenceSchema* seq_schema = nullptr;
    uint64_t seq_id = OB_INVALID_ID;
    uint64_t db_id = OB_INVALID_ID;
    bool exist = false;
    share::ObSequenceCache* sequence_cache = &share::ObSequenceCache::get_instance();

    if (OB_ISNULL(task_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task executor ctx is null", K(ret));
    } else if (OB_ISNULL(schema_service = task_ctx->schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret));
    } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, db_name, db_id))) {
      LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(db_name));
    } else if (OB_FAIL(
      schema_guard.check_sequence_exist_with_name(tenant_id, db_id, seq_name, exist, seq_id))) {
      LOG_WARN("failed to check sequence with name", K(ret), K(seq_name), K(db_id));
    } else if (!exist) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sequence is not exist", K(db_name), K(db_id), K(seq_name));
    } else if (OB_FAIL(schema_guard.get_sequence_schema(tenant_id, seq_id, seq_schema))) {
      LOG_WARN("fail get sequence schema", K(ret), K(seq_id));
    } else if (OB_ISNULL(seq_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else if (OB_FAIL(sequence_cache->nextval(*seq_schema, cal_allocator, value))) {
      LOG_WARN("failed to get sequence value from cache", K(ret), K(tenant_id), K(seq_id));
    } else if (OB_FAIL(res_num.from(value.val(), res_allocator))) {
      LOG_WARN("fail deep copy value", K(ret));
    } else {
      result.set_number(res_num);
    }
    LOG_DEBUG("trace sequence nextval", K(ret), K(res_num));
  }
  
  return ret;
}

int ObExprFuncSeqNextval::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 != rt_expr.arg_cnt_)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid arg num", K(ret), K(rt_expr.arg_cnt_));
  } else {
    rt_expr.eval_func_ = calc_sequence_nextval;
  }
  return ret;
}

int ObExprFuncSeqNextval::calc_sequence_nextval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& result)
{
  int ret = OB_SUCCESS;
  const ObString& db_name = expr.locate_param_datum(ctx, 0).get_string();
  const ObString& seq_name = expr.locate_param_datum(ctx, 1).get_string();
  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  uint64_t tenant_id = session->get_effective_tenant_id();

  common::number::ObNumber res_num;
  ObNumStackAllocator<2> allocator;
  ObSequenceValue value;

  ObExecContext& exec_ctx = ctx.exec_ctx_;
  ObTaskExecutorCtx* task_ctx = NULL;
  share::schema::ObMultiVersionSchemaService* schema_service = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObSequenceSchema* seq_schema = nullptr;
  uint64_t seq_id = OB_INVALID_ID;
  uint64_t db_id = OB_INVALID_ID;
  bool exist = false;
  share::ObSequenceCache* sequence_cache = &share::ObSequenceCache::get_instance();

  if (OB_ISNULL(task_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task executor ctx is null", K(ret));
  } else if (OB_ISNULL(schema_service = task_ctx->schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, db_name, db_id))) {
    LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(db_name));
  } else if (OB_FAIL(
    schema_guard.check_sequence_exist_with_name(tenant_id, db_id, seq_name, exist, seq_id))) {
    LOG_WARN("failed to check sequence with name", K(ret), K(seq_name), K(db_id));
  } else if (!exist) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sequence is not exist", K(db_name), K(db_id), K(seq_name));
  } else if (OB_FAIL(schema_guard.get_sequence_schema(tenant_id, seq_id, seq_schema))) {
    LOG_WARN("fail get sequence schema", K(seq_id), K(ret));
  } else if (OB_ISNULL(seq_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else if (OB_FAIL(sequence_cache->nextval(*seq_schema, allocator, value))) {
    LOG_WARN("failed to get sequence value from cache", K(ret), K(tenant_id), K(seq_id));
  } else if (OB_FAIL(res_num.from(value.val(), allocator))) {
    LOG_WARN("fail deep copy value", K(ret));
  } else {
    result.set_number(res_num);
  }
  LOG_DEBUG("trace sequence nextval", K(ret), K(res_num));
  
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
