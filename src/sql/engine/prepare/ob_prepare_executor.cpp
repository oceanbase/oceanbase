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
#include "ob_prepare_executor.h"
#include "sql/resolver/prepare/ob_prepare_stmt.h"
#include "sql/ob_sql.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObPrepareExecutor::multiple_query_check(
    const ObSQLSessionInfo &session, const ObString &sql, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 1> queries;
  ObParser parser(allocator, session.get_sql_mode(), session.get_charsets4parser());
  ObMPParseStat parse_stat;
  if (OB_FAIL(parser.split_multiple_stmt(sql, queries, parse_stat, false, true))) {
    LOG_WARN("failed to split multiple stmt", K(ret), K(sql));
  } else if (OB_UNLIKELY(queries.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("emtpy query count", K(ret));
  } else if (OB_UNLIKELY(queries.count() > 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "prepare multi-statement query");
    LOG_WARN("can't not prepare multi stmt", K(ret), K(queries.count()));
  }
  return ret;
}

int ObPrepareExecutor::execute(ObExecContext &ctx, ObPrepareStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx or session is NULL", K(ctx.get_sql_ctx()), K(ctx.get_my_session()), K(ret));
  } else {
    ObObj result;
    ParamStore params_array( (ObWrapperAllocator(ctx.get_allocator())) );
    if (OB_FAIL(ObSQLUtils::calc_const_expr(ctx, stmt.get_prepare_sql(), result, ctx.get_allocator(), params_array))) {
      LOG_WARN("failed to calc const expr", K(stmt.get_prepare_sql()), K(ret));
    } else if (!result.is_string_type()) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("prepare sql is not a string", K(result), K(ret));
    } else {
      ObString stmt_name;
      ObPsStmtId ps_id = OB_INVALID_ID;
      ObString sql_string = result.get_string();
      if (OB_FAIL(ob_simple_low_to_up(ctx.get_allocator(), stmt.get_prepare_name(), stmt_name))) {
        LOG_WARN("failed to write stirng", K(ret));
      } else if(OB_FAIL(ctx.get_my_session()->get_prepare_id(stmt_name, ps_id))) {
        if (OB_EER_UNKNOWN_STMT_HANDLER == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get old prepare id", K(ret), K(stmt_name));
        }
      } else if (OB_INVALID_ID != ps_id) {
        bool is_in_use = false;
        if (lib::is_mysql_mode() && OB_FAIL(ctx.get_my_session()->check_ps_stmt_id_in_use(ps_id, is_in_use))) {
          LOG_WARN("failed to check ps stmt id is in use",  K(ret), K(ps_id), K(is_in_use));
        } else if(!is_in_use) {
          if (OB_FAIL(ctx.get_my_session()->remove_prepare(stmt_name))) {
            LOG_WARN("failed to remove old prepare stmt", K(stmt_name), K(ret));
          } else if (OB_FAIL(ctx.get_my_session()->close_ps_stmt(ps_id))) {
            LOG_WARN("fail to deallocate old prepare stmt", K(ret), K(ps_id));
          }
        } else {
          ret = OB_ERR_PS_NO_RECURSION;
          LOG_WARN("ps stmt id is in use", K(ret), K(ps_id), K(is_in_use));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(multiple_query_check(*ctx.get_my_session(), sql_string, ctx.get_allocator()))) {
        LOG_WARN("failed to check multiple query", K(ret), K(sql_string));
      } else {
        SMART_VAR(ObResultSet, result_set, *ctx.get_my_session(), ctx.get_allocator()) {
          ctx.get_sql_ctx()->is_prepare_protocol_ = true; //set to prepare protocol
          ctx.get_sql_ctx()->is_prepare_stage_ = true;
          const bool is_inner_sql = false;
          if (OB_FAIL(GCTX.sql_engine_->stmt_prepare(sql_string, *ctx.get_sql_ctx(),
                                                            result_set, is_inner_sql))) {
            LOG_WARN("failed to prepare stmt", K(sql_string), K(ret));
          } else if (OB_FAIL(ctx.get_my_session()->add_prepare(stmt_name,
                                                               result_set.get_statement_id()))) {
            LOG_WARN("failed to add prepare", K(stmt_name),
            K(result_set.get_statement_id()), K(ret));
          } else { /*do nothing*/ }
        }
      }
    }
  }
  return ret;
}

}
}


