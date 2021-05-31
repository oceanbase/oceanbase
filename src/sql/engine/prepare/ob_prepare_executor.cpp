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

namespace oceanbase {
using namespace common;
namespace sql {

int ObPrepareExecutor::execute(ObExecContext& ctx, ObPrepareStmt& stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx or session is NULL", K(ctx.get_sql_ctx()), K(ctx.get_my_session()), K(ret));
  } else {
    ObObj result;
    ParamStore params_array((ObWrapperAllocator(ctx.get_allocator())));
    if (OB_FAIL(ObSQLUtils::calc_const_expr(
            stmt.get_stmt_type(), ctx, stmt.get_prepare_sql(), result, &ctx.get_allocator(), params_array))) {
      LOG_WARN("failed to calc const expr", K(stmt.get_prepare_sql()), K(ret));
    } else if (!result.is_string_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prepare sql is not a string", K(result), K(ret));
    } else {
      ObString sql_string = result.get_string();
      SMART_VAR(ObResultSet, result_set, *ctx.get_my_session())
      {
        ctx.get_sql_ctx()->is_prepare_protocol_ = true;  // set to prepare protocol
        ctx.get_sql_ctx()->is_prepare_stage_ = true;
        const bool is_inner_sql = false;
        if (OB_FAIL(GCTX.sql_engine_->stmt_prepare(sql_string, *ctx.get_sql_ctx(), result_set, is_inner_sql))) {
          LOG_WARN("failed to prepare stmt", K(sql_string), K(ret));
        } else if (OB_FAIL(ctx.get_my_session()->add_prepare(stmt.get_prepare_name(), result_set.get_statement_id()))) {
          LOG_WARN("failed to add prepare", K(stmt.get_prepare_name()), K(result_set.get_statement_id()), K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
