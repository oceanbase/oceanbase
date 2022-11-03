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
#include "ob_execute_executor.h"
#include "sql/resolver/prepare/ob_execute_stmt.h"
#include "sql/ob_sql.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "observer/virtual_table/ob_virtual_table_iterator_factory.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExecuteExecutor::execute(ObExecContext &ctx, ObExecuteStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx or session is NULL", K(ctx.get_sql_ctx()), K(ctx.get_my_session()), K(ret));
  } else {
    ObObjParam result;
    ParamStore params_array( (ObWrapperAllocator(ctx.get_allocator())) );
    if (OB_FAIL(params_array.reserve(stmt.get_params().count()))) {
      LOG_WARN("failed to reserve params array", K(ret), "count", stmt.get_params().count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_params().count(); ++i) {
      if (OB_FAIL(ObSQLUtils::calc_const_expr(ctx, stmt.get_params().at(i), result, ctx.get_allocator(), params_array))) {
        LOG_WARN("failed to calc const expr", K(stmt.get_params().at(i)), K(ret));
      } else {
        result.set_param_meta();
        result.set_accuracy(stmt.get_params().at(i)->get_accuracy());
        result.set_result_flag(stmt.get_params().at(i)->get_result_flag());
        result.set_collation_level(CS_LEVEL_COERCIBLE);
        if (OB_FAIL(params_array.push_back(result))) {
          LOG_WARN("push_back error", K(result), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      SMART_VAR(ObResultSet, result_set, *ctx.get_my_session(), ctx.get_allocator()) {
        result_set.set_ps_protocol();
        ObTaskExecutorCtx *task_ctx = result_set.get_exec_context().get_task_executor_ctx();
        int64_t tenant_version = 0;
        int64_t sys_version = 0;
        if (OB_ISNULL(task_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("task executor ctx can not be NULL", K(task_ctx), K(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(
                    ctx.get_my_session()->get_effective_tenant_id(), tenant_version))) {
          LOG_WARN("fail get tenant schema version", K(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_received_broadcast_version(
                    OB_SYS_TENANT_ID, sys_version))) {
          LOG_WARN("fail get sys schema version", K(ret));
        } else {
          ObSqlCtx sql_ctx;
          sql_ctx.retry_times_ = 0;
          sql_ctx.session_info_ = ctx.get_my_session();
          sql_ctx.disable_privilege_check_ = true;
          sql_ctx.secondary_namespace_ = ctx.get_sql_ctx()->secondary_namespace_;
          sql_ctx.is_prepare_protocol_ = ctx.get_sql_ctx()->is_prepare_protocol_;
          sql_ctx.is_prepare_stage_ = ctx.get_sql_ctx()->is_prepare_stage_;
          sql_ctx.schema_guard_ = ctx.get_sql_ctx()->schema_guard_;
          task_ctx->schema_service_ = GCTX.schema_service_;
          task_ctx->set_query_tenant_begin_schema_version(tenant_version);
          task_ctx->set_query_sys_begin_schema_version(sys_version);
          task_ctx->set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
          if (OB_FAIL(result_set.init())) {
            LOG_WARN("result set init failed", K(ret));
          } else if (OB_FAIL(GCTX.sql_engine_->stmt_execute(stmt.get_prepare_id(),
                                                            stmt.get_prepare_type(),
                                                            params_array,
                                                            sql_ctx,
                                                            result_set,
                                                            false/* is_inner_sql */))) {
            LOG_WARN("failed to prepare stmt", K(stmt.get_prepare_id()), K(stmt.get_prepare_type()), K(ret));
          } else {
            if (OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema guard is null");
            } else if (OB_FAIL(ctx.get_my_session()->update_query_sensitive_system_variable(*(ctx.get_sql_ctx()->schema_guard_)))) {
              LOG_WARN("update query affacted system variable failed", K(ret));
            } else if (OB_FAIL(result_set.open())) {
              LOG_WARN("result set open failed", K(result_set.get_statement_id()), K(ret));
            }
            if (OB_SUCC(ret)) {
              if (result_set.is_with_rows()) {
                while (OB_SUCC(ret)) {
                  const common::ObNewRow *row = NULL;
                  if (OB_FAIL(result_set.get_next_row(row))) {
                    if (OB_ITER_END == ret) {
                      ret = OB_SUCCESS;
                    } else {
                      LOG_WARN("get next row error", K(ret));
                    }
                    break;
                  }
                }
              }
            }
            int tmp_ret = OB_SUCCESS;
            if ((tmp_ret = result_set.close()) != OB_SUCCESS) {
              LOG_WARN("result set open failed", K(result_set.get_statement_id()), K(ret));
              ret = OB_SUCCESS == ret ? tmp_ret : ret;
            }
          }
        }
      }
    }
  }
  return ret;
}

}
}


