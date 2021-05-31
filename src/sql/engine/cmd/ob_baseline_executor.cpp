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

#include "sql/engine/cmd/ob_baseline_executor.h"

#include "lib/string/ob_string.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "observer/ob_server_struct.h"
#include "sql/resolver/ddl/ob_alter_baseline_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/plan_cache/ob_cache_object_factory.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace obrpc;
namespace sql {
int ObAlterBaselineExecutor::execute(ObExecContext& ctx, ObAlterBaselineStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy* common_rpc_proxy = NULL;
  ObAlterPlanBaselineArg& arg = stmt.alter_baseline_arg_;
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", K(ret));
  } else if (OB_FAIL(common_rpc_proxy->alter_plan_baseline(arg))) {
    LOG_WARN("rpc proxy alter baseline failed", "dst", common_rpc_proxy->get_server(), K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
