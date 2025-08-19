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

#include "sql/engine/cmd/ob_ccl_rule_executor.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/resolver/ddl/ob_create_ccl_rule_stmt.h"
#include "sql/resolver/ddl/ob_drop_ccl_rule_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_common_rpc_proxy.h"
#include "lib/worker.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/schema/ob_ccl_rule_mgr.h"
#include "sql/engine/expr/ob_expr_like.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

static uint64_t ccl_schema_mock_id = 0;
ObCreateCCLRuleExecutor::ObCreateCCLRuleExecutor()
{
}

ObCreateCCLRuleExecutor::~ObCreateCCLRuleExecutor()
{
}

int ObCreateCCLRuleExecutor::execute(ObExecContext &ctx, ObCreateCCLRuleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObCreateCCLRuleArg &create_ccl_rule_arg = stmt.get_create_ccl_rule_arg();
  obrpc::ObCreateCCLRuleArg &tmp_arg = const_cast<obrpc::ObCreateCCLRuleArg&>(create_ccl_rule_arg);
  ObString first_stmt;
  obrpc::UInt64 database_id(0);
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
   } else if (OB_ISNULL(common_rpc_proxy)
              || OB_ISNULL(ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get physical plan ctx", K(ret), K(ctx), K(common_rpc_proxy));
  } else {
    if (OB_FAIL(common_rpc_proxy->create_ccl_rule(create_ccl_rule_arg))) {
      SQL_ENG_LOG(WARN, "rpc proxy create table failed", K(ret));
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "create ccl rule execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server(),
      "ccl_rule_id", database_id,
      "schema_version", create_ccl_rule_arg.ccl_rule_schema_.get_schema_version());
  }
  return ret;
}

//////////////////
ObDropCCLRuleExecutor::ObDropCCLRuleExecutor()
{
}

ObDropCCLRuleExecutor::~ObDropCCLRuleExecutor()
{
}

int ObDropCCLRuleExecutor::execute(ObExecContext &ctx, ObDropCCLRuleStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  const obrpc::ObDropCCLRuleArg &drop_ccl_rule_arg = stmt.get_drop_ccl_rule_arg();
  obrpc::ObDropCCLRuleArg &tmp_arg = const_cast<obrpc::ObDropCCLRuleArg&>(drop_ccl_rule_arg);
  ObString first_stmt;
  uint64_t database_id = 0;
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
     SQL_ENG_LOG(WARN, "fail to get first stmt" , K(ret));
  } else {
    tmp_arg.ddl_stmt_str_ = first_stmt;
    tmp_arg.consumer_group_id_ = THIS_WORKER.get_group_id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get task executor context failed");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    SQL_ENG_LOG(WARN, "get common rpc proxy failed", K(ret));
  } else if (OB_ISNULL(common_rpc_proxy) || OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get my session", K(ctx), K(common_rpc_proxy));
  } else {
    if (OB_FAIL(common_rpc_proxy->drop_ccl_rule(drop_ccl_rule_arg))) {
      SQL_ENG_LOG(WARN, "rpc proxy drop ccl rule failed", K(ret));
    }
  }
  if (OB_NOT_NULL(common_rpc_proxy)) {
    SERVER_EVENT_ADD("ddl", "drop ccl rule execute finish",
      "tenant_id", MTL_ID(),
      "ret", ret,
      "trace_id", *ObCurTraceId::get_trace_id(),
      "rpc_dst", common_rpc_proxy->get_server());
  }
  return ret;
}


}
}
