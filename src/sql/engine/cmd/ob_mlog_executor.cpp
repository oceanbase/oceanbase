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
#include "sql/engine/cmd/ob_mlog_executor.h"
#include "sql/resolver/ddl/ob_create_mlog_stmt.h"
#include "sql/resolver/ddl/ob_drop_mlog_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "sql/engine/cmd/ob_index_executor.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObCreateMLogExecutor::ObCreateMLogExecutor()
{

}

ObCreateMLogExecutor::~ObCreateMLogExecutor()
{

}

int ObCreateMLogExecutor::execute(ObExecContext &ctx, ObCreateMLogStmt &stmt)
{
  int ret = OB_SUCCESS;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = nullptr;
  obrpc::ObCreateMLogArg &create_mlog_arg = stmt.get_create_mlog_arg();
  obrpc::ObCreateMLogRes create_mlog_res;
  ObString first_stmt;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  ObTaskExecutorCtx *task_exec_ctx = nullptr;
  bool is_sync_ddl_user = false;

  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get my session", KR(ret), K(ctx));
  } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("failed to get first statement", KR(ret));
  } else if (OB_FALSE_IT(create_mlog_arg.ddl_stmt_str_ = first_stmt)) {
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get task executor context", KR(ret));
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("faild to get common rpc proxy", KR(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", KR(ret));
  } else if (OB_INVALID_ID == create_mlog_arg.session_id_
             && FALSE_IT(create_mlog_arg.session_id_ = my_session->get_sessid_for_table())) {
    //impossible
  } else if (OB_FAIL(common_rpc_proxy->create_mlog(create_mlog_arg, create_mlog_res))) {
    LOG_WARN("failed to create mlog", KR(ret), K(create_mlog_arg));
  } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(my_session, is_sync_ddl_user))) {
    LOG_WARN("failed to check sync ddl user", KR(ret));
  } else if (!is_sync_ddl_user) {
    if (OB_UNLIKELY(OB_INVALID_ID == create_mlog_res.mlog_table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid mlog table id", KR(ret), K(create_mlog_res));
    } else if (OB_INVALID_VERSION == create_mlog_res.schema_version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected schema version", KR(ret), K(create_mlog_res));
    } else if (OB_FAIL(ObDDLExecutorUtil::wait_ddl_finish(create_mlog_arg.tenant_id_,
        create_mlog_res.task_id_, false/*do not need retry at executor*/, my_session, common_rpc_proxy))) {
      LOG_WARN("failed to wait ddl finish", KR(ret));
    }
  }

  return ret;
}

ObDropMLogExecutor::ObDropMLogExecutor()
{
}

ObDropMLogExecutor::~ObDropMLogExecutor()
{
}

int ObDropMLogExecutor::execute(ObExecContext &ctx, ObDropMLogStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx *task_exec_ctx = NULL;
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL;
  ObSQLSessionInfo *my_session = ctx.get_my_session();
  obrpc::ObDropIndexArg &drop_index_arg = stmt.get_drop_index_arg();
  obrpc::ObDropIndexRes drop_index_res;
  ObString first_stmt;

  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get my session", KR(ret), K(ctx));
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get task executor context");
  } else if (OB_FAIL(task_exec_ctx->get_common_rpc(common_rpc_proxy))) {
    LOG_WARN("failed to get common rpc proxy", KR(ret));
  } else if (OB_ISNULL(common_rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy should not be null", KR(ret));
  } else if (OB_FAIL(stmt.get_first_stmt(first_stmt))) {
    LOG_WARN("failed to get first statement", KR(ret));
  } else if (OB_FALSE_IT(drop_index_arg.ddl_stmt_str_ = first_stmt)) {
  }  else if ((OB_INVALID_ID == drop_index_arg.session_id_)
      && FALSE_IT(drop_index_arg.session_id_ = my_session->get_sessid_for_table())) {
    //impossible
  } else if (FALSE_IT(drop_index_arg.consumer_group_id_ = THIS_WORKER.get_group_id())) {
  } else if (FALSE_IT(drop_index_arg.is_add_to_scheduler_ = true)) {
  } else if (OB_FAIL(common_rpc_proxy->drop_index(drop_index_arg, drop_index_res))) {
    LOG_WARN("rpc proxy drop index failed", "dst", common_rpc_proxy->get_server(), KR(ret));
  } else if (OB_FAIL(ObDropIndexExecutor::wait_drop_index_finish(drop_index_res.tenant_id_,
                                                                 drop_index_res.task_id_,
                                                                 *my_session))) {
    LOG_WARN("failed to wait drop index finish", KR(ret));
  }
  SERVER_EVENT_ADD("ddl", "drop mlog execute finish",
    "tenant_id", MTL_ID(),
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "task_id", drop_index_res.task_id_,
    "table_id", drop_index_res.index_table_id_,
    "schema_version", drop_index_res.schema_version_);
  SQL_ENG_LOG(INFO, "finish drop mlog execute.", KR(ret),
      "ddl_event_info", ObDDLEventInfo(), K(stmt), K(drop_index_arg));

  return ret;
}
} // namespace sql
} // namespace oceanbase
