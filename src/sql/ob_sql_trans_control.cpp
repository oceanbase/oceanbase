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

#define USING_LOG_PREFIX SQL_EXE

#include "share/ob_schema_status_proxy.h"  // ObSchemaStatusProxy
#include "share/ob_encryption_util.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/ob_sql_trans_hook.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/parser/parse_malloc.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_trans_util.h"
#include "sql/ob_end_trans_callback.h"
#include "lib/oblog/ob_trace_log.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_define.h"
#include "common/ob_partition_key.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/executor/ob_task_spliter.h"
#include "lib/profile/ob_perf_event.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "storage/transaction/ob_weak_read_util.h"  //ObWeakReadUtil

#if 0
#define DEBUG_AC_TRANS(exec_ctx)                               \
  {                                                            \
    ObSQLSessionInfo* __my_session = GET_MY_SESSION(exec_ctx); \
    OB_ASSERT(NULL != __my_session);                           \
    bool ac = __my_session->get_autocommit();                  \
    bool in_trans = __my_session->get_in_transaction();        \
    LOG_INFO("ac & in_trans", K(ac), K(in_trans));             \
  }
#else
#define DEBUG_AC_TRANS(exec_ctx)
#endif

#if 0
#define DEBUG_TRANS_STAGE(stage)                                   \
  if (my_session->get_has_temp_table_flag()) {                     \
    LOG_WARN("TMP_TABLE " stage, K(my_session->get_trans_desc())); \
  }
#else
#define DEBUG_TRANS_STAGE(stage)
#endif

namespace oceanbase {
using namespace common;
using namespace transaction;
using namespace share;
using namespace share::schema;
namespace sql {
int ObSqlTransControl::on_plan_start(ObExecContext& exec_ctx, bool is_remote /* = false*/)
{
  DEBUG_AC_TRANS(exec_ctx);
  int ret = OB_SUCCESS;
  bool ac = true;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  if (OB_ISNULL(my_session) || OB_ISNULL(plan_ctx) || OB_ISNULL(plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr or plan ctx or plan_ctx->get_phy_plan is null", K(ret), K(my_session), K(plan_ctx));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(my_session->get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else if (stmt::T_SELECT == plan_ctx->get_phy_plan()->get_stmt_type() &&
             !plan_ctx->get_phy_plan()->has_for_update() &&
             (!my_session->get_trans_desc().is_valid() || my_session->get_trans_desc().is_trans_end()) &&
             my_session->is_support_external_consistent() && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260 &&
             !(my_session->is_in_transaction() && my_session->get_trans_desc().is_valid() &&
                 my_session->get_trans_desc().is_xa_local_trans()) &&
             (share::is_oracle_mode() || my_session->get_local_autocommit())) {
    OZ(start_standalone_stmt(exec_ctx, *my_session, *plan_ctx, is_remote));
  } else {
    bool in_trans = my_session->get_in_transaction();
    if (ObSqlTransUtil::plan_can_start_trans(ac, in_trans)) {
      ret = implicit_start_trans(exec_ctx, is_remote);
    }
  }
  return ret;
}

/*int ObSqlTransControl::on_plan_end(ObExecContext &exec_ctx,
                                   bool is_rollback,
                                   ObExclusiveEndTransCallback &callback)
{
  DEBUG_AC_TRANS(exec_ctx);
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = GET_MY_SESSION(exec_ctx);
  bool ac = true;
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr is null", K(ret));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    //session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed", K(ret), K(my_session->get_session_state()),
             K(my_session->get_sessid()), "proxy_sessid", my_session->get_proxy_sessid());
  } else if (OB_FAIL(my_session->get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else {
    bool in_trans = my_session->get_in_transaction();
    if (ObSqlTransUtil::plan_can_end_trans(ac, in_trans)) {
      ret = implicit_end_trans(exec_ctx, is_rollback, callback);
    }
  }
  return ret;
}*/

int ObSqlTransControl::explicit_start_trans(ObExecContext& ctx, const bool read_only)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(ctx);
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session) || OB_UNLIKELY(!task_exec_ctx.min_cluster_version_is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid param", K(ret), K(plan_ctx), K(my_session), K(task_exec_ctx.get_min_cluster_version()));
  } else {
    int32_t access_mode = read_only ? ObTransAccessMode::READ_ONLY : ObTransAccessMode::READ_WRITE;
    int32_t tx_isolation = my_session->get_tx_isolation();
    ObStartTransParam& start_trans_param = plan_ctx->get_start_trans_param();
    start_trans_param.set_access_mode(access_mode);
    start_trans_param.set_isolation(tx_isolation);
    start_trans_param.set_type(my_session->get_trans_type());
    start_trans_param.set_cluster_version(task_exec_ctx.get_min_cluster_version());
    start_trans_param.set_inner_trans(my_session->is_inner());
    NG_TRACE_EXT(start_trans, OB_ID(read_only), read_only, OB_ID(access_mode), access_mode);
    if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx, start_trans_param))) {
      LOG_WARN("fail start trans", K(ret));
    }
  }
  return ret;
}

int ObSqlTransControl::explicit_start_trans(ObExecContext& exec_ctx, transaction::ObStartTransParam& start_trans_param)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr is null", K(ret));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else {
    if (false == my_session->get_in_transaction()) {
      if (OB_FAIL(start_trans(exec_ctx, start_trans_param))) {
        LOG_WARN("fail start explicit trans", K(ret));
      } else {
        bool is_read_only =
            (start_trans_param.get_access_mode() == transaction::ObTransAccessMode::READ_ONLY) ? true : false;
        my_session->set_tx_read_only(is_read_only);
        my_session->set_in_transaction(true);
      }
    }
  }
  return ret;
}

int ObSqlTransControl::implicit_start_trans(ObExecContext& exec_ctx, bool is_remote /* = false*/)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObTaskExecutorCtx& task_exec_ctx = exec_ctx.get_task_exec_ctx();
  bool ac = true;
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session) || OB_ISNULL(plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't get plan_ctx or session or phy_plan", K(ret), K(plan_ctx), K(my_session));
  } else if (OB_UNLIKELY(!is_remote && !task_exec_ctx.min_cluster_version_is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("is not remote start trans, but task_exec_ctx.get_min_cluster_version() is invalid",
        K(ret),
        K(task_exec_ctx.get_min_cluster_version()));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(my_session->get_autocommit(ac))) {
    LOG_WARN("fail to get autocommit", K(ret));
  } else {
    bool in_trans = my_session->get_in_transaction();
    bool tx_read_only = my_session->get_tx_read_only();
    int32_t tx_isolation = my_session->get_tx_isolation();
    transaction::ObStartTransParam& start_trans_param = plan_ctx->get_start_trans_param();
    if (true == ac && false == in_trans && stmt::T_SELECT == plan_ctx->get_phy_plan()->get_stmt_type() &&
        !plan_ctx->get_phy_plan()->has_for_update()) {
      start_trans_param.set_access_mode(transaction::ObTransAccessMode::READ_ONLY);
    } else if (true == tx_read_only) {
      start_trans_param.set_access_mode(transaction::ObTransAccessMode::READ_ONLY);
    } else {
      start_trans_param.set_access_mode(transaction::ObTransAccessMode::READ_WRITE);
    }
    start_trans_param.set_type(my_session->get_trans_type());
    start_trans_param.set_isolation(tx_isolation);
    start_trans_param.set_autocommit(ac && !in_trans);
    if (task_exec_ctx.min_cluster_version_is_valid()) {
      start_trans_param.set_cluster_version(task_exec_ctx.get_min_cluster_version());
    } else {
      start_trans_param.set_cluster_version(GET_MIN_CLUSTER_VERSION());
    }
    start_trans_param.set_inner_trans(my_session->is_inner());
    if (OB_FAIL(start_trans(exec_ctx, start_trans_param, is_remote))) {
      LOG_WARN("fail start trans", K(ret));
    }
    if (OB_SUCC(ret) && false == ac) {
      my_session->set_in_transaction(true);
    }
  }
  return ret;
}

int ObSqlTransControl::start_trans(
    ObExecContext& exec_ctx, transaction::ObStartTransParam& start_trans_param, bool is_remote /* = false*/)
{
  DEBUG_AC_TRANS(exec_ctx);
  int ret = OB_SUCCESS;
  int64_t org_cluster_id = OB_INVALID_ORG_CLUSTER_ID;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  ObPhysicalPlanCtx* plan_ctx = exec_ctx.get_physical_plan_ctx();
  storage::ObPartitionService* ps = NULL;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(plan_ctx) ||
      OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K(ret), K(executor_ctx), K(my_session), K(plan_ctx), K(ps));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(my_session->get_ob_org_cluster_id(org_cluster_id))) {
    if (OB_LIKELY(is_remote && OB_ENTRY_NOT_EXIST == ret)) {
      LOG_WARN("start trans in remote, ob_org_cluster_id is not serilized to the remote server, "
               "maybe server version is different, then ignore it, "
               "use default cluster id and set ret to OB_SUCCESS");
      ret = OB_SUCCESS;
      org_cluster_id = ObServerConfig::get_instance().cluster_id;
    } else {
      LOG_WARN("fail to get ob_org_cluster_id", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    if (OB_INVALID_ORG_CLUSTER_ID == org_cluster_id || OB_INVALID_CLUSTER_ID == org_cluster_id) {
      org_cluster_id = ObServerConfig::get_instance().cluster_id;
      if (OB_UNLIKELY(org_cluster_id < OB_MIN_CLUSTER_ID || org_cluster_id > OB_MAX_CLUSTER_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("org_cluster_id is set to cluster_id, but it is out of range",
            K(ret),
            K(org_cluster_id),
            K(OB_MIN_CLUSTER_ID),
            K(OB_MAX_CLUSTER_ID));
      }
    }
    if (OB_SUCC(ret)) {
      my_session->get_trans_result().reset();
      transaction::ObTransDesc& trans_desc = my_session->get_trans_desc();
      const uint64_t tenant_id = my_session->get_effective_tenant_id();
      int64_t trans_timeout_ts = 0;
      if (OB_FAIL(get_trans_timeout_ts(*my_session, trans_timeout_ts))) {
        LOG_ERROR("fail to get trans timeout ts", K(ret));
      } else if (OB_FAIL(ps->start_trans(tenant_id,
                     static_cast<uint64_t>(org_cluster_id),
                     start_trans_param,
                     trans_timeout_ts,
                     my_session->get_sessid(),
                     my_session->get_proxy_sessid(),
                     trans_desc))) {
        LOG_WARN("fail start trans", K(ret));
      }
      DEBUG_TRANS_STAGE("start_trans");
      NG_TRACE_EXT(start_trans,
          OB_ID(trans_id),
          trans_desc.get_trans_id(),
          OB_ID(timeout),
          trans_timeout_ts,
          OB_ID(start_time),
          my_session->get_query_start_time());
    }
  }
  return ret;
}

int ObSqlTransControl::implicit_end_trans(
    ObExecContext& exec_ctx, const bool is_rollback, ObExclusiveEndTransCallback& callback)
{
  int ret = OB_SUCCESS;
  const bool is_explicit = false;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr is null, won't call end trans!!!", K(ret));
  } else if (OB_FAIL(end_trans(exec_ctx, is_rollback, is_explicit, callback))) {
    LOG_WARN("fail to end trans", K(ret), K(is_rollback));
  }
  return ret;
}

int ObSqlTransControl::explicit_end_trans(ObExecContext& exec_ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;
  if (exec_ctx.is_end_trans_async()) {
    ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
    if (OB_ISNULL(my_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("session ptr is null", K(ret));
    } else {
      ObEndTransAsyncCallback& call_back = my_session->get_end_trans_cb();
      ret = explicit_end_trans(exec_ctx, is_rollback, call_back);
    }
  } else {
    ObEndTransSyncCallback call_back;
    ret = explicit_end_trans(exec_ctx, is_rollback, call_back);
  }
  return ret;
}

int ObSqlTransControl::explicit_end_trans(
    ObExecContext& exec_ctx, const bool is_rollback, ObEndTransSyncCallback& callback)
{
  int ret = OB_SUCCESS;
  const bool is_explicit = true;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr is null", K(ret));
  } else {
    if (true == my_session->get_in_transaction()) {
      if (OB_FAIL(callback.init(&(my_session->get_trans_desc()), my_session))) {
        LOG_ERROR("fail init callback", K(ret), K(my_session->get_trans_desc()));
        // implicit commit, no rollback
      } else {
        int wait_ret = OB_SUCCESS;
        if (OB_FAIL(end_trans(exec_ctx, is_rollback, is_explicit, callback))) {
          LOG_WARN("fail end explicit trans", K(is_rollback), K(ret));
        }
        if (OB_UNLIKELY(OB_SUCCESS != (wait_ret = callback.wait()))) {
          LOG_WARN("sync end trans callback return an error!",
              K(ret),
              K(wait_ret),
              K(is_rollback),
              K(my_session->get_trans_desc()));
        }
        ret = OB_SUCCESS != ret ? ret : wait_ret;
      }
    } else {
      my_session->reset_tx_variable();
      my_session->set_early_lock_release(false);
      exec_ctx.set_need_disconnect(false);
      my_session->get_trans_desc().get_standalone_stmt_desc().reset();
    }
  }
  return ret;
}

int ObSqlTransControl::explicit_end_trans(
    ObExecContext& exec_ctx, const bool is_rollback, ObEndTransAsyncCallback& callback)
{
  int ret = OB_SUCCESS;
  const bool is_explicit = true;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session is NULL, won't call end trans!!!", K(ret), KP(my_session));
  } else {
    if (true == my_session->get_in_transaction()) {
      // other fields will be and should be (re)set correctly in call back routine.
      if (OB_FAIL(end_trans(exec_ctx, is_rollback, is_explicit, callback))) {
        LOG_WARN("fail end explicit trans", K(is_rollback), K(ret));
      }
      exec_ctx.get_trans_state().set_end_trans_executed(OB_SUCC(ret));
    } else {
      my_session->reset_first_stmt_type();
      my_session->reset_tx_variable();
      my_session->set_early_lock_release(false);
      exec_ctx.set_need_disconnect(false);
      my_session->get_trans_desc().get_standalone_stmt_desc().reset();
    }
  }
  return ret;
}

int ObSqlTransControl::kill_query_session(
    storage::ObPartitionService* ps, ObSQLSessionInfo& session, const ObSQLSessionState& status)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ps)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument for point", K(ret), K(ps), K(session));
  } else if (true == session.get_in_transaction()) {
    const transaction::ObTransDesc& trans_desc = session.get_trans_desc();
    if (OB_FAIL(ps->kill_query_session(trans_desc, status))) {
      LOG_WARN("fail kill query or session error", K(ret), K(trans_desc));
    }
  }

  return ret;
}

// may concurrency with user-query, need blocking query:
// - acquire session.query_lock by caller
// - only rollback transaction in Transaction-Layer, don't touch SQL-Layer state
int ObSqlTransControl::kill_active_trx(storage::ObPartitionService* ps, ObSQLSessionInfo* session)
{
  LOG_DEBUG("kill active trx start", "session_id", session->get_sessid(), K(*session));
  int ret = OB_SUCCESS;
  transaction::ObTransDesc& trans_desc = session->get_trans_desc();
  if (trans_desc.is_valid() && !trans_desc.is_trans_end() && !trans_desc.is_trx_idle_timeout()) {
    if (OB_FAIL(ps->internal_kill_trans(trans_desc))) {
      LOG_WARN("kill active trx by end_trans fail.",
          K(ret),
          "session_id",
          session->get_sessid(),
          K(*session),
          K(trans_desc));
    } else {
      LOG_INFO("kill active trx succeed, need user rollback in next stmt",
          "session_id",
          session->get_sessid(),
          K(*session),
          K(trans_desc));
    }
  }
  return ret;
}

int ObSqlTransControl::end_trans(
    storage::ObPartitionService* ps, ObSQLSessionInfo* session, bool& has_called_txs_end_trans)
{
  int ret = OB_SUCCESS;
  int wait_ret = OB_SUCCESS;
  int hook_ret = OB_SUCCESS;
  has_called_txs_end_trans = false;
  if (OB_ISNULL(ps) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument for point", K(ret), K(ps), K(session));
  } else if (true == session->get_in_transaction()) {
    transaction::ObTransDesc& trans_desc = session->get_trans_desc();
    int64_t timeout = 0;
    ObEndTransSyncCallback callback;
    if (OB_FAIL(get_stmt_timeout_ts(*session, timeout))) {
      LOG_ERROR("fail to get stmt timeout ts", K(ret));
    } else if (OB_FAIL(callback.init(&trans_desc, session))) {
      LOG_ERROR("fail init callback", K(ret), K(trans_desc));
    } else {
      callback.set_is_need_rollback(true);
      callback.set_end_trans_type(ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
      callback.handout();
      if (OB_FAIL(ps->end_trans(true, trans_desc, callback, timeout))) {
        LOG_WARN("fail end trans when session terminate", K(ret), K(trans_desc), K(timeout));
      }
      session->reset_first_stmt_type();
      if (OB_UNLIKELY(OB_SUCCESS != (wait_ret = callback.wait()))) {
        LOG_WARN("sync end trans callback return an error!", K(ret), K(wait_ret), K(trans_desc), K(timeout));
      }
      ret = OB_SUCCESS != ret ? ret : wait_ret;

      if (callback.is_txs_end_trans_called()) {
        has_called_txs_end_trans = true;
      } else {
        has_called_txs_end_trans = false;
        LOG_WARN("fail before trans service end trans, may disconnct", K(ret), K(session->get_sessid()));
        if (OB_UNLIKELY(OB_SUCCESS == ret)) {
          LOG_ERROR("callback before trans service end trans, but ret is OB_SUCCESS, it is BUG!!!",
              K(callback.is_txs_end_trans_called()));
        }
      }

      session->reset_tx_variable();
      session->set_early_lock_release(false);
    }
  } else {
    session->reset_first_stmt_type();
    has_called_txs_end_trans = true;
  }
  return (OB_SUCCESS != ret) ? ret : hook_ret;
}

int ObSqlTransControl::end_trans(
    ObExecContext& exec_ctx, const bool is_rollback, const bool is_explicit, ObExclusiveEndTransCallback& callback)
{
  DEBUG_AC_TRANS(exec_ctx);
  int ret = OB_SUCCESS;
  int hook_ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  ObEndTransCallbackType callback_type = callback.get_callback_type();
  storage::ObPartitionService* ps = NULL;
  int32_t xa_trans_state = transaction::ObXATransState::NON_EXISTING;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(plan_ctx) ||
      OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    LOG_ERROR("null ptr, won't call end trans!!!", K(executor_ctx), K(my_session), K(plan_ctx), K(ps));
    ret = OB_ERR_UNEXPECTED;
  } else {
    transaction::ObTransDesc& trans_desc = my_session->get_trans_desc();
    if (trans_desc.is_xa_local_trans() && !is_explicit) {
      if (OB_FAIL(ps->get_xa_trans_state(xa_trans_state, trans_desc))) {
        LOG_WARN("failed get_xa_trans_state", K(ret));
      } else if (transaction::ObXATransState::NON_EXISTING != xa_trans_state) {
        ret = OB_TRANS_XA_RMFAIL;
        // LOG_USER_ERROR(OB_TRANS_XA_RMFAIL, ObXATransState::to_string(xa_trans_state));
        LOG_WARN("invalid xa state", K(ret), K(xa_trans_state));
        callback.set_is_need_rollback(is_rollback);
        callback.set_end_trans_type(ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
        callback.handout();
        callback.callback(ret);
      }
    } else {
      int64_t timeout = get_stmt_timeout_ts(*plan_ctx);
      callback.set_is_need_rollback(is_rollback);
      if (is_explicit) {
        callback.set_end_trans_type(ObExclusiveEndTransCallback::END_TRANS_TYPE_EXPLICIT);
      } else {
        callback.set_end_trans_type(ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
      }
      callback.handout();
      if (ASYNC_CALLBACK_TYPE == callback_type && OB_FAIL(inc_session_ref(my_session))) {
        LOG_WARN("fail to inc session ref", K(ret));
      } else if (OB_FAIL(ps->end_trans(is_rollback, trans_desc, callback, timeout))) {
        LOG_WARN("fail end trans", K(ret), K(callback_type));
      }
      DEBUG_TRANS_STAGE("end_trans");
      my_session->reset_first_stmt_type();
      if (callback.get_callback_type() != ASYNC_CALLBACK_TYPE) {
        my_session->reset_tx_variable();
        my_session->set_early_lock_release(false);
      }
      // my_session->get_trans_desc().reset();
      DEBUG_AC_TRANS(exec_ctx);
      if (ASYNC_CALLBACK_TYPE == callback_type) {
        exec_ctx.set_need_disconnect(false);
      } else if (!callback.is_txs_end_trans_called()) {
        exec_ctx.set_need_disconnect(true);
        LOG_WARN("fail before trans service end trans, disconnct", K(ret), K(my_session->get_sessid()));
        if (OB_UNLIKELY(OB_SUCCESS == ret)) {
          LOG_ERROR("callback before trans service end trans, but ret is OB_SUCCESS, it is BUG!!!",
              K(is_explicit),
              K(callback.is_txs_end_trans_called()));
        }
      } else {
        bool is_need_disconnect = false;
        ObSQLUtils::check_if_need_disconnect_after_end_trans(ret, is_rollback, is_explicit, is_need_disconnect);
        exec_ctx.set_need_disconnect(is_need_disconnect);
      }
    }
  }
  return (OB_SUCCESS != ret) ? ret : hook_ret;
}

bool ObSqlTransControl::is_weak_consistency_level(const ObConsistencyLevel& consistency_level)
{
  return (WEAK == consistency_level || FROZEN == consistency_level);
}

int ObSqlTransControl::decide_trans_read_interface_specs(const char* module, const ObSQLSessionInfo& session,
    const stmt::StmtType& stmt_type, const stmt::StmtType& literal_stmt_type, const bool& is_contain_select_for_update,
    const bool& is_contain_inner_table, const ObConsistencyLevel& sql_consistency_level,
    const bool need_consistent_snapshot, int32_t& trans_consistency_level, int32_t& trans_consistency_type,
    int32_t& read_snapshot_type)
{
  int ret = OB_SUCCESS;
  const char* err_msg = "";
  ObConsistencyLevelAdaptor consistency_level_adaptor(sql_consistency_level);
  const bool is_standby_cluster = GCTX.is_standby_cluster();
  const bool is_inner_sql = session.is_inner();
  const bool is_from_show_stmt = (literal_stmt_type != stmt::T_NONE);
  const bool is_build_index_stmt = (stmt::T_BUILD_INDEX_SSTABLE == stmt_type);
  uint64_t tenant_id = session.get_effective_tenant_id();
  const bool stmt_spec_snapshot_version_is_valid = session.has_valid_read_snapshot_version();
  int32_t isolation = session.get_tx_isolation();
  const bool is_serializable_trans =
      (ObTransIsolation::SERIALIZABLE == isolation || ObTransIsolation::REPEATABLE_READ == isolation);

  trans_consistency_level = (int32_t)consistency_level_adaptor.get_consistency();
  trans_consistency_type = ObTransConsistencyType::CURRENT_READ;
  read_snapshot_type = ObTransReadSnapshotType::STATEMENT_SNAPSHOT;

  if (OB_UNLIKELY(!ObTransConsistencyLevel::is_valid(trans_consistency_level))) {
    ret = OB_ERR_UNEXPECTED;
    err_msg = "invalid trans consisntency level";
  } else if (is_build_index_stmt) {
    trans_consistency_level = ObTransConsistencyLevel::WEAK;
    trans_consistency_type = ObTransConsistencyType::BOUNDED_STALENESS_READ;
  } else if (is_from_show_stmt) {
    if (!is_standby_cluster || OB_SYS_TENANT_ID == tenant_id) {
      trans_consistency_level = ObTransConsistencyLevel::STRONG;
      trans_consistency_type = ObTransConsistencyType::CURRENT_READ;
    } else {
      trans_consistency_level = ObTransConsistencyLevel::WEAK;
      trans_consistency_type = ObTransConsistencyType::BOUNDED_STALENESS_READ;
    }
  } else if (stmt::T_SELECT != stmt_type || (stmt::T_SELECT == stmt_type && is_contain_select_for_update)) {
    trans_consistency_level = ObTransConsistencyLevel::STRONG;
    trans_consistency_type = ObTransConsistencyType::CURRENT_READ;
  } else if (is_contain_inner_table) {
    if (is_inner_sql) {
    } else {
      if (!is_standby_cluster || OB_SYS_TENANT_ID == tenant_id) {
        trans_consistency_level = ObTransConsistencyLevel::STRONG;
      } else {
      }
    }
    if (ObTransConsistencyLevel::STRONG == trans_consistency_level) {
      trans_consistency_type = ObTransConsistencyType::CURRENT_READ;
    } else {
      trans_consistency_type = ObTransConsistencyType::BOUNDED_STALENESS_READ;
    }
  } else {
    int32_t cur_stmt_consistency_type = ObTransConsistencyType::UNKNOWN;
    const int32_t first_stmt_consistency_type = session.get_trans_consistency_type();

    if (ObTransConsistencyLevel::STRONG == trans_consistency_level) {
      cur_stmt_consistency_type = ObTransConsistencyType::CURRENT_READ;
    } else {
      cur_stmt_consistency_type = ObTransConsistencyType::BOUNDED_STALENESS_READ;
    }

    if (ObTransConsistencyType::UNKNOWN == first_stmt_consistency_type) {
      trans_consistency_type = cur_stmt_consistency_type;
    } else {
      trans_consistency_type = first_stmt_consistency_type;
      if (OB_UNLIKELY(cur_stmt_consistency_type != first_stmt_consistency_type)) {
        err_msg = "current statement consistency type is different from the first statement";
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (is_serializable_trans) {
      read_snapshot_type = ObTransReadSnapshotType::TRANSACTION_SNAPSHOT;
    } else {
      if (is_contain_inner_table || !need_consistent_snapshot) {
        read_snapshot_type = ObTransReadSnapshotType::PARTICIPANT_SNAPSHOT;
      } else {
        read_snapshot_type = ObTransReadSnapshotType::STATEMENT_SNAPSHOT;
      }
    }
  }

  if (OB_FAIL(ret) || OB_UNLIKELY(0 != STRCMP(err_msg, ""))) {
    LOG_WARN(err_msg,
        K(ret),
        K(tenant_id),
        K(module),
        K(trans_consistency_type),
        K(read_snapshot_type),
        K(trans_consistency_level),
        K(sql_consistency_level),
        K(is_standby_cluster),
        K(is_inner_sql),
        K(is_from_show_stmt),
        K(is_build_index_stmt),
        K(is_serializable_trans),
        K(is_contain_inner_table),
        K(stmt_spec_snapshot_version_is_valid),
        K(is_contain_select_for_update),
        K(stmt_type),
        K(literal_stmt_type),
        K(session),
        "trans_desc",
        session.get_trans_desc());
  } else {
    LOG_DEBUG("decide_trans_read_interface_specs",
        K(ret),
        K(tenant_id),
        K(module),
        K(trans_consistency_type),
        K(read_snapshot_type),
        K(trans_consistency_level),
        K(sql_consistency_level),
        K(is_standby_cluster),
        K(is_inner_sql),
        K(is_from_show_stmt),
        K(is_build_index_stmt),
        K(is_serializable_trans),
        K(is_contain_inner_table),
        K(stmt_spec_snapshot_version_is_valid),
        K(is_contain_select_for_update),
        K(stmt_type),
        K(literal_stmt_type),
        K(session),
        "trans_desc",
        session.get_trans_desc());
  }

  return ret;
}

bool ObSqlTransControl::check_fast_select_read_uncommited(
    transaction::ObTransDesc& trans_desc, const common::ObPartitionLeaderArray& pla)
{
  bool read_uncommited = false;
  const common::ObPartitionArray& participants = trans_desc.get_participants();
  for (int i = 0; !read_uncommited && i < pla.get_partitions().count(); ++i) {
    const ObPartitionKey& pkey = pla.get_partitions().at(i);
    for (int j = 0; j < participants.count(); ++j) {
      if (pkey == participants.at(j)) {
        read_uncommited = true;
        break;
      }
    }
  }
  return read_uncommited;
}

int ObSqlTransControl::update_safe_weak_read_snapshot(
    bool is_bounded_staleness_read, ObSQLSessionInfo& session_info, int snapshot_type, int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (is_bounded_staleness_read) {
    if (ObWeakReadUtil::enable_monotonic_weak_read(session_info.get_effective_tenant_id()) &&
        !session_info.has_valid_read_snapshot_version() &&
        ObTransReadSnapshotType::STATEMENT_SNAPSHOT == snapshot_type) {
      int64_t cur_stmt_snapshot_version = snapshot_version;
      if (OB_UNLIKELY(cur_stmt_snapshot_version <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN,
            "statement snapshot version is invalid, unexpected error",
            K(ret),
            K(cur_stmt_snapshot_version),
            K(session_info),
            K(session_info.get_trans_desc()));
      } else if (OB_FAIL(session_info.update_safe_weak_read_snapshot(session_info.get_effective_tenant_id(),
                     cur_stmt_snapshot_version,
                     ObBasicSessionInfo::LAST_STMT))) {
        TRANS_LOG(WARN,
            "update safe weak read snapshot version error",
            K(ret),
            K(snapshot_type),
            K(snapshot_version),
            K(session_info),
            K(session_info.get_trans_desc()));
      } else if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        LOG_INFO("update safe weak read snapshot version success",
            K(ret),
            "snapshot_version",
            snapshot_version,
            "session",
            session_info,
            K(session_info.get_trans_desc()));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObSqlTransControl::start_stmt(
    ObExecContext& exec_ctx, const common::ObPartitionLeaderArray& pla, bool is_remote /* = false*/)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  const ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  const ObPhysicalPlan* phy_plan = NULL;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(phy_plan_ctx) ||
      OB_ISNULL(ps = executor_ctx->get_partition_service()) || OB_ISNULL(phy_plan = phy_plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K(ret), K(executor_ctx), K(my_session), K(phy_plan_ctx), K(ps), K(phy_plan));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(my_session->set_cur_stmt_tables(pla.get_partitions(), phy_plan->get_stmt_type()))) {
    LOG_WARN("set cur stmt tables fail", K(ret));
  } else if (/*my_session->reuse_cur_sql_no() ||*/ my_session->is_standalone_stmt()) {
    // skip start_stmt.
  } else if (my_session->is_nested_session()) {
    ObTransDesc& trans_desc = my_session->get_trans_desc();
    TransResult& trans_result = my_session->get_trans_result();
    if (OB_FAIL(ps->start_nested_stmt(trans_desc))) {
      LOG_WARN("start nested stmt fail", K(ret), K(trans_desc));
    } else if (OB_FAIL(trans_result.set_max_sql_no(trans_desc.get_sql_no()))) {
      LOG_WARN("refresh max sql no fail", K(ret), K(trans_result), K(trans_desc));
    }
    /*
    int64_t nested_count = my_session->get_nested_count();
    int64_t max_sql_no = trans_result.get_max_sql_no();
    const ObString nested_stmt = my_session->get_current_query_string();
    LOG_INFO("start_nested_stmt", K(nested_count), K(nested_stmt), K(max_sql_no));
    */
  } else if (my_session->is_fast_select()) {
    if (pla.count() > 0) {
      ObPartitionLeaderArray out_pla;
      OZ(change_pla_info_(executor_ctx->get_table_locations(), pla, out_pla));
      OX(my_session->set_read_uncommited(check_fast_select_read_uncommited(my_session->get_trans_desc(), out_pla)));
    }
  } else if (OB_UNLIKELY(ObSQLSessionInfo::INVALID_TYPE == my_session->get_session_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session type is not init", K(my_session), K(ret));
  } else if (OB_UNLIKELY(INVALID_CONSISTENCY == phy_plan_ctx->get_consistency_level())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql consistency level is invalid", K(ret), K(phy_plan_ctx->get_consistency_level()));
  } else {
    ObStmtParam stmt_param;
    ObPartitionArray unreachable_partitions;
    const bool is_retry_sql = (executor_ctx->get_retry_times() > 0);
    const uint64_t tenant_id = my_session->get_effective_tenant_id();
    transaction::ObTransDesc& trans_desc = my_session->get_trans_desc();
    transaction::ObStartTransParam& trans_param = trans_desc.get_trans_param();
    transaction::ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
    int32_t trans_consistency_level = ObTransConsistencyLevel::UNKNOWN;
    const int64_t last_safe_weak_read_snapshot = my_session->get_safe_weak_read_snapshot();
    const int64_t last_safe_weak_read_snapshot_source = my_session->get_safe_weak_read_snapshot_source();
    int64_t auto_spec_snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
    const bool is_trx_elr = my_session->get_early_lock_release();

    my_session->get_trans_result().clear_stmt_result();

    if (OB_FAIL(set_trans_param_(*my_session,
            *phy_plan,
            phy_plan_ctx->get_consistency_level(),
            trans_consistency_level,
            auto_spec_snapshot_version))) {
      LOG_WARN("set trans param fail", K(ret), KPC(my_session));
    } else if (OB_FAIL(stmt_param.init(tenant_id,
                   get_stmt_timeout_ts(*phy_plan_ctx),
                   is_retry_sql,
                   last_safe_weak_read_snapshot,
                   last_safe_weak_read_snapshot_source,
                   is_trx_elr))) {
      LOG_WARN("ObStmtParam init error", K(ret), K(tenant_id), K(is_retry_sql), K(last_safe_weak_read_snapshot));
    } else {
      stmt_desc.phy_plan_type_ = phy_plan->has_nested_sql() ? OB_PHY_PLAN_UNCERTAIN : phy_plan->get_location_type();
      stmt_desc.stmt_type_ = phy_plan->get_stmt_type();
      stmt_desc.is_sfu_ = phy_plan->has_for_update();
      stmt_desc.execution_id_ = my_session->get_current_execution_id();
      stmt_desc.sql_id_.assign_strive(phy_plan->stat_.sql_id_);
      stmt_desc.trace_id_adaptor_.set(ObCurTraceId::get());
      const ObString& app_trace_id_str = my_session->get_app_trace_id();
      stmt_desc.app_trace_id_str_.reset();
      stmt_desc.app_trace_id_str_.assign_buffer(stmt_desc.buffer_, sizeof(stmt_desc.buffer_));
      stmt_desc.app_trace_id_str_.write(app_trace_id_str.ptr(), app_trace_id_str.length());
      stmt_desc.cur_query_start_time_ = my_session->get_query_start_time();
      stmt_desc.stmt_tenant_id_ = my_session->get_effective_tenant_id();
      stmt_desc.inner_sql_ = my_session->is_inner();
      stmt_desc.consistency_level_ = trans_consistency_level;
      stmt_desc.trx_lock_timeout_ = my_session->get_trx_lock_timeout();

      if (my_session->has_valid_read_snapshot_version()) {
        stmt_desc.cur_stmt_specified_snapshot_version_ = my_session->get_read_snapshot_version();
      } else if (auto_spec_snapshot_version > 0) {
        stmt_desc.cur_stmt_specified_snapshot_version_ = auto_spec_snapshot_version;
      }
      /*
      const ObString root_stmt = my_session->get_current_query_string();
      LOG_INFO("start_root_stmt", K(root_stmt));
      */
      ObPartitionLeaderArray out_pla;
      if (pla.count() > 0 && OB_FAIL(change_pla_info_(executor_ctx->get_table_locations(), pla, out_pla))) {
        LOG_WARN("change pla info error", K(ret), K(stmt_param), K(trans_desc), K(pla));
      } else if (OB_FAIL(ps->start_stmt(stmt_param, trans_desc, out_pla, unreachable_partitions))) {
        LOG_WARN("fail start stmt",
            K(ret),
            K(trans_desc),
            K(pla),
            K(out_pla),
            K(is_remote),
            K(unreachable_partitions.count()));
        if ((is_transaction_rpc_timeout_err(ret) || is_data_not_readable_err(ret) || is_partition_change_error(ret)) &&
            !is_remote) {
          ObAddr unreachable_server;
          for (int64_t i = 0; i < unreachable_partitions.count(); ++i) {
            unreachable_server.reset();
            int tmp_ret = OB_SUCCESS;
            if (OB_UNLIKELY(
                    OB_SUCCESS != (tmp_ret = out_pla.find_leader(unreachable_partitions.at(i), unreachable_server)))) {
              LOG_ERROR("can not find unreachable server", K(ret), K(tmp_ret), K(i), K(unreachable_partitions.at(i)));
            } else if (OB_UNLIKELY(OB_SUCCESS !=
                                   (tmp_ret = my_session->get_retry_info_for_update().add_invalid_server_distinctly(
                                        unreachable_server, true)))) {
              LOG_WARN("fail to add invalid server distinctly",
                  K(ret),
                  K(tmp_ret),
                  K(unreachable_server),
                  K(my_session->get_retry_info()));
            }
          }
        }
      } else {
        OZ(update_safe_weak_read_snapshot(trans_param.is_bounded_staleness_read(),
            *my_session,
            trans_param.get_read_snapshot_type(),
            trans_desc.get_snapshot_version()));
        OZ(my_session->set_start_stmt());

        if (phy_plan->is_contain_oracle_trx_level_temporary_table()) {
          trans_desc.set_trx_level_temporary_table_involved();
        }

        DEBUG_TRANS_STAGE("start_stmt");
      }
    }
  }
  return ret;
}

int ObSqlTransControl::specify_stmt_snapshot_version_for_slave_cluster_sql_(const ObSQLSessionInfo& session,
    const stmt::StmtType& literal_stmt_type, const bool& is_contain_inner_table, const int32_t consistency_type,
    const int32_t read_snapshot_type, int64_t& auto_spec_snapshot_version)
{
  int ret = OB_SUCCESS;
  share::ObSchemaStatusProxy* schema_status_proxy = GCTX.schema_status_proxy_;
  share::schema::ObRefreshSchemaStatus slave_schema_status;

  const uint64_t tenant_id = session.get_effective_tenant_id();

  const bool is_standby_cluster = GCTX.is_standby_cluster();
  const bool is_inner_sql = session.is_inner();
  const bool is_from_show_stmt = (literal_stmt_type != stmt::T_NONE);
  const bool stmt_spec_snapshot_version_is_valid = session.has_valid_read_snapshot_version();

  if (is_standby_cluster && is_contain_inner_table && !is_inner_sql && OB_SYS_TENANT_ID != tenant_id &&
      !stmt_spec_snapshot_version_is_valid && ObTransConsistencyType::BOUNDED_STALENESS_READ == consistency_type) {
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid schema_status_proxy", K(schema_status_proxy));
    } else if (OB_FAIL(schema_status_proxy->nonblock_get(tenant_id, slave_schema_status))) {
      LOG_WARN("get schema status on slave cluster fail",
          K(ret),
          K(tenant_id),
          K(session),
          K(is_standby_cluster),
          K(is_from_show_stmt),
          K(is_contain_inner_table),
          K(consistency_type),
          K(read_snapshot_type));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_REPLICA_NOT_READABLE;
      }
    } else if (OB_UNLIKELY(slave_schema_status.snapshot_timestamp_ <= 0)) {
      ret = OB_REPLICA_NOT_READABLE;
      LOG_WARN("tenant schema status is invalid on slave cluster",
          K(ret),
          K(tenant_id),
          K(slave_schema_status),
          K(session),
          K(is_standby_cluster),
          K(is_from_show_stmt),
          K(is_contain_inner_table),
          K(consistency_type),
          K(read_snapshot_type));
    } else {
      auto_spec_snapshot_version = slave_schema_status.snapshot_timestamp_;
      LOG_INFO("specify statement snapshot version for slave cluster SQL",
          K(tenant_id),
          K(slave_schema_status),
          K(is_standby_cluster),
          K(is_inner_sql),
          K(is_from_show_stmt),
          K(is_contain_inner_table),
          K(consistency_type),
          K(read_snapshot_type),
          K(session));
    }

    if (OB_REPLICA_NOT_READABLE == ret) {
      LOG_WARN("slave cluster inner table readable snapshot version still not ready, need retry",
          K(ret),
          K(tenant_id),
          K(is_standby_cluster),
          K(is_inner_sql),
          K(is_from_show_stmt),
          K(consistency_type),
          K(read_snapshot_type),
          K(is_contain_inner_table),
          K(session));
    }
  } else {
  }
  return ret;
}

int ObSqlTransControl::trans_param_compat_with_cluster_before_2200_(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  if (trans_desc.get_cluster_version() < CLUSTER_VERSION_2200) {
    if (trans_desc.get_snapshot_version() == ObTransVersion::INVALID_TRANS_VERSION) {
      trans_desc.set_snapshot_version(0);
    }
  }
  return ret;
}

int ObSqlTransControl::set_trans_param_(ObSQLSessionInfo& my_session, const ObPhysicalPlan& phy_plan,
    const ObConsistencyLevel sql_consistency_level, int32_t& trans_consistency_level,
    int64_t& auto_spec_snapshot_version)
{
  int ret = OB_SUCCESS;
  transaction::ObTransDesc& trans_desc = my_session.get_trans_desc();
  int32_t trans_consistency_type = ObTransConsistencyType::UNKNOWN;
  int32_t read_snapshot_type = ObTransReadSnapshotType::UNKNOWN;

  if (OB_FAIL(decide_trans_read_interface_specs("ObSqlTransControl::start_stmt",
          my_session,
          phy_plan.get_stmt_type(),
          phy_plan.get_literal_stmt_type(),
          phy_plan.has_for_update(),
          phy_plan.is_contain_inner_table(),
          sql_consistency_level,
          phy_plan.need_consistent_snapshot(),
          trans_consistency_level,
          trans_consistency_type,
          read_snapshot_type))) {
    LOG_WARN("fail to decide trans read interface specs",
        K(ret),
        K(phy_plan.get_stmt_type()),
        K(phy_plan.get_literal_stmt_type()),
        K(phy_plan.has_for_update()),
        K(phy_plan.is_contain_inner_table()),
        K(sql_consistency_level),
        K(my_session));
  } else if (GCTX.is_standby_cluster() && OB_FAIL(specify_stmt_snapshot_version_for_slave_cluster_sql_(my_session,
                                              phy_plan.get_literal_stmt_type(),
                                              phy_plan.is_contain_inner_table(),
                                              trans_consistency_type,
                                              read_snapshot_type,
                                              auto_spec_snapshot_version))) {
    LOG_WARN("specify statement snapshot version for slave cluster sql fail", K(ret), K(my_session));
  } else if (OB_FAIL(trans_param_compat_with_cluster_before_2200_(trans_desc))) {
    LOG_WARN("trans param compat with cluster before 2200 fail", K(ret), K(trans_desc));
  } else {
    trans_desc.get_trans_param().set_read_snapshot_type(read_snapshot_type);

    if (ObTransConsistencyType::UNKNOWN == my_session.get_trans_consistency_type()) {
      my_session.set_trans_consistency_type(trans_consistency_type);
      trans_desc.get_trans_param().set_consistency_type(trans_consistency_type);
    } else if (trans_desc.get_trans_param().get_consistency_type() != trans_consistency_type) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("different consistency type in one transaction, unexpected error",
          K(ret),
          K(trans_consistency_type),
          K(trans_desc));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "different consistency type in one transaction");
    }

    if (OB_SUCCESS == ret) {
      if (ObTransConsistencyType::BOUNDED_STALENESS_READ == trans_consistency_type) {
        trans_desc.get_trans_param().set_access_mode(transaction::ObTransAccessMode::READ_ONLY);
      }
    }
  }

  LOG_DEBUG("ObSqlTransControl::start_stmt::set_trans_param",
      K(sql_consistency_level),
      K(trans_consistency_type),
      K(trans_consistency_level),
      K(read_snapshot_type),
      K(auto_spec_snapshot_version),
      K(trans_desc),
      K(my_session));

  return ret;
}

int ObSqlTransControl::get_discard_participants(const ObPartitionArray& all_partitions,
    const ObPartitionArray& response_partitions, ObPartitionArray& discard_partitions)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(all_partitions.count() > response_partitions.count())) {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_partitions.count(); ++i) {
      if (OB_UNLIKELY(!has_exist_in_array(response_partitions, all_partitions.at(i)))) {
        OZ(discard_partitions.push_back(all_partitions.at(i)), i, all_partitions.at(i));
      }
    }
    LOG_DEBUG("get discard participants", K(ret), K(all_partitions), K(response_partitions), K(discard_partitions));
  }
  return ret;
}

int ObSqlTransControl::create_savepoint(ObExecContext& exec_ctx, const ObString& sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K(ret), K(executor_ctx), K(my_session), K(ps));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(ps->savepoint(my_session->get_trans_desc(), sp_name))) {
    LOG_WARN("fail to create savepoint", K(ret), K(sp_name));
  }
  return ret;
}

int ObSqlTransControl::rollback_savepoint(ObExecContext& exec_ctx, const ObString& sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  const ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  transaction::ObStmtParam stmt_param;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K(ret), K(executor_ctx), K(my_session), K(ps));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(
                 stmt_param.init(my_session->get_effective_tenant_id(), get_stmt_timeout_ts(*phy_plan_ctx), false))) {
    LOG_WARN("ObStmtParam init error", K(ret), K(my_session));
  } else if (OB_FAIL(ps->rollback_savepoint(my_session->get_trans_desc(), sp_name, stmt_param))) {
    LOG_WARN("fail to rollback savepoint", K(ret), K(sp_name), K(stmt_param));
  }
  return ret;
}

int ObSqlTransControl::release_savepoint(ObExecContext& exec_ctx, const ObString& sp_name)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K(ret), K(executor_ctx), K(my_session), K(ps));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(ps->release_savepoint(my_session->get_trans_desc(), sp_name))) {
    LOG_WARN("fail to release savepoint", K(ret), K(sp_name));
  }
  return ret;
}

int ObSqlTransControl::xa_rollback_all_changes(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  const ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  transaction::ObStmtParam stmt_param;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ptr", K(ret), K(executor_ctx), K(my_session), K(ps));
  } else if (!my_session->is_in_transaction() || !my_session->get_trans_desc().is_xa_local_trans()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("called in wrong context", K(ret), K(my_session->get_trans_desc()));
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(
                 stmt_param.init(my_session->get_effective_tenant_id(), get_stmt_timeout_ts(*phy_plan_ctx), false))) {
    LOG_WARN("ObStmtParam init error", K(ret), K(my_session));
  } else if (OB_FAIL(ps->xa_rollback_all_changes(my_session->get_trans_desc(), stmt_param))) {
    LOG_WARN("fail to rollback savepoint", K(ret), K(stmt_param), K(my_session->get_trans_desc()));
  }
  return ret;
}

int ObSqlTransControl::end_stmt(ObExecContext& exec_ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  common::ObPartitionLeaderArray pla;
  bool skip_end_stmt = false;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    LOG_WARN("can't get partition service or my_session", K(executor_ctx), K(my_session), K(ps));
    ret = OB_ERR_UNEXPECTED;
  } else if (my_session->is_fast_select() || my_session->is_standalone_stmt()) {
    skip_end_stmt = true;
  } else if (FALSE_IT(skip_end_stmt = my_session->reuse_cur_sql_no())) {
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (OB_FAIL(get_participants(
                 executor_ctx->get_table_locations(), pla, my_session->get_is_in_retry_for_dup_tbl()))) {
    LOG_WARN("get participants failed", K(ret), K(executor_ctx->get_table_locations()));
  } else if (OB_FAIL(merge_stmt_partitions(exec_ctx, *my_session))) {
    LOG_WARN("fail to merge stmt partitions", K(ret));
  } else if (skip_end_stmt) {
    // nesetd sql will not end_stmt().
    LOG_DEBUG("skip end_stmt",
        K(ret),
        "cur_stmt",
        my_session->get_current_query_string(),
        "trans_result",
        my_session->get_trans_result());
  } else if (my_session->is_nested_session()) {
    TransResult& trans_result = my_session->get_trans_result();
    ObTransDesc& trans_desc = my_session->get_trans_desc();
    ObPartitionArray cur_stmt_all_pgs;
    if (trans_result.get_total_partitions().count() > 0 &&
        OB_FAIL(change_pkeys_to_pgs_(
            executor_ctx->get_table_locations(), trans_result.get_total_partitions(), cur_stmt_all_pgs))) {
      LOG_WARN("change cur stmt pkeys to pgs error", K(ret), K(trans_result));
      trans_desc.set_need_rollback();
    } else if (OB_FAIL(ps->end_nested_stmt(trans_desc, cur_stmt_all_pgs, is_rollback))) {
      LOG_WARN("end nested stmt fail", K(ret), K(trans_desc), K(cur_stmt_all_pgs), K(is_rollback));
    }
    /*
    int64_t nested_count = my_session->get_nested_count();
    const ObString nested_stmt = my_session->get_current_query_string();
    LOG_INFO("end_nested_stmt", K(nested_count), K(nested_stmt), K(trans_result), K(is_rollback));
    */
  } else {
    transaction::ObTransDesc& trans_desc = my_session->get_trans_desc();
    TransResult& trans_result = my_session->get_trans_result();
    ObPartitionArray discard_partitions;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2100 && !is_rollback) {
      // param discard_partitions is added in version 2.1
      // in order to compatible with the old server below version 2.1
      // only calculate the discard partition when all server are updated to version 2.1
      OZ(get_discard_participants(
             trans_result.get_total_partitions(), trans_result.get_response_partitions(), discard_partitions),
          trans_result,
          discard_partitions);
    }
    ObPartitionArray cur_stmt_all_pgs;
    ObPartitionArray cur_stmt_discard_pgs;
    ObPartitionLeaderArray cur_stmt_pla;
    if (trans_result.get_total_partitions().count() > 0 &&
        OB_FAIL(change_pkeys_to_pgs_(
            executor_ctx->get_table_locations(), trans_result.get_total_partitions(), cur_stmt_all_pgs))) {
      LOG_WARN("change cur stmt pkeys to pgs error", K(ret), K(trans_result), K(trans_desc));
      trans_desc.set_need_rollback();
    } else if (discard_partitions.count() > 0 &&
               OB_FAIL(change_pkeys_to_pgs_(
                   executor_ctx->get_table_locations(), discard_partitions, cur_stmt_discard_pgs))) {
      LOG_WARN("change cur stmt discard pkeys to pgs error", K(ret), K(trans_result), K(trans_desc));
      trans_desc.set_need_rollback();
    } else if (pla.count() > 0 && OB_FAIL(change_pla_info_(executor_ctx->get_table_locations(), pla, cur_stmt_pla))) {
      LOG_WARN("change pla info error", K(ret), K(pla), K(cur_stmt_pla), K(trans_result), K(trans_desc));
      trans_desc.set_need_rollback();
    } else {
      /*
      const ObString root_stmt = my_session->get_current_query_string();
      LOG_INFO("end_root_stmt", K(root_stmt), KP(&trans_desc),
               "trans_desc_max_sql_no", trans_desc.get_max_sql_no(),
               "trans_result_max_sql_no", trans_result.get_max_sql_no());
      */
      OZ(ps->end_stmt(is_rollback,
             trans_result.is_incomplete(),
             cur_stmt_all_pgs,
             trans_result.get_part_epoch_list(),
             cur_stmt_discard_pgs,
             cur_stmt_pla,
             trans_desc),
          trans_result,
          discard_partitions,
          trans_desc);
      DEBUG_TRANS_STAGE("end_stmt");
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("call end_stmt",
          K(ret),
          K(trans_desc),
          "cur_stmt",
          my_session->get_current_query_string(),
          K(cur_stmt_all_pgs),
          K(cur_stmt_discard_pgs),
          K(cur_stmt_pla),
          K(discard_partitions),
          K(trans_result));
    }
  }
  if (OB_NOT_NULL(my_session) && !skip_end_stmt && !my_session->is_nested_session()) {
    my_session->get_trans_result().clear_stmt_result();
    int end_ret = my_session->set_end_stmt();
    if (OB_SUCCESS != end_ret) {
      LOG_ERROR("failed to set end stmt", K(end_ret));
      if (OB_SUCCESS == ret) {
        ret = end_ret;
      }
    }
  }
  return ret;
}

int ObSqlTransControl::start_participant(
    ObExecContext& exec_ctx, const common::ObPartitionArray& participants, bool is_remote /* = false*/)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  const ObPhysicalPlan* phy_plan = NULL;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = executor_ctx->get_partition_service()) ||
      OB_ISNULL(phy_plan = GET_PHY_PLAN_CTX(exec_ctx)->get_phy_plan())) {
    LOG_WARN("can't get partition service or my_session", K(executor_ctx), K(my_session), K(ps));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (my_session->is_fast_select() || my_session->is_standalone_stmt()) {
    // do nothing ...
  } else {
    transaction::ObTransDesc& trans_desc = my_session->get_trans_desc();
    TransResult& trans_result = my_session->get_trans_result();
    if (OB_FAIL(trans_result.merge_total_partitions(participants))) {
      LOG_WARN("fail to merge partitions", K(ret), K(participants));
    }

    if (OB_SUCC(ret)) {
      ObPartitionArray pgs;
      ObPartitionEpochArray partition_epoch_arr;
      const ObTransID& trans_id = trans_desc.get_trans_id();
      int64_t sql_no = trans_desc.get_sql_no();
      bool is_forbidden = false;
      if (participants.count() > 0 &&
          OB_FAIL(change_pkeys_to_pgs_(executor_ctx->get_table_locations(), participants, pgs))) {
        LOG_WARN("change pkeys to pgs error", K(ret), K(participants), K(pgs), K(trans_desc));
      } else if (OB_FAIL(ps->is_trans_forbidden_sql_no(trans_id, pgs, sql_no, is_forbidden))) {
        LOG_WARN("get trans forbidden error", K(ret), K(participants), K(pgs), K(trans_desc));
      } else if (is_forbidden) {
        ret = OB_ERR_INTERRUPTED;
        LOG_WARN("execution is interrupted", K(ret), K(participants), K(pgs), K(trans_desc));
      } else if (OB_FAIL(ps->start_participant(trans_desc, pgs, partition_epoch_arr))) {
        if (OB_NOT_MASTER != ret) {
          LOG_WARN(
              "fail start participants", K(ret), K(is_remote), K(participants), K(pgs), K(trans_result), K(trans_desc));
        }
        if (is_data_not_readable_err(ret) && !is_remote) {
          int add_ret = OB_SUCCESS;
          if (OB_UNLIKELY(
                  OB_SUCCESS != (add_ret = my_session->get_retry_info_for_update().add_invalid_server_distinctly(
                                     exec_ctx.get_addr(), true)))) {
            LOG_WARN("fail to add local addr to invalid servers distinctly",
                K(ret),
                K(add_ret),
                K(exec_ctx.get_addr()),
                K(my_session->get_retry_info()));
          }
        }
      } else {
        DEBUG_TRANS_STAGE("start_participant");
        LOG_DEBUG("start participant", K(participants), K(trans_result));
        OZ(trans_result.merge_response_partitions(participants), participants);
        OZ(trans_result.merge_part_epoch_list(partition_epoch_arr), partition_epoch_arr);
      }
      NG_TRACE_EXT(start_part, OB_ID(ret), ret, OB_ID(trans_id), trans_desc.get_trans_id());
    }
  }
  return ret;
}

int ObSqlTransControl::end_participant(
    ObExecContext& exec_ctx, const bool is_rollback, const common::ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  if (OB_ISNULL(executor_ctx) || OB_ISNULL(my_session) || OB_ISNULL(ps = executor_ctx->get_partition_service())) {
    LOG_WARN("can't get partition service or my_session", K(executor_ctx), K(my_session), K(ps));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(my_session->is_zombie())) {
    // session has been killed some moment ago
    ret = OB_ERR_SESSION_INTERRUPTED;
    LOG_WARN("session has been killed",
        K(ret),
        K(my_session->get_session_state()),
        K(my_session->get_sessid()),
        "proxy_sessid",
        my_session->get_proxy_sessid());
  } else if (my_session->is_fast_select() || my_session->is_standalone_stmt()) {
    // do nothing ...
  } else {
    transaction::ObTransDesc& trans_desc = my_session->get_trans_desc();
    ObPartitionArray pgs;
    if (participants.count() > 0 &&
        OB_FAIL(change_pkeys_to_pgs_(executor_ctx->get_table_locations(), participants, pgs))) {
      LOG_WARN("change pkeys to pgs error", K(ret), K(participants), K(pgs), K(trans_desc));
    } else if (OB_FAIL(ps->end_participant(is_rollback, trans_desc, pgs))) {
      LOG_WARN("fail end participant", K(ret), K(is_rollback), K(participants), K(pgs), K(trans_desc));
    } else {
      // do nothing
      DEBUG_TRANS_STAGE("end_participant");
    }
  }
  NG_TRACE_EXT(end_participant, OB_ID(ret), ret);
  return ret;
}

int ObSqlTransControl::get_pg_key_(
    const ObIArray<ObPhyTableLocation>& table_locations, const ObPartitionKey& pkey, ObPartitionKey& pg_key)
{
  int ret = OB_SUCCESS;
  bool hit = false;
  for (int64_t i = 0; OB_SUCC(ret) && !hit && i < table_locations.count(); ++i) {
    const ObPartitionReplicaLocationIArray& part_locations = table_locations.at(i).get_partition_location_list();
    for (int64_t j = 0; OB_SUCC(ret) && j < part_locations.count(); ++j) {
      ObPartitionKey tmp_pkey;
      if (OB_FAIL(part_locations.at(j).get_partition_key(tmp_pkey))) {
        LOG_WARN("get partition key error", K(ret), "location", part_locations.at(j));
      } else if (pkey == tmp_pkey) {
        pg_key = part_locations.at(j).get_pg_key();
        hit = true;
        break;
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!hit || !pg_key.is_valid()) {
      LOG_DEBUG("cannot find pg key", K(ret), K(table_locations), K(pkey), K(pg_key));
      if (OB_FAIL(storage::ObPartitionService::get_instance().get_pg_key(pkey, pg_key))) {
        LOG_WARN("get pg key error", K(ret), K(pkey), K(pg_key));
      }
    }
  }
  return ret;
}

int ObSqlTransControl::change_pkeys_to_pgs_(
    const ObIArray<ObPhyTableLocation>& table_locations, const ObPartitionArray& pkeys, ObPartitionArray& pg_keys)
{
  int ret = OB_SUCCESS;
  if (pkeys.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pkeys), K(pg_keys), K(table_locations));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      ObPGKey pg_key;
      if (table_locations.count() <= 0) {
        if (OB_FAIL(storage::ObPartitionService::get_instance().get_pg_key(pkeys.at(i), pg_key))) {
          LOG_WARN("get pg key error", K(ret), K(pkeys), K(pg_key));
        }
      } else if (OB_FAIL(get_pg_key_(table_locations, pkeys.at(i), pg_key))) {
        LOG_WARN("get pg key error", K(ret), "key", pkeys.at(i));
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        int64_t j = 0;
        for (; j < pg_keys.count(); ++j) {
          if (pg_key == pg_keys.at(j)) {
            break;
          }
        }
        if (j == pg_keys.count()) {
          if (OB_FAIL(pg_keys.push_back(pg_key))) {
            LOG_WARN("pg keys push back error", K(ret), K(pg_key), K(pg_keys));
          }
        }
      }
    }
  }

  return ret;
}

int ObSqlTransControl::change_pla_info_(const ObIArray<ObPhyTableLocation>& table_locations,
    const ObPartitionLeaderArray& pla, ObPartitionLeaderArray& out_pla)
{
  int ret = OB_SUCCESS;

  if (pla.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pla), K(table_locations));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pla.count(); ++i) {
      ObPGKey pg_key;
      if (table_locations.count() <= 0) {
        if (OB_FAIL(storage::ObPartitionService::get_instance().get_pg_key(pla.get_partitions().at(i), pg_key))) {
          LOG_WARN("get pg key error", K(ret), K(table_locations), K(pla));
        }
      } else if (OB_FAIL(get_pg_key_(table_locations, pla.get_partitions().at(i), pg_key))) {
        LOG_WARN("get pg key error", K(ret), K(table_locations), K(pla));
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        const ObAddr& addr = pla.get_leaders().at(i);
        const common::ObPartitionType& type = pla.get_types().at(i);
        int64_t j = 0;
        for (; j < out_pla.count(); ++j) {
          if (pg_key == out_pla.get_partitions().at(j) && addr == out_pla.get_leaders().at(j) &&
              type == out_pla.get_types().at(j)) {
            break;
          }
        }
        if (j == out_pla.count()) {
          if (OB_FAIL(out_pla.push(pg_key, addr, type))) {
            LOG_WARN("out pla push back error", K(pg_key), K(addr), K(type));
          }
        }
      }
    }
  }

  return ret;
}

int ObSqlTransControl::get_participants(
    const ObIArray<ObPhyTableLocation>& table_locations, ObPartitionLeaderArray& pla, bool is_retry_for_dup_tbl)
{
  int ret = OB_SUCCESS;
  uint64_t N = table_locations.count();
  ObPartitionKey key;
  common::ObPartitionType type;
  // SQL_LOG(DEBUG, "phy table location size:", K(N));
  for (uint64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObPartitionReplicaLocationIArray& part_locations = table_locations.at(i).get_partition_location_list();
    int64_t M = part_locations.count();
    bool is_dup = table_locations.at(i).is_duplicate_table();
    bool is_dup_not_dml = table_locations.at(i).is_duplicate_table_not_in_dml();
    for (int64_t j = 0; OB_SUCC(ret) && j < M; ++j) {
      if (is_dup) {
        if (part_locations.at(j).get_replica_location().is_follower()) {
          type = ObPartitionType::DUPLICATE_FOLLOWER_PARTITION;
        } else if (is_dup_not_dml && false == is_retry_for_dup_tbl) {
          type = ObPartitionType::DUPLICATE_FOLLOWER_PARTITION;
        } else {
          type = ObPartitionType::DUPLICATE_LEADER_PARTITION;
        }
      } else {
        type = ObPartitionType::NORMAL_PARTITION;
      }
      // LOG_INFO("partition location", K(is_dup), K(type), K(is_dup_not_dml), K(part_locations.at(j)));
      if (OB_FAIL(part_locations.at(j).get_partition_key(key))) {
        LOG_WARN("failed to get partition key", K(ret));
      } else if (OB_UNLIKELY(is_virtual_table(key.table_id_))) {
      } else if (OB_FAIL(append_participant_to_array_distinctly(
                     pla, key, type, part_locations.at(j).get_replica_location().server_))) {
        /*
         * In the case of multiple alias table corresponding to the same physical table, the
         * key we get could be duplicate. To avoid having same partition as participants more
         * than once, we need to check if we have already added the key in the list.
         */
        LOG_WARN("fail to push back participant", K(ret), K(i), K(key), K(is_dup));
      }
    } /* for */
  }   /* for */
  return ret;
}

int ObSqlTransControl::get_participants(ObExecContext& exec_ctx, ObPartitionLeaderArray& pla)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = exec_ctx.get_task_executor_ctx();
  ObSQLSessionInfo* my_session = GET_MY_SESSION(exec_ctx);
  if (OB_ISNULL(task_exec_ctx) || OB_ISNULL(my_session)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task executor ctx should not be NULL", K(my_session));
  } else if (OB_FAIL(get_participants(
                 task_exec_ctx->get_table_locations(), pla, my_session->get_is_in_retry_for_dup_tbl()))) {
    LOG_WARN("get participants failed", K(ret));
  }
  return ret;
}

int ObSqlTransControl::append_participant_to_array_distinctly(
    ObPartitionLeaderArray& pla, const ObPartitionKey& key, const common::ObPartitionType type, const ObAddr& svr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  // check if the key already exists
  for (int64_t i = 0; !found && i < pla.count(); i++) {
    if (pla.get_partitions().at(i) == key && pla.get_leaders().at(i) == svr) {
      found = true;
    }
  }
  // if not, add it
  if (!found) {
    ret = pla.push(key, svr, type);
  }
  return ret;
}

int ObSqlTransControl::get_participants(ObExecContext& exec_ctx, common::ObPartitionArray& participants)
{
  // DO NOT reset participants for init, see ObResultSet::merge_stmt_partitions().
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = exec_ctx.get_task_exec_ctx();
  const ObIArray<ObPhyTableLocation>& table_locations = task_exec_ctx.get_table_locations();
  uint64_t N = table_locations.count();
  SQL_LOG(DEBUG, "phy table location size:", K(N));
  for (uint64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObPartitionReplicaLocationIArray& part_locations = table_locations.at(i).get_partition_location_list();
    int64_t M = part_locations.count();
    for (int64_t j = 0; OB_SUCC(ret) && j < M; ++j) {
      // LOG_INFO("partition location", K(part_locations.at(j)));
      ObPartitionKey key;
      if (OB_FAIL(part_locations.at(j).get_partition_key(key))) {
        LOG_WARN("failed to get partition key", K(ret));
      } else if (OB_UNLIKELY(is_virtual_table(key.table_id_))) {
      } else if (OB_FAIL(append_participant_to_array_distinctly(participants, key))) {
        /*
         * In the case of multiple alias table corresponding to the same physical table, the
         * key we get could be duplicate. To avoid having same partition as participants more
         * than once, we need to check if we have already added the key in the list.
         */
        LOG_WARN("fail to push back participant", K(ret), K(i), K(j), K(key));
      }
    }
  }
  return ret;
}

int ObSqlTransControl::get_root_job_participants(
    ObExecContext& exec_ctx, const ObPhyOperator& root_job_root_op, ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = exec_ctx.get_task_exec_ctx();
  ObSEArray<TableLocationKey, 2> table_location_keys;
  OZ(ObTaskSpliter::find_all_table_location_keys(table_location_keys, root_job_root_op));
  for (int64_t i = 0; OB_SUCC(ret) && i < table_location_keys.count(); ++i) {
    const TableLocationKey& table_location_key = table_location_keys.at(i);
    const ObPhyTableLocation* table_loc = NULL;
    OZ(ObTaskExecutorCtxUtil::get_phy_table_location(
        task_exec_ctx, table_location_key.table_id_, table_location_key.ref_table_id_, table_loc));
    CK(OB_NOT_NULL(table_loc));
    OZ(append_participant_by_table_loc(participants, *table_loc));
  }
  return ret;
}

int ObSqlTransControl::get_root_job_participants(
    ObExecContext& exec_ctx, const ObOperator& root_job_root_op, ObPartitionArray& participants)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx& task_exec_ctx = exec_ctx.get_task_exec_ctx();
  ObSEArray<TableLocationKey, 2> table_location_keys;
  OZ(ObTaskSpliter::find_all_table_location_keys(table_location_keys, root_job_root_op.get_spec()));
  for (int64_t i = 0; OB_SUCC(ret) && i < table_location_keys.count(); ++i) {
    const TableLocationKey& table_location_key = table_location_keys.at(i);
    const ObPhyTableLocation* table_loc = NULL;
    OZ(ObTaskExecutorCtxUtil::get_phy_table_location(
        task_exec_ctx, table_location_key.table_id_, table_location_key.ref_table_id_, table_loc));
    CK(OB_NOT_NULL(table_loc));
    OZ(append_participant_by_table_loc(participants, *table_loc));
  }
  return ret;
}

int ObSqlTransControl::append_participant_by_table_loc(
    ObPartitionIArray& participants, const ObPhyTableLocation& table_loc)
{
  int ret = OB_SUCCESS;
  const ObPartitionReplicaLocationIArray& part_locations = table_loc.get_partition_location_list();
  int64_t M = part_locations.count();
  ObPartitionKey key;
  for (int64_t j = 0; OB_SUCC(ret) && j < M; ++j) {
    key.reset();
    if (OB_FAIL(part_locations.at(j).get_partition_key(key))) {
      LOG_WARN("failed to get partition key", K(ret));
    } else if (OB_UNLIKELY(true == is_virtual_table(key.table_id_))) {
    } else if (OB_FAIL(append_participant_to_array_distinctly(participants, key))) {
      /*
       * In the case of multiple alias table corresponding to the same physical table, the
       * key we get could be duplicate. To avoid having same partition as participants more
       * than once, we need to check if we have already added the key in the list.
       */
      LOG_WARN("fail to push back participant", K(ret), K(j), K(key));
    }
  }
  return ret;
}

int ObSqlTransControl::append_participant_to_array_distinctly(
    ObPartitionIArray& participants, const ObPartitionKey& key)
{
  int ret = OB_SUCCESS;
  bool found = false;
  // check if the key already exists
  for (int64_t i = 0; !found && i < participants.count(); i++) {
    if (participants.at(i) == key) {
      found = true;
    }
  }
  // if not, add it
  if (!found) {
    ret = participants.push_back(key);
  }
  return ret;
}

int ObSqlTransControl::inc_session_ref(const ObSQLSessionInfo* my_session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(my_session), K(GCTX.session_mgr_), K(ret));
  } else {
    ret = GCTX.session_mgr_->inc_session_ref(my_session);
  }
  return ret;
}

int ObSqlTransControl::merge_stmt_partitions(ObExecContext& exec_ctx, ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  ObPartitionArray total_partitions;
  // ObSqlTransControl::get_participants() can merge partitions.
  if (OB_FAIL(ObSqlTransControl::get_participants(exec_ctx, total_partitions))) {
    LOG_WARN("get participants failed", K(ret));
  } else if (OB_FAIL(session.get_trans_result().merge_total_partitions(total_partitions))) {
    LOG_WARN("merge total participants failed", K(ret));
  }
  return ret;
}

int ObSqlTransControl::get_stmt_snapshot_info(
    ObExecContext& exec_ctx, const bool is_cursor, transaction::ObTransDesc& trans_desc, ObTransSnapInfo& snap_info)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  transaction::ObTransService* txs = NULL;
  CK(OB_NOT_NULL(executor_ctx));
  CK(OB_NOT_NULL(ps = executor_ctx->get_partition_service()));
  CK(OB_NOT_NULL(txs = ps->get_trans_service()));
  OZ(txs->get_stmt_snapshot_info(is_cursor, trans_desc, snap_info));
  return ret;
}

int ObSqlTransControl::start_standalone_stmt(
    ObExecContext& exec_ctx, ObSQLSessionInfo& session_info, ObPhysicalPlanCtx& phy_plan_ctx, bool is_remote)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("start_standalone_stmt", "session_id", session_info.get_sessid());
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  storage::ObPartitionService* ps = NULL;
  transaction::ObTransService* txs = NULL;
  bool is_local_single_partition_stmt = false;
  const ObPhysicalPlan* phy_plan = phy_plan_ctx.get_phy_plan();
  transaction::ObStandaloneStmtDesc& standalone_stmt_desc = session_info.get_trans_desc().get_standalone_stmt_desc();

  int32_t consistency_level = ObTransConsistencyLevel::UNKNOWN;
  int32_t consistency_type = ObTransConsistencyType::UNKNOWN;
  int32_t read_snapshot_type = ObTransReadSnapshotType::UNKNOWN;
  int64_t auto_spec_snapshot_version = ObTransVersion::INVALID_TRANS_VERSION;
  CK(OB_NOT_NULL(executor_ctx));
  CK(OB_NOT_NULL(ps = executor_ctx->get_partition_service()));
  CK(OB_NOT_NULL(txs = ps->get_trans_service()));
  CK(OB_NOT_NULL(phy_plan));
  OZ(decide_trans_read_interface_specs("ObSqlTransControl::start_standalone_stmt",
      session_info,
      phy_plan_ctx.get_phy_plan()->get_stmt_type(),
      phy_plan_ctx.get_phy_plan()->get_literal_stmt_type(),
      phy_plan_ctx.get_phy_plan()->has_for_update(),
      phy_plan_ctx.get_phy_plan()->is_contain_inner_table(),
      phy_plan_ctx.get_consistency_level(),
      phy_plan_ctx.get_phy_plan()->need_consistent_snapshot(),
      consistency_level,
      consistency_type,
      read_snapshot_type));
  if (GCTX.is_standby_cluster()) {
    OZ(specify_stmt_snapshot_version_for_slave_cluster_sql_(session_info,
        phy_plan_ctx.get_phy_plan()->get_literal_stmt_type(),
        phy_plan_ctx.get_phy_plan()->is_contain_inner_table(),
        consistency_type,
        read_snapshot_type,
        auto_spec_snapshot_version));
  }
  if (OB_SUCC(ret) && session_info.has_valid_read_snapshot_version()) {
    auto_spec_snapshot_version = session_info.get_read_snapshot_version();
  }
  if (OB_SUCC(ret)) {
    common::ObPartitionLeaderArray in_pla;
    if (!(phy_plan->has_nested_sql() || OB_PHY_PLAN_UNCERTAIN == phy_plan->get_location_type())) {
      OZ(ObSqlTransControl::get_participants(exec_ctx, in_pla));
      if (OB_SUCC(ret)) {
        is_local_single_partition_stmt = (in_pla.count() == 1 && in_pla.get_leaders().at(0) == MYADDR &&
                                          read_snapshot_type == ObTransReadSnapshotType::STATEMENT_SNAPSHOT);
      }
    }
    OZ(standalone_stmt_desc.init(txs->get_server(),
        session_info.get_effective_tenant_id(),
        get_stmt_timeout_ts(phy_plan_ctx),
        session_info.get_trx_lock_timeout(),
        is_local_single_partition_stmt,
        consistency_type,
        read_snapshot_type,
        is_local_single_partition_stmt ? in_pla.get_partitions().at(0) : ObPartitionKey()));
  }
  if (OB_FAIL(ret)) {
  } else if (standalone_stmt_desc.is_bounded_staleness_read() && !ObWeakReadUtil::check_weak_read_service_available()) {
    if (ObSqlTransUtil::plan_can_start_trans(session_info.get_local_autocommit(), session_info.get_in_transaction())) {
      OZ(implicit_start_trans(exec_ctx, is_remote));
    }
  } else {
    OZ(txs->get_stmt_snapshot_info(session_info.get_trans_desc(), auto_spec_snapshot_version));
    if (standalone_stmt_desc.is_bounded_staleness_read()) {
      OZ(update_safe_weak_read_snapshot(standalone_stmt_desc.is_bounded_staleness_read(),
          session_info,
          read_snapshot_type,
          standalone_stmt_desc.get_snapshot_version()));
    }
  }
  return ret;
}

bool ObSqlTransControl::is_isolation_RR_or_SE(int32_t isolation)
{
  return (isolation == ObTransIsolation::REPEATABLE_READ || isolation == ObTransIsolation::SERIALIZABLE);
}

int ObSqlTransControl::start_cursor_stmt(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* executor_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx);
  storage::ObPartitionService* ps = nullptr;
  transaction::ObTransService* txs = nullptr;
  ObSQLSessionInfo* session = nullptr;
  ObPhysicalPlanCtx* plan_ctx = nullptr;
  CK(OB_NOT_NULL(executor_ctx));
  CK(OB_NOT_NULL(ps = executor_ctx->get_partition_service()));
  CK(OB_NOT_NULL(txs = ps->get_trans_service()));
  CK(OB_NOT_NULL(session = exec_ctx.get_my_session()));
  CK(OB_NOT_NULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx)));
  OZ(txs->start_cursor_stmt(session->get_trans_desc(), plan_ctx->get_trans_timeout_timestamp()));
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
