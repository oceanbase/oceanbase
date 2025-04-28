/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX TABLELOCK
#include "storage/tablelock/ob_lock_executor.h"

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/alloc/alloc_assist.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/ob_table_access_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_common_id_utils.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"

namespace oceanbase
{
using namespace sql;
using namespace transaction;
using namespace common;
using namespace observer;

namespace transaction
{

namespace tablelock
{

int ObLockContext::init(ObExecContext &ctx,
                        const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = nullptr;

  if (OB_ISNULL(session_info = ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info is null in ObExecContext", K(ret));
  } else {
    // use smaller timeout if we specified the lock timeout us.
    if (timeout_us > 0
        && (ObTimeUtility::current_time() + timeout_us) < THIS_WORKER.get_timeout_ts()) {
      OX (old_worker_timeout_ts_ = THIS_WORKER.get_timeout_ts());
      OX (THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + timeout_us));
      if (OB_SUCC(ret) && OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
        old_phy_plan_timeout_ts_ = ctx.get_physical_plan_ctx()->get_timeout_timestamp();
        ctx.get_physical_plan_ctx()
          ->set_timeout_timestamp(ObTimeUtility::current_time() + timeout_us);
      }
    }
    if (OB_SUCC(ret)) {
      if (session_info->get_local_autocommit()) {
        OX (reset_autocommit_ = true);
        OZ (session_info->set_autocommit(false));
      }
      has_inner_dml_write_ = session_info->has_exec_inner_dml();
      last_insert_id_ = session_info->get_local_last_insert_id();
      session_info->set_has_exec_inner_dml(false);

      ObTransID parent_tx_id;
      int64_t parent_tx_start_ts = 0;
      parent_tx_id = session_info->get_tx_id();
      transaction::ObTxDesc *desc = session_info->get_tx_desc();
      if (OB_NOT_NULL(desc)) {
        parent_tx_start_ts = desc->get_active_ts();
      }
      OZ (session_info->begin_autonomous_session(saved_session_));
      OX (have_saved_session_ = true);
      OZ (ObSqlTransControl::explicit_start_trans(ctx, false));
      if (OB_SUCC(ret)) {
        has_autonomous_tx_ = true;
      }
      if (OB_SUCC(ret) && parent_tx_id.is_valid()) {
        (void) register_for_deadlock_(*session_info, parent_tx_id, parent_tx_start_ts);
      }
    }
    OX (my_exec_ctx_ = &ctx);
    OZ (open_inner_conn_());
  }
  return ret;
}

int ObLockContext::destroy(ObExecContext &ctx,
                           bool is_rollback)
{
  int tmp_ret = OB_SUCCESS;
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = nullptr;

  if (OB_ISNULL(session_info = ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session_info is null in ObExecContext", K(ret));
  } else {
    if (has_autonomous_tx_) {
      if (OB_TMP_FAIL(implicit_end_trans_(*session_info, ctx, is_rollback))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
    if (OB_TMP_FAIL(close_inner_conn_())) {
      LOG_WARN("close inner connection failed", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (have_saved_session_) {
      if (OB_TMP_FAIL(session_info->end_autonomous_session(saved_session_))) {
        LOG_WARN("failed to switch trans", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }

    // WHY WE NEED THIS
    uint64_t cur_last_insert_id = session_info->get_local_last_insert_id();
    if (cur_last_insert_id != last_insert_id_) {
      ObObj last_insert_id;
      last_insert_id.set_uint64(last_insert_id_);
      tmp_ret = session_info->update_sys_variable(SYS_VAR_LAST_INSERT_ID, last_insert_id);
      if (OB_SUCCESS == tmp_ret &&
          OB_TMP_FAIL(session_info->update_sys_variable(SYS_VAR_IDENTITY, last_insert_id))) {
        LOG_WARN("succ update last_insert_id, but fail to update identity", K(tmp_ret));
      }
      ret = COVER_SUCC(tmp_ret);
    }
    session_info->set_has_exec_inner_dml(has_inner_dml_write_);
    if (old_worker_timeout_ts_ != 0) {
      THIS_WORKER.set_timeout_ts(old_worker_timeout_ts_);
      if (OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
        ctx.get_physical_plan_ctx()->set_timeout_timestamp(old_phy_plan_timeout_ts_);
      }
    }
    if (reset_autocommit_) {
      if (OB_TMP_FAIL(session_info->set_autocommit(true))) {
        ret = COVER_SUCC(tmp_ret);
        LOG_ERROR("restore autocommit value failed", K(tmp_ret), K(ret));
      }
    }
  }
  return ret;
}

int ObLockContext::implicit_end_trans_(ObSQLSessionInfo &session_info,
                                       ObExecContext &ctx,
                                       bool is_rollback,
                                       bool can_async)
{
  int ret = OB_SUCCESS;
  bool is_async = false;
  if (session_info.is_in_transaction()) {
    is_async = !is_rollback && ctx.is_end_trans_async() && can_async;
    if (!is_async) {
      if (OB_FAIL(ObSqlTransControl::implicit_end_trans(ctx, is_rollback))) {
        LOG_WARN("failed to implicit end trans with sync callback", K(ret));
      }
    } else {
      ObEndTransAsyncCallback &callback = session_info.get_end_trans_cb();
      if (OB_FAIL(ObSqlTransControl::implicit_end_trans(ctx, is_rollback, &callback))) {
        LOG_WARN("failed implicit end trans with async callback", K(ret));
      }
      ctx.get_trans_state().set_end_trans_executed(OB_SUCCESS == ret);
    }
  } else {
    ObSqlTransControl::reset_session_tx_state(&session_info, true);
    ctx.set_need_disconnect(false);
  }
  LOG_TRACE("lock function implicit_end_trans", K(is_async), K(session_info),
            K(can_async), K(is_rollback));
  return ret;
}

int ObLockContext::valid_execute_context(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  CK (OB_NOT_NULL(ctx.get_package_guard()));
  return ret;
}

void ObLockContext::register_for_deadlock_(ObSQLSessionInfo &session_info,
                                           const ObTransID &parent_tx_id,
                                           const int64_t parent_tx_start_ts)
{
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  ObTransID child_tx_id = session_info.get_tx_id();

  if (parent_tx_id != child_tx_id &&
      parent_tx_id.is_valid() &&
      child_tx_id.is_valid()) {
    if (OB_FAIL(session_info.get_query_timeout(query_timeout))) {
      LOG_WARN("get query timeout failed", K(parent_tx_id), K(child_tx_id), KR(ret));
    } else {
      if (OB_FAIL(ObTransDeadlockDetectorAdapter::
                  autonomous_register_to_deadlock(parent_tx_id,
                                                  child_tx_id,
                                                  parent_tx_start_ts,
                                                  query_timeout))) {
        LOG_WARN("autonomous register to deadlock failed", K(parent_tx_id),
                 K(child_tx_id), KR(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not register to deadlock", K(ret), K(parent_tx_id), K(child_tx_id));
  }
}

int ObLockContext::open_inner_conn_()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  common::ObMySQLProxy *sql_proxy = nullptr;
  observer::ObInnerSQLConnection *inner_conn = nullptr;

  if (OB_ISNULL(my_exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObExecContext in ObLockFuncContext is null", K(ret));
  } else if (OB_ISNULL(session = my_exec_ctx_->get_my_session()) || OB_ISNULL(sql_proxy = my_exec_ctx_->get_sql_proxy())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session or sql_proxy in ObExecContext is NULL", K(ret), KP(session), KP(sql_proxy));
  } else if (OB_NOT_NULL(inner_conn_) || OB_NOT_NULL(store_inner_conn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_conn_ or store_inner_conn_ should be null", K(ret), KP(inner_conn_), KP(store_inner_conn_));
  } else if (FALSE_IT(store_inner_conn_ = static_cast<observer::ObInnerSQLConnection *>(session->get_inner_conn()))) {
  } else if (FALSE_IT(session->set_inner_conn(nullptr))) {
  } else if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session, sql_proxy, inner_conn))) {
    LOG_WARN("create inner connection failed", K(ret), KPC(session));
  } else if (OB_ISNULL(inner_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner connection is still null", KPC(session));
  } else {
    /**
     * session is the only data struct which can pass through multi layer nested sql,
     * so we put inner conn in session to share it within multi layer nested sql.
     */
    inner_conn_ = inner_conn;
    session->set_inner_conn(inner_conn);
    LOG_DEBUG("ObLockFuncContext::open_inner_conn_ successfully",
              KP(inner_conn_),
              KP(store_inner_conn_),
              K(inner_conn_->is_oracle_compat_mode()));
  }
  return ret;
}

int ObLockContext::close_inner_conn_()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  common::ObMySQLProxy *sql_proxy = nullptr;

  if (OB_ISNULL(my_exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObExecContext in ObLockFuncContext is null", K(ret));
  } else {
    if (OB_ISNULL(sql_proxy = my_exec_ctx_->get_sql_proxy()) || OB_ISNULL(inner_conn_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql_proxy or inner_conn of session is NULL", K(ret), KP(sql_proxy), KP(session), KP(inner_conn_));
    } else {
      OZ (sql_proxy->close(inner_conn_, true));
    }
    if (OB_ISNULL(session = my_exec_ctx_->get_my_session())) {
      ret = OB_NOT_INIT;
      LOG_WARN("session is NULL", K(ret), KP(session));
    } else if (OB_NOT_NULL(inner_conn_) || OB_NOT_NULL(store_inner_conn_)) {
      // 1. if inner_conn_ is not null, means that we have created inner_conn successfully before, so we must have already
      // set store_inner_conn_ successfully, just restore it to the session.
      // 2. if inner_conn_ is null, it's uncertain whether store_inner_conn_ has been set before. If store_inner_conn_
      // is not null, it must have been set. Otherwise, the inner_conn on the session may be null, or it may have existed
      // with an error code before store_inner_conn_ being set. At this case, we do not set inner_conn on the session.
      session->set_inner_conn(store_inner_conn_);
    }
  }
  inner_conn_ = nullptr;
  store_inner_conn_ = nullptr;
  return ret;
}

int ObLockContext::execute_write(const ObSqlString &sql,
                                 int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;

  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_write_sql(inner_conn_, sql, affected_rows))) {
    LOG_WARN("execute write sql failed", K(ret));
  }
  return ret;
}

int ObLockContext::execute_read(const ObSqlString &sql,
                                ObMySQLProxy::MySQLResult &res)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(inner_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inner connection is NULL", K(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_read_sql(inner_conn_, sql, res))) {
    LOG_WARN("execute read sql failed", K(ret));
  }
  return ret;
}

bool ObLockExecutor::proxy_is_support(sql::ObExecContext &exec_ctx)
{
  return proxy_is_support(exec_ctx.get_my_session());
}

bool ObLockExecutor::proxy_is_support(sql::ObSQLSessionInfo *session)
{
  bool is_support = false;
  if (OB_ISNULL(session)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "session is null!");
  } else {
    is_support = ((session->is_feedback_proxy_info_support() && session->is_client_sessid_support())
                  || !session->is_obproxy_mode())
                 && session->get_client_sid() != INVALID_SESSID;
    if (!is_support) {
      LOG_WARN_RET(OB_NOT_SUPPORTED,
                   "proxy is not support this feature",
                   K(session->get_server_sid()),
                   K(session->is_feedback_proxy_info_support()),
                   K(session->is_client_sessid_support()));
    }
  }
  return is_support;
}

int ObLockExecutor::check_lock_exist_(const uint64_t &lock_id)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObStringHolder query_lock_handle;
  char where_cond[WHERE_CONDITION_BUFFER_SIZE] = {0};
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  bool is_existed = false;

  OZ (databuff_printf(where_cond,
                      WHERE_CONDITION_BUFFER_SIZE,
                      "WHERE obj_id = %" PRIu64
                      " and obj_type = %d",
                      lock_id,
                      static_cast<int>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC)));
  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_V2_TNAME));
  OZ (ObTableAccessHelper::read_single_row(tenant_id,
                                           { "1" },
                                           table_name,
                                           where_cond,
                                           is_existed));
  return ret;
}

int ObLockExecutor::check_lock_exist_(ObLockContext &ctx,
                                      ObSqlString &where_cond,
                                      bool &exist)
{
  int ret = OB_SUCCESS;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};

  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_V2_TNAME));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT owner_id FROM %s WHERE %s",
                                 table_name,
                                 where_cond.ptr()))) {
        LOG_WARN("fail to assign fmt", KR(ret));
      } else if (OB_FAIL(ctx.execute_read(sql, res))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          exist = false;
        } else {
          LOG_WARN("fail to get next", KR(ret));
        }
      } else {
        exist = true;
      }
    } // end SMART_VAR
  }
  return ret;
}

int ObLockExecutor::check_lock_exist_(ObLockContext &ctx,
                                      const int64_t raw_owner_id,
                                      const uint64_t &lock_id,
                                      bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  if (OB_FAIL(where_cond.assign_fmt("obj_id = %ld AND owner_id = %ld",
                                    lock_id,
                                    raw_owner_id))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(check_lock_exist_(ctx, where_cond, exist))) {
    LOG_WARN("check lock exist failed", K(ret));
  }
  return ret;
}

int ObLockExecutor::check_lock_exist_(ObLockContext &ctx,
                                      const uint64_t &lock_id,
                                      bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  if (OB_FAIL(where_cond.assign_fmt("obj_id = %ld", lock_id))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(check_lock_exist_(ctx, where_cond, exist))) {
    LOG_WARN("check lock exist failed", K(ret));
  }
  return ret;
}

int ObLockExecutor::check_owner_exist_(ObLockContext &ctx,
                                       const uint32_t client_session_id,
                                       const uint64_t client_session_create_ts,
                                       bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  ObTableLockOwnerID lock_owner;
  int64_t raw_id = 0;

  OZ (lock_owner.convert_from_client_sessid(client_session_id, client_session_create_ts));
  if (client_session_create_ts > 0) {
    OZ (where_cond.assign_fmt("owner_id = %" PRId64, lock_owner.id()));
  } else {
    // if client_session_create_ts <= 0, means there's no accurate client_session_create_ts
    // (from lock live detector), so we only judge client_session_id in this situation
    OZ (where_cond.assign_fmt("(owner_id & %" PRId64 ") = %" PRIu32, ObTableLockOwnerID::CLIENT_SESS_ID_MASK, client_session_id));
  }
  OZ (check_lock_exist_(ctx, where_cond, exist));
  return ret;
}

int ObLockExecutor::check_client_ssid(ObLockContext &ctx,
                                      const uint32_t client_session_id,
                                      const uint64_t client_session_create_ts)
{
  int ret = OB_SUCCESS;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  int64_t record_client_session_create_ts = 0;
  ObSqlString sql;
  ObTableLockOwnerID owner_id;
  common::sqlclient::ObMySQLResult *result = nullptr;

  OZ (owner_id.convert_from_client_sessid(client_session_id, client_session_create_ts));
  OZ (databuff_printf(
    table_name, MAX_FULL_TABLE_NAME_LENGTH, "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME));
  OZ (sql.assign_fmt("SELECT time_to_usec(client_session_create_ts)"
                     " FROM %s WHERE client_session_id = %" PRIu32,
                     table_name,
                     client_session_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    OZ (ctx.execute_read(sql, res));
    OV (OB_NOT_NULL(result = res.get_result()), OB_ERR_UNEXPECTED, client_session_id);
    OZ (result->next());
    // there's no record, means the client_sessid is not used before, or has been cleaned
    if (OB_ITER_END == ret) {
      ret = OB_EMPTY_RESULT;
    }
    OX (GET_COL_IGNORE_NULL(result->get_int,
                            "time_to_usec(client_session_create_ts)",
                            record_client_session_create_ts));
  }
  OX(
    if (OB_UNLIKELY(record_client_session_create_ts != client_session_create_ts)) {
      ObTableLockOwnerID rec_owner_id;
      ObTableLockOwnerID cur_owner_id;
      OZ (rec_owner_id.convert_from_client_sessid(client_session_id, record_client_session_create_ts));
      OZ (cur_owner_id.convert_from_client_sessid(client_session_id, client_session_create_ts));
      if (rec_owner_id == cur_owner_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("meet client_session_id reuse, and has the same owner_id", K(rec_owner_id), K(cur_owner_id));
      } else if (record_client_session_create_ts > client_session_create_ts) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("there's a client_session with larger create_ts",
                K(client_session_id),
                K(record_client_session_create_ts),
                K(client_session_create_ts));
      } else if (record_client_session_create_ts < client_session_create_ts) {
        int tmp_ret = OB_SUCCESS;
        LOG_INFO("meet reuse client_session_id, will recycle the eariler one",
                K(client_session_id),
                K(record_client_session_create_ts),
                K(client_session_create_ts));
        // Although the client_session_id is consistent, there is a high probability that the owner_id will not be
        // consistent. Therefore, the failure to recycle here will not affect the subsequent locking process
        if (OB_TMP_FAIL(ObTableLockDetector::remove_lock_by_owner_id(rec_owner_id))) {
          LOG_WARN("recycle old lock with the same client_session_id failed, keep locking process",
                   K(tmp_ret),
                   K(client_session_id),
                   K(record_client_session_create_ts),
                   K(client_session_create_ts));
        }
      }
    });
  return ret;
}

int ObLockExecutor::remove_session_record(ObLockContext &ctx,
                                          const uint32_t client_session_id,
                                          const uint64_t client_session_create_ts)
{
  int ret = OB_SUCCESS;
  bool owner_exist = false;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  ObTableLockOwnerID lock_owner;
  ObSqlString delete_sql;
  int64_t affected_rows = 0;
  ObSQLSessionInfo *session = nullptr;

  OZ (check_owner_exist_(ctx, client_session_id, client_session_create_ts, owner_exist));
  if (OB_SUCC(ret) && !owner_exist) {
    lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
    OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                        "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME));
    OZ (delete_sql.assign_fmt("DELETE FROM %s WHERE client_session_id = %" PRIu32,
                              table_name,
                              client_session_id));
    OZ (ctx.execute_write(delete_sql, affected_rows));
    OV (OB_NOT_NULL(ctx.my_exec_ctx_), OB_INVALID_ARGUMENT);
    OV (OB_NOT_NULL(session = ctx.my_exec_ctx_->get_my_session()), OB_INVALID_ARGUMENT);
    OX (mark_lock_session_(session, false));
  }
  return ret;
}

int ObLockExecutor::unlock_obj_(ObTxDesc *tx_desc,
                                const ObTxParam &tx_param,
                                const ObUnLockObjsRequest &arg)
{
  int ret = OB_SUCCESS;
  ObTableLockService *lock_service = MTL(ObTableLockService *);
  if (OB_FAIL(lock_service->unlock_obj(*tx_desc, tx_param, arg))) {
    LOG_WARN("unlock obj failed", K(ret), KPC(tx_desc), K(arg));
  }
  return ret;
}

int ObLockExecutor::unlock_table_(ObTxDesc *tx_desc,
                                  const ObTxParam &tx_param,
                                  const ObUnLockTableRequest &arg)
{
  int ret = OB_SUCCESS;
  ObTableLockService *lock_service = MTL(ObTableLockService *);
  if (OB_FAIL(lock_service->unlock_table(*tx_desc, tx_param, arg))) {
    LOG_WARN("unlock obj failed", K(ret), KPC(tx_desc), K(arg));
  }
  return ret;
}

int ObLockExecutor::query_lock_id_(const ObString &lock_name,
                                   uint64_t &lock_id)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObStringHolder query_lock_handle;
  // 1. check if there's a lock with the same lock name
  char where_cond[WHERE_CONDITION_BUFFER_SIZE] = {0};
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  // generate corresponding lock handle for the lock name,
  // and insert them into the inner table DBMS_LOCK_ALLOCATED

  lock_id = OB_INVALID_OBJECT_ID;

  OZ (databuff_printf(where_cond, WHERE_CONDITION_BUFFER_SIZE,
                      "WHERE name = '%.*s'", lock_name.length(), lock_name.ptr()));
  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DBMS_LOCK_ALLOCATED_TNAME));
  OZ (ObTableAccessHelper::read_single_row(tenant_id,
                                           { "lockhandle" },
                                           table_name,
                                           where_cond,
                                           query_lock_handle));
  if (OB_EMPTY_RESULT == ret) {
    // there is no lock name.
  } else if (OB_SUCC(ret)) {
    OZ (extract_lock_id_(query_lock_handle.get_ob_string(), lock_id));
  }
  return ret;
}

int ObLockExecutor::query_lock_id_and_lock_handle_(const ObString &lock_name,
                                                   uint64_t &lock_id,
                                                   char *lock_handle_buf)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObStringHolder query_lock_handle;
  // 1. check if there's a lock with the same lock name
  char where_cond[WHERE_CONDITION_BUFFER_SIZE] = {0};
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  int64_t lock_handle_len = 0;
  // generate corresponding lock handle for the lock name,
  // and insert them into the inner table DBMS_LOCK_ALLOCATED

  lock_id = OB_INVALID_OBJECT_ID;

  OZ (databuff_printf(where_cond, WHERE_CONDITION_BUFFER_SIZE,
                      "WHERE name = '%.*s'", lock_name.length(), lock_name.ptr()));
  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DBMS_LOCK_ALLOCATED_TNAME));
  OZ (ObTableAccessHelper::read_single_row(tenant_id,
                                           { "lockhandle" },
                                           table_name,
                                           where_cond,
                                           query_lock_handle));
  if (OB_EMPTY_RESULT == ret) {
    // there is no lock name.
  } else if (OB_SUCC(ret)) {
    ObString lock_handle_string = query_lock_handle.get_ob_string();
    OZ (extract_lock_id_(lock_handle_string, lock_id));
    OV (lock_handle_string.length() < MAX_LOCK_HANDLE_LEGNTH, OB_INVALID_ARGUMENT, lock_handle_string);
    OX (MEMCPY(lock_handle_buf, lock_handle_string.ptr(), lock_handle_string.length()));
    lock_handle_buf[lock_handle_string.length()] = '\0';
  }
  return ret;
}

int ObLockExecutor::extract_lock_id_(const ObString &lock_handle,
                                     uint64_t &lock_id)
{
  int ret = OB_SUCCESS;

  OV (lock_handle.is_numeric(), OB_INVALID_ARGUMENT, lock_handle);
  OV (lock_handle.length() >= LOCK_ID_LENGTH, OB_INVALID_ARGUMENT, lock_handle, lock_handle.length());
  OX (lock_id = ObFastAtoi<uint64_t>::atoi_positive_unchecked(lock_handle.ptr(), lock_handle.ptr() + LOCK_ID_LENGTH));
  OV (lock_id >= MIN_LOCK_HANDLE_ID && lock_id <= MAX_LOCK_HANDLE_ID, OB_INVALID_ARGUMENT, lock_id);

  return ret;
}

int ObLockExecutor::query_lock_owner_(const uint64_t &lock_id,
                                      int64_t &owner_id,
                                      int64_t &owner_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  char where_cond[WHERE_CONDITION_BUFFER_SIZE] = {0};
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};

  OZ (databuff_printf(where_cond, WHERE_CONDITION_BUFFER_SIZE,
                      "WHERE obj_type = '%d' AND"
                      " obj_id = %ld AND lock_mode = %d",
                      static_cast<int>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC),
                      lock_id,
                      static_cast<int>(EXCLUSIVE)));
  OZ (databuff_printf(table_name,
                      MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_V2_TNAME));
  OZ (ObTableAccessHelper::read_single_row(tenant_id,
                                           { "owner_id", "owner_type" },
                                           table_name,
                                           where_cond,
                                           owner_id,
                                           owner_type));
  if (OB_EMPTY_RESULT == ret) {
    // there is no lock of the lock id.
  } else if (OB_FAIL(ret)) {
    LOG_WARN("get lock owner failed", K(owner_id));
  } else {
    // we have get the owner_id.
  }
  return ret;
}

void ObLockExecutor::mark_lock_session_(sql::ObSQLSessionInfo *session,
                                        const bool is_lock_session)
{
  if (session->is_lock_session() != is_lock_session) {
    LOG_INFO("mark lock_session", K(session->get_server_sid()), K(is_lock_session));
    session->set_is_lock_session(is_lock_session);
    session->set_need_send_feedback_proxy_info(true);
  } else {
    LOG_DEBUG("the lock_session status on the session won't be changed, no need to mark again",
              K(session->get_server_sid()),
              K(session->is_lock_session()),
              K(session->is_need_send_feedback_proxy_info()));
  }
}

int ObLockExecutor::remove_expired_lock_id()
{
  int ret = OB_SUCCESS;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  char where_cond[WHERE_CONDITION_BUFFER_SIZE] = {0};
  const int64_t now = ObTimeUtility::current_time();
  // delete 10 rows each time, to avoid causing abnormal delays due to deleting too many rows
  const int delete_limit = 10;

  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DBMS_LOCK_ALLOCATED_TNAME));
  OZ (databuff_printf(where_cond,
                      WHERE_CONDITION_BUFFER_SIZE,
                      "expiration <= usec_to_time(%" PRId64
                      ") AND lockid NOT IN (SELECT obj_id FROM %s.%s where obj_type = %d or obj_type = %d)"
                      "LIMIT %d",
                      now,
                      OB_SYS_DATABASE_NAME,
                      OB_ALL_DETECT_LOCK_INFO_V2_TNAME,
                      static_cast<int>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC),
                      static_cast<int>(ObLockOBJType::OBJ_TYPE_DBMS_LOCK),
                      delete_limit));
  OZ (ObTableAccessHelper::delete_row(MTL_ID(), table_name, where_cond));
  return ret;
}

int ObLockExecutor::get_lock_session_(ObLockContext &ctx,
                                      const uint32_t client_session_id,
                                      const uint64_t client_session_create_ts,
                                      ObAddr &lock_session_addr,
                                      uint32_t &lock_session_id)
{
  int ret = OB_SUCCESS;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {'\0'};
  OZ (databuff_printf(
     table_name, MAX_FULL_TABLE_NAME_LENGTH, "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME));
  OX (
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *result = nullptr;
      OZ (sql.assign_fmt("SELECT svr_ip, svr_port, server_session_id"
                         " FROM %s WHERE client_session_id = %" PRIu32 " AND client_session_create_ts = %" PRIu64,
                         table_name,
                         client_session_id,
                         client_session_create_ts));
      OZ (ctx.execute_read(sql, res));
      OV (OB_NOT_NULL(result = res.get_result()), OB_ERR_UNEXPECTED, client_session_id);
      OZ (get_first_session_info_(*result, lock_session_addr, lock_session_id));
    }  // end SMART_VAR
  )
  return ret;
}

int ObLockExecutor::get_first_session_info_(common::sqlclient::ObMySQLResult &res,
                                            ObAddr &session_addr,
                                            uint32_t &server_session_id)
{
  int ret = OB_SUCCESS;
  ObString svr_ip;
  int64_t svr_port;
  uint64_t tmp_session_id = 0;

  OZ (res.next());
  if (OB_ITER_END == ret) {
    ret = OB_EMPTY_RESULT;
  }
  OX (GET_COL_IGNORE_NULL(res.get_varchar, "svr_ip", svr_ip));
  OX (GET_COL_IGNORE_NULL(res.get_int, "svr_port", svr_port));
  OX (GET_COL_IGNORE_NULL(res.get_uint, "server_session_id", tmp_session_id));
  OX (server_session_id = static_cast<uint32_t>(tmp_session_id));
  OX (session_addr.reset());
  OV (session_addr.set_ip_addr(svr_ip, svr_port), OB_ERR_UNEXPECTED, svr_ip, svr_port);

  return ret;
}

int ObLockExecutor::update_session_table_(ObLockContext &ctx,
                                          const uint32_t client_session_id,
                                          const uint64_t client_session_create_ts,
                                          const uint32_t server_session_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer insert_dml;
  ObSqlString insert_sql;
  char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const ObAddr &self_addr = GCTX.self_addr();

  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME));
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  OV (self_addr.ip_to_string(svr_ip, MAX_IP_ADDR_LENGTH), OB_INVALID_ARGUMENT, self_addr);
  OZ (insert_dml.add_gmt_create(now));
  OZ (insert_dml.add_gmt_modified(now));
  OZ (insert_dml.add_pk_column("server_session_id", server_session_id));
  OZ (insert_dml.add_column("client_session_id", client_session_id));
  OZ (insert_dml.add_time_column("client_session_create_ts", client_session_create_ts));
  OZ (insert_dml.add_column("svr_ip", svr_ip));
  OZ (insert_dml.add_column("svr_port", self_addr.get_port()));
  OZ (insert_dml.splice_insert_update_sql(table_name,
                                          insert_sql));
  OZ (ctx.execute_write(insert_sql, affected_rows));
  CK (OB_LIKELY(1 == affected_rows || 2 == affected_rows));

  return ret;
}

int ObLockExecutor::get_sql_port_(ObLockContext &ctx,
                                  const ObAddr &svr_addr,
                                  int32_t &sql_port)
{
  int ret = OB_SUCCESS;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {'\0'};
  char svr_ip[MAX_IP_ADDR_LENGTH] = {'\0'};
  int32_t svr_port = svr_addr.get_port();
  int64_t tmp_sql_port = 0;

  OZ (databuff_printf(
     table_name, MAX_FULL_TABLE_NAME_LENGTH, "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_VIRTUAL_LS_META_TABLE_TNAME));
  OV (svr_addr.ip_to_string(svr_ip, MAX_IP_ADDR_LENGTH), OB_ERR_UNEXPECTED, svr_addr);
  OX (
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *result = nullptr;
      OZ (sql.assign_fmt("SELECT sql_port FROM %s"
                        " WHERE svr_ip = '%s' AND svr_port = %" PRId32 " LIMIT 1",
                        table_name, svr_ip, svr_port));
      OZ (ctx.execute_read(sql, res));
      OV (OB_NOT_NULL(result = res.get_result()), OB_ERR_UNEXPECTED, svr_addr, svr_ip, svr_port);
      OZ (result->next());
      OX (GET_COL_IGNORE_NULL(result->get_int, "sql_port", tmp_sql_port));
    }  // end SMART_VAR
  )
  OX (sql_port = static_cast<int32_t>(tmp_sql_port));
  return ret;
}

int ObLockExecutor::check_need_reroute_(ObLockContext &ctx,
                                        const uint32_t client_session_id,
                                        const uint64_t client_session_create_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObAddr lock_session_addr;
  uint32_t lock_session_id = 0;
  int32_t sql_port = 0;
  ObSqlCtx *sql_ctx = nullptr;
  ObSQLSessionInfo *session = nullptr;

  OV (OB_NOT_NULL(ctx.my_exec_ctx_), OB_INVALID_ARGUMENT);
  OV (OB_NOT_NULL(sql_ctx = ctx.my_exec_ctx_->get_sql_ctx()) &&
      OB_NOT_NULL(session = ctx.my_exec_ctx_->get_my_session()),
      OB_NOT_INIT);
  OX (
    if (!session->is_lock_session()) {
      OZ (get_lock_session_(ctx, client_session_id, client_session_create_ts, lock_session_addr, lock_session_id));
      // no lock_session in this client, continue
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
      // get lock_session successfully, compare with current session
      } else if (OB_SUCCESS == ret) {
        OV (lock_session_addr == GCTX.self_addr(), OB_ERR_PROXY_REROUTE, lock_session_addr, GCTX.self_addr());
        // can not reroute in one observer, so just return OB_SUCCESS
        OV (lock_session_id == session->get_server_sid(), OB_SUCCESS, lock_session_id, session->get_server_sid());
        // to avoid this session wasn't marked as lock_session before
        if (lock_session_addr == GCTX.self_addr() && lock_session_id == session->get_server_sid()) {
          mark_lock_session_(session, true);
        }
      }

      if (OB_ERR_PROXY_REROUTE == ret) {
        if (OB_TMP_FAIL(get_sql_port_(ctx, lock_session_addr, sql_port))) {
          LOG_WARN("can not get sql_port, reroute to this server this time", K(tmp_ret), K(lock_session_addr), K(GCTX.self_addr()));
          sql_ctx->get_or_create_reroute_info()->server_ = GCTX.self_addr();
          sql_ctx->get_reroute_info()->server_.set_port(GCONF.mysql_port);
          sql_ctx->get_reroute_info()->for_session_reroute_ = true;
        } else {
          sql_ctx->get_or_create_reroute_info()->server_ = lock_session_addr;
          sql_ctx->get_reroute_info()->server_.set_port(sql_port);
          sql_ctx->get_reroute_info()->for_session_reroute_ = true;
        }
      }
    }
  );
  return ret;
}

int ObUnLockExecutor::execute(ObExecContext &ctx,
                              const int64_t release_type,
                              int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint32_t client_session_id = 0;
  uint64_t client_session_create_ts = 0;
  bool is_rollback = false;
  OZ (ObLockContext::valid_execute_context(ctx));
  OX (client_session_id = ctx.get_my_session()->get_client_sid());
  OX (client_session_create_ts = ctx.get_my_session()->get_client_create_time());
  OZ (execute_(ctx,
               client_session_id,
               client_session_create_ts,
               release_type,
               release_cnt));
  return ret;
}

int ObUnLockExecutor::execute(const ObTableLockOwnerID &owner_id)
{
  int ret = OB_SUCCESS;
  int64_t release_cnt = 0;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
  SMART_VAR(sql::ObSQLSessionInfo, session) {
    SMART_VAR(sql::ObExecContext, exec_ctx, allocator) {
      ObSqlCtx sql_ctx;
      uint64_t tenant_id = MTL_ID();
      const ObTenantSchema *tenant_schema = NULL;
      ObSchemaGetterGuard guard;
      LinkExecCtxGuard link_guard(session, exec_ctx);
      sql::ObPhysicalPlanCtx phy_plan_ctx(allocator);
      OZ (session.init(0 /*default session id*/,
                       0 /*default proxy id*/,
                       &allocator));
      OX (session.set_inner_session());
      OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard));
      OZ (guard.get_tenant_info(tenant_id, tenant_schema));
      OZ (session.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id));
      OZ (session.load_all_sys_vars(guard));
      OZ (session.load_default_configs_in_pc());
      OX (sql_ctx.schema_guard_ = &guard);
      OX (exec_ctx.set_my_session(&session));
      OX (exec_ctx.set_sql_ctx(&sql_ctx));
      OX (exec_ctx.set_physical_plan_ctx(&phy_plan_ctx));

      OZ (ObLockContext::valid_execute_context(exec_ctx));
      OZ (execute_(exec_ctx, owner_id, release_cnt));
      OX (exec_ctx.set_physical_plan_ctx(nullptr));  // avoid core during release exec_ctx
    }
  }

  return ret;
}

int ObUnLockExecutor::execute_(ObExecContext &ctx,
                               const uint32_t client_session_id,
                               const uint64_t client_session_create_ts,
                               const int64_t release_type,
                               int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_rollback = false;
  ObTableLockOwnerID owner_id;
  release_cnt = INVALID_RELEASE_CNT;  // means not release successfully
  OZ (ObLockContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx) {
      OZ (stack_ctx.init(ctx));
      if (OB_SUCC(ret)) {
        ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
        ObTxDesc *tx_desc = session->get_tx_desc();
        ObTxParam tx_param;
        if (ctx.get_my_session()->is_obproxy_mode()) {
          OZ (check_client_ssid(stack_ctx, client_session_id, client_session_create_ts));
          if (OB_EMPTY_RESULT == ret) {
            release_cnt = LOCK_NOT_OWN_RELEASE_CNT;
          }
        }
        OZ (ObInnerConnectionLockUtil::build_tx_param(session, tx_param));
        OZ (owner_id.convert_from_client_sessid(client_session_id, client_session_create_ts));
        OZ (release_all_locks_(stack_ctx,
                               session,
                               tx_param,
                               owner_id,
                               release_type,
                               release_cnt));
        OZ (remove_session_record(stack_ctx, client_session_id, client_session_create_ts));
      }
      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        COVER_SUCC(tmp_ret);
      }
    }
  }
  // if release_cnt is valid, means we have tried to release,
  // and have not encountered any failures before
  if (INVALID_RELEASE_CNT != release_cnt) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObUnLockExecutor::execute_(ObExecContext &ctx,
                               const ObTableLockOwnerID &owner_id,
                               int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_rollback = false;
  uint32_t client_session_id = 0;
  int64_t release_type = RELEASE_ALL_LOCKS;

  OZ (owner_id.convert_to_sessid(client_session_id));
  OZ (ObLockContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx) {
      OZ (stack_ctx.init(ctx));
      if (OB_SUCC(ret)) {
        ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
        ObTxDesc *tx_desc = session->get_tx_desc();
        ObTxParam tx_param;
        OZ (ObInnerConnectionLockUtil::build_tx_param(session, tx_param));
        OZ (release_all_locks_(stack_ctx,
                               session,
                               tx_param,
                               owner_id,
                               release_type,
                               release_cnt));
        OZ (remove_session_record(stack_ctx, client_session_id, 0));
      }
      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObUnLockExecutor::release_all_locks_(ObLockContext &ctx,
                                         ObSQLSessionInfo *session,
                                         const ObTxParam &tx_param,
                                         const ObTableLockOwnerID &owner_id,
                                         const int64_t release_type,
                                         int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  ObArray<ObLockRequest*> arg_list;
  ObSqlString sql;

  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_V2_TNAME));
  if (OB_SUCC(ret)) {
    switch(release_type) {
      case RELEASE_OBJ_LOCK: {
        if (OB_FAIL(sql.assign_fmt("SELECT task_type, obj_type, obj_id, lock_mode, owner_id, owner_type, cnt"
                                   " FROM %s WHERE owner_id = %ld"
                                   " AND owner_type = %d"
                                   " AND task_type=%d",
                                   table_name,
                                   owner_id.id(),
                                   owner_id.type(),
                                   LOCK_OBJECT))) {
          LOG_WARN("fail to assign fmt", KR(ret));
        }
        break;
      }
      case RELEASE_TABLE_LOCK: {
        if (OB_FAIL(sql.assign_fmt("SELECT task_type, obj_type, obj_id, lock_mode, owner_id, owner_type, cnt"
                                   " FROM %s WHERE owner_id = %ld"
                                   " AND owner_type = %d"
                                   " AND task_type=%d",
                                   table_name,
                                   owner_id.id(),
                                   owner_id.type(),
                                   LOCK_TABLE))) {
          LOG_WARN("fail to assign fmt", KR(ret));
        }
        break;
      }
      case RELEASE_ALL_LOCKS: {
        if (OB_FAIL(sql.assign_fmt("SELECT task_type, obj_type, obj_id, lock_mode, owner_id, owner_type, cnt"
                                   " FROM %s WHERE owner_id = %ld"
                                   " AND owner_type = %d",
                                   table_name,
                                   owner_id.id(),
                                   owner_id.type()))) {
          LOG_WARN("fail to assign fmt", KR(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown release type", K(ret), K(release_type));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(ctx.execute_read(sql, res))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", KR(ret));
      } else if (OB_FAIL(get_unlock_request_list_(result, arg_list))) {
        LOG_WARN("get unlock_reuqest list failed", KR(ret));
      }
    }  // end SMART_VAR
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("get result failed before", K(arg_list));
  } else if (OB_FAIL(release_all_locks_(ctx, arg_list, session, tx_param, release_cnt))) {
    LOG_WARN("release all locks failed", K(ret));
  }
  // clean the unlock request list
  for (int64_t i = 0; i < arg_list.count(); i++) {
    ObLockRequest *arg = arg_list.at(i);
    if (OB_ISNULL(arg)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("the lock argument should not be null", K(tmp_ret));
    } else {
      arg->~ObLockRequest();
      allocator_.free(arg);
    }
  }
  return ret;
}

int ObUnLockExecutor::get_unlock_request_list_(common::sqlclient::ObMySQLResult *res,
                                               ObIArray<ObLockRequest *> &arg_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLockRequest *unlock_arg = NULL;
  int64_t tmp_cnt = 0;
  while (OB_SUCC(ret) && OB_SUCC(res->next())) {
    if (OB_FAIL(parse_unlock_request_(*res, unlock_arg, tmp_cnt))) {
      LOG_WARN("parse lock request failed", K(ret));
    } else if (OB_FAIL(arg_list.push_back(unlock_arg))) {
      LOG_WARN("add unlock arg to the list failed", K(ret), K(unlock_arg));
    }
    if (OB_FAIL(ret) && unlock_arg != NULL) {
      unlock_arg->~ObLockRequest();
      allocator_.free(unlock_arg);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObUnLockExecutor::release_all_locks_(ObLockContext &ctx,
                                         const ObIArray<ObLockRequest *> &arg_list,
                                         sql::ObSQLSessionInfo *session,
                                         const ObTxParam &tx_param,
                                         int64_t &cnt)
{
  int ret = OB_SUCCESS;
  int64_t tmp_cnt = 0;
  cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg_list.count(); i++) {
    tmp_cnt = 0;
    const ObLockRequest *arg = arg_list.at(i);
    if (OB_ISNULL(arg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lock request should not be null", K(ret));
    } else {
      switch(arg->type_) {
        case ObLockRequest::ObLockMsgType::UNLOCK_OBJ_REQ: {
          const ObUnLockObjsRequest *real_arg = static_cast<const ObUnLockObjsRequest *>(arg);
          if (real_arg->objs_.count() != 1) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("do not support batch unlock right now", KPC(real_arg));
          } else if (OB_FAIL(ObTableLockDetector::remove_detect_info_from_inner_table(
                      session, LOCK_OBJECT, *real_arg, tmp_cnt))) {
            LOG_WARN("remove_detect_info_from_inner_table failed", K(ret), K(real_arg));
          } else if (OB_FAIL(unlock_obj_(session->get_tx_desc(), tx_param, *real_arg))) {
            LOG_WARN("unlock obj failed", K(ret), K(arg));
          } else if (FALSE_IT(cnt = cnt + tmp_cnt)) {
          }
          break;
        }
        case ObLockRequest::ObLockMsgType::UNLOCK_TABLE_REQ: {
          const ObUnLockTableRequest *real_arg = static_cast<const ObUnLockTableRequest*>(arg);
          if (OB_FAIL(ObTableLockDetector::remove_detect_info_from_inner_table(session,
                                                                               LOCK_TABLE,
                                                                               *real_arg,
                                                                               tmp_cnt))) {
            LOG_WARN("remove_detect_info_from_inner_table failed", K(ret), K(real_arg));
          } else if (OB_FAIL(unlock_table_(session->get_tx_desc(), tx_param, *real_arg))) {
            LOG_WARN("unlock obj failed", K(ret), K(arg));
          } else if (FALSE_IT(cnt = cnt + tmp_cnt)) {
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not support lock request", KPC(arg));
          break;
        }
      }
    }
  }

  // if meet fails during release lock, should reset the release cnt to be INVALID_RELEASE_CNT,
  // which means release failed. And all release opeartions will be rollbacked later.
  if (OB_FAIL(ret)) {
    cnt = INVALID_RELEASE_CNT;
  }

  return ret;
}

int ObUnLockExecutor::parse_unlock_request_(common::sqlclient::ObMySQLResult &res,
                                            ObLockRequest *&arg,
                                            int64_t &cnt)
{
  int ret = OB_SUCCESS;
  int64_t task_type = 0;
  int64_t obj_type = 0;
  int64_t obj_id = 0;
  int64_t lock_mode = 0;
  int64_t owner_id = 0;
  int64_t owner_type = 0;
  ObLockID lock_id;
  void *ptr = NULL;

  arg = NULL;
  cnt = 1;
  (void)GET_COL_IGNORE_NULL(res.get_int, "task_type", task_type);
  (void)GET_COL_IGNORE_NULL(res.get_int, "obj_type", obj_type);
  (void)GET_COL_IGNORE_NULL(res.get_int, "obj_id", obj_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "lock_mode", lock_mode);
  (void)GET_COL_IGNORE_NULL(res.get_int, "owner_id", owner_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "owner_type", owner_type);
  (void)GET_COL_IGNORE_NULL(res.get_int, "cnt", cnt);
  if (OB_FAIL(ret)) {
  } else {
    switch (task_type) {
      case LOCK_TABLE: {
        ObUnLockTableRequest *unlock_arg = NULL;
        if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObUnLockTableRequest)))) {
          ret = OB_EAGAIN;
          LOG_WARN("get unlock request failed", K(ret));
        } else if (FALSE_IT(unlock_arg = new (ptr) ObUnLockTableRequest())) {
        } else if (OB_FAIL(unlock_arg->owner_id_.convert_from_value(static_cast<ObLockOwnerType>(owner_type),
                                                                    owner_id))) {
          LOG_WARN("get owner id failed", K(ret), K(owner_type), K(owner_id));
        } else {
          unlock_arg->table_id_ = obj_id;
          unlock_arg->lock_mode_ = lock_mode;
          unlock_arg->op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK;
          unlock_arg->timeout_us_ = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : 1000 * 1000L;
          unlock_arg->is_from_sql_ = true;
          arg = unlock_arg;
        }
        if (OB_FAIL(ret) && unlock_arg != NULL) {
          unlock_arg->~ObUnLockTableRequest();
          allocator_.free(ptr);
        }
        break;
      }
      case LOCK_OBJECT: {
        ObUnLockObjsRequest *unlock_arg = NULL;
        bool is_dbms_lock = static_cast<int64_t>(ObLockOBJType::OBJ_TYPE_DBMS_LOCK) == obj_type;
        if (!is_dbms_lock
            && !(static_cast<int64_t>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC) == obj_type
                 && static_cast<int64_t>(EXCLUSIVE) == lock_mode)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid object type and lock mode", K(ret), K(obj_type), K(lock_mode));
        } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObUnLockObjsRequest)))) {
          ret = OB_EAGAIN;
          LOG_WARN("get unlock request failed", K(ret));
        } else if (FALSE_IT(unlock_arg = new (ptr) ObUnLockObjsRequest())) {
        } else if (OB_FAIL(unlock_arg->owner_id_.convert_from_value(static_cast<ObLockOwnerType>(owner_type),
                                                                    owner_id))) {
          LOG_WARN("get owner id failed", K(ret), K(owner_type), K(owner_id));
        } else if (OB_FAIL(lock_id.set(static_cast<ObLockOBJType>(obj_type), obj_id))) {
          LOG_WARN("get lock id failed", K(ret), K(obj_type), K(obj_id));
        } else if (OB_FAIL(unlock_arg->objs_.push_back(lock_id))) {
          LOG_WARN("get unlock argument failed", K(ret), K(lock_id));
        } else {
          unlock_arg->lock_mode_ = is_dbms_lock ? lock_mode : EXCLUSIVE;
          unlock_arg->op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK;
          unlock_arg->timeout_us_ = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : 1000 * 1000L;
          unlock_arg->is_from_sql_ = true;
          arg = unlock_arg;
        }
        if (OB_FAIL(ret) && unlock_arg != NULL) {
          unlock_arg->~ObUnLockObjsRequest();
          allocator_.free(ptr);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not supported lock task type", K(ret), K(task_type));
        break;
      }
    }
  }

  return ret;
}

} // tablelock
} // transaction
} // oceanbase
