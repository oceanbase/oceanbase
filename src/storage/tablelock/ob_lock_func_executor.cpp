/**
 * Copyright (c) 2023 OceanBase
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
#include "storage/tablelock/ob_lock_func_executor.h"

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

int ObLockFuncContext::init(ObSQLSessionInfo &session_info,
                            ObExecContext &ctx,
                            const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t query_start_time = session_info.get_query_start_time();
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
    if (session_info.get_local_autocommit()) {
      OX (reset_autocommit_ = true);
      OZ (session_info.set_autocommit(false));
    }
    has_inner_dml_write_ = session_info.has_exec_inner_dml();
    last_insert_id_ = session_info.get_local_last_insert_id();
    session_info.set_has_exec_inner_dml(false);

    ObTransID parent_tx_id;
    parent_tx_id = session_info.get_tx_id();
    OZ (session_info.begin_autonomous_session(saved_session_));
    OX (have_saved_session_ = true);
    OZ (ObSqlTransControl::explicit_start_trans(ctx, false));
    if (OB_SUCC(ret)) {
      has_autonomous_tx_ = true;
    }
    if (OB_SUCC(ret) && parent_tx_id.is_valid()) {
      (void) register_for_deadlock_(session_info, parent_tx_id);
    }
  }
  OX (session_info_ = &session_info);
  OX (my_exec_ctx_ = &ctx);
  OZ (open_inner_conn_());
  return ret;
}

int ObLockFuncContext::destroy(ObExecContext &ctx,
                               ObSQLSessionInfo &session_info,
                               bool is_rollback)
{
  int tmp_ret = OB_SUCCESS;
  int ret = OB_SUCCESS;

  if (has_autonomous_tx_) {
    if (OB_TMP_FAIL(implicit_end_trans_(session_info, ctx, is_rollback))) {
      LOG_WARN("failed to rollback trans", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  if (OB_TMP_FAIL(close_inner_conn_())) {
    LOG_WARN("close inner connection failed", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  if (have_saved_session_) {
    if (OB_TMP_FAIL(session_info.end_autonomous_session(saved_session_))) {
      LOG_WARN("failed to switch trans", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }

  // WHY WE NEED THIS
  uint64_t cur_last_insert_id = session_info.get_local_last_insert_id();
  if (cur_last_insert_id != last_insert_id_) {
    ObObj last_insert_id;
    last_insert_id.set_uint64(last_insert_id_);
    tmp_ret = session_info.update_sys_variable(SYS_VAR_LAST_INSERT_ID, last_insert_id);
    if (OB_SUCCESS == tmp_ret &&
        OB_TMP_FAIL(session_info.update_sys_variable(SYS_VAR_IDENTITY, last_insert_id))) {
      LOG_WARN("succ update last_insert_id, but fail to update identity", K(tmp_ret));
    }
    ret = COVER_SUCC(tmp_ret);
  }
  session_info.set_has_exec_inner_dml(has_inner_dml_write_);
  if (old_worker_timeout_ts_ != 0) {
    THIS_WORKER.set_timeout_ts(old_worker_timeout_ts_);
    if (OB_NOT_NULL(ctx.get_physical_plan_ctx())) {
      ctx.get_physical_plan_ctx()->set_timeout_timestamp(old_phy_plan_timeout_ts_);
    }
  }
  if (reset_autocommit_) {
    if (OB_TMP_FAIL(session_info.set_autocommit(true))) {
      ret = COVER_SUCC(tmp_ret);
      LOG_ERROR("restore autocommit value failed", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObLockFuncContext::implicit_end_trans_(ObSQLSessionInfo &session_info,
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

int ObLockFuncContext::valid_execute_context(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(ctx.get_sql_ctx()));
  CK (OB_NOT_NULL(ctx.get_my_session()));
  CK (OB_NOT_NULL(ctx.get_sql_proxy()));
  CK (OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  CK (OB_NOT_NULL(ctx.get_package_guard()));
  return ret;
}

void ObLockFuncContext::register_for_deadlock_(ObSQLSessionInfo &session_info,
                                               const ObTransID &parent_tx_id)
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

int ObLockFuncContext::open_inner_conn_()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = my_exec_ctx_->get_my_session();

  if (OB_ISNULL(session) || OB_ISNULL(sql_proxy_ = my_exec_ctx_->get_sql_proxy())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session or sql_proxy is NULL", K(ret), KP(session), KP(sql_proxy_));
  } else if (OB_NOT_NULL(inner_conn_ = static_cast<observer::ObInnerSQLConnection *>(session->get_inner_conn()))) {
    // do nothing.
  } else if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session, my_exec_ctx_->get_sql_proxy(), inner_conn_))) {
    LOG_WARN("create inner connection failed", K(ret), KPC(session));
  } else if (OB_ISNULL(inner_conn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("inner connection is still null", KPC(session));
  } else {
    /**
     * session is the only data struct which can pass through multi layer nested sql,
     * so we put inner conn in session to share it within multi layer nested sql.
     */
    session->set_inner_conn(inner_conn_);
    need_close_conn_ = true;
  }
  return ret;
}

int ObLockFuncContext::close_inner_conn_()
{
  int ret = OB_SUCCESS;
  if (need_close_conn_) {
    ObSQLSessionInfo *session = my_exec_ctx_->get_my_session();
    if (OB_ISNULL(sql_proxy_) || OB_ISNULL(session) || OB_ISNULL(inner_conn_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql_proxy or inner_conn of session is NULL", K(ret), KP(sql_proxy_), KP(session), KP(inner_conn_));
    } else {
      OZ (sql_proxy_->close(inner_conn_, true));
      OX (session->set_inner_conn(NULL));
    }
    need_close_conn_ = false;
  }
  sql_proxy_ = NULL;
  inner_conn_ = NULL;
  return ret;
}

int ObLockFuncContext::execute_write(const ObSqlString &sql, int64_t &affected_rows)
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

int ObLockFuncContext::execute_read(const ObSqlString &sql, ObMySQLProxy::MySQLResult &res)
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
int ObLockFuncExecutor::check_lock_exist_(const uint64_t &lock_id)
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
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_TNAME));
  OZ (ObTableAccessHelper::read_single_row(tenant_id,
                                           { "1" },
                                           table_name,
                                           where_cond,
                                           is_existed));
  return ret;
}

int ObLockFuncExecutor::check_lock_exist_(ObLockFuncContext &ctx,
                                          ObSqlString &where_cond,
                                          bool &exist)
{
  int ret = OB_SUCCESS;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};

  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_TNAME));
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

int ObLockFuncExecutor::check_lock_exist_(ObLockFuncContext &ctx,
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

int ObLockFuncExecutor::check_lock_exist_(ObLockFuncContext &ctx, const uint64_t &lock_id, bool &exist)
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

int ObLockFuncExecutor::check_owner_exist_(ObLockFuncContext &ctx,
                                           const uint32_t client_session_id,
                                           const uint64_t client_session_create_ts,
                                           bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  ObTableLockOwnerID lock_owner;

  OZ (lock_owner.convert_from_client_sessid(client_session_id, client_session_create_ts));
  if (client_session_create_ts > 0) {
    OZ (where_cond.assign_fmt("owner_id = %" PRId64, lock_owner.raw_value()));
  } else {
    // if client_session_create_ts <= 0, means there's no accurate client_session_create_ts
    // (from lock live detector), so we only judge client_session_id in this situation
    OZ (where_cond.assign_fmt("(owner_id & %" PRId64 ") = %" PRIu32, ObTableLockOwnerID::CLIENT_SESS_ID_MASK, client_session_id));
  }
  OZ (check_lock_exist_(ctx, where_cond, exist));
  return ret;
}

int ObLockFuncExecutor::check_client_ssid_(ObLockFuncContext &ctx,
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
        if (OB_TMP_FAIL(ObTableLockDetector::remove_lock_by_owner_id(rec_owner_id.raw_value()))) {
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

int ObLockFuncExecutor::remove_session_record_(ObLockFuncContext &ctx,
                                               const uint32_t client_session_id,
                                               const uint64_t client_session_create_ts)
{
  int ret = OB_SUCCESS;
  bool owner_exist = false;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  ObTableLockOwnerID lock_owner;
  ObSqlString delete_sql;
  int64_t affected_rows = 0;

  OZ (check_owner_exist_(ctx, client_session_id, client_session_create_ts, owner_exist));
  if (OB_SUCC(ret) && !owner_exist) {
    lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
    OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                        "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME));
    OZ (delete_sql.assign_fmt("DELETE FROM %s WHERE client_session_id = %" PRIu32,
                              table_name,
                              client_session_id));
    OZ (ctx.execute_write(delete_sql, affected_rows));
    OX (mark_lock_session_(ctx.session_info_, false));
  }
  return ret;
}

int ObLockFuncExecutor::unlock_obj_(ObTxDesc *tx_desc,
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

int ObLockFuncExecutor::query_lock_id_(const ObString &lock_name,
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
                      "WHERE name = '%s'", to_cstring(lock_name)));
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

int ObLockFuncExecutor::query_lock_id_and_lock_handle_(const ObString &lock_name,
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
                      "WHERE name = '%s'", to_cstring(lock_name)));
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

int ObLockFuncExecutor::extract_lock_id_(const ObString &lock_handle,
                                         uint64_t &lock_id)
{
  int ret = OB_SUCCESS;

  OV (lock_handle.is_numeric(), OB_INVALID_ARGUMENT, lock_handle);
  OV (lock_handle.length() >= LOCK_ID_LENGTH, OB_INVALID_ARGUMENT, lock_handle, lock_handle.length());
  OX (lock_id = ObFastAtoi<uint64_t>::atoi_positive_unchecked(lock_handle.ptr(), lock_handle.ptr() + LOCK_ID_LENGTH));
  OV (lock_id >= MIN_LOCK_HANDLE_ID && lock_id <= MAX_LOCK_HANDLE_ID, OB_INVALID_ARGUMENT, lock_id);

  return ret;
}

int ObLockFuncExecutor::query_lock_owner_(const uint64_t &lock_id,
                                          int64_t &owner_id)
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
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_TNAME));
  OZ (ObTableAccessHelper::read_single_row(tenant_id,
                                           { "owner_id" },
                                           table_name,
                                           where_cond,
                                           owner_id));
  if (OB_EMPTY_RESULT == ret) {
    // there is no lock of the lock id.
  } else if (OB_FAIL(ret)) {
    LOG_WARN("get lock owner failed", K(owner_id));
  } else {
    // we have get the owner_id.
  }
  return ret;
}

void ObLockFuncExecutor::mark_lock_session_(sql::ObSQLSessionInfo *session, const bool is_lock_session)
{
  if (session->is_lock_session() != is_lock_session) {
    LOG_INFO("mark lock_session", K(session->get_sessid()), K(is_lock_session));
    session->set_is_lock_session(is_lock_session);
    session->set_need_send_feedback_proxy_info(true);
  } else {
    LOG_DEBUG("the lock_session status on the session won't be changed, no need to mark again",
              K(session->get_sessid()),
              K(session->is_lock_session()),
              K(session->is_need_send_feedback_proxy_info()));
  }
}

int ObLockFuncExecutor::remove_expired_lock_id()
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
                      OB_ALL_DETECT_LOCK_INFO_TNAME,
                      static_cast<int>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC),
                      static_cast<int>(ObLockOBJType::OBJ_TYPE_DBMS_LOCK),
                      delete_limit));
  OZ (ObTableAccessHelper::delete_row(MTL_ID(), table_name, where_cond));
  return ret;
}

int ObGetLockExecutor::execute(ObExecContext &ctx,
                               const ObString &lock_name,
                               const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = ctx.get_my_session();
  uint32_t client_session_id = sess->get_client_sessid();
  uint32_t server_session_id = sess->get_sessid();
  uint64_t client_session_create_ts = sess->get_client_create_time();
  uint64_t lock_id = 0;
  bool is_rollback = false;
  ObTxParam tx_param;

  OZ (ObLockFuncContext::valid_execute_context(ctx));
  // 1. generate lock_id and update DBMS_LOCK_ALLOCATED table
  //
  // 2. check client_session_id is valid
  // 3. check whether need reroute (skip temporarily)
  // 4. modify inner table
  // 4.1 add session into CLIENT_TO_SERVER_SESSION_INFO table
  // 4.2 add lock_obj into DETECT_LOCK_INFO table
  // 5. lock obj

  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockFuncContext, stack_ctx1) {
      OZ (stack_ctx1.init(*sess, ctx, timeout_us));
      OZ (generate_lock_id_(stack_ctx1, lock_name, timeout_us, lock_id));

      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx1.destroy(ctx, *(ctx.get_my_session()), is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        COVER_SUCC(tmp_ret);
      }
    }
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockFuncContext, stack_ctx2) {
      OZ (stack_ctx2.init(*sess, ctx, timeout_us));
      // only when connect by proxy should check client_session
      if (sess->is_obproxy_mode()) {
        OZ (check_client_ssid_(stack_ctx2, client_session_id, client_session_create_ts));
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;  // there're no same client_session_id records, continue
        } else {
          // TODO(yangyifei.yyf): some SQL can not redo, reroute may cause errors, so skip this step temporarily
          // OZ (check_need_reroute_(stack_ctx1, sess, client_session_id, client_session_create_ts));
        }
      }
      OZ (update_session_table_(stack_ctx2,
                                client_session_id,
                                client_session_create_ts,
                                server_session_id));
      OZ (ObInnerConnectionLockUtil::build_tx_param(sess, tx_param));
      OZ (lock_obj_(sess, tx_param, client_session_id, client_session_create_ts, lock_id, timeout_us));
      OX (mark_lock_session_(sess, true));

      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx2.destroy(ctx, *(ctx.get_my_session()), is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObGetLockExecutor::lock_obj_(sql::ObSQLSessionInfo *session,
                                 const ObTxParam &tx_param,
                                 const uint32_t client_session_id,
                                 const uint64_t client_session_create_ts,
                                 const int64_t obj_id,
                                 const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTableLockService *lock_service = MTL(ObTableLockService *);
  ObLockObjsRequest arg;
  bool need_record_to_lock_table = true;
  ObTxDesc *tx_desc = session->get_tx_desc();
  ObLockID lock_id;

  arg.lock_mode_ = EXCLUSIVE;  // only X mode
  arg.op_type_ = ObTableLockOpType::OUT_TRANS_LOCK;
  arg.timeout_us_ = timeout_us;
  arg.is_from_sql_ = true;
  arg.detect_func_no_ = ObTableLockDetectType::DETECT_SESSION_ALIVE;

  if (OB_FAIL(lock_id.set(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC, obj_id))) {
    LOG_WARN("set lock_id failed", K(ret), K(obj_id));
  } else if (OB_FAIL(arg.owner_id_.convert_from_client_sessid(client_session_id, client_session_create_ts))) {
    LOG_WARN("convert client_session_id to owner_id failed", K(ret), K(client_session_id));
  } else if (OB_FAIL(arg.objs_.push_back(lock_id))) {
    LOG_WARN("push_back lock_id to arg.objs_ failed", K(ret), K(arg), K(lock_id));
  } else if (OB_FAIL(ObTableLockDetector::record_detect_info_to_inner_table(
               session, LOCK_OBJECT, arg, /*for_dbms_lock*/ false, need_record_to_lock_table))) {
    LOG_WARN("record_detect_info_to_inner_table failed", K(ret), K(arg));
  } else if (need_record_to_lock_table) {
    if (OB_FAIL(lock_service->lock_obj(*tx_desc, tx_param, arg))) {
      LOG_WARN("lock obj failed", K(ret), KPC(tx_desc), K(arg));
    }
  }

  return ret;
}

int ObGetLockExecutor::generate_lock_id_(ObLockFuncContext &ctx,
                                         const ObString &lock_name,
                                         const int64_t timeout_us,
                                         uint64_t &lock_id)
{
  int ret = OB_SUCCESS;
  char lock_handle_buf[MAX_LOCK_HANDLE_LEGNTH] = {0};
  OZ (query_lock_id_and_lock_handle_(lock_name, lock_id, lock_handle_buf));
  if (OB_EMPTY_RESULT == ret) {
    // there is no result, should create one
    ret = OB_SUCCESS;
    OZ (generate_lock_id_(lock_name, lock_id, lock_handle_buf));
  }
  OZ (write_lock_id_(ctx, lock_name, timeout_us, lock_id, lock_handle_buf));
  return ret;
}

int ObGetLockExecutor::generate_lock_id_(const ObString &lock_name,
                                         uint64_t &lock_id,
                                         char *lock_handle)
{
  int ret = OB_SUCCESS;
  ObCommonID unique_lock_id;
  if (OB_ISNULL(lock_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock handle can not be null", K(ret));
  } else if (OB_FAIL(ObCommonIDUtils::gen_unique_id(MTL_ID(), unique_lock_id))) {
    LOG_WARN("generate unique id for lock handle failed", K(ret));
  } else {
    uint64_t hash_val = 0;
    hash_val = murmurhash(lock_name.ptr(), lock_name.length(), hash_val);
    // the range of the lock id in lock handle is [MIN_LOCK_HANDLE_ID, MAX_LOCK_HANDLE_ID]
    lock_id = unique_lock_id.id() % (MAX_LOCK_HANDLE_ID - MIN_LOCK_HANDLE_ID + 1) + MIN_LOCK_HANDLE_ID;
    snprintf(lock_handle, MAX_LOCK_HANDLE_LEGNTH, "%" PRIu64 "%" PRIu64, lock_id, hash_val);
  }
  return ret;
}

int ObGetLockExecutor::write_lock_id_(ObLockFuncContext &ctx,
                                      const ObString &lock_name,
                                      const int64_t timeout_us,
                                      const uint64_t &lock_id,
                                      const char *lock_handle_buf)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer insert_dml;
  ObSqlString delete_sql;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DBMS_LOCK_ALLOCATED_TNAME));

  OZ (insert_dml.add_gmt_create(now));
  OZ (insert_dml.add_gmt_modified(now));
  OZ (insert_dml.add_pk_column("name", lock_name));
  // make sure lock_obj will be timeout or success before lock_id is expired
  OZ (insert_dml.add_time_column("expiration", now + timeout_us + DEFAULT_EXPIRATION_US));
  OZ (insert_dml.add_column("lockid", lock_id));
  OZ (insert_dml.add_column("lockhandle", lock_handle_buf));
  OZ (insert_dml.splice_insert_update_sql(table_name,
                                          insert_sql));
  OZ (ctx.execute_write(insert_sql, affected_rows));
  CK (OB_LIKELY(1 == affected_rows || 2 == affected_rows));

  return ret;
}

int ObGetLockExecutor::update_session_table_(ObLockFuncContext &ctx,
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

int ObGetLockExecutor::check_need_reroute_(ObLockFuncContext &ctx,
                                           sql::ObSQLSessionInfo *session,
                                           const uint32_t client_session_id,
                                           const uint64_t client_session_create_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObAddr lock_session_addr;
  uint32_t lock_session_id = 0;
  int32_t sql_port = 0;
  ObSqlCtx *sql_ctx = ctx.my_exec_ctx_->get_sql_ctx();

  if (!session->is_lock_session()) {
    OZ (get_lock_session_(ctx, client_session_id, client_session_create_ts, lock_session_addr, lock_session_id));
    // no lock_session in this client, continue
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
    } else {
      OV (lock_session_addr == GCTX.self_addr(), OB_ERR_PROXY_REROUTE, lock_session_addr, GCTX.self_addr());
      // can not reroute in one observer, so just return OB_SUCCESS
      OV (lock_session_id == session->get_sessid(), OB_SUCCESS, lock_session_id, session->get_sessid());
      // to avoid this session wasn't marked as lock_session before
      if (lock_session_addr == GCTX.self_addr() && lock_session_id == session->get_sessid()) {
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
  return ret;
}

int ObGetLockExecutor::get_lock_session_(ObLockFuncContext &ctx,
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

int ObGetLockExecutor::get_first_session_info_(common::sqlclient::ObMySQLResult &res,
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
  OV (session_addr.set_ip_addr(to_cstring(svr_ip), svr_port), OB_ERR_UNEXPECTED, svr_ip, svr_port);

  return ret;
}

int ObGetLockExecutor::get_sql_port_(ObLockFuncContext &ctx,
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

int ObReleaseLockExecutor::execute(ObExecContext &ctx,
                                   const ObString &lock_name,
                                   int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint32_t client_session_id = 0;
  uint64_t client_session_create_ts = 0;
  uint64_t lock_id = 0;
  bool is_rollback = false;
  bool need_remove_from_lock_table = true;
  bool lock_id_existed = false;
  ObLockID tmp_lock_id;

  release_cnt = INVALID_RELEASE_CNT;  // means not release successfully

  OZ (ObLockFuncContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockFuncContext, stack_ctx) {
      client_session_id = ctx.get_my_session()->get_client_sessid();
      client_session_create_ts = ctx.get_my_session()->get_client_create_time();
      OZ (stack_ctx.init(*(ctx.get_my_session()), ctx));
      if (OB_SUCC(ret)) {
        ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
        ObTxDesc *tx_desc = session->get_tx_desc();
        ObTxParam tx_param;
        ObUnLockObjsRequest arg;
        ObTableLockOwnerID lock_owner;
        // 1. get lock id from inner table
        // 2. check client_session_id is valid
        // 3. unlock obj
        // 4. modify inner table
        // 4.1 lock table: dec cnt if the cnt is greater than 1, else remove the record.
        // (this is processed internal)
        // 4.2 lock name-id table: check the lock table if there is no lock of the same
        // lock id, remove the record at lock name-id table.
        // 4.3 session table: check the lock table if there is no lock of the same
        // client session, remove the record of the same client session id.
        OZ (query_lock_id_(lock_name, lock_id));
        if (OB_EMPTY_RESULT == ret) {
          release_cnt = LOCK_NOT_EXIST_RELEASE_CNT;
        }
        if (ctx.get_my_session()->is_obproxy_mode()) {
          OZ (check_client_ssid_(stack_ctx, client_session_id, client_session_create_ts));
          if (OB_EMPTY_RESULT == ret) {
            release_cnt = LOCK_NOT_OWN_RELEASE_CNT;
          }
        }
        OZ (lock_owner.convert_from_client_sessid(client_session_id, client_session_create_ts));
        OZ (ObInnerConnectionLockUtil::build_tx_param(session, tx_param));

        OZ (tmp_lock_id.set(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC, lock_id));
        OZ (arg.objs_.push_back(tmp_lock_id));
        OX (arg.lock_mode_ = EXCLUSIVE);
        OX (arg.op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK);
        OX (arg.timeout_us_ = 0);
        OX (arg.is_from_sql_ = true);
        OX (arg.owner_id_ = lock_owner);

        OZ (ObTableLockDetector::remove_detect_info_from_inner_table(
               session, LOCK_OBJECT, arg, need_remove_from_lock_table));
        if (OB_SUCC(ret) && need_remove_from_lock_table) {
          OZ (unlock_obj_(tx_desc, tx_param, arg));
          OZ (remove_session_record_(stack_ctx, client_session_id, client_session_create_ts));
          OX (release_cnt = 1);
        } else if (OB_EMPTY_RESULT == ret) {
          if (OB_TMP_FAIL(check_lock_exist_(stack_ctx, lock_id, lock_id_existed))) {
            LOG_WARN("check lock_id existed failed", K(tmp_ret), K(lock_id));
          } else if (lock_id_existed) {
            release_cnt = LOCK_NOT_OWN_RELEASE_CNT;
          } else {
            release_cnt = LOCK_NOT_EXIST_RELEASE_CNT;
          }
        }
      }
      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, *(ctx.get_my_session()), is_rollback))) {
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

int ObReleaseAllLockExecutor::execute(ObExecContext &ctx,
                                      int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint32_t client_session_id = 0;
  uint64_t client_session_create_ts = 0;
  bool is_rollback = false;
  OZ (ObLockFuncContext::valid_execute_context(ctx));
  OX (client_session_id = ctx.get_my_session()->get_client_sessid());
  OX (client_session_create_ts = ctx.get_my_session()->get_client_create_time());
  OZ (execute_(ctx, client_session_id, client_session_create_ts, release_cnt));
  return ret;
}

int ObReleaseAllLockExecutor::execute(const int64_t raw_owner_id)
{
  int ret = OB_SUCCESS;
  int64_t release_cnt = 0;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
  ObTableLockOwnerID owner_id;
  OX (owner_id.convert_from_value(raw_owner_id));
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

      OZ (ObLockFuncContext::valid_execute_context(exec_ctx));
      OZ (execute_(exec_ctx, owner_id, release_cnt));
      OX (exec_ctx.set_physical_plan_ctx(nullptr));  // avoid core during release exec_ctx
    }
  }

  return ret;
}

int ObReleaseAllLockExecutor::execute_(ObExecContext &ctx,
                                       const uint32_t client_session_id,
                                       const uint64_t client_session_create_ts,
                                       int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_rollback = false;
  ObTableLockOwnerID owner_id;
  release_cnt = INVALID_RELEASE_CNT;  // means not release successfully
  OZ (ObLockFuncContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockFuncContext, stack_ctx) {
      OZ (stack_ctx.init(*(ctx.get_my_session()), ctx));
      if (OB_SUCC(ret)) {
        ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
        ObTxDesc *tx_desc = session->get_tx_desc();
        ObTxParam tx_param;
        if (ctx.get_my_session()->is_obproxy_mode()) {
          OZ (check_client_ssid_(stack_ctx, client_session_id, client_session_create_ts));
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
                               release_cnt));
        OZ (remove_session_record_(stack_ctx, client_session_id, client_session_create_ts));
      }
      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, *(ctx.get_my_session()), is_rollback))) {
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

int ObReleaseAllLockExecutor::execute_(ObExecContext &ctx,
                                       const ObTableLockOwnerID &owner_id,
                                       int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_rollback = false;
  uint32_t client_session_id = 0;

  OZ (owner_id.convert_to_sessid(client_session_id));
  OZ (ObLockFuncContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockFuncContext, stack_ctx) {
      OZ (stack_ctx.init(*(ctx.get_my_session()), ctx));
      if (OB_SUCC(ret)) {
        ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
        ObTxDesc *tx_desc = session->get_tx_desc();
        ObTxParam tx_param;
        OZ (ObInnerConnectionLockUtil::build_tx_param(session, tx_param));
        OZ (release_all_locks_(stack_ctx, session, tx_param, owner_id, release_cnt));
        OZ (remove_session_record_(stack_ctx, client_session_id, 0));
      }
      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, *(ctx.get_my_session()), is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObReleaseAllLockExecutor::release_all_locks_(ObLockFuncContext &ctx,
                                                 ObSQLSessionInfo *session,
                                                 const ObTxParam &tx_param,
                                                 const ObTableLockOwnerID &owner_id,
                                                 int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  ObTableLockOwnerID lock_owner;
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  ObArray<ObUnLockObjsRequest> arg_list;

  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_TNAME));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT obj_type, obj_id, lock_mode, owner_id, cnt"
                                        " FROM %s WHERE owner_id = %ld",
                                        table_name,
                                        owner_id.raw_value()))) {
        LOG_WARN("fail to assign fmt", KR(ret));
      } else if (OB_FAIL(ctx.execute_read(sql, res))) {
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
  }
  return ret;
}

int ObReleaseAllLockExecutor::get_unlock_request_list_(common::sqlclient::ObMySQLResult *res,
                                                       ObIArray<ObUnLockObjsRequest> &arg_list)
{
  int ret = OB_SUCCESS;
  ObLockObjsRequest lock_arg;
  ObUnLockObjsRequest unlock_arg;
  int64_t tmp_cnt = 0;
  while (OB_SUCC(ret) && OB_SUCC(res->next())) {
    if (OB_FAIL(parse_lock_request_(*res, lock_arg, tmp_cnt))) {
      LOG_WARN("parse lock request failed", K(ret));
    } else if (OB_FAIL(get_unlock_request_(lock_arg, unlock_arg))) {
      LOG_WARN("get unlock request failed", K(ret), K(lock_arg));
    } else if (OB_FAIL(arg_list.push_back(unlock_arg))) {
      LOG_WARN("add unlock arg to the list failed", K(ret), K(unlock_arg));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObReleaseAllLockExecutor::release_all_locks_(ObLockFuncContext &ctx,
                                                 const ObIArray<ObUnLockObjsRequest> &arg_list,
                                                 sql::ObSQLSessionInfo *session,
                                                 const ObTxParam &tx_param,
                                                 int64_t &cnt)
{
  int ret = OB_SUCCESS;
  int64_t tmp_cnt = 0;
  cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg_list.count(); i++) {
    tmp_cnt = 0;
    const ObUnLockObjsRequest &arg = arg_list.at(i);
    if (OB_FAIL(ObTableLockDetector::remove_detect_info_from_inner_table(session, LOCK_OBJECT, arg, tmp_cnt))) {
      LOG_WARN("remove_detect_info_from_inner_table failed", K(ret), K(arg));
    } else if (FALSE_IT(cnt = cnt + tmp_cnt)) {
    } else if (OB_FAIL(unlock_obj_(session->get_tx_desc(), tx_param, arg))) {
      LOG_WARN("unlock obj failed", K(ret), K(arg));
    } else if (arg.objs_.count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support batch unlock right now", K(arg));
    }
  }

  return ret;
}

int ObReleaseAllLockExecutor::parse_lock_request_(common::sqlclient::ObMySQLResult &res,
                                                  ObLockObjsRequest &arg,
                                                  int64_t &cnt)
{
  int ret = OB_SUCCESS;
  int64_t obj_type = 0;
  int64_t obj_id = 0;
  int64_t lock_mode = 0;
  int64_t owner_id = 0;
  ObLockID lock_id;

  cnt = 1;
  (void)GET_COL_IGNORE_NULL(res.get_int, "obj_type", obj_type);
  (void)GET_COL_IGNORE_NULL(res.get_int, "obj_id", obj_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "lock_mode", lock_mode);
  (void)GET_COL_IGNORE_NULL(res.get_int, "owner_id", owner_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "cnt", cnt);
  bool is_dbms_lock = static_cast<int64_t>(ObLockOBJType::OBJ_TYPE_DBMS_LOCK) == obj_type;

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(arg.reset())) {
  } else if (!is_dbms_lock
             && !(static_cast<int64_t>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC) == obj_type
                  && static_cast<int64_t>(EXCLUSIVE) == lock_mode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid object type and lock mode", K(ret), K(obj_type), K(lock_mode));
  } else if (OB_FAIL(arg.owner_id_.convert_from_value(owner_id))) {
    LOG_WARN("get owner id failed", K(ret), K(owner_id));
  } else if (OB_FAIL(lock_id.set(static_cast<ObLockOBJType>(obj_type), obj_id))) {
  } else if (OB_FAIL(arg.objs_.push_back(lock_id))) {
  } else {
    arg.lock_mode_ = is_dbms_lock ? lock_mode : EXCLUSIVE;
    arg.op_type_ = ObTableLockOpType::OUT_TRANS_LOCK;
    arg.timeout_us_ = 0;
    arg.is_from_sql_ = true;
  }

  return ret;
}

int ObReleaseAllLockExecutor::get_unlock_request_(const ObLockObjsRequest &arg,
                                                  ObUnLockObjsRequest &unlock_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (FALSE_IT(unlock_arg.reset())) {
  } else if (OB_FAIL(unlock_arg.objs_.assign(arg.objs_))) {
    LOG_WARN("can not assign objs_ for unlock_arg with lock_arg", K(ret), K(arg));
  } else {
    unlock_arg.lock_mode_ = arg.lock_mode_;
    unlock_arg.op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK;
    unlock_arg.timeout_us_ = 0;
    unlock_arg.is_from_sql_ = true;
    unlock_arg.owner_id_ = arg.owner_id_;
  }
  return ret;
}

int ObISFreeLockExecutor::execute(ObExecContext &ctx,
                                  const ObString &lock_name)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  uint64_t lock_id = 0;
  OZ(query_lock_id_(lock_name, lock_id));
  OZ(check_lock_exist_(lock_id));
  return ret;
}

int ObISUsedLockExecutor::execute(ObExecContext &ctx,
                                  const ObString &lock_name,
                                  uint32_t &sess_id)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  uint64_t lock_id = 0;
  int64_t raw_owner_id = 0;
  ObTableLockOwnerID lock_owner;
  OZ (query_lock_id_(lock_name, lock_id));
  OZ (query_lock_owner_(lock_id, raw_owner_id));
  OZ (lock_owner.convert_from_value(raw_owner_id));
  OZ (lock_owner.convert_to_sessid(sess_id));
  return ret;
}


} // tablelock
} // transaction
} // oceanbase
