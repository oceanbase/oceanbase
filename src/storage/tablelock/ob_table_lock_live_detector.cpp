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

#define USING_LOG_PREFIX TABLELOCK
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "storage/tablelock/ob_lock_func_executor.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_table_access_helper.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_inner_sql_connection.h"

namespace oceanbase
{
using namespace share;
namespace transaction
{
namespace tablelock
{
int ObTableLockDetectFuncList::detect_session_alive(const uint32_t session_id, bool &is_alive)
{
  int ret = OB_SUCCESS;
  is_alive = true;
  sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
  sql::ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there's no session mgr in GCTX", K(ret), K(session_id));
  } else if (OB_FAIL(session_mgr->get_session(session_id, session))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_alive = false;
      LOG_INFO("can not find the session, it's not alive", K(session_id));
    } else {
      LOG_WARN("get session info failed", K(ret), K(session_id));
    }
  } else if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    is_alive = false;
    LOG_WARN("session is null, it's not alive", K(session_id));
  }
  if (OB_NOT_NULL(session)) {
    session_mgr->revert_session(session);
  }
  return ret;
}

int ObTableLockDetectFuncList::detect_session_alive_for_rpc(const uint32_t session_id, obrpc::Bool &is_alive)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool tmp_is_alive = true;

  // We should catch the error code by tmp_ret here, for 'is_alive' will be returned correctly
  if (OB_TMP_FAIL(detect_session_alive(session_id, tmp_is_alive))) {
    if (tmp_is_alive) {
      LOG_WARN("detect session alive failed", K(tmp_ret), K(session_id), K(tmp_is_alive));
    }
  }

  // No matter detect_session_alive is success or not,
  // we should set tmp_is_alive to is_alive here.
  is_alive = tmp_is_alive;
  LOG_DEBUG("detect_session_alive_for_rpc", K(tmp_ret), K(session_id), K(is_alive));
  return ret;
}

int ObTableLockDetectFuncList::do_session_alive_detect()
{
  int ret = OB_SUCCESS;
  char where_cond[64] = {"WHERE detect_func_no = 1 GROUP BY owner_id"};
  ObArray<ObTuple<int64_t>> owner_ids;
  bool client_session_alive = true;
  ObTableLockOwnerID owner_id;
  uint32_t client_session_id = sql::ObSQLSessionInfo::INVALID_SESSID;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  if (OB_FAIL(databuff_printf(
        full_table_name, OB_MAX_TABLE_NAME_BUF_LENGTH, "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_DETECT_LOCK_INFO_TNAME));
  } else if (OB_FAIL(
               ObTableAccessHelper::read_multi_row(MTL_ID(), {"owner_id"}, full_table_name, where_cond, owner_ids))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("read from inner table __all_detect_lock_info failed", K(ret));
    }
  } else {
    for (int64_t i = 0; i < owner_ids.count() && OB_SUCC(ret); i++) {
      client_session_alive = true;
      if (OB_FAIL(owner_id.convert_from_value(owner_ids[i].element<0>()))) {
        LOG_WARN("get owner_id failed", K(owner_ids[i]));
      } else if (!owner_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("owner_id is invalid", K(ret), K(owner_id));
      } else if (OB_FAIL(owner_id.convert_to_sessid(client_session_id))) {
        LOG_WARN("get client_sesion_id failed", K(ret), K(owner_id));
      } else if (OB_FAIL(do_session_alive_detect_for_a_client_session_(client_session_id, client_session_alive))) {
        LOG_WARN("do session alive detect for a client_session failed", K(client_session_id));
      } else if (!client_session_alive) {
        LOG_INFO(
          "find client session is not alive, we will clean all recodrs of it later", K(ret), K(client_session_id));
        ObTableLockDetector::remove_lock_by_owner_id(owner_ids[i].element<0>());
      }
    }
  }
  return ret;
}

int ObTableLockDetectFuncList::do_session_alive_detect_for_a_client_session_(const int64_t &client_session_id,
                                                                             bool &all_alive)
{
  int ret = OB_SUCCESS;
  ObArray<ObTuple<ObAddr, int64_t>> server_session_list;
  all_alive = true;
  if (OB_FAIL(get_active_server_session_list_(client_session_id, server_session_list, all_alive))) {
    LOG_WARN("get server address list failed", K(client_session_id));
  } else if (all_alive) {
    for (int64_t i = 0; i < server_session_list.count() && OB_SUCC(ret) && all_alive; i++) {
      bool server_session_is_alive = true;
      if (OB_FAIL(do_session_alive_detect_for_a_server_session_(
            server_session_list[i].element<0>(), server_session_list[i].element<1>(), server_session_is_alive))) {
        if (server_session_is_alive) {
          LOG_WARN("retry sending rpc until reaching the maximumn retry times, still getting error",
                   K(server_session_list[i]));
        }
      }
      if (!server_session_is_alive) {
        all_alive = false;
      }
    }
  }
  return ret;
}

int ObTableLockDetectFuncList::do_session_alive_detect_for_a_server_session_(const ObAddr &addr,
                                                                             const int64_t &server_session_id,
                                                                             bool &is_alive)
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  obrpc::Bool tmp_is_alive(true);
  int64_t retry_times = 0;

  is_alive = true;

  if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("srv_rpc_proxy is null");
  } else {
    do {
      ret = OB_SUCCESS;
      if (OB_FAIL(srv_rpc_proxy->to(addr).detect_session_alive(server_session_id, tmp_is_alive))) {
        if (!tmp_is_alive) {
          LOG_INFO("find server session is not alive", K(ret), K(addr), K(server_session_id), K(tmp_is_alive));
          is_alive = false;
          ret = OB_SUCCESS;
          break;
        } else {
          retry_times++;
          LOG_WARN("fail to call detect_session_alive by rpc",
                   KR(ret),
                   K(retry_times),
                   K(addr),
                   K(server_session_id),
                   K(tmp_is_alive));
          ob_usleep(RPC_INTERVAL_TIME_US);
        }
      } else {
        is_alive = tmp_is_alive;
      }
    } while (OB_FAIL(ret) && retry_times <= RETRY_RPC_TIMES);
  }

  return ret;
}

int ObTableLockDetectFuncList::get_active_server_session_list_(const int64_t &client_session_id,
                                                               ObIArray<ObTuple<ObAddr, int64_t>> &server_session_list,
                                                               bool &all_alive)
{
  int ret = OB_SUCCESS;
  char where_cond[128] = {'\0'};
  ObArray<ObTuple<ObStringHolder, int64_t, int64_t>> server_session_str_list;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  all_alive = true;

  if (OB_FAIL(databuff_printf(full_table_name,
                              OB_MAX_TABLE_NAME_BUF_LENGTH,
                              "%s.%s",
                              OB_SYS_DATABASE_NAME,
                              OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_DETECT_LOCK_INFO_TNAME));
  } else if (OB_FAIL(databuff_printf(where_cond, 128, "WHERE client_session_id = %ld", client_session_id))) {
    LOG_WARN("generate where condition failed", K(client_session_id));
  } else if (OB_FAIL(ObTableAccessHelper::read_multi_row(MTL_ID(),
                                                         {"svr_ip", "svr_port", "server_session_id"},
                                                         full_table_name,
                                                         where_cond,
                                                         server_session_str_list))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("read from inner table __all_client_to_server_session_info failed", K(ret), K(MTL_ID()));
    }
  } else {
    ObAddr server_addr;
    ObString svr_ip;
    int64_t svr_port;
    int64_t session_id;
    bool is_online = false;
    for (int64_t i = 0; i < server_session_str_list.count() && OB_SUCC(ret); i++) {
      server_addr.reset();
      svr_ip = server_session_str_list[i].element<0>().get_ob_string();
      svr_port = server_session_str_list[i].element<1>();
      session_id = server_session_str_list[i].element<2>();
      if (OB_FAIL(check_server_is_online_(svr_ip, svr_port, is_online))) {
        LOG_WARN("check server is online failed", K(svr_ip), K(svr_port));
      } else if (!is_online) {
        all_alive = false;
        break;
      } else if (!server_addr.set_ip_addr(to_cstring(svr_ip), svr_port)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set svr_addr failed", K(svr_ip), K(svr_port));
      } else if (OB_FAIL(server_session_list.push_back(ObTuple<ObAddr, int64_t>(server_addr, session_id)))) {
        LOG_WARN("add svr_addr and session_id into list failed", K(server_addr), K(session_id));
      }
    }
  }
  return ret;
}

int ObTableLockDetectFuncList::check_server_is_online_(const ObString &svr_ip, const int64_t svr_port, bool &is_online)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  char where_cond[512] = {'\0'};
  is_online = false;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];

  if (OB_FAIL(
        databuff_printf(where_cond,
                        512,
                        "WHERE (svr_ip, svr_port) IN (SELECT u.svr_ip, u.svr_port FROM %s.%s AS u JOIN %s.%s AS r on "
                        "r.resource_pool_id = u.resource_pool_id WHERE tenant_id = %ld) and svr_ip = '%s' and svr_port "
                        "= %ld and status = 'active'",
                        OB_SYS_DATABASE_NAME,
                        OB_ALL_UNIT_TNAME,
                        OB_SYS_DATABASE_NAME,
                        OB_ALL_RESOURCE_POOL_TNAME,
                        MTL_ID(),
                        to_cstring(svr_ip),
                        svr_port))) {
    LOG_WARN("generate where condition for select sql failed", K(svr_ip), K(svr_port));
  } else if (OB_FAIL(databuff_printf(
               full_table_name, OB_MAX_TABLE_NAME_BUF_LENGTH, "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_SERVER_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_DETECT_LOCK_INFO_TNAME));
  } else if (OB_FAIL(
               ObTableAccessHelper::read_single_row(OB_SYS_TENANT_ID, {"1"}, full_table_name, where_cond, is_online))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      is_online = false;
      LOG_INFO("can not find the server in the server_list, it has been removed", K(svr_ip), K(svr_port));
    } else {
      LOG_WARN("read from __all_server table failed", K(where_cond));
    }
  }
  return ret;
}

ObTableLockDetectFunc<> ObTableLockDetector::func1(DETECT_SESSION_ALIVE,
                                                   ObTableLockDetectFuncList::do_session_alive_detect);

const char *ObTableLockDetector::detect_columns[8] = {
  "task_type", "obj_type", "obj_id", "lock_mode", "owner_id", "cnt", "detect_func_no", "detect_func_param"};

int ObTableLockDetector::record_detect_info_to_inner_table(sql::ObSQLSessionInfo *session_info,
                                                           const ObTableLockTaskType &task_type,
                                                           const ObLockRequest &lock_req,
                                                           const bool for_dbms_lock,
                                                           bool &need_record_to_lock_table)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *inner_conn = nullptr;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  bool need_release_conn = false;
  bool is_existed = false;

  need_record_to_lock_table = true;
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  if (LOCK_OBJECT != task_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support register detect task which is not LOCK_OBJECT right now");
  } else if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_sessid()));

    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_sessid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret) || OB_ISNULL(inner_conn)) {
  } else if (OB_FAIL(databuff_printf(full_table_name,
                                     OB_MAX_TABLE_NAME_BUF_LENGTH,
                                     "%s.%s",
                                     OB_SYS_DATABASE_NAME,
                                     OB_ALL_DETECT_LOCK_INFO_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_DETECT_LOCK_INFO_TNAME));
  } else if (for_dbms_lock && OB_FAIL(check_dbms_lock_record_exist_(inner_conn, full_table_name, task_type, lock_req, is_existed))) {
    LOG_WARN("check dbms_lock record exist failed", K(ret), K(task_type), K(lock_req));
  }

  if (OB_FAIL(ret)) {
  } else if (for_dbms_lock && is_existed) {
    need_record_to_lock_table = false;
  } else if (OB_FAIL(record_detect_info_to_inner_table_(
               inner_conn, full_table_name, task_type, lock_req, need_record_to_lock_table))) {
    LOG_WARN("record_detect_info_to_inner_table_ failed", K(ret), K(task_type), K(lock_req));
  }

  if (need_release_conn && OB_NOT_NULL(inner_conn)) {
    GCTX.sql_proxy_->close(inner_conn, ret);
  }
  return ret;
}

int ObTableLockDetector::remove_detect_info_from_inner_table(sql::ObSQLSessionInfo *session_info,
                                                             const ObTableLockTaskType &task_type,
                                                             const ObLockRequest &lock_req,
                                                             bool &need_remove_from_lock_table)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *inner_conn = nullptr;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  share::ObDMLSqlSplicer dml;
  ObSqlString write_sql;
  int64_t cnt = 0;
  int64_t affected_rows = 0;
  bool need_release_conn = false;

  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  // Only when delete_record successfully, it needs to be removed from lock_table.
  // So we initialize it to false here, if delete failed, we will try to removed
  // it next time.
  need_remove_from_lock_table = false;
  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_sessid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_sessid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret) || OB_ISNULL(inner_conn)) {
  } else if (OB_FAIL(databuff_printf(full_table_name,
                                     OB_MAX_TABLE_NAME_BUF_LENGTH,
                                     "%s.%s",
                                     OB_SYS_DATABASE_NAME,
                                     OB_ALL_DETECT_LOCK_INFO_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_DETECT_LOCK_INFO_TNAME));
  } else if (OB_FAIL(add_pk_column_to_dml_(task_type, lock_req, dml))) {
    LOG_WARN(
      "add_pk_column_to_dml_ for remove_detect_info_from_inner_table failed", KR(ret), K(task_type), K(lock_req));
  } else if (OB_FAIL(get_cnt_of_lock_(full_table_name, inner_conn, dml, cnt))) {
    LOG_WARN("get_cnt_of_lock_ for remove_detect_info_from_inner_table failed",
             KR(ret),
             K(full_table_name),
             K(task_type),
             K(lock_req));
  } else {
    if (cnt <= 1) {
      if (cnt <= 0) {
        LOG_WARN("the reocrd in __all_detect_lock_info didn't remove before", K(lock_req));
      }
      if (OB_FAIL(delete_record_(full_table_name, inner_conn, dml))) {
        LOG_WARN("delete record failed", KR(ret), K(full_table_name), K(task_type), K(lock_req), K(cnt));
      } else {
        need_remove_from_lock_table = true;
      }
    } else {
      if (OB_FAIL(update_cnt_of_lock_(full_table_name, inner_conn, dml))) {
        LOG_WARN("update the cnt of record failed", KR(ret), K(full_table_name), K(task_type), K(lock_req), K(cnt));
      } else {
        need_remove_from_lock_table = false;
      }
    }
  }
  if (need_release_conn && OB_NOT_NULL(inner_conn)) {
    GCTX.sql_proxy_->close(inner_conn, ret);
  }
  return ret;
}

int ObTableLockDetector::remove_detect_info_from_inner_table(sql::ObSQLSessionInfo *session_info,
                                                             const ObTableLockTaskType &task_type,
                                                             const ObLockRequest &lock_req,
                                                             int64_t &cnt)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *inner_conn = nullptr;
  char full_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  share::ObDMLSqlSplicer dml;
  ObSqlString delete_sql;
  bool need_release_conn = false;
  int64_t affected_rows = 0;

  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_sessid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_sessid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret) || OB_ISNULL(inner_conn)) {
  } else if (OB_FAIL(databuff_printf(full_table_name,
                                     OB_MAX_TABLE_NAME_BUF_LENGTH,
                                     "%s.%s",
                                     OB_SYS_DATABASE_NAME,
                                     OB_ALL_DETECT_LOCK_INFO_TNAME))) {
    LOG_WARN("generate full table_name failed", KR(ret), K(OB_SYS_DATABASE_NAME), K(OB_ALL_DETECT_LOCK_INFO_TNAME));
  } else if (OB_FAIL(add_pk_column_to_dml_(task_type, lock_req, dml))) {
    LOG_WARN(
      "add_pk_column_to_dml_ for remove_detect_info_from_inner_table failed", KR(ret), K(task_type), K(lock_req));
  } else if (OB_FAIL(get_cnt_of_lock_(full_table_name, inner_conn, dml, cnt))) {
    LOG_WARN("get_cnt_of_lock_ for remove_detect_info_from_inner_table failed", KR(ret), K(task_type), K(lock_req));
  } else if (cnt > 0) {
    if (OB_FAIL(dml.splice_delete_sql(full_table_name, delete_sql))) {
      LOG_WARN("splice_delete_sql for remove_detect_info_from_inner_table failed",
               KR(ret),
               K(full_table_name),
               K(task_type),
               K(lock_req));
    } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_write_sql(inner_conn, delete_sql, affected_rows))) {
      LOG_WARN("execute_write_sql for remove_detect_info_from_inner_table failed", KR(ret), K(delete_sql));
    }
  }
  if (need_release_conn && OB_NOT_NULL(inner_conn)) {
    GCTX.sql_proxy_->close(inner_conn, ret);
  }
  return ret;
}

int ObTableLockDetector::do_detect_and_clear()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(func1.call_function_directly())) {
    LOG_WARN("do session_alive detect failed", K(ret));
  }
  remove_expired_lock_id();

  return ret;
}

int ObTableLockDetector::remove_lock_by_owner_id(const int64_t raw_owner_id)
{
  int ret = OB_SUCCESS;
  ObReleaseAllLockExecutor executor;
  if (OB_FAIL(executor.execute(raw_owner_id))) {
    LOG_WARN("remove lock by owner_id failed", K(raw_owner_id));
  }
  return ret;
}

int ObTableLockDetector::remove_expired_lock_id()
{
  int ret =OB_SUCCESS;
  ObLockFuncExecutor executor;
  if (OB_FAIL(executor.remove_expired_lock_id())) {
    LOG_WARN("remove expired lock_id failed", K(ret));
  }
  return ret;
}

int ObTableLockDetector::record_detect_info_to_inner_table_(observer::ObInnerSQLConnection *inner_conn,
                                                            const char *table_name,
                                                            const ObTableLockTaskType &task_type,
                                                            const ObLockRequest &lock_req,
                                                            bool &need_record_to_lock_table)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;

  if (OB_FAIL(generate_insert_dml_(task_type, lock_req, dml))) {
    LOG_WARN("generate insert dml failed", K(ret));
  } else if (OB_FAIL(dml.splice_insert_sql(table_name, insert_sql))) {
    LOG_WARN("generate insert sql failed", K(ret), K(table_name));
  } else if (OB_FAIL(insert_sql.append(" ON DUPLICATE  KEY UPDATE cnt = cnt + 1"))) {
    LOG_WARN("append 'cnt = cnt + 1' to the insert_sql failed", K(ret), K(insert_sql));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_write_sql(inner_conn, insert_sql, affected_rows))) {
    LOG_WARN("execute insert sql failed", K(ret), K(insert_sql));
  } else if (affected_rows == 2) {
    need_record_to_lock_table = false;
    LOG_INFO("there's the same lock in __all_detect_lock_info table, no need to record it to the lock table",
             K(lock_req));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only can affetct 1 row due to insert, or 2 rows due to insert on duplicate key", K(affected_rows));
  }
  return ret;
}

int ObTableLockDetector::check_dbms_lock_record_exist_(observer::ObInnerSQLConnection *inner_conn,
                                                            const char *table_name,
                                                            const ObTableLockTaskType &task_type,
                                                            const ObLockRequest &lock_req,
                                                            bool &is_existed)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t obj_type = static_cast<uint64_t>(ObLockOBJType::OBJ_TYPE_INVALID);
  uint64_t obj_id = OB_INVALID_ID;

  if (LOCK_OBJECT != task_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the task_type for DBMS_LOCK should be LOCK_OBJECT", K(ret), K(task_type), K(lock_req));
  } else {
    const ObLockObjsRequest &arg = static_cast<const ObLockObjsRequest &>(lock_req);
    if (arg.objs_.count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support detect batch lock obj request right now", K(arg));
    } else {
      obj_type = static_cast<uint64_t>(arg.objs_[0].obj_type_);
      obj_id = arg.objs_[0].obj_id_;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt("SELECT 1 FROM %s WHERE task_type = %d AND obj_type = %" PRIu64 " AND obj_id = %" PRIu64
                            " AND owner_id = %" PRId64,
                            table_name,
                            static_cast<int>(task_type),
                            obj_type,
                            obj_id,
                            lock_req.owner_id_.raw_value()))) {
    LOG_WARN("make select sql failed", K(ret), K(task_type), K(lock_req));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(ObInnerConnectionLockUtil::execute_read_sql(inner_conn, sql, res))) {
        LOG_WARN("execute_read_sql for check_dbms_lock_record_exist_ failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_existed = false;
        }
      } else {
        is_existed = true;
      }
    }
  }
  return ret;
}

int ObTableLockDetector::generate_insert_dml_(const ObTableLockTaskType &task_type,
                                              const ObLockRequest &lock_req,
                                              share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t cnt = 1;
  if (LOCK_OBJECT != task_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support register detect task which is not LOCK_OBJECT right now");
  } else {
    const ObLockObjsRequest &arg = static_cast<const ObLockObjsRequest &>(lock_req);
    if (OB_FAIL(add_pk_column_to_dml_(task_type, lock_req, dml))) {
      LOG_WARN("add_pk_column_to_dml_ for generate_insert_dml_ failed", KR(ret), K(task_type), K(lock_req));
    } else if (OB_FAIL(dml.add_column("cnt", cnt))
               || OB_FAIL(dml.add_column("detect_func_no", static_cast<uint64_t>(arg.detect_func_no_)))
               || OB_FAIL(dml.add_column("detect_func_param", ObHexEscapeSqlStr(arg.detect_param_)))) {
      LOG_WARN("add column for insert dml failed", K(ret));
    }
  }

  return ret;
}

int ObTableLockDetector::add_pk_column_to_dml_(const ObTableLockTaskType &task_type,
                                               const ObLockRequest &lock_req,
                                               share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t obj_type = static_cast<uint64_t>(ObLockOBJType::OBJ_TYPE_INVALID);
  uint64_t obj_id = OB_INVALID_ID;

  switch (task_type) {
  case LOCK_OBJECT: {
    const ObLockObjsRequest &arg = static_cast<const ObLockObjsRequest &>(lock_req);
    if (arg.objs_.count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support detect batch lock obj request right now", K(arg));
    } else {
      obj_type = static_cast<uint64_t>(arg.objs_[0].obj_type_);
      obj_id = arg.objs_[0].obj_id_;
    }
    break;
  }
  default: {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support detect lock live for the task_type which is not LOCK_OBJECT right now",
             K(task_type),
             K(lock_req));
  }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("task_type", static_cast<uint64_t>(task_type)))
             || OB_FAIL(dml.add_pk_column("obj_type", obj_type)) || OB_FAIL(dml.add_pk_column("obj_id", obj_id))
             || OB_FAIL(dml.add_pk_column("lock_mode", static_cast<uint64_t>(lock_req.lock_mode_)))
             || OB_FAIL(dml.add_pk_column("owner_id", lock_req.owner_id_.raw_value()))) {
    LOG_WARN("add pk column to dml failed", K(ret), K(lock_req));
  }

  return ret;
}

int ObTableLockDetector::generate_update_sql_(const char *table_name,
                                              const share::ObDMLSqlSplicer &dml,
                                              ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt("UPDATE %s SET cnt = cnt - 1 WHERE ", table_name))) {
    LOG_WARN("append prefix for update sql failed", KR(ret), K(table_name));
  } else if (OB_FAIL(dml.splice_predicates(sql))) {
    LOG_WARN("splice_predicates for update sql failed", KR(ret), K(table_name));
  }
  return ret;
}

int ObTableLockDetector::generate_select_sql_(const char *table_name,
                                              const share::ObDMLSqlSplicer &dml,
                                              ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt("SELECT cnt FROM %s WHERE ", table_name))) {
    LOG_WARN("append prefix for select sql failed", KR(ret), K(table_name));
  } else if (OB_FAIL(dml.splice_predicates(sql))) {
    LOG_WARN("splice_predicates for select sql failed", KR(ret), K(table_name));
  }
  return ret;
}

int ObTableLockDetector::delete_record_(const char *table_name,
                                        observer::ObInnerSQLConnection *conn,
                                        const share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(dml.splice_delete_sql(table_name, sql))) {
    LOG_WARN("splice_delete_sql for delete_record_ failed", KR(ret), K(table_name));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_write_sql(conn, sql, affected_rows))) {
    LOG_WARN("execute_write_sql for delete_record_ failed", KR(ret), K(sql));
  } else if (affected_rows != 1) {
    LOG_WARN("do not delete the record", KR(ret), K(sql), K(affected_rows));
  }
  return ret;
}

int ObTableLockDetector::update_cnt_of_lock_(const char *table_name,
                                             observer::ObInnerSQLConnection *conn,
                                             const share::ObDMLSqlSplicer &dml)
{

  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(generate_update_sql_(table_name, dml, sql))) {
  } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_write_sql(conn, sql, affected_rows))) {
    LOG_WARN("execute_write_sql for update_cnt_of_lock_ failed", KR(ret), K(sql));
  } else if (affected_rows != 1) {
    LOG_WARN("do not update the record", K(sql));
  }
  return ret;
}

int ObTableLockDetector::get_cnt_of_lock_(const char *table_name,
                                          observer::ObInnerSQLConnection *conn,
                                          const share::ObDMLSqlSplicer &dml,
                                          int64_t &cnt)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(generate_select_sql_(table_name, dml, sql))) {
      LOG_WARN("generate_select_sql_ for get_cnt_of_lock_ failed", KR(ret), K(table_name));
    } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_read_sql(conn, sql, res))) {
      LOG_WARN("execute_read_sql for get_cnt_of_lock_ failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_EMPTY_RESULT;
        LOG_WARN("can not find lock", K(ret), K(sql));
      }
    } else {
      (void)GET_COL_IGNORE_NULL(result->get_int, "cnt", cnt);
    }
  }  // end SMART_VAR


  return ret;
}

bool ObTableLockDetector::is_unlock_task_(const ObTableLockTaskType &task_type)
{
  return (UNLOCK_TABLE == task_type || UNLOCK_PARTITION == task_type || UNLOCK_SUBPARTITION == task_type
          || UNLOCK_TABLET == task_type || UNLOCK_OBJECT == task_type || UNLOCK_ALONE_TABLET == task_type);
}
}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase
