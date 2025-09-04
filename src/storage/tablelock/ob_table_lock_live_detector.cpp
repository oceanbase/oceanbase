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
#include "storage/tablelock/ob_lock_executor.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "storage/tablelock/ob_lock_func_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "share/ob_table_lock_compat_versions.h"

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
  ObArray<ObTableLockOwnerID *> owner_ids;
  bool client_session_alive = true;
  ObTableLockOwnerID owner_id;
  uint32_t client_session_id = sql::ObSQLSessionInfo::INVALID_SESSID;
  ObArenaAllocator allocator;
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);

  if (OB_FAIL(get_owner_id_list_from_table_(allocator, owner_ids))) {
    LOG_WARN("get owner_id_list from table failed", K(ret));
  } else {
    for (int64_t i = 0; i < owner_ids.count() && OB_SUCC(ret); i++) {
      client_session_alive = true;
      owner_id = *owner_ids.at(i);
      if (!owner_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("owner_id is invalid", K(ret), K(owner_id));
      } else if (OB_FAIL(owner_id.convert_to_sessid(client_session_id))) {
        LOG_WARN("get client_sesion_id failed", K(ret), K(owner_id));
      } else if (OB_FAIL(do_session_alive_detect_for_a_client_session_(client_session_id,
                                                                       client_session_alive))) {
        LOG_WARN("do session alive detect for a client_session failed", K(client_session_id));
      } else if (!client_session_alive) {
        LOG_INFO(
          "find client session is not alive, we will clean all recodrs of it later", K(ret), K(client_session_id), K(owner_id));
        ObTableLockDetector::remove_lock_by_owner_id(owner_id);
      }
    }
  }
  for (int64_t i = 0; i < owner_ids.count(); i++) {
    ObTableLockOwnerID *ptr = owner_ids.at(i);
    if (OB_ISNULL(ptr)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "the owner_id should not be null", K(ret));
    } else {
      ptr->~ObTableLockOwnerID();
      allocator.free(ptr);
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
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_CLIENT_TO_SERVER_SESSION_INFO_TNAME));
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
      } else if (!server_addr.set_ip_addr(svr_ip, svr_port)) {
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
                        "r.resource_pool_id = u.resource_pool_id WHERE tenant_id = %ld) and svr_ip = '%.*s' and svr_port "
                        "= %ld and status = 'active'",
                        OB_SYS_DATABASE_NAME,
                        OB_ALL_UNIT_TNAME,
                        OB_SYS_DATABASE_NAME,
                        OB_ALL_RESOURCE_POOL_TNAME,
                        MTL_ID(),
                        svr_ip.length(),
                        svr_ip.ptr(),
                        svr_port))) {
    LOG_WARN("generate where condition for select sql failed", K(svr_ip), K(svr_port));
  } else if (OB_FAIL(databuff_printf(full_table_name,
                                     OB_MAX_TABLE_NAME_BUF_LENGTH,
                                     "%s.%s",
                                     OB_SYS_DATABASE_NAME,
                                     OB_ALL_SERVER_TNAME))) {
    LOG_WARN("generate full table_name failed", K(OB_SYS_DATABASE_NAME), K(OB_ALL_SERVER_TNAME));
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

int ObTableLockDetectFuncList::get_owner_id_list_from_table_(ObIAllocator &allocator, ObArray<ObTableLockOwnerID *> &owner_ids)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  if (OB_FAIL((GET_MIN_DATA_VERSION(MTL_ID(), data_version)))) {
    LOG_WARN("get data_version failed", K(ret));
  } else if (is_mysql_lock_table_data_version(data_version)) {
    if (OB_FAIL(get_owner_id_list_from_table_(allocator, true, owner_ids))) {
      LOG_WARN("get owner_ids from new table failed", K(ret));
    } else if (!ObTableLockDetector::old_detect_table_is_empty() && OB_FAIL(get_owner_id_list_from_table_(allocator, false, owner_ids))) {
      LOG_WARN("get owner_ids from old table failed", K(ret));
    }
  } else if (OB_FAIL(get_owner_id_list_from_table_(allocator, false, owner_ids))) {
    LOG_WARN("get owner_ids from old table failed", K(ret));
  }
  return ret;
}

int ObTableLockDetectFuncList::get_owner_id_list_from_table_(ObIAllocator &allocator,
                                                             const bool is_new_table,
                                                             ObArray<ObTableLockOwnerID *> &owner_ids)
{
  int ret = OB_SUCCESS;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  char where_cond[64] = {"WHERE detect_func_no = 1 GROUP BY owner_id"};
  void *ptr = nullptr;
  ObTableLockOwnerID *new_owner_id = nullptr;

  if (OB_FAIL(ObTableLockDetector::get_table_name(is_new_table, table_name))) {
    LOG_WARN("generate full table_name failed", K(is_new_table));
  } else if (is_new_table) {
    ObArray<ObTuple<int64_t, int64_t>> tmp_owner_ids;
    if (OB_FAIL(ObTableAccessHelper::read_multi_row(MTL_ID(), {"owner_type", "owner_id"}, table_name, where_cond, tmp_owner_ids))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("read from inner table __all_detect_lock_info_v2 failed", K(ret), K(is_new_table));
      }
    } else {
      for (int64_t i = 0; i < tmp_owner_ids.count() && OB_SUCC(ret); i++) {
        if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTableLockOwnerID)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for ObTableLockOwnerID failed", K(ret), K(is_new_table), K(tmp_owner_ids.at(i)));
        } else if (FALSE_IT(new_owner_id = new (ptr) ObTableLockOwnerID(static_cast<unsigned char>(tmp_owner_ids.at(i).element<0>()), tmp_owner_ids.at(i).element<1>()))) {
        } else if (OB_FAIL(owner_ids.push_back(new_owner_id))) {
          LOG_WARN("add owner_id into list failed", K(ret), K(new_owner_id), K(is_new_table));
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(new_owner_id)) {
          new_owner_id->~ObTableLockOwnerID();
          allocator.free(ptr);
        }
      }
    }
  } else {
    ObArray<ObTuple<int64_t>> tmp_owner_ids;
    if (OB_FAIL(ObTableAccessHelper::read_multi_row(MTL_ID(), {"owner_id"}, table_name, where_cond, tmp_owner_ids))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("read from inner table __all_detect_lock_info_v2 failed", K(ret), K(is_new_table));
      }
    } else {
      for (int64_t i = 0; i < tmp_owner_ids.count() && OB_SUCC(ret); i++) {
        ObOldLockOwner old_owner_id;
        old_owner_id.convert_from_value(tmp_owner_ids.at(i).element<0>());
        new_owner_id = nullptr;

        if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTableLockOwnerID)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory for ObTableLockOwnerID failed", K(ret), K(is_new_table), K(tmp_owner_ids.at(i)));
        } else if (FALSE_IT(new_owner_id = new (ptr) ObTableLockOwnerID(static_cast<unsigned char>(old_owner_id.type()), old_owner_id.id()))) {
        } else if (OB_FAIL(owner_ids.push_back(new_owner_id))) {
          LOG_WARN("add owner_id into list failed", K(ret), K(new_owner_id), K(is_new_table));
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(new_owner_id)) {
          new_owner_id->~ObTableLockOwnerID();
          allocator.free(ptr);
        }
      }
    }
  }
  return ret;
}

int ObTableLockDetectFuncList::do_session_at_least_one_alive_detect_for_rpc(const uint32_t session_id, obrpc::Bool &is_alive)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSessionAliveChecker session_alive_task(session_id);
  sql::ObSQLSessionMgr *session_mgr = GCTX.session_mgr_;
  if (OB_ISNULL(session_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there's no session mgr in GCTX", KR(ret), K(session_id));
  } else if (OB_FAIL(session_mgr->for_each_session(session_alive_task))) {
    LOG_WARN("fail to check session alive", KR(ret), K(session_alive_task));
  } else {
    is_alive = session_alive_task.is_alive();
  }
  LOG_INFO("detect session alive", K(session_id), K(is_alive));
  return ret;
}

int ObTableLockDetectFuncList::get_tenant_servers(const uint64_t tenant_id, common::ObIArray<common::ObAddr> &server_array)
{
  int ret = OB_SUCCESS;
  ObUnitTableOperator ut_operator;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.root_service_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ut_operator.init(*GCTX.sql_proxy_))) {
    LOG_WARN("fail to init unit table operator", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ut_operator.get_all_servers_by_tenant(tenant_id, server_array, true /*strict*/))) {
    LOG_WARN("get alive server failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTableLockDetectFuncList::do_session_at_least_one_alive_detect(const uint64_t tenant_id, const uint32_t session_id, const ObArray<ObAddr> *dest_server, bool &is_alive)
{
  int ret = OB_SUCCESS;
  is_alive = true;
  const ObArray<ObAddr> *server_list = nullptr;
  ObArray<ObAddr> tmp_server;
  if (OB_ISNULL(dest_server)){
    if (OB_FAIL(get_tenant_servers(tenant_id, tmp_server))) {
      LOG_WARN("fail to get tenant server", KR(ret), K(tenant_id));
    }
    server_list = &tmp_server;
  } else {
    server_list = dest_server;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server rpc proxy is null", KR(ret));
  } else {
    rootserver::ObDetectClientSessionAliveProxy proxy(*GCTX.srv_rpc_proxy_, &ObSrvRpcProxy::detect_client_session_alive);
    const int64_t rpc_timeout = GCONF.rpc_timeout;
    int64_t timeout = 0;
    FOREACH_X(s, *server_list, OB_SUCC(ret)) {
      if (OB_ISNULL(s)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("s is null", K(ret));
      } else {
        timeout = std::min(THIS_WORKER.get_timeout_remain(), rpc_timeout);
        if (FAILEDx(proxy.call(*s, timeout, session_id))) {
          LOG_WARN("send session alive detect rpc failed", KR(ret),
              K(timeout), K(session_id), "server", *s);
        }
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      ObArray<int> return_code_array;
      if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
        LOG_WARN("wait all batch result failed", KR(ret), KR(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
      if (FAILEDx(proxy.check_return_cnt(return_code_array.count()))) {
        LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
      }
      is_alive = false;
      ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
        const obrpc::Bool *result = proxy.get_results().at(idx);
        const ObAddr &dest_addr = proxy.get_dests().at(idx);
        tmp_ret = return_code_array.at(idx);
        if (OB_SUCCESS != tmp_ret) {
          is_alive = true;
          LOG_WARN("fail to send rpc to detect alive session", KR(ret), KR(tmp_ret), K(dest_addr),
                                                               K(session_id), K(idx));
          break;
        } else if (OB_ISNULL(result)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret), KR(tmp_ret), KP(result));
        } else if (true == *result) {
          is_alive = true;
          break;
        }
        LOG_INFO("detect session alive", K(session_id), K(dest_addr), K(*result), K(is_alive));
      }
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
  bool need_release_conn = false;
  bool is_existed = false;

  need_record_to_lock_table = true;
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  if (!(LOCK_OBJECT == task_type || LOCK_TABLE == task_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support detect task type", K(ret), K(task_type));
  } else if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_server_sid()));

    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_server_sid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(inner_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner connection is null", K(ret), K(session_info->get_server_sid()));
  } else if (for_dbms_lock
             && OB_FAIL(check_lock_exist_in_inner_table(session_info, task_type, lock_req, is_existed))) {
    LOG_WARN("check dbms_lock record exist failed", K(ret), K(task_type), K(lock_req));
  }

  if (OB_FAIL(ret)) {
  } else if (for_dbms_lock && is_existed) {
    need_record_to_lock_table = false;
  } else if (OB_FAIL(record_detect_info_to_inner_table_(
               inner_conn, task_type, lock_req, need_record_to_lock_table))) {
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
  ObSqlString write_sql;
  int64_t cnt = 0;
  int64_t cnt_in_old_table = 0;
  int64_t affected_rows = 0;
  bool need_release_conn = false;
  bool is_from_old_table = false;
  share::ObDMLSqlSplicer dml;

  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  // Only when delete_record successfully, it needs to be removed from lock_table.
  // So we initialize it to false here, if delete failed, we will try to removed
  // it next time.
  need_remove_from_lock_table = false;
  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_server_sid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_server_sid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if(OB_ISNULL(inner_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner connection is null", K(ret), K(session_info->get_server_sid()));
  } else if (OB_FAIL(get_cnt_of_lock_(
               inner_conn, task_type, lock_req, false /*only_check_old_table*/, cnt, is_from_old_table))) {
    LOG_WARN("get_cnt_of_lock_ for remove_detect_info_from_inner_table failed",
             KR(ret),
             K(full_table_name),
             K(task_type),
             K(lock_req));
  } else if (OB_FAIL(
               get_table_name_and_dml_with_pk_column_(!is_from_old_table, task_type, lock_req, full_table_name, dml))) {
    LOG_WARN("get_table_name_and_dml_with_pk_column_ for remove_detect_info_from_inner_table failed",
             K(ret),
             K(task_type),
             K(lock_req),
             K(is_from_old_table));
  } else if (cnt <= 1) {
    if (cnt <= 0) {
      LOG_WARN("the reocrd in __all_detect_lock_info didn't remove before", K(lock_req));
    } else if (OB_FAIL(delete_record_(full_table_name, inner_conn, dml))) {
      LOG_WARN("delete record failed", KR(ret), K(full_table_name), K(task_type), K(lock_req), K(cnt));
    } else if (is_from_old_table) {
      need_remove_from_lock_table = true;
    } else {
      // delete the record in new table, and recheck whether there's other records in old table
      if (OB_FAIL(get_cnt_of_lock_(inner_conn, task_type, lock_req, true /*only_check_old_table*/, cnt_in_old_table, is_from_old_table))) {
        LOG_WARN("get_cnt_of_lock_ for remove_detect_info_from_inner_table failed",
                 KR(ret),
                 K(full_table_name),
                 K(task_type),
                 K(lock_req));
      } else if (cnt_in_old_table == 0) {
        need_remove_from_lock_table = true;
      }
    }
  } else {
    if (OB_FAIL(update_cnt_of_lock_(full_table_name, inner_conn, dml))) {
      LOG_WARN("update the cnt of record failed", KR(ret), K(full_table_name), K(task_type), K(lock_req), K(cnt));
    } else {
      need_remove_from_lock_table = false;
    }
  }

  // didn't get any lock in old and new tables, should return OB_EMPTY_RESULT to make release_lock return NULL
  if (cnt == 0) {
    ret = OB_EMPTY_RESULT;
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
  bool need_release_conn = false;
  int64_t affected_rows = 0;
  int64_t cnt_in_new_table = 0;
  int64_t cnt_in_old_table = 0;

  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_server_sid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_server_sid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(inner_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner connection is null", K(ret), K(session_info->get_server_sid()));
  } else if (OB_FAIL(get_cnt_of_lock_(inner_conn, task_type, lock_req, cnt_in_new_table, cnt_in_old_table))) {
    LOG_WARN("get_cnt_of_lock_ for remove_detect_info_from_inner_table failed", KR(ret), K(task_type), K(lock_req));
  } else if (OB_FAIL(remove_detect_info_from_table_(inner_conn, task_type, lock_req, cnt_in_old_table, cnt_in_new_table, cnt))) {
    LOG_WARN("remove_detect_info_from_table_ failed", K(ret), K(task_type), K(lock_req));
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
  check_and_set_old_detect_table_is_empty();

  return ret;
}

int ObTableLockDetector::remove_lock_by_owner_id(const ObTableLockOwnerID &owner_id)
{
  int ret = OB_SUCCESS;
  ObUnLockExecutor executor;
  if (OB_FAIL(executor.execute(owner_id))) {
    LOG_WARN("remove lock by owner_id failed", K(owner_id));
  }
  return ret;
}

int ObTableLockDetector::remove_expired_lock_id()
{
  int ret =OB_SUCCESS;
  char dbms_lock_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  char old_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  char new_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  ObSqlString where_cond;
  ObSqlString detect_table_cond;
  ObSqlString obj_type_cond;
  uint64_t data_version = 0;
  const int64_t now = ObTimeUtility::current_time();
  // delete 10 rows each time, to avoid causing abnormal delays due to deleting too many rows
  const int delete_limit = 10;

  OZ (databuff_printf(dbms_lock_table_name, OB_MAX_TABLE_NAME_BUF_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DBMS_LOCK_ALLOCATED_TNAME));
  OZ (get_table_name(false, old_table_name));
  OZ (get_table_name(true, new_table_name));
  OZ (obj_type_cond.assign_fmt(" WHERE obj_type = %d or obj_type = %d",
                               static_cast<int>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC),
                               static_cast<int>(ObLockOBJType::OBJ_TYPE_DBMS_LOCK)));

  OZ (GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  if (is_mysql_lock_table_data_version(data_version)) {
    OZ (detect_table_cond.assign_fmt("SELECT obj_id FROM %s", new_table_name));
    OZ (detect_table_cond.append(obj_type_cond.ptr(), obj_type_cond.length()));
    if (!old_detect_table_is_empty()) {
      OZ (detect_table_cond.append_fmt(" UNION SELECT obj_id FROM %s", old_table_name));
      OZ (detect_table_cond.append(obj_type_cond.ptr(), obj_type_cond.length()));
    }
  } else {
    OZ (detect_table_cond.assign_fmt("SELECT obj_id FROM %s", old_table_name));
    OZ (detect_table_cond.append(obj_type_cond.ptr(), obj_type_cond.length()));
  }

  OZ (where_cond.assign_fmt("expiration <= usec_to_time(%" PRId64 ")"
                            "AND lockid NOT IN"
                            "( %s )"
                            " LIMIT %d",
                            now,
                            detect_table_cond.ptr(),
                            delete_limit));
  OZ (ObTableAccessHelper::delete_row(MTL_ID(), dbms_lock_table_name, where_cond.string()));
  return ret;
}

int ObTableLockDetector::check_and_set_old_detect_table_is_empty()
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  bool exist_row = false;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  char where_cond[16] = {"LIMIT 1"};
  ObTableLockService *lock_service = MTL(ObTableLockService *);

  if (!old_detect_table_is_empty()) {
    OZ (GET_MIN_DATA_VERSION(MTL_ID(), data_version));
    if (is_mysql_lock_table_data_version(data_version)) {
      OZ (get_table_name(false, table_name));
      OZ (ObTableAccessHelper::read_single_row(MTL_ID(), {"1"}, table_name, where_cond, exist_row));
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
        if (OB_ISNULL(lock_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ObTableLockService is null!", K(ret));
        } else {
          lock_service->set_old_detect_table_is_empty();
          FLOG_INFO("old detect_lock_info table is empty, no need to visit again");
        }
      }
    }
  }
  return ret;
}

int ObTableLockDetector::check_lock_id_exist_in_inner_table(sql::ObSQLSessionInfo *session_info,
                                                            const uint64_t &obj_id,
                                                            const ObLockOBJType &obj_type,
                                                            bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  observer::ObInnerSQLConnection *inner_conn = nullptr;
  bool need_release_conn = false;

  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_server_sid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_server_sid()));
    } else {
      need_release_conn = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(inner_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner connection is null", K(ret), K(session_info->get_server_sid()));
  } else if (OB_FAIL(where_cond.assign_fmt("obj_id = %" PRIu64 " AND obj_type = %d", obj_id, static_cast<int>(obj_type)))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(check_lock_exist_(inner_conn, where_cond, exist))) {
    LOG_WARN("check lock exist failed", K(ret));
  }

  if (need_release_conn && OB_NOT_NULL(inner_conn)) {
    GCTX.sql_proxy_->close(inner_conn, ret);
  }
  return ret;
}

int ObTableLockDetector::check_lock_owner_exist_in_inner_table(sql::ObSQLSessionInfo *session_info,
                                                               const uint32_t client_session_id,
                                                               const uint64_t client_session_create_ts,
                                                               bool &exist)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *inner_conn = nullptr;
  bool need_release_conn = false;
  ObSqlString where_cond;

  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_server_sid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_server_sid()));
    } else {
      need_release_conn = true;
    }
  }
 
  if (client_session_create_ts <= 0) {
    // if client_session_create_ts <= 0, means there's no accurate client_session_create_ts
    // (from lock live detector), so we only judge client_session_id in this situation
    OZ (where_cond.assign_fmt(
        "(owner_id & %" PRId64 ") = %" PRIu32, ObTableLockOwnerID::CLIENT_SESS_ID_MASK, client_session_id));
    OZ (check_lock_exist_(inner_conn, where_cond, exist));
  } else {
    ObTableLockOwnerID lock_owner;
    OZ (lock_owner.convert_from_client_sessid(client_session_id, client_session_create_ts));
    OZ (check_lock_exist_(inner_conn, where_cond, lock_owner, exist));
  }

  if (need_release_conn && OB_NOT_NULL(inner_conn)) {
    GCTX.sql_proxy_->close(inner_conn, ret);
  }
  return ret;
}

int ObTableLockDetector::check_lock_exist_in_inner_table(sql::ObSQLSessionInfo *session_info,
                                                         const ObTableLockTaskType &task_type,
                                                         const ObLockRequest &lock_req,
                                                         bool &exist)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  uint64_t obj_type = static_cast<uint64_t>(ObLockOBJType::OBJ_TYPE_INVALID);
  uint64_t obj_id = OB_INVALID_ID;

  observer::ObInnerSQLConnection *inner_conn = nullptr;
  bool need_release_conn = false;

  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session_info->get_server_sid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session_info, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session_info->get_server_sid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (LOCK_OBJECT != task_type) {
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
  // to ensure subsequent SQL queries, we do not check the owner_type column
  } else if (OB_FAIL(where_cond.assign_fmt("task_type = %d AND obj_type = %" PRIu64 " AND obj_id = %" PRIu64,
                                           static_cast<int>(task_type),
                                           obj_type,
                                           obj_id))) {
    LOG_WARN("make where_cond of select sql failed", K(ret), K(task_type), K(lock_req));
  } else if (OB_FAIL(check_lock_exist_(inner_conn, where_cond, lock_req.owner_id_, exist))) {
    LOG_WARN("check_lock_exist_ failed", K(ret), K(task_type), K(lock_req));
  }

  if (need_release_conn && OB_NOT_NULL(inner_conn)) {
    GCTX.sql_proxy_->close(inner_conn, ret);
  }

  return ret;
}

int ObTableLockDetector::get_lock_owner_by_lock_id(const uint64_t &lock_id, ObTableLockOwnerID &lock_owner)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  uint64_t tenant_id = MTL_ID();
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  uint64_t data_version = 0;
  int64_t owner_id = 0;
  int64_t owner_type = 0;

  OZ (GET_MIN_DATA_VERSION(tenant_id, data_version));
  OZ (where_cond.assign_fmt("WHERE obj_type = '%d' AND"
                            " obj_id = %ld AND lock_mode = %d",
                            static_cast<int>(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC),
                            lock_id,
                            static_cast<int>(EXCLUSIVE)));
  if (is_mysql_lock_table_data_version(data_version)) {
    OZ (get_table_name(true, table_name));
    OZ (ObTableAccessHelper::read_single_row(
       tenant_id, {"owner_id", "owner_type"}, table_name, where_cond.string(), owner_id, owner_type));
    OX (lock_owner.convert_from_value(static_cast<ObLockOwnerType>(owner_type), owner_id));
  }
  if (OB_EMPTY_RESULT == ret || (OB_SUCC(ret) && !is_mysql_lock_table_data_version(data_version))) {
    memset(table_name, 0, OB_MAX_TABLE_NAME_BUF_LENGTH);
    ret = OB_SUCCESS;
    OZ (get_table_name(false, table_name));
    OZ (ObTableAccessHelper::read_single_row(tenant_id, {"owner_id"}, table_name, where_cond.string(), owner_id));
    ObOldLockOwner old_owner_id;
    OZ (old_owner_id.convert_from_value(owner_id));
    OX (lock_owner.convert_from_value(static_cast<ObLockOwnerType>(old_owner_id.type()), old_owner_id.id()));
  }
  return ret;
}

int ObTableLockDetector::get_unlock_request_list(sql::ObSQLSessionInfo *session,
                                                 const ObTableLockOwnerID &owner_id,
                                                 const ObTableLockTaskType task_type,
                                                 ObIAllocator &allocator,
                                                 ObIArray<ObLockRequest *> &arg_list)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  bool need_release_conn = false;
  observer::ObInnerSQLConnection *inner_conn = nullptr;
  arg_list.reset();

  if (OB_ISNULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session->get_inner_conn()))) {
    LOG_INFO("there is no inner connection in the session, we will try to create one", K(session->get_server_sid()));
    if (OB_FAIL(ObInnerConnectionLockUtil::create_inner_conn(session, GCTX.sql_proxy_, inner_conn))) {
      LOG_WARN("get inner connection failed", K(session->get_server_sid()));
    } else {
      need_release_conn = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(inner_conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner connection is null", K(ret), K(session->get_server_sid()));
  } else if (OB_FAIL(generate_get_unlock_request_sql_(owner_id, task_type, sql))) {
    LOG_WARN("generate get_unlock_request sql failed", K(ret), K(owner_id), K(task_type), K(sql));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(ObInnerConnectionLockUtil::execute_read_sql(inner_conn, sql, res))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", KR(ret));
      } else if (OB_FAIL(get_unlock_request_list_(result, allocator, arg_list))) {
        LOG_WARN("get unlock_reuqest list failed", KR(ret));
      } else if (OB_FAIL(fill_owner_id_for_unlock_request_(owner_id, arg_list))) {
        LOG_WARN("fill unlock_reuqest list with owner_id failed", K(owner_id), K(arg_list));
      }
    }  // end SMART_VAR
  }

  if (need_release_conn && OB_NOT_NULL(inner_conn)) {
    GCTX.sql_proxy_->close(inner_conn, ret);
  }
  return ret;
}

bool ObTableLockDetector::old_detect_table_is_empty()
{
  bool old_is_empty = false;
  ObTableLockService *lock_service = MTL(ObTableLockService *);
  if (OB_NOT_NULL(lock_service)) {
    old_is_empty = lock_service->old_detect_table_is_empty();
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ObTableLockService is null!");
  }
  return old_is_empty;
}

int ObTableLockDetector::get_lock_mode_by_owner_id_and_lock_id(const int64_t tenant_id,
                                                               const ObTableLockOwnerID &owner_id,
                                                               const uint64_t lock_id,
                                                               ObTableLockMode &lock_mode)
{
  int ret = OB_SUCCESS;
  ObSqlString where_cond;
  int64_t raw_lock_mode = 0;
  uint64_t data_version = 0;
  lock_mode = NO_LOCK;

  OZ (GET_MIN_DATA_VERSION(tenant_id, data_version));
  if (is_mysql_lock_table_data_version(data_version)) {
    OZ (where_cond.assign_fmt("WHERE obj_id = %" PRIu64 " AND owner_id = %" PRId64 " AND owner_type = %d",
                              lock_id,
                              owner_id.id(),
                              owner_id.type()));
    OZ (ObTableAccessHelper::read_single_row(
       tenant_id, {"lock_mode"}, OB_ALL_DETECT_LOCK_INFO_V2_TNAME, where_cond.string(), raw_lock_mode));
  }
  if (OB_EMPTY_RESULT == ret || (OB_SUCC(ret) && !is_mysql_lock_table_data_version(data_version))) {
    ObOldLockOwner old_owner_id(owner_id);
    where_cond.reset();
    OZ (where_cond.assign_fmt("WHERE obj_id = %" PRIu64 " AND owner_id = %" PRId64, lock_id, old_owner_id.raw_value()));
    OZ (ObTableAccessHelper::read_single_row(
       tenant_id, {"lock_mode"}, OB_ALL_DETECT_LOCK_INFO_TNAME, where_cond.string(), raw_lock_mode));
  }
  OX (lock_mode = raw_lock_mode);
  return ret;
}

int ObTableLockDetector::check_lock_exist_(observer::ObInnerSQLConnection *inner_conn,
                                           const ObSqlString &where_cond,
                                           const ObTableLockOwnerID &lock_owner,
                                           bool &exist)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  OZ (GET_MIN_DATA_VERSION(MTL_ID(), data_version));

  // Only check the V2 table when the owner_id is the new version
  if (is_mysql_lock_table_data_version(data_version)) {
    OZ (check_lock_exist_in_table_(inner_conn, where_cond, lock_owner, true, exist));
  }

  // If the lock is not existed (didn't check before or there's no lock in V2 table),
  // we will try to check the V1 table.
  if (!exist && !old_detect_table_is_empty()) {
    OZ (check_lock_exist_in_table_(inner_conn, where_cond, lock_owner, false, exist));
  }
  return ret;
}

int ObTableLockDetector::check_lock_exist_(observer::ObInnerSQLConnection *inner_conn,
                                           const ObSqlString &where_cond,
                                           bool &exist)
{
  int ret = OB_SUCCESS;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  uint64_t data_version = 0;

  OZ (GET_MIN_DATA_VERSION(MTL_ID(), data_version));

  // Only check the V2 table when the owner_id is the new version
  if (is_mysql_lock_table_data_version(data_version)) {
    OZ (get_table_name(true, table_name));
    OZ (check_lock_exist_in_table_(inner_conn, table_name, where_cond, exist));
  }

  // If the lock is not existed (didn't check before or there's no lock in V2 table),
  // we will try to check the V1 table.
  if (!exist && !old_detect_table_is_empty()) {
    memset(table_name, 0, sizeof(table_name));
    OZ (get_table_name(false, table_name));
    OZ (check_lock_exist_in_table_(inner_conn, table_name, where_cond, exist));
  }
  return ret;
}

int ObTableLockDetector::check_lock_exist_in_table_(observer::ObInnerSQLConnection *inner_conn,
                                                    const char *table_name,
                                                    const ObSqlString &where_cond,
                                                    bool &exist)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT owner_id FROM %s WHERE %s", table_name, where_cond.ptr()))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(ObInnerConnectionLockUtil::execute_read_sql(inner_conn, sql, res))) {
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
  }
  return ret;
}

int ObTableLockDetector::check_lock_exist_in_table_(observer::ObInnerSQLConnection *inner_conn,
                                                    const ObSqlString &where_cond,
                                                    const ObTableLockOwnerID lock_owner,
                                                    const bool is_new_table,
                                                    bool &exist)
{
  int ret = OB_SUCCESS;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  ObSqlString lock_owner_cond;
  ObSqlString new_where_cond;

  OZ (get_table_name(is_new_table, table_name));
  OZ (get_lock_owner_where_cond_(lock_owner, is_new_table, lock_owner_cond));
  OZ (new_where_cond.assign(lock_owner_cond));
  if (!where_cond.empty()) {
    OZ (new_where_cond.append_fmt(" AND %s", where_cond.ptr()));
  }
  OZ (check_lock_exist_in_table_(inner_conn, table_name, new_where_cond, exist));

  return ret;
}

int ObTableLockDetector::record_detect_info_to_inner_table_(observer::ObInnerSQLConnection *inner_conn,
                                                            const ObTableLockTaskType &task_type,
                                                            const ObLockRequest &lock_req,
                                                            bool &need_record_to_lock_table)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;
  uint64_t data_version = 0;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  bool is_new_table = false;

  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get data_version failed", K(ret));
  } else if (FALSE_IT(is_new_table = is_mysql_lock_table_data_version(data_version))) {
  } else if (OB_FAIL(get_table_name(is_new_table, table_name))) {
    LOG_WARN("generate full table_name failed", K(task_type), K(lock_req), K(is_new_table));
  } else if (OB_FAIL(generate_insert_dml_(task_type, lock_req, is_new_table, dml))) {
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
  LOG_DEBUG("lock_live_detector debug: record_detect_info_to_inner_table_", K(ret), K(insert_sql), K(affected_rows));
  return ret;
}

int ObTableLockDetector::generate_insert_dml_(const ObTableLockTaskType &task_type,
                                              const ObLockRequest &lock_req,
                                              const bool is_new_table,
                                              share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t cnt = 1;
  if (!(LOCK_OBJECT == task_type || LOCK_TABLE == task_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support detect task type", K(ret), K(task_type));
  } else if (OB_FAIL(add_pk_column_to_dml_(task_type, lock_req, is_new_table, dml))) {
    LOG_WARN("add_pk_column_to_dml_ for generate_insert_dml_ failed", KR(ret), K(task_type), K(lock_req));
  } else if (OB_FAIL(dml.add_column("cnt", cnt))) {
    LOG_WARN("add column for insert dml failed", K(ret));
  } else {
    switch (task_type) {
    case LOCK_OBJECT: {
      const ObLockObjsRequest &arg = static_cast<const ObLockObjsRequest &>(lock_req);
      if (OB_FAIL(dml.add_column("detect_func_no", static_cast<uint64_t>(arg.detect_func_no_)))
          || OB_FAIL(dml.add_column("detect_func_param", ObHexEscapeSqlStr(arg.detect_param_)))) {
        LOG_WARN("add column for insert dml failed", K(ret));
      }
      break;
    }
    case LOCK_TABLE: {
      const ObLockTableRequest &arg = static_cast<const ObLockTableRequest&>(lock_req);
      if (OB_FAIL(dml.add_column("detect_func_no", static_cast<uint64_t>(arg.detect_func_no_)))
          || OB_FAIL(dml.add_column("detect_func_param", ObHexEscapeSqlStr(arg.detect_param_)))) {
        LOG_WARN("add column for insert dml failed", K(ret));
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
  }

  return ret;
}

int ObTableLockDetector::add_pk_column_to_dml_(const ObTableLockTaskType &task_type,
                                               const ObLockRequest &lock_req,
                                               const bool is_new_table,
                                               share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  uint64_t obj_type = static_cast<uint64_t>(ObLockOBJType::OBJ_TYPE_INVALID);
  uint64_t obj_id = OB_INVALID_ID;
  int64_t raw_owner_id = ObTableLockOwnerID::INVALID_ID;

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
  case LOCK_TABLE: {
    const ObLockTableRequest &arg = static_cast<const ObLockTableRequest&>(lock_req);
    obj_type = static_cast<uint64_t>(ObLockOBJType::OBJ_TYPE_TABLE);
    obj_id = arg.table_id_;
    break;
  }
  default: {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support detect lock live for the task_type which is not LOCK_OBJECT right now",
             K(task_type),
             K(lock_req));
  }
  }

  if (!is_new_table) {
    ObOldLockOwner old_owner_id(lock_req.owner_id_);
    raw_owner_id = old_owner_id.raw_value();
  } else {
    raw_owner_id = lock_req.owner_id_.id();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("task_type", static_cast<uint64_t>(task_type)))
             || OB_FAIL(dml.add_pk_column("obj_type", obj_type))
             || OB_FAIL(dml.add_pk_column("obj_id", obj_id))
             || OB_FAIL(dml.add_pk_column("lock_mode", static_cast<uint64_t>(lock_req.lock_mode_)))
             || OB_FAIL(dml.add_pk_column("owner_id", raw_owner_id))) {
    LOG_WARN("add pk column to dml failed", K(ret), K(lock_req));
  } else if (is_new_table) {
    if (OB_FAIL(dml.add_pk_column("owner_type", lock_req.owner_id_.type()))) {
      LOG_WARN("add owner_type failed", K(ret), K(lock_req));
    }
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
  LOG_DEBUG("lock_live_detector debug: delete_record_", K(sql), K(affected_rows));
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

int ObTableLockDetector::get_cnt_of_lock_(observer::ObInnerSQLConnection *conn,
                                          const ObTableLockTaskType &task_type,
                                          const ObLockRequest &lock_req,
                                          const bool &only_check_old_table,
                                          int64_t &cnt,
                                          bool &is_from_old_table)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  is_from_old_table = false;
  cnt = 0;

  if (!only_check_old_table) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
      LOG_WARN("get data_version failed", K(ret));
    } else if (is_mysql_lock_table_data_version(data_version)) {
      if (OB_FAIL(get_lock_cnt_in_table_(conn, true /*is_new_table*/, task_type, lock_req, cnt))) {
        LOG_WARN("get lock_cnt in new table failed", K(ret), K(task_type), K(lock_req));
      } else if (cnt > 0) {
        is_from_old_table = false;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (cnt == 0 && !old_detect_table_is_empty()) {
    if (OB_FAIL(get_lock_cnt_in_table_(conn, false /*is_new_table*/, task_type, lock_req, cnt))) {
      LOG_WARN("get lock_cnt in old table failed", K(ret), K(task_type), K(lock_req));
    } else if (cnt > 0) {
      is_from_old_table = true;
    }
  }
  LOG_DEBUG("lock_live_detector debug: get_cnt_of_lock_",
            K(ret),
            K(old_detect_table_is_empty()),
            K(lock_req),
            K(only_check_old_table),
            K(cnt),
            K(is_from_old_table));
  return ret;
}

int ObTableLockDetector::get_cnt_of_lock_(observer::ObInnerSQLConnection *conn,
                                          const ObTableLockTaskType &task_type,
                                          const ObLockRequest &lock_req,
                                          int64_t &cnt_in_new_table,
                                          int64_t &cnt_in_old_table)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  bool is_new_table = false;

  cnt_in_new_table = 0;
  cnt_in_old_table = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get data_version failed", K(ret));
  } else if (is_mysql_lock_table_data_version(data_version)) {
    if (OB_FAIL(get_lock_cnt_in_table_(conn, true /*is_new_table*/, task_type, lock_req, cnt_in_new_table))) {
      LOG_WARN("get lock_cnt in new table failed", K(ret), K(task_type), K(lock_req));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!old_detect_table_is_empty()
             && OB_FAIL(get_lock_cnt_in_table_(conn, false /*is_new_table*/, task_type, lock_req, cnt_in_old_table))) {
    LOG_WARN("get lock_cnt in old table failed", K(ret), K(task_type), K(lock_req));
  }

  LOG_DEBUG("lock_live_detector debug: get_cnt_of_lock_",
            K(ret),
            K(task_type),
            K(lock_req),
            K(cnt_in_new_table),
            K(cnt_in_old_table),
            K(old_detect_table_is_empty()));
  return ret;
}

int ObTableLockDetector::get_lock_cnt_in_table_(observer::ObInnerSQLConnection *conn,
                                                const bool is_new_table,
                                                const ObTableLockTaskType &task_type,
                                                const ObLockRequest &lock_req,
                                                int64_t &cnt)
{
  int ret = OB_SUCCESS;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  share::ObDMLSqlSplicer dml;

  if (OB_FAIL(get_table_name_and_dml_with_pk_column_(is_new_table, task_type, lock_req, table_name, dml))) {
    LOG_WARN("get_table_name_and_dml_with_pk_column_ for get_lock_cnt_in_table_ failed", K(task_type), K(lock_req));
  } else if (OB_FAIL(get_lock_cnt_in_table_(conn, table_name, dml, cnt))) {
    LOG_WARN("get_lock_cnt_in_table_ failed", K(ret));
  }
  return ret;
}

int ObTableLockDetector::get_lock_cnt_in_table_(observer::ObInnerSQLConnection *conn,
                                                const char *table_name,
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
        ret = OB_SUCCESS;
        cnt = 0;
        LOG_DEBUG("can not find lock", K(ret), K(sql));
      }
    } else {
      (void)GET_COL_IGNORE_NULL(result->get_int, "cnt", cnt);
    }
    LOG_DEBUG("lock_live_detector debug: get_cnt sql", K(ret), K(sql), K(cnt));
  }  // end SMART_VAR
  return ret;
}

int ObTableLockDetector::remove_detect_info_from_table_(observer::ObInnerSQLConnection *conn,
                                                        const ObTableLockTaskType &task_type,
                                                        const ObLockRequest &lock_req,
                                                        const int64_t cnt_in_old_table,
                                                        const int64_t cnt_in_new_table,
                                                        int64_t &real_del_cnt)
{
  int ret = OB_SUCCESS;
  real_del_cnt = 0;

  if (cnt_in_old_table > 0) {
    OZ (remove_detect_info_from_table_(conn, task_type, lock_req, false));
  }

  if (cnt_in_new_table > 0) {
    OZ (remove_detect_info_from_table_(conn, task_type, lock_req, true));
  }

  OX (real_del_cnt = cnt_in_old_table + cnt_in_new_table);
  return ret;
}

int ObTableLockDetector::remove_detect_info_from_table_(observer::ObInnerSQLConnection *conn,
                                                        const ObTableLockTaskType &task_type,
                                                        const ObLockRequest &lock_req,
                                                        const bool is_new_table)
{
  int ret = OB_SUCCESS;
  char table_name[OB_MAX_TABLE_NAME_BUF_LENGTH];
  ObSqlString delete_sql;
  share::ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;

  OZ (get_table_name_and_dml_with_pk_column_(is_new_table, task_type, lock_req, table_name, dml));
  OZ (dml.splice_delete_sql(table_name, delete_sql));
  OZ (ObInnerConnectionLockUtil::execute_write_sql(conn, delete_sql, affected_rows));
  LOG_DEBUG("lock_live_detector debug: delete sql", K(ret), K(delete_sql), K(lock_req), K(affected_rows));
  return ret;
}

int ObTableLockDetector::get_table_name(const bool is_new_table, char *table_name)
{
  int ret = OB_SUCCESS;
  memset(table_name, 0, OB_MAX_TABLE_NAME_BUF_LENGTH);
  if (is_new_table) {
    OZ (databuff_printf(
        table_name, OB_MAX_TABLE_NAME_BUF_LENGTH, "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_V2_TNAME));
  } else {
    OZ (databuff_printf(
        table_name, OB_MAX_TABLE_NAME_BUF_LENGTH, "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DETECT_LOCK_INFO_TNAME));
  }
  return ret;
}

int ObTableLockDetector::get_table_name_and_dml_with_pk_column_(const bool is_new_table,
                                                                const ObTableLockTaskType &task_type,
                                                                const ObLockRequest &lock_req,
                                                                char *table_name,
                                                                share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  memset(table_name, 0, OB_MAX_TABLE_NAME_BUF_LENGTH);
  dml.reset();
  OZ (get_table_name(is_new_table, table_name));
  OZ (add_pk_column_to_dml_(task_type, lock_req, is_new_table, dml));
  return ret;
}

int ObTableLockDetector::get_unlock_request_list_(common::sqlclient::ObMySQLResult *res,
                                                  ObIAllocator &allocator,
                                                  ObIArray<ObLockRequest *> &arg_list)
{
  int ret = OB_SUCCESS;
  ObLockRequest *unlock_arg = nullptr;

  while (OB_SUCC(ret) && OB_SUCC(res->next())) {
    if (OB_FAIL(parse_unlock_request_(*res, allocator, unlock_arg))) {
      LOG_WARN("parse lock request failed", K(ret));
    } else if (OB_ISNULL(unlock_arg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse unlock request failed", K(ret));
    } else if (OB_FAIL(arg_list.push_back(unlock_arg))) {
      LOG_WARN("add unlock arg to the list failed", K(ret), K(unlock_arg));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(unlock_arg)) {
      unlock_arg->~ObLockRequest();
      allocator.free(unlock_arg);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTableLockDetector::generate_get_unlock_request_sql_(const ObTableLockOwnerID &owner_id,
                                                          const ObTableLockTaskType task_type,
                                                          ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  char new_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  char old_table_name[OB_MAX_TABLE_NAME_BUF_LENGTH] = {0};
  uint64_t data_version = 0;
  ObSqlString select_sql;
  ObSqlString where_cond;
  ObOldLockOwner old_owner_id(owner_id);

  sql.reset();
  OZ (GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  OZ (select_sql.assign_fmt("SELECT task_type, obj_type, obj_id, lock_mode FROM"));
  OZ (where_cond.assign_fmt(" WHERE"));
  if (LOCK_OBJECT == task_type || LOCK_TABLE == task_type) {
    OZ (where_cond.append_fmt(" task_type = %d AND", static_cast<int>(task_type)));
  }

  if (is_mysql_lock_table_data_version(data_version)) {
    OZ (get_table_name(true, new_table_name));
    OZ (sql.assign_fmt("%s %s", select_sql.ptr(), new_table_name));
    OZ (sql.append_fmt("%s", where_cond.ptr()));
    OZ (sql.append_fmt(" owner_id = %" PRId64 " AND owner_type = %d", owner_id.id(), static_cast<int>(owner_id.type())));
    if (!old_detect_table_is_empty()) {
      OZ (get_table_name(false, old_table_name));
      OZ (sql.append_fmt(" UNION %s %s %s", select_sql.ptr(), old_table_name, where_cond.ptr()));
      OZ (sql.append_fmt(" owner_id = %" PRId64, old_owner_id.raw_value()));
    }
  } else {
    OZ (get_table_name(false, old_table_name));
    OZ (sql.assign_fmt("%s %s %s", select_sql.ptr(), old_table_name, where_cond.ptr()));
    OZ (sql.append_fmt(" owner_id = %" PRId64, old_owner_id.raw_value()));
  }

  LOG_DEBUG("lock_live_detector debug: generate_get_unlock_request_sql_", K(ret), K(owner_id), K(task_type), K(sql));

  return ret;
}

int ObTableLockDetector::parse_unlock_request_(common::sqlclient::ObMySQLResult &res,
                                               ObIAllocator &allocator,
                                               ObLockRequest *&arg)
{
  int ret = OB_SUCCESS;
  int64_t task_type = 0;
  int64_t obj_type = 0;
  int64_t obj_id = 0;
  int64_t lock_mode = 0;
  ObLockID lock_id;
  void *ptr = nullptr;

  arg = NULL;

  (void)GET_COL_IGNORE_NULL(res.get_int, "task_type", task_type);
  (void)GET_COL_IGNORE_NULL(res.get_int, "obj_type", obj_type);
  (void)GET_COL_IGNORE_NULL(res.get_int, "obj_id", obj_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "lock_mode", lock_mode);
  if (OB_FAIL(ret)) {
  } else {
    switch (task_type) {
      case LOCK_TABLE: {
        ObUnLockTableRequest *unlock_arg = NULL;
        if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObUnLockTableRequest)))) {
          ret = OB_EAGAIN;
          LOG_WARN("get unlock request failed", K(ret));
        } else if (FALSE_IT(unlock_arg = new (ptr) ObUnLockTableRequest())) {
        } else {
          unlock_arg->table_id_ = obj_id;
          arg = unlock_arg;
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(unlock_arg)) {
          unlock_arg->~ObUnLockTableRequest();
          allocator.free(ptr);
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
        } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObUnLockObjsRequest)))) {
          ret = OB_EAGAIN;
          LOG_WARN("get unlock request failed", K(ret));
        } else if (FALSE_IT(unlock_arg = new (ptr) ObUnLockObjsRequest())) {
        } else if (OB_FAIL(lock_id.set(static_cast<ObLockOBJType>(obj_type), obj_id))) {
          LOG_WARN("get lock id failed", K(ret), K(obj_type), K(obj_id));
        } else if (OB_FAIL(unlock_arg->objs_.push_back(lock_id))) {
          LOG_WARN("get unlock argument failed", K(ret), K(lock_id));
        } else {
          arg = unlock_arg;
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(unlock_arg)) {
          unlock_arg->~ObUnLockObjsRequest();
          allocator.free(ptr);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not supported lock task type", K(ret), K(task_type));
      }
    }
    if (OB_SUCC(ret)) {
      arg->lock_mode_ = lock_mode;
      arg->op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK;
      arg->timeout_us_ = 0;
      arg->is_from_sql_ = true;
    }
  }
  return ret;
}

int ObTableLockDetector::fill_owner_id_for_unlock_request_(const ObTableLockOwnerID &owner_id, ObIArray<ObLockRequest *> &arg_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < arg_list.count(); i++) {
    arg_list.at(i)->owner_id_ = owner_id;
  }
  return ret;
}

int ObTableLockDetector::get_lock_owner_where_cond_(const ObTableLockOwnerID lock_owner,
                                                    const bool is_new_table,
                                                    ObSqlString &where_cond)
{
  int ret = OB_SUCCESS;

  where_cond.reset();
  if (is_new_table) {
    OZ (where_cond.assign_fmt("owner_id = %" PRId64 " AND owner_type = %d", lock_owner.id(), lock_owner.type()));
  } else {
    ObOldLockOwner old_owner_id(lock_owner);
    OZ (where_cond.assign_fmt("owner_id = %" PRId64, old_owner_id.raw_value()));
  }
  return ret;
}

bool ObTableLockDetector::is_unlock_task_(const ObTableLockTaskType &task_type)
{
  return (UNLOCK_TABLE == task_type || UNLOCK_PARTITION == task_type || UNLOCK_SUBPARTITION == task_type
          || UNLOCK_TABLET == task_type || UNLOCK_OBJECT == task_type || UNLOCK_ALONE_TABLET == task_type);
}

bool ObSessionAliveChecker::operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info) || OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", KR(ret), KP(sess_info), KP(GCTX.session_mgr_));
  } else if (sess_info->get_sid() == session_id_) {
    is_alive_ = true;
  }
  LOG_INFO("check session qual", K(sess_info->get_sid()), K(session_id_), K(is_alive_));
  return OB_SUCCESS == ret;
}

}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase
