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

#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_lock_inner_connection_util.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_result.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tablelock/ob_table_lock_service.h"


using namespace oceanbase::observer;
using namespace oceanbase::obrpc;
namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

#define __REQUEST_LOCK_CHECK_VERSION(v, T, operation_type, arg, conn)                  \
  {                                                                                    \
    int64_t pos = 0;                                                                   \
    T lock_arg;                                                                        \
    if (GET_MIN_CLUSTER_VERSION() < v) {                                               \
      ret = OB_NOT_SUPPORTED;                                                          \
      LOG_WARN("cluster version check faild", KR(ret), K(v), K(GET_MIN_CLUSTER_VERSION())); \
    } else if (OB_FAIL(lock_arg.deserialize(arg.get_inner_sql().ptr(),                 \
                                            arg.get_inner_sql().length(),              \
                                            pos))) {                                   \
      LOG_WARN("deserialize multi source data str failed", K(ret), K(arg), K(pos));    \
     } else if (OB_FAIL(request_lock_(arg.get_tenant_id(),                             \
                                      lock_arg,                                        \
                                      operation_type,                                  \
                                      conn))) {                                        \
      LOG_WARN("request lock failed", K(ret), K(arg.get_tenant_id()), K(lock_arg));    \
     }                                                                                 \
  }

#define REQUEST_LOCK_4_1(T, operation_type, arg, conn)                                 \
  __REQUEST_LOCK_CHECK_VERSION(CLUSTER_VERSION_4_1_0_0, T, operation_type, arg, conn)

#define REQUEST_LOCK_4_2(T, operation_type, arg, conn)                                 \
  __REQUEST_LOCK_CHECK_VERSION(CLUSTER_VERSION_4_2_0_0, T, operation_type, arg, conn)

int ObInnerConnectionLockUtil::process_lock_rpc(
    const ObInnerSQLTransmitArg &arg,
    common::sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *inner_conn = static_cast<observer::ObInnerSQLConnection *>(conn);
  if (OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg), KP(conn));
  } else {
    const obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType operation_type = arg.get_operation_type();
    switch (operation_type) {
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE: {
        if (OB_FAIL(process_lock_table_(operation_type,
                                        arg,
                                        inner_conn))) {
          LOG_WARN("process lock table failed", K(ret));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLE: {
        REQUEST_LOCK_4_1(ObUnLockTableRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET: {
        if (OB_FAIL(process_lock_tablet_(operation_type,
                                         arg,
                                         inner_conn))) {
          LOG_WARN("process lock tablet failed", K(ret));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLET: {
        REQUEST_LOCK_4_1(ObUnLockTabletRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJ: {
        REQUEST_LOCK_4_1(ObLockObjRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJ: {
        REQUEST_LOCK_4_1(ObUnLockObjRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJS: {
        REQUEST_LOCK_4_2(ObLockObjsRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJS: {
        REQUEST_LOCK_4_2(ObUnLockObjsRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_PART: {
        REQUEST_LOCK_4_1(ObLockPartitionRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_PART: {
        REQUEST_LOCK_4_1(ObUnLockPartitionRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_SUBPART: {
        REQUEST_LOCK_4_1(ObLockPartitionRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_SUBPART: {
        REQUEST_LOCK_4_1(ObUnLockPartitionRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_ALONE_TABLET: {
        REQUEST_LOCK_4_2(ObLockAloneTabletRequest, operation_type, arg, inner_conn);
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_ALONE_TABLET: {
        REQUEST_LOCK_4_2(ObUnLockAloneTabletRequest, operation_type, arg, inner_conn);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Unknown operation type", K(ret), K(operation_type));
        break;
      }
    }
  }
  return ret;
}

int ObInnerConnectionLockUtil::process_lock_table_(
    const obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType operation_type,
    const ObInnerSQLTransmitArg &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletID no_used;

  if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_4_0_0_0) {
    ObInTransLockTableRequest lock_arg;
    if (OB_FAIL(lock_arg.deserialize(arg.get_inner_sql().ptr(), arg.get_inner_sql().length(), pos))) {
      LOG_WARN("deserialize multi source data str failed", K(ret), K(arg), K(pos));
    } else {
      // we can only rewrite the arg to new arg type at the execute place.
      if (OB_FAIL(request_lock_(arg.get_tenant_id(),
                                lock_arg.table_id_,
                                no_used,
                                lock_arg.lock_mode_,
                                lock_arg.timeout_us_,
                                operation_type,
                                conn))) {
        LOG_WARN("lock table failed", K(ret), K(arg.get_tenant_id()), K(lock_arg));
      }
    }
  } else {
    REQUEST_LOCK_4_1(ObLockTableRequest, operation_type, arg, conn);
  }

  return ret;
}

int ObInnerConnectionLockUtil::process_lock_tablet_(
    const obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType operation_type,
    const ObInnerSQLTransmitArg &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_4_0_0_0) {
    ObInTransLockTabletRequest lock_arg;
    if (OB_FAIL(lock_arg.deserialize(arg.get_inner_sql().ptr(), arg.get_inner_sql().length(), pos))) {
      LOG_WARN("deserialize multi source data str failed", K(ret), K(arg), K(pos));
    } else {
      // we can only rewrite the argument at local.
      if (OB_FAIL(request_lock_(arg.get_tenant_id(),
                                lock_arg.table_id_,
                                lock_arg.tablet_id_,
                                lock_arg.lock_mode_,
                                lock_arg.timeout_us_,
                                operation_type,
                                conn))) {
        LOG_WARN("lock tablet failed", K(ret), K(arg.get_tenant_id()), K(lock_arg));
      }
    }
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
    REQUEST_LOCK_4_1(ObLockTabletRequest, operation_type, arg, conn);
  } else {
    REQUEST_LOCK_4_2(ObLockTabletsRequest, operation_type, arg, conn);
  }

  return ret;
}

int ObInnerConnectionLockUtil::lock_table(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableLockMode lock_mode,
    const int64_t timeout_us,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObTabletID no_used;
  if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_4_0_0_0) {
    ObLockTableRequest lock_arg;
    lock_arg.owner_id_.set_default();
    lock_arg.lock_mode_ = lock_mode;
    lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    lock_arg.timeout_us_ = timeout_us;
    lock_arg.table_id_ = table_id;

    ret = request_lock_(tenant_id, lock_arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE, conn);
  } else {
    ret = request_lock_(tenant_id, table_id, no_used, lock_mode, timeout_us,
                        ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_table(
    const uint64_t tenant_id,
    const ObLockTableRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  return request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE, conn);
}

int ObInnerConnectionLockUtil::unlock_table(
    const uint64_t tenant_id,
    const ObUnLockTableRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only OUT_TRANS_LOCK should unlock.", K(ret), K(arg));
  } else {
    ret = request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLE, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_partition(
    const uint64_t tenant_id,
    const ObLockPartitionRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  return request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_PART, conn);
}

int ObInnerConnectionLockUtil::unlock_partition(
    const uint64_t tenant_id,
    const ObUnLockPartitionRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only OUT_TRANS_LOCK should unlock.", K(ret), K(arg));
  } else {
    ret = request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_PART, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_subpartition(
    const uint64_t tenant_id,
    const ObLockPartitionRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  return request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_SUBPART, conn);
}

int ObInnerConnectionLockUtil::unlock_subpartition(
    const uint64_t tenant_id,
    const ObUnLockPartitionRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only OUT_TRANS_LOCK should unlock.", K(ret), K(arg));
  } else {
    ret = request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_SUBPART, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_tablet(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTabletID tablet_id,
    const ObTableLockMode lock_mode,
    const int64_t timeout_us,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0) {
    ObLockTabletsRequest lock_arg;
    lock_arg.owner_id_.set_default();
    lock_arg.lock_mode_ = lock_mode;
    lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    lock_arg.timeout_us_ = timeout_us;
    lock_arg.table_id_ = table_id;
    if (OB_FAIL(lock_arg.tablet_ids_.push_back(tablet_id))) {
      LOG_WARN("add tablet id failed", K(ret), K(tablet_id));
    } else if (OB_FAIL(request_lock_(tenant_id, lock_arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET, conn))) {
      LOG_WARN("request lock for tablet failed", K(ret), K(lock_arg));
    }
  } else if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_4_0_0_0) {
    ObLockTabletRequest lock_arg;
    lock_arg.owner_id_.set_default();
    lock_arg.lock_mode_ = lock_mode;
    lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    lock_arg.timeout_us_ = timeout_us;
    lock_arg.table_id_ = table_id;
    lock_arg.tablet_id_ = tablet_id;

    ret = request_lock_(tenant_id, lock_arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET, conn);
  } else {
    // for 4.0
    ret = request_lock_(tenant_id, table_id, tablet_id, lock_mode,
                        timeout_us, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_tablet(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObTableLockMode lock_mode,
    const int64_t timeout_us,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObLockTabletsRequest lock_arg;
  lock_arg.owner_id_.set_default();
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = timeout_us;
  lock_arg.table_id_ = table_id;

  if (OB_FAIL(lock_arg.tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("assign tablet id failed", K(ret), K(tablet_ids));
  } else if (OB_FAIL(request_lock_(tenant_id, lock_arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET, conn))) {
    LOG_WARN("request lock for tablets failed", K(ret), K(lock_arg));
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_tablet(
    const uint64_t tenant_id,
    const ObLockTabletRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  return request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET, conn);
}

int ObInnerConnectionLockUtil::unlock_tablet(
    const uint64_t tenant_id,
    const ObUnLockTabletRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only OUT_TRANS_LOCK should unlock.", K(ret), K(arg));
  } else {
    ret = request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLET, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_tablet(
    const uint64_t tenant_id,
    const ObLockAloneTabletRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  return request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_ALONE_TABLET, conn);
}

int ObInnerConnectionLockUtil::unlock_tablet(
    const uint64_t tenant_id,
    const ObUnLockAloneTabletRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only OUT_TRANS_LOCK should unlock.", K(ret), K(arg));
  } else {
    ret = request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_ALONE_TABLET, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_obj(
    const uint64_t tenant_id,
    const ObLockObjRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  return request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJ, conn);
}

int ObInnerConnectionLockUtil::unlock_obj(
    const uint64_t tenant_id,
    const ObUnLockObjRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only OUT_TRANS_LOCK should unlock.", K(ret), K(arg));
  } else {
    ret = request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJ, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::lock_obj(
    const uint64_t tenant_id,
    const ObLockObjsRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  return request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJS, conn);
}

int ObInnerConnectionLockUtil::unlock_obj(
    const uint64_t tenant_id,
    const ObUnLockObjsRequest &arg,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLockOpType::OUT_TRANS_UNLOCK != arg.op_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only OUT_TRANS_LOCK should unlock.", K(ret), K(arg));
  } else {
    ret = request_lock_(tenant_id, arg, ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJS, conn);
  }
  return ret;
}

int ObInnerConnectionLockUtil::create_inner_conn(sql::ObSQLSessionInfo *session_info,
                                                 common::ObMySQLProxy *sql_proxy,
                                                 observer::ObInnerSQLConnection *&inner_conn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObObj mysql_mode;
  ObObj current_mode;
  mysql_mode.set_int(0);
  current_mode.set_int(-1);

  observer::ObInnerSQLConnectionPool *pool = nullptr;
  common::sqlclient::ObISQLConnection *conn = nullptr;

  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  if (OB_ISNULL(session_info) || OB_ISNULL(sql_proxy)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session or sql_proxy is NULL", KP(session_info), KP(sql_proxy));
  } else if (OB_NOT_NULL(inner_conn = static_cast<observer::ObInnerSQLConnection *>(session_info->get_inner_conn()))) {
    LOG_INFO("session has had inner connection, no need to create again", KPC(session_info));
  } else if (OB_ISNULL(pool = static_cast<observer::ObInnerSQLConnectionPool *>(sql_proxy->get_pool()))) {
    ret = OB_NOT_INIT;
    LOG_WARN("connection pool is NULL", K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, current_mode))) {
    LOG_WARN("can not get the compat_mode", KPC(session_info));
  } else if (current_mode != mysql_mode
             && OB_FAIL(session_info->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, mysql_mode))) {
    LOG_WARN("update session_info to msyql_mode failed", KR(ret), KPC(session_info));
  } else if (common::sqlclient::INNER_POOL != pool->get_type()) {
    LOG_WARN("connection pool type is not inner", K(ret), K(pool->get_type()));
  } else if (OB_FAIL(pool->acquire(session_info, conn))) {
    LOG_WARN("acquire connection from inner sql connection pool failed", KR(ret), KPC(session_info));
  } else {
    inner_conn = static_cast<observer::ObInnerSQLConnection *>(conn);
  }
  if (current_mode != mysql_mode && current_mode.get_int() != -1
      && OB_TMP_FAIL(session_info->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, current_mode))) {
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    LOG_WARN("failed to update sys variable for compatibility mode", K(current_mode), KPC(session_info));
  }
  return ret;
}

int ObInnerConnectionLockUtil::execute_write_sql(observer::ObInnerSQLConnection *conn,
                                                 const ObSqlString &sql,
                                                 int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = nullptr;
  ObObj current_mode;
  ObObj mysql_mode;
  mysql_mode.set_int(0);
  current_mode.set_int(-1);

  // If we want to executre dml with mysql_mode, thread worker and session need to meet 2 conditions:
  // 1. The thread worker is in mysql_mode, it depends on the connection itself, we cannot modify it;
  // 2. The sys_variable of session should be mysql_mode. Otherwise, it will trigger defensive check.
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_conn is nullptr", K(sql));
  } else if (OB_ISNULL(session = &conn->get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is nullptr", K(sql));
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, current_mode))) {
    LOG_WARN("can not get the compat_mode", K(sql));
  } else if (current_mode != mysql_mode
             && OB_FAIL(session->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, mysql_mode))) {
    LOG_WARN("update compat_mode to mysql_mode failed", K(sql));
  } else if (OB_FAIL(conn->execute_write(MTL_ID(), sql.ptr(), affected_rows))) {
    LOG_WARN("execute write for inner_sql failed", K(sql));
  }
  if (current_mode != mysql_mode && current_mode.get_int() != -1
      && OB_TMP_FAIL(session->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, current_mode))) {
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    LOG_WARN("failed to update sys variable for compatibility mode", K(current_mode), K(sql));
  }
  return ret;
}
int ObInnerConnectionLockUtil::execute_read_sql(observer::ObInnerSQLConnection *conn,
                                                const ObSqlString &sql,
                                                ObISQLClient::ReadResult &res)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = nullptr;
  ObObj current_mode;
  ObObj mysql_mode;
  mysql_mode.set_int(0);
  current_mode.set_int(-1);

  // If we want to executre dml with mysql_mode, thread worker and session need to meet 2 conditions:
  // 1. The thread worker is in mysql_mode, it depends on the connection itself, we cannot modify it;
  // 2. The sys_variable of session should be mysql_mode. Otherwise, it will trigger defensive check.
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner_conn is nullptr", K(sql));
  } else if (OB_ISNULL(session = &conn->get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info is nullptr", K(sql));
  } else if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, current_mode))) {
    LOG_WARN("can not get the compat_mode", K(sql));
  } else if (current_mode != mysql_mode
             && OB_FAIL(session->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, mysql_mode))) {
    LOG_WARN("update compat_mode to mysql_mode failed", K(sql));
  } else if (OB_FAIL(conn->execute_read(MTL_ID(), sql.ptr(), res))) {
    LOG_WARN("execute write for inner_sql failed", K(sql));
  }
  if (current_mode != mysql_mode && current_mode.get_int() != -1
      && OB_TMP_FAIL(session->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, current_mode))) {
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    LOG_WARN("failed to update sys variable for compatibility mode", K(current_mode), K(sql));
  }
  return ret;
}

int ObInnerConnectionLockUtil::build_tx_param(sql::ObSQLSessionInfo *session_info, ObTxParam &tx_param, const bool *readonly)
{
  int ret = OB_SUCCESS;
  int64_t org_cluster_id = OB_INVALID_ORG_CLUSTER_ID;
  int64_t tx_timeout_us = 0;
  OZ (get_org_cluster_id_(session_info, org_cluster_id));
  OX (
    session_info->get_tx_timeout(tx_timeout_us);

    tx_param.timeout_us_ = tx_timeout_us;
    tx_param.lock_timeout_us_ = session_info->get_trx_lock_timeout();
    bool ro = OB_NOT_NULL(readonly) ? *readonly : session_info->get_tx_read_only();
    tx_param.access_mode_ = ro ? ObTxAccessMode::RD_ONLY : ObTxAccessMode::RW;
    tx_param.isolation_ = session_info->get_tx_isolation();
    tx_param.cluster_id_ = org_cluster_id;
  )

  return ret;
}

int ObInnerConnectionLockUtil::do_obj_lock_(
    const uint64_t tenant_id,
    const ObLockRequest &arg,
    const obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType operation_type,
    observer::ObInnerSQLConnection *conn,
    observer::ObInnerSQLResult &res)
{
  int ret = OB_SUCCESS;
  transaction::ObTxDesc *tx_desc = nullptr;

  if (OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid conn", KR(ret));
  } else if (OB_ISNULL(tx_desc = conn->get_session().get_tx_desc())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tx_desc");
  } else {
    transaction::ObTxParam tx_param;
    tx_param.access_mode_ = transaction::ObTxAccessMode::RW;
    tx_param.isolation_ = conn->get_session().get_tx_isolation();
    tx_param.cluster_id_ = GCONF.cluster_id;
    conn->get_session().get_tx_timeout(tx_param.timeout_us_);
    tx_param.lock_timeout_us_ = conn->get_session().get_trx_lock_timeout();

    MTL_SWITCH(tenant_id) {
      switch (operation_type) {
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE: {
        const ObLockTableRequest &lock_arg = static_cast<const ObLockTableRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->lock_table(*tx_desc,
                                                         tx_param,
                                                         lock_arg))) {
          LOG_WARN("lock table failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLE: {
        const ObUnLockTableRequest &lock_arg = static_cast<const ObUnLockTableRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->unlock_table(*tx_desc,
                                                           tx_param,
                                                           lock_arg))) {
          LOG_WARN("unlock table failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET: {
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
          const ObLockTabletRequest &lock_arg = static_cast<const ObLockTabletRequest &>(arg);
          if (OB_FAIL(MTL(ObTableLockService *)->lock_tablet(*tx_desc, tx_param, lock_arg))) {
            LOG_WARN("lock tablet failed", K(ret), K(tenant_id), K(lock_arg));
          }
        } else {
          const ObLockTabletsRequest &lock_arg = static_cast<const ObLockTabletsRequest &>(arg);
          if (OB_FAIL(MTL(ObTableLockService *)->lock_tablet(*tx_desc, tx_param, lock_arg))) {
            LOG_WARN("lock tablets failed", K(ret), K(tenant_id), K(lock_arg));
          }
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLET: {
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
          const ObUnLockTabletRequest &lock_arg = static_cast<const ObUnLockTabletRequest &>(arg);
          if (OB_FAIL(MTL(ObTableLockService *)->unlock_tablet(*tx_desc, tx_param, lock_arg))) {
            LOG_WARN("unlock tablet failed", K(ret), K(tenant_id), K(lock_arg));
          }
        } else {
          const ObUnLockTabletsRequest &lock_arg = static_cast<const ObUnLockTabletsRequest &>(arg);
          if (OB_FAIL(MTL(ObTableLockService *)->unlock_tablet(*tx_desc, tx_param, lock_arg))) {
            LOG_WARN("unlock tablets failed", K(ret), K(tenant_id), K(lock_arg));
          }
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_PART: {
        const ObLockPartitionRequest &lock_arg = static_cast<const ObLockPartitionRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->lock_partition(*tx_desc,
                                                             tx_param,
                                                             lock_arg))) {
          LOG_WARN("lock partition failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_PART: {
        const ObUnLockPartitionRequest &lock_arg = static_cast<const ObUnLockPartitionRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->unlock_partition(*tx_desc,
                                                               tx_param,
                                                               lock_arg))) {
          LOG_WARN("unlock partition failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJ: {
        const ObLockObjRequest &lock_arg = static_cast<const ObLockObjRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService *)->lock_obj(*tx_desc, tx_param, lock_arg))) {
          LOG_WARN("lock object failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJS: {
        const ObLockObjsRequest &lock_arg = static_cast<const ObLockObjsRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService *)->lock_obj(*tx_desc, tx_param, lock_arg))) {
          LOG_WARN("lock objects failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJ: {
        const ObUnLockObjRequest &lock_arg = static_cast<const ObUnLockObjRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService *)->unlock_obj(*tx_desc, tx_param, lock_arg))) {
          LOG_WARN("unlock object failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJS: {
        const ObUnLockObjsRequest &lock_arg = static_cast<const ObUnLockObjsRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService *)->unlock_obj(*tx_desc, tx_param, lock_arg))) {
          LOG_WARN("unlock objects failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_SUBPART: {
        const ObLockPartitionRequest &lock_arg = static_cast<const ObLockPartitionRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->lock_subpartition(*tx_desc,
                                                                tx_param,
                                                                lock_arg))) {
          LOG_WARN("lock subpartition failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_SUBPART: {
        const ObUnLockPartitionRequest &lock_arg = static_cast<const ObUnLockPartitionRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->unlock_subpartition(*tx_desc,
                                                                  tx_param,
                                                                  lock_arg))) {
          LOG_WARN("unlock subpartition failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_ALONE_TABLET: {
        const ObLockAloneTabletRequest &lock_arg = static_cast<const ObLockAloneTabletRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->lock_tablet(*tx_desc,
                                                          tx_param,
                                                          lock_arg))) {
          LOG_WARN("lock alone tablet failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_ALONE_TABLET: {
        const ObUnLockAloneTabletRequest &lock_arg = static_cast<const ObUnLockAloneTabletRequest &>(arg);
        if (OB_FAIL(MTL(ObTableLockService*)->unlock_tablet(*tx_desc,
                                                            tx_param,
                                                            lock_arg))) {
          LOG_WARN("unlock alone tablet failed", K(ret), K(tenant_id), K(lock_arg));
        }
        break;
      }
      default: {
        LOG_WARN("operation_type is not expected", K(operation_type));
        ret = OB_ERR_UNEXPECTED;
      } // default
      } // switch
      if (OB_SUCC(ret) && OB_FAIL(res.close())) {
        LOG_WARN("close result set failed", K(ret), K(tenant_id));
      }
    } // MTL_SWITCH
  } // else
  return ret;
}

int ObInnerConnectionLockUtil::request_lock_(
    const uint64_t tenant_id,
    const ObLockRequest &arg,
    const obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType operation_type,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;

  const bool local_execute = conn->is_local_execute(GCONF.cluster_id, tenant_id);

  SMART_VAR(ObInnerSQLResult, res, conn->get_session())
  {
    if (OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id));
    } else if (local_execute) {
      if (OB_FAIL(conn->switch_tenant(tenant_id))) {
        LOG_WARN("set system tenant id failed", K(ret), K(tenant_id));
      }
    } else {
      LOG_DEBUG("tenant not in server", K(ret), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      if (!conn->is_in_trans()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner conn must be already in trans", K(ret));
      } else if (OB_FAIL(res.init(local_execute))) {
        LOG_WARN("init result set", K(ret), K(local_execute));
      } else if (local_execute) {
        if (OB_FAIL(do_obj_lock_(tenant_id, arg, operation_type, conn, res))) {
          LOG_WARN("do obj lock failed", KR(ret), K(operation_type), K(arg));
        }
      } else {
        char *tmp_str = nullptr;
        int64_t pos = 0;
        ObString sql;
        if (OB_ISNULL(tmp_str = static_cast<char *>(ob_malloc(arg.get_serialize_size(), "InnerLock")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory for sql_str failed", K(ret), K(arg.get_serialize_size()));
        } else if (OB_FAIL(arg.serialize(tmp_str, arg.get_serialize_size(), pos))) {
          LOG_WARN("serialize lock table arg failed", K(ret), K(arg));
        } else {
          int32_t group_id = 0;
          const int64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
          if ((min_cluster_version >= MOCK_CLUSTER_VERSION_4_2_1_4 && min_cluster_version < CLUSTER_VERSION_4_2_2_0)
              || (min_cluster_version >= MOCK_CLUSTER_VERSION_4_2_3_0 && min_cluster_version < CLUSTER_VERSION_4_3_0_0)
              || (min_cluster_version >= CLUSTER_VERSION_4_3_0_0)) {
            if (arg.is_unlock_request()) {
              group_id = share::OBCG_UNLOCK;
            } else {
              group_id = share::OBCG_LOCK;
            }
          }
          sql.assign_ptr(tmp_str, arg.get_serialize_size());
          ret = conn->forward_request(tenant_id, operation_type, sql, res, group_id);
        }

        if (OB_NOT_NULL(tmp_str)) {
          ob_free(tmp_str);
        }
      }
    }
  }

  return ret;
}

// for version 4.0
int ObInnerConnectionLockUtil::request_lock_(
    const uint64_t tenant_id,
    const uint64_t table_id, // as obj_id when lock_obj
    const ObTabletID tablet_id, //just used when lock_tablet
    const ObTableLockMode lock_mode,
    const int64_t timeout_us,
    const obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType operation_type,
    observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;

  const bool local_execute = conn->is_local_execute(GCONF.cluster_id, tenant_id);

  SMART_VAR(ObInnerSQLResult, res, conn->get_session())
  {
    if (OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tenant_id));
    } else if (local_execute) {
      if (OB_FAIL(conn->switch_tenant(tenant_id))) {
        LOG_WARN("set system tenant id failed", K(ret), K(tenant_id));
      }
    } else {
      LOG_DEBUG("tenant not in server", K(ret), K(tenant_id));
    }

    if (OB_SUCC(ret)) {
      if (!conn->is_in_trans()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner conn must be already in trans", K(ret));
      } else if (OB_FAIL(res.init(local_execute))) {
        LOG_WARN("init result set", K(ret), K(local_execute));
      } else if (local_execute) {
        // we can safely rewrite the argument here, because it is only used local.
        ObLockRequest *lock_arg = nullptr;
        ObLockTableRequest lock_table_arg;
        ObLockTabletRequest lock_tablet_arg;
        switch (operation_type) {
        case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE: {
          lock_arg = &lock_table_arg;
          lock_table_arg.owner_id_.set_default();
          lock_table_arg.lock_mode_ = lock_mode;
          lock_table_arg.op_type_ = IN_TRANS_COMMON_LOCK;
          lock_table_arg.timeout_us_ = timeout_us;
          lock_table_arg.table_id_ = table_id;
          break;
        }
        case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET: {
          lock_arg = &lock_tablet_arg;
          lock_tablet_arg.owner_id_.set_default();
          lock_tablet_arg.lock_mode_ = lock_mode;
          lock_tablet_arg.op_type_ = IN_TRANS_COMMON_LOCK;
          lock_tablet_arg.timeout_us_ = timeout_us;
          lock_tablet_arg.table_id_ = table_id;
          lock_tablet_arg.tablet_id_ = tablet_id;
          break;
        }
        default:
          LOG_WARN("operation_type is not expected", K(operation_type));
          ret = OB_ERR_UNEXPECTED;
        }
        if (OB_SUCC(ret) && OB_FAIL(do_obj_lock_(tenant_id, *lock_arg, operation_type, conn, res))) {
          LOG_WARN("close result set failed", K(ret), K(tenant_id));
        }
      } else {
        char *tmp_str = nullptr;
        int64_t pos = 0;
        ObString sql;
        switch (operation_type) {
        case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE: {
          ObInTransLockTableRequest arg;
          arg.table_id_ = table_id;
          arg.lock_mode_ = lock_mode;
          arg.timeout_us_ = timeout_us;

          if (OB_ISNULL(tmp_str = static_cast<char *>(ob_malloc(arg.get_serialize_size(), "InnerLock")))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory for sql_str failed", K(ret), K(arg.get_serialize_size()));
          } else if (OB_FAIL(arg.serialize(tmp_str, arg.get_serialize_size(), pos))) {
            LOG_WARN("serialize lock table arg failed", K(ret), K(arg));
          } else {
            sql.assign_ptr(tmp_str, arg.get_serialize_size());
          }
          break;
        }
        case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET: {
          ObInTransLockTabletRequest arg;
          arg.table_id_ = table_id;
          arg.tablet_id_ = tablet_id;
          arg.lock_mode_ = lock_mode;
          arg.timeout_us_ = timeout_us;

          if (OB_ISNULL(tmp_str = static_cast<char *>(ob_malloc(arg.get_serialize_size(), "InnerLock")))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory for sql_str failed", K(ret), K(arg.get_serialize_size()));
          } else if (OB_FAIL(arg.serialize(tmp_str, arg.get_serialize_size(), pos))) {
            LOG_WARN("serialize lock table arg failed", K(ret), K(arg));
          } else {
            sql.assign_ptr(tmp_str, arg.get_serialize_size());
          }
          break;
        }
        default: {
          LOG_WARN("operation_type is not expected", K(operation_type));
          ret = OB_ERR_UNEXPECTED;
        } // default
        } // switch
        ret = conn->forward_request(tenant_id, operation_type, sql, res);
        if (OB_NOT_NULL(tmp_str)) {
          ob_free(tmp_str);
        }
      }
    }
  }

  return ret;
}

int ObInnerConnectionLockUtil::get_org_cluster_id_(ObSQLSessionInfo *session, int64_t &org_cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session->get_ob_org_cluster_id(org_cluster_id))) {
    LOG_WARN("fail to get ob_org_cluster_id", K(ret));
  } else if (OB_INVALID_ORG_CLUSTER_ID == org_cluster_id || OB_INVALID_CLUSTER_ID == org_cluster_id) {
    org_cluster_id = ObServerConfig::get_instance().cluster_id;
    if (org_cluster_id < OB_MIN_CLUSTER_ID || org_cluster_id > OB_MAX_CLUSTER_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("org_cluster_id is set to cluster_id, but it is out of range",
                K(ret),
                K(org_cluster_id),
                K(OB_MIN_CLUSTER_ID),
                K(OB_MAX_CLUSTER_ID));
    }
  }
  return ret;
}

#undef REQUEST_LOCK_4_1
} // tablelock
} // transaction
} // oceanbase
