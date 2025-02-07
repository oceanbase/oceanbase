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

#define REPLACE_LOCK(T, arg, conn, replace_req, buf, len, pos)                        \
  T unlock_req;                                                                       \
  if (OB_FAIL(unlock_req.deserialize(buf, len, pos))) {                               \
    LOG_WARN("deserialize unlock_req in replace_req failed", K(ret), K(replace_req)); \
  } else if (FALSE_IT(replace_req.unlock_req_ = &unlock_req)) {                       \
  } else if (OB_FAIL(replace_lock(arg.get_tenant_id(), replace_req, conn))) {         \
    LOG_WARN("replace lock failed", K(ret), K(replace_req));                          \
  }                                                                                   \
  break;

#define CONVERT_TYPE_AND_DO_LOCK(T, arg, tx_desc, tx_param)                    \
  const T lock_req = static_cast<const T &>(arg);                              \
  if (OB_FAIL(MTL(ObTableLockService *)->lock(tx_desc, tx_param, lock_req))) { \
    LOG_WARN("lock failed", K(ret), K(lock_req));                              \
  }                                                                            \
  break;

#define CONVERT_TYPE_AND_DO_UNLOCK(T, arg, tx_desc, tx_param)                      \
  const T lock_req = static_cast<const T &>(arg);                                  \
  T &unlock_req = const_cast<T &>(lock_req);                                       \
  unlock_req.set_to_unlock_type();                                                 \
  if (OB_FAIL(MTL(ObTableLockService *)->unlock(tx_desc, tx_param, unlock_req))) { \
    LOG_WARN("unlock failed", K(ret), K(unlock_req));                              \
  }                                                                                \
  break;

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
      case ObInnerSQLTransmitArg::OPERATION_TYPE_REPLACE_LOCK: {
        if (OB_FAIL(process_replace_lock_(arg, inner_conn))) {
          LOG_WARN("process replace lock failed", K(ret));
        }
        break;
      }
      case ObInnerSQLTransmitArg::OPERATION_TYPE_REPLACE_LOCKS: {
        if (OB_FAIL(process_replace_all_locks_(arg, inner_conn))) {
          LOG_WARN("process replace all locks failed", K(ret));
        }
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

int ObInnerConnectionLockUtil::process_replace_lock_(
  const ObInnerSQLTransmitArg &arg,
  observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObReplaceLockRequest replace_req;
  ObUnLockRequest unlock_req;
  const char *buf = arg.get_inner_sql().ptr();
  const int64_t data_len = arg.get_inner_sql().length();
  int64_t pos = 0;
  int64_t tmp_pos = 0;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_4_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version check faild", KR(ret), K(GET_MIN_CLUSTER_VERSION()));
  } else if (OB_FAIL(replace_req.deserialize_and_check_header(buf, data_len, pos))) {
    LOG_WARN("deserialize and check header of ObReplaceLockRequest failed", K(ret), K(arg), K(pos));
  } else if (OB_FAIL(replace_req.deserialize_new_lock_mode_and_owner(buf, data_len, pos))) {
    LOG_WARN("deserialize new_lock_mode and new_lock_owner of ObReplaceLockRequest failed", K(ret), K(arg), K(pos));
  // it's an temporary deserialization to get unlock request type
  } else if (FALSE_IT(tmp_pos = pos)) {
  } else if (OB_FAIL(unlock_req.deserialize(buf, data_len, tmp_pos))) {
    LOG_WARN("deserialize unlock_req failed", K(ret), K(arg), K(tmp_pos));
  } else {
    switch (unlock_req.type_) {
      case ObLockRequest::ObLockMsgType::UNLOCK_OBJ_REQ:{
        REPLACE_LOCK(ObUnLockObjsRequest, arg, conn, replace_req, buf, data_len, pos);
      }
      case ObLockRequest::ObLockMsgType::UNLOCK_TABLE_REQ: {
        REPLACE_LOCK(ObUnLockTableRequest, arg, conn, replace_req, buf, data_len, pos);
      }
      case ObLockRequest::ObLockMsgType::UNLOCK_PARTITION_REQ: {
        REPLACE_LOCK(ObUnLockPartitionRequest, arg, conn, replace_req, buf, data_len, pos);
      }
      case ObLockRequest::ObLockMsgType::UNLOCK_TABLET_REQ: {
        REPLACE_LOCK(ObUnLockTabletsRequest, arg, conn, replace_req, buf, data_len, pos);
      }
      case ObLockRequest::ObLockMsgType::UNLOCK_ALONE_TABLET_REQ: {
        REPLACE_LOCK(ObUnLockAloneTabletRequest, arg, conn, replace_req, buf, data_len, pos);
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("meet not supportted replace request", K(unlock_req), K(arg));
      }
    }
  }
  return ret;
}

int ObInnerConnectionLockUtil::process_replace_all_locks_(
  const ObInnerSQLTransmitArg &arg,
  observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObReplaceAllLocksRequest replace_req(allocator);
  const char *buf = arg.get_inner_sql().ptr();
  const int64_t data_len = arg.get_inner_sql().length();
  int64_t pos = 0;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_4_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version check faild", KR(ret), K(GET_MIN_CLUSTER_VERSION()));
  } else if (OB_FAIL(replace_req.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize and check header of ObReplaceLockRequest failed", K(ret), K(arg), K(pos));
  } else if (OB_FAIL(replace_lock(arg.get_tenant_id(), replace_req, conn))) {
    LOG_WARN("replace all locks failed", K(ret), K(replace_req));
  }
  replace_req.reset();
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

int ObInnerConnectionLockUtil::replace_lock(
      const uint64_t tenant_id,
      const ObReplaceLockRequest &req,
      observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;

  const bool local_execute = conn->is_local_execute(GCONF.cluster_id, tenant_id);

  SMART_VAR(ObInnerSQLResult, res, conn->get_session(), conn->is_inner_session())
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
        if (OB_FAIL(replace_lock_(tenant_id, req, conn, res))) {
          LOG_WARN("replace lock failed", KR(ret), K(req));
        }
      } else {
        char *tmp_str = nullptr;
        int64_t pos = 0;
        ObString sql;
        if (OB_ISNULL(tmp_str = static_cast<char *>(ob_malloc(req.get_serialize_size(), "InnerLock")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory for sql_str failed", K(ret), K(req.get_serialize_size()));
        } else if (OB_FAIL(req.serialize(tmp_str, req.get_serialize_size(), pos))) {
          LOG_WARN("serialize replace lock table arg failed", K(ret), K(req));
        } else {
          sql.assign_ptr(tmp_str, pos);
          ret = conn->forward_request(tenant_id, obrpc::ObInnerSQLTransmitArg::OPERATION_TYPE_REPLACE_LOCK, sql, res);
        }

        if (OB_NOT_NULL(tmp_str)) {
          ob_free(tmp_str);
        }
      }
    }
  }

  return ret;
}

int ObInnerConnectionLockUtil::replace_lock(const uint64_t tenant_id,
                                            const ObReplaceAllLocksRequest &req,
                                            observer::ObInnerSQLConnection *conn)
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard;

  const bool local_execute = conn->is_local_execute(GCONF.cluster_id, tenant_id);

  SMART_VAR(ObInnerSQLResult, res, conn->get_session(), conn->is_inner_session())
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
        if (OB_FAIL(replace_lock_(tenant_id, req, conn, res))) {
          LOG_WARN("replace lock failed", KR(ret), K(req));
        }
      } else {
        char *tmp_str = nullptr;
        int64_t pos = 0;
        ObString sql;
        if (OB_ISNULL(tmp_str = static_cast<char *>(ob_malloc(req.get_serialize_size(), "InnerLock")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory for sql_str failed", K(ret), K(req.get_serialize_size()));
        } else if (OB_FAIL(req.serialize(tmp_str, req.get_serialize_size(), pos))) {
          LOG_WARN("serialize replace lock table arg failed", K(ret), K(req));
        } else {
          sql.assign_ptr(tmp_str, pos);
          ret = conn->forward_request(tenant_id, obrpc::ObInnerSQLTransmitArg::OPERATION_TYPE_REPLACE_LOCKS, sql, res);
        }

        if (OB_NOT_NULL(tmp_str)) {
          ob_free(tmp_str);
        }
      }
    }
  }
  return ret;
}

int ObInnerConnectionLockUtil::replace_lock_(
    const uint64_t tenant_id,
    const ObReplaceLockRequest &req,
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
      if (OB_FAIL(MTL(ObTableLockService *)->replace_lock(*tx_desc, tx_param, req))) {
        LOG_WARN("replace lock failed", K(ret), K(tenant_id), K(req));
      } else if (OB_FAIL(res.close())) {
        LOG_WARN("close result set failed", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObInnerConnectionLockUtil::replace_lock_(
    const uint64_t tenant_id,
    const ObReplaceAllLocksRequest &req,
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
      if (OB_FAIL(MTL(ObTableLockService *)->replace_lock(*tx_desc, tx_param, req))) {
        LOG_WARN("replace lock failed", K(ret), K(tenant_id), K(req));
      } else if (OB_FAIL(res.close())) {
        LOG_WARN("close result set failed", K(ret), K(tenant_id));
      }
    }
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
    // NOTICE: although we can set is_oracle_mode here, internally it will prioritize referencing
    // the system variables on the session, so this parameter actually has no effect
  } else if (OB_FAIL(pool->acquire(session_info, conn, false /* is_oracle_mode */))) {
    LOG_WARN("acquire connection from inner sql connection pool failed", KR(ret), KPC(session_info));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("acquire new connection but it's null", KR(ret), KPC(session_info));
  } else {
    inner_conn = static_cast<observer::ObInnerSQLConnection *>(conn);
    // we must use mysql_mode connection to write inner_table
    if (OB_UNLIKELY(inner_conn->is_oracle_compat_mode())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("create an oracle mode inner connection", K(ret));
    }
  }

  if (current_mode != mysql_mode && current_mode.get_int() != -1 && OB_NOT_NULL(session_info)
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
  bool need_reset_sess_mode = false;
  bool need_reset_conn_mode = false;

  // NOTICE: This will be overwritten by the oracle_made_ field on connection,
  // but for safety reason, we still set up a compat_guard here.
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);

  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("inner_conn is nullptr", K(ret), K(sql));
  } else {
    if (OB_FAIL(set_to_mysql_compat_mode_(conn, need_reset_sess_mode, need_reset_conn_mode))) {
      LOG_WARN("set to mysql compat_mode failed", K(ret), K(sql));
    } else if (OB_FAIL(conn->execute_write(MTL_ID(), sql.ptr(), affected_rows))) {
      LOG_WARN("execute write sql failed", K(ret), K(sql));
    }
    if (OB_TMP_FAIL(reset_compat_mode_(conn, need_reset_sess_mode, need_reset_conn_mode))) {
      LOG_WARN("reset compat_mode failed", K(ret), K(tmp_ret), K(sql));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObInnerConnectionLockUtil::execute_read_sql(observer::ObInnerSQLConnection *conn,
                                                const ObSqlString &sql,
                                                ObISQLClient::ReadResult &res)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_reset_sess_mode = false;
  bool need_reset_conn_mode = false;

  // NOTICE: This will be overwritten by the oracle_made_ field on connection,
  // but for safety reason, we still set up a compat_guard here.
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);

  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("inner_conn is nullptr", K(ret), K(sql));
  } else {
    if (OB_FAIL(set_to_mysql_compat_mode_(conn, need_reset_sess_mode, need_reset_conn_mode))) {
      LOG_WARN("set to mysql compat_mode failed", K(ret), K(sql));
    } else if (OB_FAIL(conn->execute_read(MTL_ID(), sql.ptr(), res))) {
      LOG_WARN("execute read sql failed", K(ret), K(sql));
    }
    if (OB_TMP_FAIL(reset_compat_mode_(conn, need_reset_sess_mode, need_reset_conn_mode))) {
      LOG_WARN("reset compat_mode failed", K(ret), K(tmp_ret), K(sql));
      ret = COVER_SUCC(tmp_ret);
    }
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
      if (OB_FAIL(handle_request_by_operation_type_(*tx_desc, tx_param, arg, operation_type))) {
        LOG_WARN("handle request by operation_type failed", K(tx_param), K(arg), K(operation_type));
      }
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

  SMART_VAR(ObInnerSQLResult, res, conn->get_session(), conn->is_inner_session())
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

  SMART_VAR(ObInnerSQLResult, res, conn->get_session(), conn->is_inner_session())
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

int ObInnerConnectionLockUtil::handle_request_by_operation_type_(
  ObTxDesc &tx_desc,
  const ObTxParam &tx_param,
  const ObLockRequest &arg,
  const obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType operation_type)
{
  int ret = OB_SUCCESS;
  switch (operation_type) {
  case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLE: {
    CONVERT_TYPE_AND_DO_LOCK(ObLockTableRequest, arg, tx_desc, tx_param);
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLE: {
    CONVERT_TYPE_AND_DO_UNLOCK(ObUnLockTableRequest, arg, tx_desc, tx_param);
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_TABLET: {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
      const ObLockTabletRequest &lock_arg = static_cast<const ObLockTabletRequest &>(arg);
      ObLockTabletsRequest new_lock_arg;
      if (OB_FAIL(new_lock_arg.assign(lock_arg))) {
        LOG_WARN("assign ObLockTabletsRequest failed", K(ret), K(lock_arg));
      } else {
        CONVERT_TYPE_AND_DO_LOCK(ObLockTabletsRequest, new_lock_arg, tx_desc, tx_param);
      }
    } else {
      CONVERT_TYPE_AND_DO_LOCK(ObLockTabletsRequest, arg, tx_desc, tx_param);
    }
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_TABLET: {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
      const ObUnLockTabletRequest &lock_arg = static_cast<const ObUnLockTabletRequest &>(arg);
      ObUnLockTabletsRequest new_lock_arg;
      if (OB_FAIL(new_lock_arg.assign(lock_arg))) {
        LOG_WARN("assign ObLockTabletsRequest failed", K(ret), K(lock_arg));
      } else {
        CONVERT_TYPE_AND_DO_UNLOCK(ObUnLockTabletsRequest, new_lock_arg, tx_desc, tx_param);
      }
    } else {
      CONVERT_TYPE_AND_DO_UNLOCK(ObUnLockTabletsRequest, arg, tx_desc, tx_param);
    }
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_PART:
  case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_SUBPART: {
    CONVERT_TYPE_AND_DO_LOCK(ObLockPartitionRequest, arg, tx_desc, tx_param);
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_PART:
  case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_SUBPART: {
    CONVERT_TYPE_AND_DO_UNLOCK(ObUnLockPartitionRequest, arg, tx_desc, tx_param);
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJ: {
    const ObLockObjRequest &lock_arg = static_cast<const ObLockObjRequest &>(arg);
    ObLockObjsRequest new_lock_arg;
    if (OB_FAIL(new_lock_arg.assign(lock_arg))) {
      LOG_WARN("assign ObLockObjsRequest failed", K(ret), K(lock_arg));
    } else {
      CONVERT_TYPE_AND_DO_LOCK(ObLockObjsRequest, new_lock_arg, tx_desc, tx_param);
    }
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_OBJS: {
    CONVERT_TYPE_AND_DO_LOCK(ObLockObjsRequest, arg, tx_desc, tx_param);
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJ: {
    const ObUnLockObjRequest &lock_arg = static_cast<const ObUnLockObjRequest &>(arg);
    ObUnLockObjsRequest new_lock_arg;
    if (OB_FAIL(new_lock_arg.assign(lock_arg))) {
      LOG_WARN("assign ObLockObjsRequest failed", K(ret), K(lock_arg));
    } else {
      CONVERT_TYPE_AND_DO_UNLOCK(ObUnLockObjsRequest, new_lock_arg, tx_desc, tx_param);
    }
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_OBJS: {
    CONVERT_TYPE_AND_DO_UNLOCK(ObUnLockObjsRequest, arg, tx_desc, tx_param);
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_LOCK_ALONE_TABLET: {
    CONVERT_TYPE_AND_DO_LOCK(ObLockAloneTabletRequest, arg, tx_desc, tx_param);
  }
  case ObInnerSQLTransmitArg::OPERATION_TYPE_UNLOCK_ALONE_TABLET: {
    CONVERT_TYPE_AND_DO_UNLOCK(ObUnLockAloneTabletRequest, arg, tx_desc, tx_param);
  }
  default: {
    LOG_WARN("operation_type is not expected", K(operation_type));
    ret = OB_ERR_UNEXPECTED;
  }
  }
  return ret;
}

bool ObInnerConnectionLockUtil::is_unlock_operation(obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType type)
{
  bool is_unlock_operation = false;
  switch (type) {
  case obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType::OPERATION_TYPE_UNLOCK_TABLE:
  case obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType::OPERATION_TYPE_UNLOCK_TABLET:
  case obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType::OPERATION_TYPE_UNLOCK_PART:
  case obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType::OPERATION_TYPE_UNLOCK_OBJ:
  case obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType::OPERATION_TYPE_UNLOCK_SUBPART:
  case obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType::OPERATION_TYPE_UNLOCK_ALONE_TABLET:
  case obrpc::ObInnerSQLTransmitArg::InnerSQLOperationType::OPERATION_TYPE_UNLOCK_OBJS: {
    is_unlock_operation = true;
    break;
  }
  default: {
    is_unlock_operation = false;
  }
  }
  return is_unlock_operation;
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

int ObInnerConnectionLockUtil::set_to_mysql_compat_mode_(observer::ObInnerSQLConnection *conn, bool &need_reset_sess_mode, bool &need_reset_conn_mode)
{
  int ret = OB_SUCCESS;
  ObObj current_mode;
  ObObj mysql_mode;
  current_mode.set_int(-1);
  mysql_mode.set_int(0);
  need_reset_sess_mode = false;
  need_reset_conn_mode = false;

  if (OB_FAIL(conn->get_session().get_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, current_mode))) {
    LOG_WARN("can not get the compat_mode", );
  } else if (FALSE_IT(need_reset_sess_mode = (current_mode != mysql_mode))) {
  } else if (need_reset_sess_mode
             && OB_FAIL(conn->get_session().update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, mysql_mode))) {
    LOG_WARN("update compat_mode to mysql_mode failed");
  } else if (conn->is_oracle_compat_mode()) {
    need_reset_conn_mode = true;
    conn->set_mysql_compat_mode();
  }
  return ret;
}

int ObInnerConnectionLockUtil::reset_compat_mode_(observer::ObInnerSQLConnection *conn, const bool need_reset_sess_mode, const bool need_reset_conn_mode)
{
  int ret = OB_SUCCESS;
  ObObj oracle_mode;
  oracle_mode.set_int(1);

  if (need_reset_sess_mode
      && OB_FAIL(conn->get_session().update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, oracle_mode))) {
    LOG_WARN("failed to update sys variable for compatibility mode", K(ret));
  }
  if (need_reset_conn_mode) {
    conn->set_oracle_compat_mode();
  }
  return ret;
}
#undef REQUEST_LOCK_4_1
} // tablelock
} // transaction
} // oceanbase
