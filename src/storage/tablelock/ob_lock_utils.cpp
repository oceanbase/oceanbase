/**
 * Copyright (c) 2022 OceanBase
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

#include "storage/tablelock/ob_lock_utils.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h" // ObLockObjRequest
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMysqlTransaction
#include "observer/ob_inner_sql_connection.h" // ObInnerSQLConnection
#include "share/ob_share_util.h" // ObShareUtil
#include "storage/tablelock/ob_lock_inner_connection_util.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
using namespace share;
namespace transaction
{
namespace tablelock
{
int ObInnerTableLockUtil::lock_inner_table_in_trans(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t inner_table_id,
    const ObTableLockMode &lock_mode,
    const bool is_from_sql)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = NULL;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !is_inner_table(inner_table_id)
      || !is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(inner_table_id), K(lock_mode));
  } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("fail to set default_timeout_ctx", KR(ret));
  } else {
    ObLockTableRequest table_lock_arg;
    table_lock_arg.lock_mode_ = lock_mode;
    table_lock_arg.timeout_us_ = ctx.get_timeout();
    table_lock_arg.table_id_ = inner_table_id;
    table_lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    table_lock_arg.is_from_sql_ = is_from_sql;
    if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id, table_lock_arg, conn))) {
      LOG_WARN("lock table failed", KR(ret), K(table_lock_arg));
    }
  }
  return ret;
}

int ObLSObjLockUtil::lock_ls_in_trans(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const transaction::tablelock::ObTableLockMode &lock_mode)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = NULL;
  ObTimeoutCtx ctx;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id) || !is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(ls_id), K(lock_mode));
  } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("fail to set default_timeout_ctx", KR(ret));
  } else {
    ObLockObjRequest lock_arg;
    lock_arg.lock_mode_ = lock_mode;
    lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
    lock_arg.timeout_us_ = ctx.get_timeout();
    lock_arg.obj_type_ = ObLockOBJType::OBJ_TYPE_LS;
    lock_arg.obj_id_ = ls_id.id();
    if (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, lock_arg, conn))) {
      LOG_WARN("lock obj failed", KR(ret), K(tenant_id), K(lock_arg));
    }
  }
  return ret;
}

} // end namespace tablelock
} // end namespace transaction
} // end namespace oceanbase
