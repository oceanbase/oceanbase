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

#define USING_LOG_PREFIX STORAGE

#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/ob_ddl_common.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "storage/tablelock/ob_lock_utils.h"

using namespace oceanbase::transaction::tablelock;
using oceanbase::share::ObLSID;
using oceanbase::share::schema::ObTableSchema;
using oceanbase::observer::ObInnerSQLConnection;

namespace oceanbase
{
namespace storage
{

bool ObDDLLock::need_lock(const ObTableSchema &table_schema)
{
  const int64_t table_id = table_schema.get_table_id();
  return table_schema.is_user_table()
      || table_schema.is_tmp_table()
      || ObInnerTableLockUtil::in_inner_table_lock_white_list(table_id);
};

int ObDDLLock::lock_for_add_drop_index_in_trans(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  const uint64_t index_table_id = index_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  ObSEArray<ObTabletID, 1> data_tablet_ids;
  ObInnerSQLConnection *iconn = nullptr;
  if (data_table_schema.is_user_hidden_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock for rebuild hidden table index", K(ret));
  } else if (!need_lock(data_table_schema)) {
    LOG_INFO("skip ddl lock", K(data_table_id));
  } else if (OB_FAIL(data_table_schema.get_tablet_ids(data_tablet_ids))) {
    LOG_WARN("failed to get data tablet ids", K(ret));
  } else if (index_schema.is_storage_local_index_table()) {
    if (OB_FAIL(lock_table_lock_in_trans(tenant_id, data_table_id, data_tablet_ids, ROW_SHARE, timeout_us, trans))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, data_table_id, ROW_EXCLUSIVE, timeout_us, trans))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets_in_trans(tenant_id, data_tablet_ids, ROW_EXCLUSIVE, timeout_us, trans))) {
      LOG_WARN("failed to lock data table tablet", K(ret));
    }
  } else {
    if (OB_FAIL(lock_table_lock_in_trans(tenant_id, data_table_id, data_tablet_ids, ROW_SHARE, timeout_us, trans))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, data_table_id, ROW_EXCLUSIVE, timeout_us, trans))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, index_table_id, EXCLUSIVE, timeout_us, trans))) {
      LOG_WARN("failed to lock index table", K(ret));
    }
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::lock_for_add_drop_index(
    const ObTableSchema &data_table_schema,
    const ObIArray<ObTabletID> *inc_data_tablet_ids,
    const ObIArray<ObTabletID> *del_data_tablet_ids,
    const ObTableSchema &index_schema,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  const uint64_t index_table_id = index_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  ObSEArray<ObTabletID, 1> data_tablet_ids;
  ObInnerSQLConnection *iconn = nullptr;
  if (data_table_schema.is_user_hidden_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock for rebuild hidden table index", K(ret));
  } else if (!need_lock(data_table_schema)) {
    LOG_INFO("skip ddl lock", K(data_table_id));
  } else {
    if (OB_FAIL(data_table_schema.get_tablet_ids(data_tablet_ids))) {
      LOG_WARN("failed to get data tablet ids", K(ret));
    } else if (nullptr != del_data_tablet_ids) {
      ObArray<ObTabletID> tmp_tablet_ids;
      if (OB_FAIL(get_difference(data_tablet_ids, *del_data_tablet_ids, tmp_tablet_ids))) {
        LOG_WARN("failed to get diff tablet ids", K(ret));
      } else if (OB_FAIL(data_tablet_ids.assign(tmp_tablet_ids))) {
        LOG_WARN("failed to assign data tablet ids", K(ret));
      }
    }
    if (OB_SUCC(ret) && nullptr != inc_data_tablet_ids) {
      if (OB_FAIL(append(data_tablet_ids, *inc_data_tablet_ids))) {
        LOG_WARN("failed to append inc data tablet ids", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, data_table_id, ROW_EXCLUSIVE, lock_owner, timeout_us, trans))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets(tenant_id, data_tablet_ids, ROW_EXCLUSIVE, lock_owner, timeout_us, trans))) {
      LOG_WARN("failed to lock data table tablet", K(ret));
    } else if (OB_FAIL(do_table_lock(tenant_id, data_table_id, data_tablet_ids, ROW_SHARE, lock_owner, timeout_us, true/*is_lock*/, trans))) {
      LOG_WARN("failed to lock data tablet", K(ret));
    } else if (index_schema.is_storage_local_index_table()) {
      if (OB_FAIL(check_tablet_in_same_ls(data_table_schema, index_schema, trans))) {
        LOG_WARN("failed to check tablet in same ls", K(ret));
      }
    } else {
      if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock index table", K(ret));
      }
    }
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::unlock_for_add_drop_index(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_schema,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = index_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  const uint64_t index_table_id = index_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  ObSEArray<ObTabletID, 1> data_tablet_ids;
  bool some_lock_not_exist = false;
  if (!need_lock(data_table_schema) || data_table_schema.is_user_hidden_table()) {
    LOG_INFO("skip ddl lock", K(data_table_id));
  } else if (OB_FAIL(data_table_schema.get_tablet_ids(data_tablet_ids))) {
    LOG_WARN("failed to get data tablet ids", K(ret));
  } else if (OB_FAIL(do_table_lock(tenant_id, data_table_id, data_tablet_ids, ROW_SHARE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
    LOG_WARN("failed to unlock data tablet", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_tablets(tenant_id, data_tablet_ids, ROW_EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to unlock data tablet", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, data_table_id, ROW_EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to unlock data table", K(ret));
  } else if (!index_schema.is_storage_local_index_table()) {
    if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to unlock index table", K(ret));
    }
  }
  return ret;
}

int ObDDLLock::lock_for_add_lob_in_trans(
    const ObTableSchema &data_table_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  ObSEArray<ObTabletID, 1> data_tablet_ids;
  if (!need_lock(data_table_schema)) {
    LOG_INFO("skip ddl lock", K(data_table_id));
  } else if (OB_FAIL(data_table_schema.get_tablet_ids(data_tablet_ids))) {
    LOG_WARN("failed to get tablet ids", K(ret));
  } else if (OB_FAIL(lock_table_lock_in_trans(tenant_id, data_table_id, data_tablet_ids, ROW_EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock tablet", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, data_table_id, ROW_EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock data table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets_in_trans(tenant_id, data_tablet_ids, ROW_EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock data table tablets", K(ret));
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::lock_for_add_partition_in_trans(
    const ObTableSchema &table_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const ObArray<ObTabletID> no_tablet_ids;
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  if (table_schema.is_global_index_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to add partition to global index", K(ret));
  } else if (need_lock(table_schema)) {
    if (OB_FAIL(lock_table_lock_in_trans(tenant_id, table_id, no_tablet_ids, ROW_EXCLUSIVE, timeout_us, trans))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, table_id, SHARE, timeout_us, trans))) {
      LOG_WARN("failed to lock ddl table", K(ret));
    }
  } else {
    LOG_INFO("skip ddl lock", K(ret), K(table_id));
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::lock_for_drop_partition_in_trans(
    const ObTableSchema &table_schema,
    const ObIArray<ObTabletID> &del_tablet_ids,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const uint64_t data_table_id = table_schema.get_data_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  if (table_schema.is_global_index_table()) {
    ObArray<ObTabletID> no_tablet_ids;
    if (OB_FAIL(lock_table_lock_in_trans(tenant_id, data_table_id, no_tablet_ids, SHARE, timeout_us, trans))) {
      LOG_WARN("failed to lock table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, table_id, SHARE, timeout_us, trans))) {
      LOG_WARN("failed to lock ddl table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets_in_trans(tenant_id, del_tablet_ids, EXCLUSIVE, timeout_us, trans))) {
      LOG_WARN("failed to lock ddl table", K(ret));
    }
  } else if (need_lock(table_schema)) {
    if (OB_FAIL(lock_table_lock_in_trans(tenant_id, table_id, del_tablet_ids, EXCLUSIVE, timeout_us, trans))) {
      LOG_WARN("failed to lock table", K(ret));
    }
  } else {
    LOG_INFO("skip ddl lock", K(ret), K(table_id));
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::lock_for_common_ddl_in_trans(const ObTableSchema &table_schema, ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  ObSEArray<ObTabletID, 1> tablet_ids;
  if (!need_lock(table_schema)) {
    LOG_INFO("skip ddl lock for non-user table", K(table_schema.get_table_id()));
  } else if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
    LOG_WARN("failed to get tablet ids", K(ret));
  } else if (OB_FAIL(lock_table_lock_in_trans(tenant_id, table_id, tablet_ids, ROW_EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, table_id, ROW_SHARE, timeout_us, trans))) {
    LOG_WARN("failed to lock ddl table", K(ret));
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::lock_for_common_ddl(
    const ObTableSchema &table_schema,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  if (!need_lock(table_schema)) {
    LOG_INFO("skip ddl lock for non-user table", K(table_schema.get_table_id()));
  } else if (OB_FAIL(do_table_lock(tenant_id, table_id, ROW_EXCLUSIVE, lock_owner, timeout_us, true/*is_lock*/, trans))) {
    LOG_WARN("failed to lock table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans))) {
    LOG_WARN("failed to lock ddl table", K(ret));
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::unlock_for_common_ddl(
    const ObTableSchema &table_schema,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  bool some_lock_not_exist = false;
  if (!need_lock(table_schema)) {
    LOG_INFO("skip ddl lock for non-user table", K(table_schema.get_table_id()));
  } else if (OB_FAIL(do_table_lock(tenant_id, table_id, ROW_EXCLUSIVE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
    LOG_WARN("failed to unlock table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to unlock ddl table", K(ret));
  }
  return ret;
}

int ObDDLLock::lock_for_offline_ddl(
    const ObTableSchema &table_schema,
    const ObTableSchema *hidden_table_schema_to_check_bind,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  if (!need_lock(table_schema)) {
    LOG_INFO("skip ddl lock for non-user table", K(table_id));
  } else if (OB_FAIL(do_table_lock(tenant_id, table_id, EXCLUSIVE, lock_owner, timeout_us, true/*is_lock*/, trans))) {
    LOG_WARN("failed to lock table lock", K(ret));
  } else if (nullptr != hidden_table_schema_to_check_bind) {
    if (OB_FAIL(check_tablet_in_same_ls(table_schema, *hidden_table_schema_to_check_bind, trans))) {
      LOG_WARN("failed to check tablet in same ls", K(ret));
    }
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::unlock_for_offline_ddl(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  if (OB_FAIL(do_table_lock(tenant_id, table_id, EXCLUSIVE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
    LOG_WARN("failed to lock table lock", K(ret));
  }
  return ret;
}

// TODO(lihongqin.lhq): batch rpc
int ObDDLLock::lock_table_lock_in_trans(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObTableLockMode lock_mode,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else if (tablet_ids.empty()) {
    if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id, table_id, lock_mode, timeout_us, iconn))) {
      LOG_WARN("failed to lock table", K(ret));
    }
  } else {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
        if (OB_FAIL(ObInnerConnectionLockUtil::lock_tablet(
              tenant_id, table_id, tablet_ids.at(i), lock_mode, timeout_us, iconn))) {
          LOG_WARN("failed to lock tablet", K(ret), K(table_id), K(tablet_ids), K(lock_mode), K(timeout_us));
        }
      }
    } else {
      if (OB_FAIL(
            ObInnerConnectionLockUtil::lock_tablet(tenant_id, table_id, tablet_ids, lock_mode, timeout_us, iconn))) {
        LOG_WARN("failed to lock tablets", K(ret), K(table_id), K(tablet_ids), K(lock_mode), K(timeout_us));
      }
    }
  }
  return ret;
}

int ObDDLLock::do_table_lock(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    const bool is_lock,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObTableLockOpType op_type = is_lock ? ObTableLockOpType::OUT_TRANS_LOCK : ObTableLockOpType::OUT_TRANS_UNLOCK;
  ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else {
    ObLockTableRequest arg;
    arg.table_id_ = table_id;
    arg.owner_id_ = lock_owner;
    arg.lock_mode_ = lock_mode;
    arg.op_type_ = op_type;
    arg.timeout_us_ = timeout_us;
    if (is_lock) {
      if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id, arg, iconn))) {
        LOG_WARN("failed to lock table", K(ret));
      }
    } else {
      if (OB_FAIL(ObInnerConnectionLockUtil::unlock_table(tenant_id, arg, iconn))) {
        if (OB_OBJ_LOCK_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("table lock already unlocked", K(ret), K(arg));
        } else {
          LOG_WARN("failed to unlock table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLLock::do_table_lock(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    const bool is_lock,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObTableLockOpType op_type = is_lock ? ObTableLockOpType::OUT_TRANS_LOCK : ObTableLockOpType::OUT_TRANS_UNLOCK;
  ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else if (OB_UNLIKELY(tablet_ids.empty()
      || (lock_mode != ROW_SHARE && lock_mode != ROW_EXCLUSIVE && lock_mode != SHARE && lock_mode != EXCLUSIVE))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(table_id), K(tablet_ids.count()), K(lock_mode));
  }

  if (OB_SUCC(ret) && OB_INVALID_ID != table_id) {
    const ObTableLockMode table_level_lock_mode = lock_mode == ROW_SHARE || lock_mode == SHARE ? ROW_SHARE : ROW_EXCLUSIVE;
    if (OB_FAIL(do_table_lock(tenant_id, table_id, table_level_lock_mode, lock_owner, timeout_us, is_lock, trans))) {
      LOG_WARN("failed to lock table", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObArray<ObLSID> ls_ids;
    ObArray<ObLockAloneTabletRequest> args;
    if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, tablet_ids, ls_ids))) {
      LOG_WARN("failed to get tablet ls", K(ret));
    } else if (OB_UNLIKELY(ls_ids.count() != tablet_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls ids count", K(ret), K(ls_ids.count()), K(tablet_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      const ObLSID &ls_id = ls_ids[i];
      int64_t j = 0;
      for (; j < args.count(); j++) {
        if (args[j].ls_id_ == ls_id) {
          break;
        }
      }
      if (j == args.count()) {
        ObLockAloneTabletRequest arg;
        arg.owner_id_ = lock_owner;
        arg.lock_mode_ = lock_mode;
        arg.op_type_ = op_type;
        arg.timeout_us_ = timeout_us;
        arg.ls_id_ = ls_id;
        if (OB_FAIL(args.push_back(arg))) {
          LOG_WARN("failed to push back modify arg", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObLockAloneTabletRequest &arg = args.at(j);
        if (OB_FAIL(arg.tablet_ids_.push_back(tablet_ids.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); i++) {
      if (is_lock) {
        if (OB_FAIL(ObInnerConnectionLockUtil::lock_tablet(tenant_id, args[i], iconn))) {
          LOG_WARN("failed to lock tablet", K(ret));
        }
      } else {
        if (OB_FAIL(ObInnerConnectionLockUtil::unlock_tablet(tenant_id, args[i], iconn))) {
          if (OB_OBJ_LOCK_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("table lock already unlocked", K(ret), K(args[i]));
          } else {
            LOG_WARN("failed to unlock tablet", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLLock::check_tablet_in_same_ls(
    const ObTableSchema &lhs_schema,
    const ObTableSchema &rhs_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = lhs_schema.get_tenant_id();
  ObSArray<ObTabletID> lhs_tablet_ids;
  ObSArray<ObTabletID> rhs_tablet_ids;
  ObSArray<share::ObLSID> lhs_ls_ids;
  ObSArray<share::ObLSID> rhs_ls_ids;
  if (OB_UNLIKELY(tenant_id != rhs_schema.get_tenant_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id not match", K(ret));
  } else if (OB_FAIL(lhs_schema.get_tablet_ids(lhs_tablet_ids))) {
    LOG_WARN("failed to get orig tablet ids", K(ret));
  } else if (OB_FAIL(rhs_schema.get_tablet_ids(rhs_tablet_ids))) {
    LOG_WARN("failed to get index tablet ids", K(ret));
  } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, lhs_tablet_ids, lhs_ls_ids))) {
    LOG_WARN("failed to get data tablet ls", K(ret));
  } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, rhs_tablet_ids, rhs_ls_ids))) {
    LOG_WARN("failed to get data tablet ls", K(ret));
  } else if (!is_array_equal(lhs_ls_ids, rhs_ls_ids)) {
    ret = OB_EAGAIN;
    LOG_WARN("tablet not in same ls", K(ret), K(lhs_ls_ids), K(rhs_ls_ids));
  }
  return ret;
}

int ObOnlineDDLLock::lock_for_transfer_in_trans(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTabletID &tablet_id,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 1> tablet_ids;
  if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to push back tablet id", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, table_id, ROW_SHARE, timeout_us, trans))) {
    LOG_WARN("failed to lock table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets_in_trans(tenant_id, tablet_ids, EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock tablet", K(ret));
  }
  return ret;
}

int ObOnlineDDLLock::lock_for_transfer(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTabletID &tablet_id,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 1> tablet_ids;
  if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to push back tablet id", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans))) {
    LOG_WARN("failed to lock table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets(tenant_id, tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans))) {
    LOG_WARN("failed to lock tablet", K(ret));
  }
  return ret;
}

int ObOnlineDDLLock::unlock_for_transfer(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTabletID &tablet_id,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool some_lock_not_exist = false;
  ObSEArray<ObTabletID, 1> tablet_ids;
  if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to push back tablet id", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to unlock ddl table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_tablets(tenant_id, tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to unlock ddl tablet", K(ret));
  }

  if (OB_SUCC(ret) && some_lock_not_exist) {
    ret = OB_OBJ_LOCK_NOT_EXIST;
  }
  return ret;
}

int ObOnlineDDLLock::lock_table_in_trans(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableLockMode lock_mode,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *iconn = nullptr;
  ObLockObjRequest arg;
  arg.obj_type_ = ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLE;
  arg.obj_id_ = table_id;
  arg.timeout_us_ = timeout_us;
  arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
  arg.lock_mode_ = lock_mode;
  arg.owner_id_ = 0;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, arg, iconn))) {
    LOG_WARN("failed to lock online ddl table in trans", K(ret), K(arg));
  }
  return ret;
}

// TODO(lihongqin.lhq): batch rpc
int ObOnlineDDLLock::lock_tablets_in_trans(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObTableLockMode lock_mode,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else {
    ObLockObjsRequest arg;
    ObLockID lock_id;
    arg.timeout_us_ = timeout_us;
    arg.op_type_ = ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    arg.lock_mode_ = lock_mode;
    arg.owner_id_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      if (OB_FAIL(lock_id.set(ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLET, tablet_ids.at(i).id()))) {
        LOG_WARN("set lock id failed", K(ret), K(i), K(tablet_ids));
      } else if (OB_FAIL(arg.objs_.push_back(lock_id))) {
        LOG_WARN("add lock id failed", K(ret), K(lock_id));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, arg, iconn))) {
      LOG_WARN("failed to lock online ddl tablets in trans", K(ret), K(arg));
    }
  }
  return ret;
}

int ObOnlineDDLLock::lock_table(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *iconn = nullptr;
  ObLockObjRequest arg;
  arg.obj_type_ = ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLE;
  arg.obj_id_ = table_id;
  arg.timeout_us_ = timeout_us;
  arg.op_type_ = ObTableLockOpType::OUT_TRANS_LOCK;
  arg.lock_mode_ = lock_mode;
  arg.owner_id_ = lock_owner;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, arg, iconn))) {
    LOG_WARN("failed to lock online ddl table", K(ret));
  }
  return ret;
}

// TODO(lihongqin.lhq): batch rpc
int ObOnlineDDLLock::lock_tablets(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else {
    ObLockObjsRequest arg;
    ObLockID lock_id;
    arg.timeout_us_ = timeout_us;
    arg.op_type_ = ObTableLockOpType::OUT_TRANS_LOCK;
    arg.lock_mode_ = lock_mode;
    arg.owner_id_ = lock_owner;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      if (OB_FAIL(lock_id.set(ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLET, tablet_ids.at(i).id()))) {
        LOG_WARN("set lock id failed", K(ret), K(i), K(tablet_ids));
      } else if (OB_FAIL(arg.objs_.push_back(lock_id))) {
        LOG_WARN("add lock id failed", K(ret), K(lock_id));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObInnerConnectionLockUtil::lock_obj(tenant_id, arg, iconn))) {
      LOG_WARN("failed to lock online ddl tablets", K(ret), K(arg));
    }
  }
  return ret;
}

int ObOnlineDDLLock::unlock_table(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans,
    bool &some_lock_not_exist)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *iconn = nullptr;
  ObLockObjRequest arg;
  arg.obj_type_ = ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLE;
  arg.obj_id_ = table_id;
  arg.timeout_us_ = timeout_us;
  arg.op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK;
  arg.lock_mode_ = lock_mode;
  arg.owner_id_ = lock_owner;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::unlock_obj(tenant_id, arg, iconn))) {
    if (OB_OBJ_LOCK_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      some_lock_not_exist = true;
      LOG_INFO("online ddl table already unlocked", K(ret), K(arg));
    } else {
      LOG_WARN("failed to lock online ddl table", K(ret), K(arg));
    }
  }
  return ret;
}

// TODO(lihongqin.lhq): batch rpc
int ObOnlineDDLLock::unlock_tablets(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObTableLockMode lock_mode,
    const ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans,
    bool &some_lock_not_exist)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else {
    ObUnLockObjsRequest arg;
    ObLockID lock_id;
    arg.timeout_us_ = timeout_us;
    arg.op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK;
    arg.lock_mode_ = lock_mode;
    arg.owner_id_ = lock_owner;

    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      if (OB_FAIL(lock_id.set(ObLockOBJType::OBJ_TYPE_ONLINE_DDL_TABLET, tablet_ids.at(i).id()))) {
        LOG_WARN("set lock id failed", K(ret), K(i), K(tablet_ids));
      } else if (OB_FAIL(arg.objs_.push_back(lock_id))) {
        LOG_WARN("add lock id failed", K(ret), K(lock_id));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObInnerConnectionLockUtil::unlock_obj(tenant_id, arg, iconn))) {
      LOG_WARN("meet fail during unlock online ddl tablets", K(ret), K(arg));
      if (OB_OBJ_LOCK_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        some_lock_not_exist = true;
        LOG_WARN("online ddl tablet already unlocked", K(ret));
      } else {
        LOG_WARN("failed to unlock online ddl tablet", K(ret));
      }
    }
  }

  return ret;
}

}  // namespace storage
}
