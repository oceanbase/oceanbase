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
#include "storage/ddl/ob_ddl_lock.h"
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
  if (OB_UNLIKELY(data_table_schema.is_user_hidden_table() || data_table_id != index_schema.get_data_table_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock for rebuild hidden table index", K(ret), K(tenant_id), K(data_table_id), K(index_table_id), K(index_schema.get_data_table_id()));
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
    } else if (index_schema.is_storage_local_index_table() || index_schema.is_mlog_table()) {
      if (OB_FAIL(check_tablet_in_same_ls(data_table_schema, index_schema, trans))) {
        LOG_WARN("failed to check tablet in same ls", K(ret));
      }
    } else {
      if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock index table", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLLock::unlock_for_add_drop_index(
    const ObTableSchema &data_table_schema,
    const uint64_t index_table_id,
    const bool is_global_index,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
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
  } else if (is_global_index) {
    if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to unlock index table", K(ret));
    }
  }
  return ret;
}

int ObDDLLock::lock_for_rebuild_index(
    const share::schema::ObTableSchema &data_table_schema,
    const uint64_t old_index_table_id,
    const uint64_t new_index_table_id,
    const bool is_global_index,
    const transaction::tablelock::ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
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
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, data_table_id, ROW_EXCLUSIVE, lock_owner, timeout_us, trans))) {
    LOG_WARN("failed to lock data table", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets(tenant_id, data_tablet_ids, ROW_EXCLUSIVE, lock_owner, timeout_us, trans))) {
    LOG_WARN("failed to lock data table tablet", K(ret));
  } else if (OB_FAIL(do_table_lock(tenant_id, data_table_id, data_tablet_ids, ROW_SHARE, lock_owner, timeout_us, true/*is_lock*/, trans))) {
    LOG_WARN("failed to lock data tablet", K(ret));
  } else if (is_global_index) {
    if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, old_index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans))) {
      LOG_WARN("failed to lock index table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, new_index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans))) {
      LOG_WARN("failed to lock index table", K(ret));
    }
  }
  return ret;
}

int ObDDLLock::unlock_for_rebuild_index(
    const share::schema::ObTableSchema &data_table_schema,
    const uint64_t old_index_table_id,
    const uint64_t new_index_table_id,
    const bool is_global_index,
    const transaction::tablelock::ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
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
  } else if (is_global_index) {
    if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, old_index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to unlock index table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, new_index_table_id, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to unlock index table", K(ret));
    }
  }
  return ret;
}

int ObDDLLock::lock_for_split_partition(
    const ObTableSchema &table_schema,
    const ObLSID *ls_id,
    const ObIArray<ObTabletID> *src_tablet_ids,
    const ObIArray<ObTabletID> &dst_tablet_ids,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  const bool lock_src_and_dst_od = nullptr == ls_id && nullptr != src_tablet_ids;
  const bool lock_dst_table_lock = nullptr != ls_id && nullptr == src_tablet_ids && ls_id->is_valid();
  if (OB_UNLIKELY(!(lock_src_and_dst_od ^ lock_dst_table_lock))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KPC(ls_id), KPC(src_tablet_ids), K(dst_tablet_ids));
  } else if (table_schema.is_global_index_table()) {
    if (lock_src_and_dst_od) {
      if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock data table", K(ret));
      } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets(tenant_id, *src_tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock tablets", K(ret));
      } else if (OB_FAIL(do_table_lock(tenant_id, table_schema.get_data_table_id(), ROW_SHARE, lock_owner, timeout_us, true/*is_lock*/, trans))) {
        LOG_WARN("failed to lock data table", K(ret));
      } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets(tenant_id, dst_tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock tablets", K(ret));
      }
    }
  } else if (need_lock(table_schema)) {
    if (lock_src_and_dst_od) {
      if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock data table", K(ret));
      } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets(tenant_id, *src_tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock tablets", K(ret));
      } else if (OB_FAIL(do_table_lock(tenant_id, table_id, *src_tablet_ids, ROW_EXCLUSIVE, lock_owner, timeout_us, true/*is_lock*/, trans))) {
        LOG_WARN("failed to lock data table", K(ret));
      } else if (OB_FAIL(ObOnlineDDLLock::lock_tablets(tenant_id, dst_tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans))) {
        LOG_WARN("failed to lock tablets", K(ret));
      }
    } else {
      // performance crucial
      ObLockAloneTabletRequest arg;
      arg.lock_mode_ = ROW_EXCLUSIVE;
      arg.op_type_ = ObTableLockOpType::OUT_TRANS_LOCK;
      arg.owner_id_ = lock_owner;
      arg.timeout_us_ = timeout_us;
      arg.ls_id_ = *ls_id;
      ObInnerSQLConnection *iconn = nullptr;
      if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid conn", K(ret));
      } else if (OB_FAIL(append(arg.tablet_ids_, dst_tablet_ids))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_tablet(tenant_id, arg, iconn))) {
        LOG_WARN("failed to lock tablet", K(ret), K(arg));
      }
    }
  } else {
    LOG_INFO("skip ddl lock", K(ret), K(table_id));
  }
  return ret;
}

int ObDDLLock::unlock_for_split_partition(
    const ObTableSchema &table_schema,
    const ObIArray<ObTabletID> &src_tablet_ids,
    const ObIArray<ObTabletID> &dst_tablet_ids,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  bool some_lock_not_exist = false;
  ObArray<ObTabletID> tablet_ids;
  if (OB_FAIL(append(tablet_ids, src_tablet_ids))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(append(tablet_ids, dst_tablet_ids))) {
    LOG_WARN("failed to append", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (table_schema.is_global_index_table()) {
    if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::unlock_tablets(tenant_id, tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to lock tablets", K(ret));
    } else if (OB_FAIL(do_table_lock(tenant_id, table_schema.get_data_table_id(), ROW_SHARE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
      LOG_WARN("failed to lock data table", K(ret));
    }
  } else if (need_lock(table_schema)) {
    if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to lock data table", K(ret));
    } else if (OB_FAIL(ObOnlineDDLLock::unlock_tablets(tenant_id, tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
      LOG_WARN("failed to lock tablets", K(ret));
    } else if (OB_FAIL(do_table_lock(tenant_id, table_id, tablet_ids, ROW_EXCLUSIVE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
      LOG_WARN("failed to lock data tablets", K(ret));
    }
  } else {
    LOG_INFO("skip ddl lock", K(ret), K(table_id));
  }
  return ret;
}

int ObDDLLock::replace_tablet_lock_for_split(
    const uint64_t tenant_id,
    const ObTableSchema &table_schema,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObTableLockOwnerID &lock_owner,
    const ObTableLockOwnerID new_lock_owner,
    const bool is_global_idx,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  bool some_lock_not_exist = false;
  uint64_t table_id =OB_INVALID_ID;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !table_schema.is_valid() || tablet_ids.empty() || !lock_owner.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(tablet_ids), K(lock_owner));
  } else if (OB_FALSE_IT(table_id = table_schema.get_table_id())) {
  } else if (!is_global_idx && !need_lock(table_schema)) {
    LOG_INFO("skip ddl lock", K(ret), K(table_id));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, table_id, ROW_SHARE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to lock data table", K(ret), K(tenant_id), K(table_id), K(lock_owner), K(timeout_us));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_tablets(tenant_id, tablet_ids, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to lock tablets", K(ret), K(tenant_id), K(tablet_ids), K(lock_owner), K(timeout_us));
    /*wait split source tablets trans end, split source tablets table lock will be unlocked in unlock_for_offline_ddl function*/
  } else if (!is_global_idx && OB_FAIL(replace_tablet_lock(tenant_id, table_id, tablet_ids, ROW_EXCLUSIVE, lock_owner, EXCLUSIVE, new_lock_owner, timeout_us, trans))) {
    LOG_WARN("failed to replace table lock", K(ret), K(tenant_id), K(table_id), K(tablet_ids), K(lock_owner), K(new_lock_owner), K(timeout_us));
  }
  return ret;
}
int ObDDLLock::replace_table_lock_for_split(
    const share::schema::ObTableSchema &table_schema,
    const ObIArray<transaction::tablelock::ObTableLockOwnerID> &global_split_owner_ids,
    const ObIArray<transaction::tablelock::ObTableLockOwnerID> &main_split_owner_ids,
    const transaction::tablelock::ObTableLockOwnerID new_lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_schema.is_valid() || !new_lock_owner.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(new_lock_owner));
  } else {
    ObSEArray<ObUnLockTableRequest, 10> unlock_req_list;
    ObArenaAllocator lock_req_allocator("SplitRecov");
    ObReplaceAllLocksRequest rep_all_lock_req(lock_req_allocator);

    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const int64_t timeout_us = DEFAULT_TIMEOUT;
    ObUnLockTableRequest req;
    for (int64_t i = 0; OB_SUCC(ret) && i < main_split_owner_ids.count(); ++i) {
      req.reset();
      req.table_id_ = table_id;
      req.owner_id_ = main_split_owner_ids.at(i);
      req.lock_mode_ = ROW_EXCLUSIVE; //old lock mode
      req.op_type_ = OUT_TRANS_UNLOCK;
      req.timeout_us_ = timeout_us;
      if (OB_FAIL(unlock_req_list.push_back(req))) {
        LOG_WARN("failed to push back unlock_req_list", K(ret), K(i));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < global_split_owner_ids.count(); ++i) {
      /*the global split task holds the main table RS lock, we also need to replace them*/
      req.reset();
      const ObTableLockOwnerID &old_lock_owner = global_split_owner_ids.at(i);
      req.table_id_ = table_id;
      req.owner_id_ = old_lock_owner;
      req.lock_mode_ = ROW_SHARE; //old lock mode
      req.op_type_ = OUT_TRANS_UNLOCK;
      req.timeout_us_ = timeout_us;
      if (OB_FAIL(unlock_req_list.push_back(req))) {
        LOG_WARN("failed to push back unlock_req_list", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < unlock_req_list.count(); ++i) {
      if (OB_FAIL(rep_all_lock_req.unlock_req_list_.push_back(&unlock_req_list.at(i)))) {
        LOG_WARN("faild to push back unlock_req_list_", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      ObLockTableRequest lock_req;
      lock_req.owner_id_ = new_lock_owner;
      lock_req.op_type_ = OUT_TRANS_LOCK;
      lock_req.type_ = ObLockRequest::ObLockMsgType::LOCK_TABLE_REQ;
      lock_req.lock_mode_ = EXCLUSIVE;
      lock_req.table_id_ = table_schema.is_global_index_table() ? table_schema.get_data_table_id() : table_schema.get_table_id();
      rep_all_lock_req.lock_req_ = &lock_req;
      ObInnerSQLConnection *iconn = nullptr;
      if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid conn", K(ret));
      } else if (OB_FAIL(ObInnerConnectionLockUtil::replace_lock(tenant_id, rep_all_lock_req, iconn))) {
        LOG_WARN("failed to replace all lock", K(ret), K(rep_all_lock_req));
      }
    }
  }
  return ret;
}

int ObDDLLock::lock_for_modify_auto_part_size_in_trans(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const ObIArray<uint64_t> &global_index_table_ids,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObArray<ObTabletID> no_tablet_ids;
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, data_table_id, EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock data table", K(ret));
  } else if (OB_FAIL(lock_table_lock_in_trans(tenant_id, data_table_id, no_tablet_ids, ROW_SHARE, timeout_us, trans))) {
    LOG_WARN("failed to lock tablet", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < global_index_table_ids.count(); i++) {
      const uint64_t table_id = global_index_table_ids.at(i);
      if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, table_id, EXCLUSIVE, timeout_us, trans))) {
        LOG_WARN("failed to lock data table tablets", K(ret));
      }
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
  return ret;
}

int ObDDLLock::lock_for_online_drop_column_in_trans(const ObTableSchema &table_schema, ObMySQLTransaction &trans)
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
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, table_id, EXCLUSIVE, timeout_us, trans))) {
    LOG_WARN("failed to lock ddl table", K(ret));
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::lock_for_drop_lob(
    const ObTableSchema &data_table_schema,
    const ObTableLockOwnerID lock_owner,
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
  } else if (OB_FAIL(do_table_lock(tenant_id, data_table_id, data_tablet_ids, ROW_EXCLUSIVE, lock_owner, timeout_us, true/*is_lock*/, trans))) {
    LOG_WARN("failed to lock data tablet", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table(tenant_id, data_table_id, EXCLUSIVE, lock_owner, timeout_us, trans))) {
    LOG_WARN("failed to lock data table", K(ret));
  }
  ret = share::ObDDLUtil::is_table_lock_retry_ret_code(ret) ? OB_EAGAIN : ret;
  return ret;
}

int ObDDLLock::unlock_for_drop_lob(
    const ObTableSchema &data_table_schema,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  const uint64_t data_table_id = data_table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  bool some_lock_not_exist = false;
  ObSEArray<ObTabletID, 1> data_tablet_ids;
  if (!need_lock(data_table_schema)) {
    LOG_INFO("skip ddl lock", K(data_table_id));
  } else if (OB_FAIL(data_table_schema.get_tablet_ids(data_tablet_ids))) {
    LOG_WARN("failed to get tablet ids", K(ret));
  } else if (OB_FAIL(do_table_lock(tenant_id, data_table_id, data_tablet_ids, ROW_EXCLUSIVE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
    LOG_WARN("failed to unlock data tablet", K(ret));
  } else if (OB_FAIL(ObOnlineDDLLock::unlock_table(tenant_id, data_table_id, EXCLUSIVE, lock_owner, timeout_us, trans, some_lock_not_exist))) {
    LOG_WARN("failed to unlock data table", K(ret));
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
  return ret;
}

int ObDDLLock::lock_for_common_ddl_in_trans(
    const ObTableSchema &table_schema,
    const bool require_strict_binary_format,
    ObMySQLTransaction &trans)
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
  } else if (OB_FAIL(ObOnlineDDLLock::lock_table_in_trans(tenant_id, table_id, require_strict_binary_format ? SHARE : ROW_SHARE, timeout_us, trans))) {
    LOG_WARN("failed to lock ddl table", K(ret));
  }
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
  return ret;
}

int ObDDLLock::unlock_for_offline_ddl(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObTabletID> *hidden_tablet_ids_alone,
    const ObTableLockOwnerID lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  if (OB_FAIL(do_table_lock(tenant_id, table_id, EXCLUSIVE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
    LOG_WARN("failed to lock table lock", K(ret));
  } else if (nullptr != hidden_tablet_ids_alone) {
    if (OB_FAIL(do_table_lock(tenant_id, OB_INVALID_ID/*table_id*/, *hidden_tablet_ids_alone, EXCLUSIVE, lock_owner, timeout_us, false/*is_lock*/, trans))) {
      LOG_WARN("failed to unlock tablets", K(ret), K(tenant_id), KPC(hidden_tablet_ids_alone), K(lock_owner));
    }
  }
  return ret;
}

int ObDDLLock::lock_table_in_trans(
    const ObTableSchema &table_schema,
    const ObTableLockMode lock_mode,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t table_id = table_schema.get_table_id();
  const int64_t timeout_us = DEFAULT_TIMEOUT;
  ObInnerSQLConnection *iconn = nullptr;
  if (!need_lock(table_schema)) {
    LOG_INFO("skip ddl lock for non-user table", K(table_id));
  } else if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id, table_id, lock_mode, timeout_us, iconn))) {
    LOG_WARN("failed to lock table", K(ret), K(tenant_id), K(table_id));
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
    if (is_lock) {
      ObLockTableRequest arg;
      arg.table_id_ = table_id;
      arg.owner_id_ = lock_owner;
      arg.lock_mode_ = lock_mode;
      arg.op_type_ = op_type;
      arg.timeout_us_ = timeout_us;
      if (OB_FAIL(ObInnerConnectionLockUtil::lock_table(tenant_id, arg, iconn))) {
        LOG_WARN("failed to lock table", K(ret));
      }
    } else {
      ObUnLockTableRequest arg;
      arg.table_id_ = table_id;
      arg.owner_id_ = lock_owner;
      arg.lock_mode_ = lock_mode;
      arg.op_type_ = op_type;
      arg.timeout_us_ = timeout_us;
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
    ObArray<ObLockAloneTabletRequest> lock_args;
    ObArray<ObUnLockAloneTabletRequest> unlock_args;
    int64_t arg_count = 0;
    if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, tablet_ids, ls_ids))) {
      LOG_WARN("failed to get tablet ls", K(ret));
    } else if (OB_UNLIKELY(ls_ids.count() != tablet_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls ids count", K(ret), K(ls_ids.count()), K(tablet_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      const ObLSID &ls_id = ls_ids[i];
      int64_t j = 0;
      for (; j < (is_lock ? lock_args.count() : unlock_args.count()); j++) {
        const ObLSID &tmp_ls_id = is_lock ? lock_args[j].ls_id_ : unlock_args[j].ls_id_;
        if (tmp_ls_id == ls_id) {
          break;
        }
      }
      arg_count = is_lock ? lock_args.count() : unlock_args.count();
      if (j == arg_count) {
        ObLockAloneTabletRequest lock_arg;
        ObUnLockAloneTabletRequest unlock_arg;
        ObLockAloneTabletRequest &arg = is_lock ? lock_arg : unlock_arg;
        arg.owner_id_ = lock_owner;
        arg.lock_mode_ = lock_mode;
        arg.op_type_ = op_type;
        arg.timeout_us_ = timeout_us;
        arg.ls_id_ = ls_id;
        if (OB_FAIL(is_lock ? lock_args.push_back(lock_arg) : unlock_args.push_back(unlock_arg))) {
          LOG_WARN("failed to push back modify arg", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObLockAloneTabletRequest &arg = is_lock ? lock_args.at(j) : unlock_args.at(j);
        if (OB_FAIL(arg.tablet_ids_.push_back(tablet_ids.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    arg_count = is_lock ? lock_args.count() : unlock_args.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_count; i++) {
      if (is_lock) {
        if (OB_FAIL(ObInnerConnectionLockUtil::lock_tablet(tenant_id, lock_args[i], iconn))) {
          LOG_WARN("failed to lock tablet", K(ret), K(lock_args[i]));
        }
      } else {
        if (OB_FAIL(ObInnerConnectionLockUtil::unlock_tablet(tenant_id, unlock_args[i], iconn))) {
          if (OB_OBJ_LOCK_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("table lock already unlocked", K(ret), K(unlock_args[i]));
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

int ObDDLLock::replace_table_lock(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const transaction::tablelock::ObTableLockMode old_lock_mode,
    const transaction::tablelock::ObTableLockOwnerID old_lock_owner,
    const transaction::tablelock::ObTableLockMode new_lock_mode,
    const transaction::tablelock::ObTableLockOwnerID new_lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObTableLockOpType op_type = ObTableLockOpType::OUT_TRANS_UNLOCK;
  ObInnerSQLConnection *iconn = nullptr;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else {
    ObUnLockTableRequest unlock_arg;
    unlock_arg.table_id_ = table_id;
    unlock_arg.owner_id_ = old_lock_owner;
    unlock_arg.lock_mode_ = old_lock_mode;
    unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
    unlock_arg.timeout_us_ = timeout_us;
    ObReplaceLockRequest replace_lock_arg;
    replace_lock_arg.unlock_req_ = &unlock_arg;
    replace_lock_arg.new_lock_mode_ = new_lock_mode;
    replace_lock_arg.new_lock_owner_ = new_lock_owner;
    if (OB_FAIL(ObInnerConnectionLockUtil::replace_lock(tenant_id, replace_lock_arg, iconn))) {
      LOG_WARN("failed to replace lock", K(ret), K(tenant_id), K(replace_lock_arg));
    }
  }
  return ret;
}

int ObDDLLock::replace_tablet_lock(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const transaction::tablelock::ObTableLockMode old_lock_mode,
    const transaction::tablelock::ObTableLockOwnerID old_lock_owner,
    const transaction::tablelock::ObTableLockMode new_lock_mode,
    const transaction::tablelock::ObTableLockOwnerID new_lock_owner,
    const int64_t timeout_us,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObTableLockOpType op_type = ObTableLockOpType::OUT_TRANS_UNLOCK;
  ObInnerSQLConnection *iconn = nullptr;
  ObArray<ObUnLockAloneTabletRequest> unlock_args;
  if (OB_ISNULL(iconn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid conn", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id || tablet_ids.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(table_id), K(tablet_ids.count()));
  } else if (OB_FAIL(get_unlock_alone_tablet_request_args(tenant_id, old_lock_mode, old_lock_owner, timeout_us, tablet_ids, unlock_args, trans))) {
    LOG_WARN("fail to get unlock alone tablet request args", K(ret), K(tenant_id), K(old_lock_mode), K(old_lock_owner), K(timeout_us), K(tablet_ids));
  } else {
    int64_t arg_count = unlock_args.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_count; i++) {
      ObReplaceLockRequest replace_req;
      replace_req.new_lock_mode_ = new_lock_mode;
      replace_req.new_lock_owner_ = new_lock_owner;
      replace_req.unlock_req_ = &unlock_args[i];
      if (OB_FAIL(ObInnerConnectionLockUtil::replace_lock(tenant_id, replace_req, iconn))) {
        LOG_WARN("failed to replace lock", K(ret), K(tenant_id), K(replace_req));
      }
    }
  }
  return ret;
}

int ObDDLLock::get_unlock_alone_tablet_request_args(
    const uint64_t tenant_id,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const transaction::tablelock::ObTableLockOwnerID lock_owner,
    const int64_t timeout_us,
    const ObIArray<ObTabletID> &tablet_ids,
    ObArray<transaction::tablelock::ObUnLockAloneTabletRequest> &unlock_args,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  unlock_args.reset();
  const ObTableLockOpType op_type = ObTableLockOpType::OUT_TRANS_UNLOCK;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_ids.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(tablet_ids.count()));
  } else {
    ObArray<ObLSID> ls_ids;
    int64_t arg_count = 0;
    if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, tablet_ids, ls_ids))) {
      LOG_WARN("failed to get tablet ls", K(ret));
    } else if (OB_UNLIKELY(ls_ids.count() != tablet_ids.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls ids count", K(ret), K(ls_ids.count()), K(tablet_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      const ObLSID &ls_id = ls_ids[i];
      int64_t j = 0;
      for (; j < unlock_args.count(); j++) {
        const ObLSID &tmp_ls_id = unlock_args[j].ls_id_;
        if (tmp_ls_id == ls_id) {
          break;
        }
      }
      arg_count = unlock_args.count();
      if (j == arg_count) {
        ObUnLockAloneTabletRequest unlock_arg;
        unlock_arg.owner_id_ = lock_owner;
        unlock_arg.lock_mode_ = lock_mode;
        unlock_arg.op_type_ = op_type;
        unlock_arg.timeout_us_ = timeout_us;
        unlock_arg.ls_id_ = ls_id;
        if (OB_FAIL(unlock_args.push_back(unlock_arg))) {
          LOG_WARN("failed to push back modify arg", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObLockAloneTabletRequest &arg = unlock_args.at(j);
        if (OB_FAIL(arg.tablet_ids_.push_back(tablet_ids.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
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
  arg.owner_id_.set_default();
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
    arg.owner_id_.set_default();
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
  ObUnLockObjRequest arg;
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
