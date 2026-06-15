/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "src/storage/fts/dict/ob_dic_lock.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"

namespace oceanbase
{
namespace storage
{
int ObDicLock::lock_dic_tables_out_trans(
    const uint64_t tenant_id,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const transaction::tablelock::ObTableLockOwnerID &lock_owner,
    ObMySQLTransaction &trans,
    const common::ObIArray<uint64_t> &dict_table_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || dict_table_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant id or dict table ids is invalid", K(ret), K(tenant_id), K(dict_table_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dict_table_ids.count(); ++i) {
      const uint64_t table_id = dict_table_ids.at(i);
      if (OB_INVALID_ID == table_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table id", K(ret), K(tenant_id), K(table_id));
      } else if (OB_FAIL(do_table_lock(tenant_id, table_id, lock_mode, lock_owner, DEFAULT_TIMEOUT, true/*is_lock*/, trans))) {
        LOG_WARN("fail to do lock table", K(ret), K(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}

int ObDicLock::unlock_dict_tables(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &dict_table_ids,
    const transaction::tablelock::ObTableLockMode lock_mode,
    const transaction::tablelock::ObTableLockOwnerID &lock_owner,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || dict_table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant id or dict table ids is invalid", K(ret), K(tenant_id), K(dict_table_ids.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dict_table_ids.count(); ++i) {
      const uint64_t table_id = dict_table_ids.at(i);
      if (OB_INVALID_ID == table_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table id", K(ret), K(tenant_id), K(table_id));
      } else if (OB_FAIL(do_table_lock(tenant_id, table_id, lock_mode, lock_owner, DEFAULT_TIMEOUT, false/*is_lock*/, trans))) {
        LOG_WARN("fail to do unlock table", K(ret), K(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}

int ObDicLock::lock_dic_tables_in_trans(
    const int64_t tenant_id,
    const ObTenantDicLoader &dic_loader,
    const transaction::tablelock::ObTableLockMode lock_mode,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = NULL;
  const ObArray<ObTenantDicLoader::ObDicTableInfo> &dic_tables_info = dic_loader.get_dic_tables_info();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || dic_tables_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant id or dic loader is invalid", K(ret), K(tenant_id), K(dic_tables_info));
  } else if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dic_tables_info.count(); ++i) {
      const uint64_t table_id = dic_tables_info.at(i).table_id_;
      LOG_INFO("lock table", KR(ret), K(table_id), K(tenant_id), KPC(conn));
      if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id, table_id, lock_mode, DEFAULT_TIMEOUT, conn))) {
        LOG_WARN("lock dest table failed", KR(ret), K(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}

int ObDicLock::lock_dic_tables_in_trans(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &dict_table_ids,
    const transaction::tablelock::ObTableLockMode lock_mode,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = NULL;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || dict_table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id or dict table ids", K(ret), K(tenant_id), K(dict_table_ids.count()));
  } else if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is null", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dict_table_ids.count(); ++i) {
      const uint64_t table_id = dict_table_ids.at(i);
      if (OB_INVALID_ID == table_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table id", K(ret), K(tenant_id), K(table_id));
      } else if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_table(tenant_id, table_id, lock_mode, DEFAULT_TIMEOUT, conn))) {
        LOG_WARN("lock dict table failed", KR(ret), K(tenant_id), K(table_id));
      }
    }
  }
  return ret;
}
} // end storage
} // end oceanbase